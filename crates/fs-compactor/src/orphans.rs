//! The orphan sweep: objects in the bucket without a file row.
//!
//! A crash between an upload and its registration, or a compaction that
//! did not publish, leaves objects no row points at. The sweep walks the
//! engine's prefix, remembers in the catalog the span files and sidecars
//! without a row, and deletes an object only once it has been remembered
//! for longer than the orphan TTL and the catalog has marked it for
//! deletion. The mark is what makes the delete safe: it is refused when a
//! row exists, it is taken under the path's lock, and every registration
//! takes the same lock and checks the same mark, so a writer still
//! retrying its registration either commits before the mark or is refused
//! and uploads again once the object is gone. A mark is never undone by
//! another sweeper.
//!
//! Only paths the engine lays out are considered
//! ([`fs_index::SpanObject`]), checked again just before an object is
//! marked; anything else, in the bucket or remembered by an older
//! version, is left alone or forgotten. The bucket is walked by
//! partitions, one tenant, level and day each, found with delimiter
//! listings as the walk reaches them, so no listing order is assumed and
//! nothing is enumerated ahead. A pass walks whole partitions until it
//! has listed `objects_per_pass` objects, then stops and the next pass
//! continues from the next partition; it deletes at most
//! `deletes_per_pass`. Memory holds one chunk of paths and one partition
//! list at a time.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fs_catalog::{Catalog, Doom};
use fs_index::SpanObject;
use futures_util::StreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tracing::{info, warn};

use crate::CompactError;

/// Objects per membership query and sighting insert.
const CHUNK: usize = 1000;

/// The kinds of partition under a tenant's traces, in walk order.
const KINDS: [&str; 4] = ["l0", "l1", "l2", "index/trace_id"];

/// Bounds of the sweep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepConfig {
    /// How long an object must have been seen without a row before it is
    /// deleted; longer than any upload waits for its registration.
    pub ttl: Duration,
    /// A pass stops walking once it has listed this many objects, after
    /// finishing the partition it is in; one partition is one tenant,
    /// level and day.
    pub objects_per_pass: usize,
    /// Most objects one pass deletes.
    pub deletes_per_pass: usize,
}

/// Counters of the orphan sweep.
#[derive(Debug, Default)]
pub struct OrphanStats {
    /// Passes completed.
    pub sweeps: AtomicU64,
    /// Objects walked by completed passes.
    pub listed: AtomicU64,
    /// Objects seen without a row, over all passes.
    pub seen: AtomicU64,
    /// Objects deleted after a TTL without a row.
    pub deleted: AtomicU64,
    /// Remembered paths forgotten: owned by a row after all, or not the
    /// engine's.
    pub forgotten: AtomicU64,
}

/// What one pass did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Swept {
    /// Objects walked.
    pub listed: usize,
    /// Objects without a row, remembered.
    pub seen: usize,
    /// Objects deleted.
    pub deleted: usize,
    /// Remembered paths forgotten.
    pub forgotten: usize,
    /// Partitions walked.
    pub prefixes: usize,
    /// True when the walk reached the last partition.
    pub wrapped: bool,
}

/// The sweep's place in the bucket between passes.
pub struct Sweeper {
    catalog: Catalog,
    store: Arc<dyn ObjectStore>,
    root: Path,
    config: SweepConfig,
    stats: Arc<OrphanStats>,
    inventory: Inventory,
}

/// Where the walk is: the tenants of the current walk, and within the
/// current tenant and kind, its days. Days are listed when reached. The
/// partition the next pass starts with is looked up at the end of a pass,
/// so a pass knows whether it finished the walk.
#[derive(Default)]
struct Inventory {
    tenants: Vec<Path>,
    tenant: usize,
    kind: usize,
    days: Option<Vec<Path>>,
    day: usize,
    pending: Option<Path>,
}

impl Sweeper {
    /// A sweep over `root` starting at the beginning of the prefix.
    pub fn new(
        catalog: Catalog,
        store: Arc<dyn ObjectStore>,
        root: Path,
        config: SweepConfig,
        stats: Arc<OrphanStats>,
    ) -> Self {
        Self {
            catalog,
            store,
            root,
            config,
            stats,
            inventory: Inventory::default(),
        }
    }

    /// The counters.
    pub fn stats(&self) -> Arc<OrphanStats> {
        Arc::clone(&self.stats)
    }

    /// One pass: finishes deletions a previous pass marked, walks the next
    /// partitions remembering objects without a row, then marks and
    /// deletes orphans past the TTL.
    ///
    /// # Errors
    /// Catalog or bucket failure; the next pass redoes what this one left.
    pub async fn sweep_once(&mut self) -> Result<Swept, CompactError> {
        let mut swept = Swept::default();
        swept.deleted += self.finish_marked().await?;
        self.walk(&mut swept).await?;
        self.reap(&mut swept).await?;
        self.stats.sweeps.fetch_add(1, Ordering::Relaxed);
        self.stats
            .listed
            .fetch_add(swept.listed as u64, Ordering::Relaxed);
        self.stats
            .seen
            .fetch_add(swept.seen as u64, Ordering::Relaxed);
        self.stats
            .deleted
            .fetch_add(swept.deleted as u64, Ordering::Relaxed);
        self.stats
            .forgotten
            .fetch_add(swept.forgotten as u64, Ordering::Relaxed);
        if swept.seen > 0 || swept.deleted > 0 || swept.forgotten > 0 {
            info!(
                role = "compactor",
                listed = swept.listed,
                without_row = swept.seen,
                deleted = swept.deleted,
                forgotten = swept.forgotten,
                partitions = swept.prefixes,
                wrapped = swept.wrapped,
                "orphan sweep"
            );
        }
        Ok(swept)
    }

    /// Deletes the objects of orphans already marked, as a pass that
    /// stopped between marking and deleting would leave.
    async fn finish_marked(&self) -> Result<usize, CompactError> {
        let marked = self
            .catalog
            .orphans_doomed(i64::try_from(self.config.deletes_per_pass).unwrap_or(i64::MAX))
            .await?;
        self.delete_all(&marked).await
    }

    /// Walks partitions, whole, until `objects_per_pass` objects have been
    /// listed or the walk wraps, remembering the engine's objects without
    /// a row. A pass that ends the walk starts the next one over.
    async fn walk(&mut self, swept: &mut Swept) -> Result<(), CompactError> {
        if self.inventory.pending.is_none() {
            self.inventory = Inventory {
                tenants: self.tenants().await?,
                ..Inventory::default()
            };
            self.inventory.pending = self.next_partition().await?;
        }
        while swept.listed < self.config.objects_per_pass.max(1) {
            let Some(partition) = self.inventory.pending.take() else {
                break;
            };
            self.walk_prefix(&partition, swept).await?;
            swept.prefixes += 1;
            self.inventory.pending = self.next_partition().await?;
        }
        swept.wrapped = self.inventory.pending.is_none();
        Ok(())
    }

    /// The tenant prefixes under the root whose name is a tenant id.
    async fn tenants(&self) -> Result<Vec<Path>, CompactError> {
        let listed = self.store.list_with_delimiter(Some(&self.prefix())).await?;
        let mut tenants: Vec<Path> = listed
            .common_prefixes
            .into_iter()
            .filter(|p| {
                p.parts().next_back().is_some_and(|id| {
                    SpanObject::parse(&format!(
                        "t/{}/traces/l0/dt=2000-01-01/hr=00/00000000-0000-0000-0000-000000000000.parquet",
                        id.as_ref()
                    ))
                    .is_some()
                })
            })
            .collect();
        tenants.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
        Ok(tenants)
    }

    /// The next day partition of the walk, listing a tenant's days of a
    /// kind when the walk reaches them. `None` when the walk is over.
    async fn next_partition(&mut self) -> Result<Option<Path>, CompactError> {
        loop {
            let Some(tenant) = self.inventory.tenants.get(self.inventory.tenant) else {
                return Ok(None);
            };
            let Some(kind) = KINDS.get(self.inventory.kind) else {
                self.inventory.tenant += 1;
                self.inventory.kind = 0;
                self.inventory.days = None;
                continue;
            };
            if self.inventory.days.is_none() {
                let level = Path::from(format!("{tenant}/traces/{kind}"));
                let listed = self.store.list_with_delimiter(Some(&level)).await?;
                let mut days: Vec<Path> = listed
                    .common_prefixes
                    .into_iter()
                    .filter(|p| {
                        p.parts()
                            .next_back()
                            .is_some_and(|d| d.as_ref().starts_with("dt="))
                    })
                    .collect();
                days.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
                self.inventory.days = Some(days);
                self.inventory.day = 0;
            }
            let day = self
                .inventory
                .days
                .as_ref()
                .and_then(|days| days.get(self.inventory.day))
                .cloned();
            if let Some(day) = day {
                self.inventory.day += 1;
                return Ok(Some(day));
            }
            self.inventory.kind += 1;
            self.inventory.days = None;
        }
    }

    /// Walks one partition, whole, in chunks.
    async fn walk_prefix(&self, prefix: &Path, swept: &mut Swept) -> Result<(), CompactError> {
        let mut stream = self.store.list(Some(prefix));
        let mut chunk: Vec<String> = Vec::with_capacity(CHUNK);
        while let Some(meta) = stream.next().await.transpose()? {
            swept.listed += 1;
            let location = meta.location;
            if self.is_engine_object(location.as_ref()) {
                chunk.push(location.to_string());
            }
            if chunk.len() >= CHUNK {
                swept.seen += self.remember(&chunk).await?;
                chunk.clear();
            }
        }
        if !chunk.is_empty() {
            swept.seen += self.remember(&chunk).await?;
        }
        Ok(())
    }

    /// Remembers the objects of `paths` that have no row; returns how many.
    async fn remember(&self, paths: &[String]) -> Result<usize, CompactError> {
        let candidates: Vec<String> = paths.iter().flat_map(|p| owners(p)).collect();
        let registered = self.catalog.registered_paths(&candidates).await?;
        let without_row: Vec<String> = paths
            .iter()
            .filter(|p| !owners(p).iter().any(|o| registered.contains(o)))
            .cloned()
            .collect();
        self.catalog.orphans_seen(&without_row).await?;
        Ok(without_row.len())
    }

    /// Marks and deletes orphans past the TTL, up to the pass's cap. A
    /// remembered path that is not the engine's, or that got a row, is
    /// forgotten; one another sweeper marked is deleted all the same.
    async fn reap(&self, swept: &mut Swept) -> Result<(), CompactError> {
        let budget = self.config.deletes_per_pass.saturating_sub(swept.deleted);
        if budget == 0 {
            return Ok(());
        }
        let due = self
            .catalog
            .orphans_due(self.config.ttl, i64::try_from(budget).unwrap_or(i64::MAX))
            .await?;
        let mut doomed = Vec::new();
        let mut forgotten = Vec::new();
        for path in due {
            if !self.is_engine_object(&path) {
                warn!(role = "compactor", path = %path, "remembered orphan is not an engine object; forgotten, not deleted");
                forgotten.push(path);
                continue;
            }
            match self.catalog.orphan_doom(&path, &owners(&path)).await? {
                Doom::Marked | Doom::AlreadyMarked => doomed.push(path),
                Doom::Owned => forgotten.push(path),
                Doom::Missing => {}
            }
        }
        self.catalog.orphans_forget(&forgotten).await?;
        swept.forgotten += forgotten.len();
        swept.deleted += self.delete_all(&doomed).await?;
        Ok(())
    }

    /// Deletes marked objects and forgets them; an object already gone
    /// counts as deleted, one that fails to delete stays marked for the
    /// next pass.
    async fn delete_all(&self, marked: &[String]) -> Result<usize, CompactError> {
        let mut deleted = Vec::new();
        for path in marked {
            if !self.is_engine_object(path) {
                warn!(role = "compactor", path = %path, "marked path is not an engine object; left in place");
                continue;
            }
            match self.store.delete(&Path::from(path.as_str())).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => deleted.push(path.clone()),
                Err(e) => {
                    warn!(role = "compactor", path = %path, error = %e, "orphan delete failed; the next sweep retries");
                }
            }
        }
        self.catalog.orphans_forget(&deleted).await?;
        Ok(deleted.len())
    }

    /// True when `path`, a full object path, is one the engine lays out
    /// under the root.
    fn is_engine_object(&self, path: &str) -> bool {
        self.relative(path)
            .is_some_and(|relative| SpanObject::parse(relative).is_some())
    }

    fn prefix(&self) -> Path {
        if self.root.as_ref().is_empty() {
            Path::from("t")
        } else {
            Path::from(format!("{}/t", self.root))
        }
    }

    /// `path` without the root, when it is under it.
    fn relative<'a>(&self, path: &'a str) -> Option<&'a str> {
        if self.root.as_ref().is_empty() {
            return Some(path);
        }
        path.strip_prefix(self.root.as_ref())?.strip_prefix('/')
    }
}

/// The file rows that would own `path`: itself for a span file, the span
/// file of each level for a sidecar.
fn owners(path: &str) -> Vec<String> {
    if path.rsplit_once('.').is_some_and(|(_, ext)| ext == "tix") {
        fs_index::owners_of_sidecar(path)
    } else {
        vec![path.to_owned()]
    }
}
