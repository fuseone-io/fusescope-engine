//! The orphan sweep: objects in the bucket without a file row.
//!
//! A crash between an upload and its registration, or a compaction that
//! did not publish, leaves objects no row points at. The sweep walks the
//! engine's prefix in bounded steps, remembers in the catalog the span
//! files and sidecars without a row, and deletes an object only once it
//! has been remembered for longer than the orphan TTL and the catalog has
//! marked it for deletion. The mark is what makes the delete safe: it is
//! refused when a row exists, it takes the orphan's row lock, and every
//! registration locks and checks the same row, so a writer still retrying
//! its registration either commits before the mark or is refused and
//! uploads again once the object is gone.
//!
//! Only paths the engine lays out are considered
//! ([`fs_index::SpanObject`]); anything else under the prefix is left
//! alone. The bucket is walked by partitions, one tenant, level and day
//! each, enumerated with delimiter listings; one pass walks at most
//! `prefixes_per_pass` of them, whole, from where the last pass stopped,
//! and deletes at most `deletes_per_pass`, so memory, queries and the
//! time taken from purge and compaction are bounded. Walking whole
//! partitions needs no order from the store's listing, which the local
//! store does not give.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fs_catalog::Catalog;
use fs_index::SpanObject;
use futures_util::StreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tracing::{info, warn};

use crate::CompactError;

/// Objects per membership query and sighting insert.
const CHUNK: usize = 1000;

/// Bounds of the sweep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepConfig {
    /// How long an object must have been seen without a row before it is
    /// deleted; longer than any upload waits for its registration.
    pub ttl: Duration,
    /// Most partitions, one tenant, level and day each, one pass walks
    /// before it stops and the next pass continues from there.
    pub prefixes_per_pass: usize,
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
    /// Remembered orphans found with a row before their TTL.
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
    /// Remembered orphans forgotten.
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
    /// The partitions of the current walk and the next one to visit;
    /// enumerated anew when the walk starts over.
    partitions: Vec<Path>,
    next: usize,
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
            partitions: Vec::new(),
            next: 0,
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

    /// Walks up to `prefixes_per_pass` partitions, whole, remembering the
    /// engine's objects without a row.
    async fn walk(&mut self, swept: &mut Swept) -> Result<(), CompactError> {
        if self.next >= self.partitions.len() {
            self.partitions = self.enumerate().await?;
            self.next = 0;
        }
        let end = self
            .next
            .saturating_add(self.config.prefixes_per_pass.max(1))
            .min(self.partitions.len());
        let batch: Vec<Path> = self.partitions.get(self.next..end).unwrap_or(&[]).to_vec();
        for prefix in &batch {
            self.walk_prefix(prefix, swept).await?;
            swept.prefixes += 1;
        }
        self.next = end;
        swept.wrapped = self.next >= self.partitions.len();
        Ok(())
    }

    /// Every partition under the root: for each tenant, each level and
    /// the sidecar index, each day. Only names the engine lays out.
    async fn enumerate(&self) -> Result<Vec<Path>, CompactError> {
        let mut out = Vec::new();
        let tenants = self.store.list_with_delimiter(Some(&self.prefix())).await?;
        for tenant in tenants.common_prefixes {
            let Some(id) = tenant.parts().next_back().map(|p| p.as_ref().to_owned()) else {
                continue;
            };
            if SpanObject::parse(&format!(
                "t/{id}/traces/l0/dt=2000-01-01/hr=00/00000000-0000-0000-0000-000000000000.parquet"
            ))
            .is_none()
            {
                continue;
            }
            for kind in ["l0", "l1", "l2", "index/trace_id"] {
                let level = Path::from(format!("{tenant}/traces/{kind}"));
                let days = self.store.list_with_delimiter(Some(&level)).await?;
                for day in days.common_prefixes {
                    if day
                        .parts()
                        .next_back()
                        .is_some_and(|p| p.as_ref().starts_with("dt="))
                    {
                        out.push(day);
                    }
                }
            }
        }
        out.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
        Ok(out)
    }

    /// Walks one partition, whole, in chunks.
    async fn walk_prefix(&self, prefix: &Path, swept: &mut Swept) -> Result<(), CompactError> {
        let mut stream = self.store.list(Some(prefix));
        let mut chunk: Vec<String> = Vec::with_capacity(CHUNK);
        while let Some(meta) = stream.next().await.transpose()? {
            swept.listed += 1;
            let location = meta.location;
            if let Some(relative) = self.relative(&location) {
                if SpanObject::parse(relative).is_some() {
                    chunk.push(location.to_string());
                }
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

    /// Marks and deletes orphans past the TTL, up to the pass's cap; the
    /// ones that got a row meanwhile are forgotten.
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
        let mut adopted = Vec::new();
        for path in due {
            if self.catalog.orphan_doom(&path, &owners(&path)).await? {
                doomed.push(path);
            } else {
                adopted.push(path);
            }
        }
        self.catalog.orphans_forget(&adopted).await?;
        swept.forgotten += adopted.len();
        swept.deleted += self.delete_all(&doomed).await?;
        Ok(())
    }

    /// Deletes marked objects and forgets them; an object already gone
    /// counts as deleted, one that fails to delete stays marked for the
    /// next pass.
    async fn delete_all(&self, marked: &[String]) -> Result<usize, CompactError> {
        let mut deleted = Vec::new();
        for path in marked {
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

    fn prefix(&self) -> Path {
        if self.root.as_ref().is_empty() {
            Path::from("t")
        } else {
            Path::from(format!("{}/t", self.root))
        }
    }

    /// `location` without the root, when it is under it.
    fn relative<'a>(&self, location: &'a Path) -> Option<&'a str> {
        let full = location.as_ref();
        if self.root.as_ref().is_empty() {
            return Some(full);
        }
        full.strip_prefix(self.root.as_ref())?.strip_prefix('/')
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
