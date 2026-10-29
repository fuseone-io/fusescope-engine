//! One compaction pass: the oldest cold hour with enough L0 files is
//! merged into L1 files, each uploaded as it closes, then swapped in under
//! the leader's fence. Without a fence there is no pass.
//!
//! A crash before the swap leaves the inputs visible and the outputs as
//! objects without rows, which the orphan sweep removes; a crash after it
//! leaves the inputs quarantined for the purge. A swap refused for a lost
//! fence or a conflict publishes nothing and leaves the outputs the same
//! way.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use chrono::TimeZone;
use fs_catalog::{
    Catalog, CatalogError, Compaction, FileId, FileRecord, LeaseFence, Level, NewFile, Signal,
    TimeRange,
};
use fs_schema::attribute_keys::KeyStats;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use tracing::{info, warn};

use crate::CompactError;
use crate::merge::{L1Merger, MergeConfig, attribute_keys_path};

/// What one pass is allowed to take on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionConfig {
    /// An hour is compacted only once every file in it ended this long
    /// ago, so nothing the hot window still serves is touched.
    pub hour_close_delay: Duration,
    /// An L1 file closes once it reaches this many bytes.
    pub l1_target_bytes: usize,
    /// Most L0 files one pass merges; memory holds one row group of each.
    pub max_inputs_per_pass: usize,
    /// Most input bytes one pass merges, a ceiling: an hour whose two
    /// oldest files do not fit is skipped and reported.
    pub max_input_bytes: u64,
    /// Rows per output row group.
    pub row_group_rows: usize,
    /// Make this many swaps fail artificially after the outputs are
    /// uploaded. Zero in production; tests use it to crash before the swap.
    pub inject_swap_failures: u32,
    /// Make the n-th object put of a pass fail artificially, counting
    /// Parquet and sidecar puts from zero. `None` in production; tests use
    /// it to crash between outputs.
    pub inject_put_failure_at: Option<usize>,
    /// Hold the n-th object put of a pass this long before it starts.
    /// `None` in production; tests use it to cancel a pass between outputs.
    pub inject_put_hold: Option<(usize, Duration)>,
}

/// Counters of the compactor.
#[derive(Debug, Default)]
pub struct CompactorStats {
    /// Passes that swapped outputs in.
    pub passes: AtomicU64,
    /// Inputs merged by completed passes.
    pub inputs: AtomicU64,
    /// Outputs published by completed passes.
    pub outputs: AtomicU64,
    /// Rows written by completed passes.
    pub rows: AtomicU64,
    /// Bytes of inputs merged by completed passes.
    pub input_bytes: AtomicU64,
    /// Bytes of outputs published by completed passes.
    pub output_bytes: AtomicU64,
    /// Swaps refused because an input was no longer visible.
    pub conflicts: AtomicU64,
    /// Swaps refused because the lease had changed hands.
    pub lease_lost: AtomicU64,
    /// Passes that failed before or at the swap for any other reason.
    pub failures: AtomicU64,
    /// Outputs uploaded by passes that did not publish them.
    pub orphaned: AtomicU64,
    /// Hours skipped because their two oldest files exceed the input cap.
    pub skipped: AtomicU64,
    /// `attribute_keys` tables rebuilt by completed passes.
    pub derived: AtomicU64,
    /// Passes aborted because the `attribute_keys` table before could not
    /// be read; inputs and table stay as they were.
    pub derived_unreadable: AtomicU64,
    /// Entries of keys beyond the tenant's `max_keys_hour` a pass did not
    /// count.
    pub derived_keys_dropped: AtomicU64,
}

/// The compaction side of the compactor.
pub struct Compactor {
    catalog: Catalog,
    store: Arc<dyn ObjectStore>,
    root: Path,
    config: CompactionConfig,
    stats: Arc<CompactorStats>,
    inject_swap_failures: AtomicU32,
    /// Hours already reported as exceeding the input cap.
    reported: std::sync::Mutex<
        std::collections::HashSet<(fs_catalog::TenantId, chrono::DateTime<chrono::Utc>)>,
    >,
}

impl Compactor {
    /// Compaction over `catalog` and `store`, writing under `root`.
    pub fn new(
        catalog: Catalog,
        store: Arc<dyn ObjectStore>,
        root: Path,
        config: CompactionConfig,
        stats: Arc<CompactorStats>,
    ) -> Self {
        let inject = config.inject_swap_failures;
        Self {
            catalog,
            store,
            root,
            config,
            stats,
            inject_swap_failures: AtomicU32::new(inject),
            reported: std::sync::Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// The counters.
    pub fn stats(&self) -> Arc<CompactorStats> {
        Arc::clone(&self.stats)
    }

    /// Compacts the oldest cold hour with at least two L0 files, up to the
    /// configured inputs, and swaps the outputs in under `fence`. An hour
    /// whose two oldest files exceed the input cap is passed over, reported
    /// once, and the next hour is tried. `None` when nothing qualifies.
    ///
    /// # Errors
    /// A refused swap (conflict or lost lease), or a store, Parquet or
    /// catalog failure. The counters record which.
    pub async fn compact_once(
        &self,
        fence: &LeaseFence,
    ) -> Result<Option<Compaction>, CompactError> {
        let mut skip = 0;
        let (candidate, inputs) = loop {
            let Some(candidate) = self
                .catalog
                .compaction_inputs(Signal::Traces, self.config.hour_close_delay, 2, skip)
                .await?
            else {
                return Ok(None);
            };
            let taken = self.taken(&candidate.files);
            if taken >= 2 {
                break (candidate, taken);
            }
            self.report_over_cap(candidate.tenant_id, candidate.hour);
            skip += 1;
        };
        let inputs = candidate.files.get(..inputs).unwrap_or(&[]);
        let result = self
            .compact_inputs(candidate.tenant_id, candidate.hour, inputs, fence)
            .await;
        match &result {
            Ok(_) => {}
            Err(CompactError::Catalog(CatalogError::LeaseLost { .. })) => {
                self.stats.lease_lost.fetch_add(1, Ordering::Relaxed);
            }
            Err(CompactError::Catalog(CatalogError::CompactionConflict { .. })) => {
                self.stats.conflicts.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.stats.failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        result.map(Some)
    }

    /// Counts and warns once per hour whose two oldest files exceed the cap.
    fn report_over_cap(&self, tenant: fs_catalog::TenantId, hour: chrono::DateTime<chrono::Utc>) {
        let first_time = self
            .reported
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert((tenant, hour));
        if first_time {
            self.stats.skipped.fetch_add(1, Ordering::Relaxed);
            warn!(
                role = "compactor",
                tenant = %tenant.0,
                hour = %hour,
                cap = self.config.max_input_bytes,
                "the hour's two oldest files exceed the input cap; passed over"
            );
        }
    }

    /// How many of the hour's oldest files fit one pass under both caps;
    /// fewer than two when the two oldest do not fit the byte cap.
    fn taken(&self, all: &[FileRecord]) -> usize {
        let mut taken = 0;
        let mut bytes = 0u64;
        for file in all {
            if taken >= self.config.max_inputs_per_pass.max(2) {
                break;
            }
            let size = u64::try_from(file.size_bytes).unwrap_or(u64::MAX);
            match bytes.checked_add(size) {
                Some(total) if total <= self.config.max_input_bytes => bytes = total,
                _ => break,
            }
            taken += 1;
        }
        taken
    }

    async fn compact_inputs(
        &self,
        tenant: fs_catalog::TenantId,
        hour: chrono::DateTime<chrono::Utc>,
        inputs: &[FileRecord],
        fence: &LeaseFence,
    ) -> Result<Compaction, CompactError> {
        let owned: Vec<FileRecord> = inputs.to_vec();
        let max_keys_hour = self
            .catalog
            .tenant(tenant)
            .await?
            .ok_or(CompactError::TenantGone(tenant))
            .map(|t| usize::try_from(t.max_keys_hour).unwrap_or(0))?;
        let (table_before, replaced) = self.table_before(tenant).await?;
        // The hour's budget is what the table remembers admitting on it,
        // so passes of one hour share it, across restarts and pass sizes.
        let hour_ns = hour
            .timestamp_nanos_opt()
            .ok_or(CompactError::Merge("hour outside the nanosecond range"))?;
        let max_new_keys = max_keys_hour.saturating_sub(table_before.admitted_in(hour_ns));
        let mut merger = L1Merger::open(
            Arc::clone(&self.store),
            &self.root,
            tenant,
            hour,
            &owned,
            MergeConfig {
                row_group_rows: self.config.row_group_rows,
                target_bytes: self.config.l1_target_bytes,
                max_new_keys,
            },
            table_before,
        )
        .await?;
        // Each output is uploaded as it closes; only its row stays. Any
        // exit before the swap commits, an error or the task being dropped,
        // leaves the objects created so far as orphans: the guard counts
        // them unless the swap disarms it.
        let mut outputs: Vec<Published> = Vec::new();
        let mut guard = OrphanGuard {
            stats: Arc::clone(&self.stats),
            tenant,
            hour,
            created: 0,
            armed: true,
        };
        let mut puts = 0usize;
        while let Some(output) = merger.next_output().await? {
            self.put_once(&output.file.path, output.bytes.clone(), &mut puts)
                .await?;
            guard.created += 1;
            self.put_once(&output.index_path, output.index.clone(), &mut puts)
                .await?;
            outputs.push(Published {
                bytes: output.bytes.len() as u64,
                file: output.file,
            });
        }
        let keys = merger.take_keys();
        drop(merger);
        if keys.dropped() > 0 {
            self.stats
                .derived_keys_dropped
                .fetch_add(keys.dropped(), Ordering::Relaxed);
            warn!(
                role = "compactor",
                tenant = %tenant.0,
                hour = %hour,
                admitted = keys.new_keys(),
                dropped = keys.dropped(),
                "attribute keys beyond max_keys_hour were not counted"
            );
        }
        let derived = self
            .publish_attribute_keys(tenant, hour, &keys, &mut guard, &mut puts)
            .await?;
        let compaction = self
            .swap(tenant, &owned, &replaced, &outputs, derived.as_ref(), fence)
            .await?;
        guard.armed = false;
        self.count(&owned, &outputs);
        if derived.is_some() {
            self.stats.derived.fetch_add(1, Ordering::Relaxed);
        }
        info!(
            role = "compactor",
            tenant = %tenant.0,
            hour = %hour,
            inputs = owned.len(),
            outputs = outputs.len(),
            derived = derived.is_some(),
            "compacted"
        );
        Ok(compaction)
    }

    /// The tenant's `attribute_keys` table before this pass, read whole,
    /// and the rows the pass will replace. Any failure to read it ends the
    /// pass before anything is created, so inputs and table stay as they
    /// are: the history of the table is never replaced by one pass's view.
    async fn table_before(
        &self,
        tenant: fs_catalog::TenantId,
    ) -> Result<(KeyStats, Vec<(Signal, FileId)>), CompactError> {
        let previous = self
            .catalog
            .all_visible_files(tenant, Signal::AttributeKeys)
            .await?;
        let mut keys = KeyStats::default();
        let mut replaced = Vec::with_capacity(previous.len());
        for file in &previous {
            match self.read_attribute_keys(&file.path).await {
                Ok(known) => keys.merge(known),
                Err(e) => {
                    self.stats
                        .derived_unreadable
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        role = "compactor",
                        tenant = %tenant.0,
                        path = %file.path,
                        error = %e,
                        "attribute_keys table unreadable; pass aborted"
                    );
                    return Err(e);
                }
            }
            replaced.push((Signal::AttributeKeys, file.id));
        }
        Ok((keys.settled(), replaced))
    }

    /// Uploads `keys` as the tenant's `attribute_keys` table of this pass:
    /// the file for the swap, or `None` when there is nothing to list.
    async fn publish_attribute_keys(
        &self,
        tenant: fs_catalog::TenantId,
        hour: chrono::DateTime<chrono::Utc>,
        keys: &KeyStats,
        guard: &mut OrphanGuard,
        puts: &mut usize,
    ) -> Result<Option<NewFile>, CompactError> {
        let Some((first, last)) = keys.range() else {
            return Ok(None);
        };
        let bytes = Bytes::from(fs_schema::attribute_keys::encode(keys)?);
        let id = FileId::new();
        let path = attribute_keys_path(&self.root, tenant, hour, id);
        self.put_once(path.as_ref(), bytes.clone(), puts).await?;
        guard.created += 1;
        let file = NewFile {
            id,
            origin: id,
            tenant_id: tenant,
            signal: Signal::AttributeKeys,
            level: Level::L1,
            path: path.to_string(),
            size_bytes: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
            row_count: i64::try_from(keys.len()).unwrap_or(i64::MAX),
            range: TimeRange {
                start: chrono::Utc.timestamp_nanos(first),
                end: chrono::Utc.timestamp_nanos(last),
            },
            services: Vec::new(),
            summary: None,
        };
        Ok(Some(file))
    }

    async fn read_attribute_keys(&self, path: &str) -> Result<KeyStats, CompactError> {
        let bytes = self.store.get(&Path::from(path)).await?.bytes().await?;
        Ok(fs_schema::attribute_keys::decode(bytes)?)
    }

    async fn swap(
        &self,
        tenant: fs_catalog::TenantId,
        inputs: &[FileRecord],
        replaced: &[(Signal, FileId)],
        outputs: &[Published],
        derived: Option<&NewFile>,
        fence: &LeaseFence,
    ) -> Result<Compaction, CompactError> {
        if take_injected(&self.inject_swap_failures) {
            return Err(CompactError::Injected("swap"));
        }
        let ids: Vec<_> = inputs.iter().map(|f| f.id).collect();
        let files: Vec<NewFile> = outputs
            .iter()
            .map(|o| o.file.clone())
            .chain(derived.cloned())
            .collect();
        Ok(self
            .catalog
            .swap_files_fenced(tenant, Signal::Traces, &ids, replaced, &files, Some(fence))
            .await?)
    }

    async fn put_once(
        &self,
        path: &str,
        bytes: Bytes,
        puts: &mut usize,
    ) -> Result<(), CompactError> {
        let this = *puts;
        *puts += 1;
        if self.config.inject_put_failure_at == Some(this) {
            return Err(CompactError::Injected("object put"));
        }
        if let Some((at, hold)) = self.config.inject_put_hold {
            if at == this {
                tokio::time::sleep(hold).await;
            }
        }
        let create = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(&Path::from(path), PutPayload::from_bytes(bytes), create)
            .await
        {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                Err(CompactError::ObjectExists(path.to_owned()))
            }
            Err(e) => Err(e.into()),
        }
    }

    fn count(&self, inputs: &[FileRecord], outputs: &[Published]) {
        let s = &self.stats;
        s.passes.fetch_add(1, Ordering::Relaxed);
        s.inputs.fetch_add(inputs.len() as u64, Ordering::Relaxed);
        s.outputs.fetch_add(outputs.len() as u64, Ordering::Relaxed);
        s.rows.fetch_add(
            outputs
                .iter()
                .map(|o| u64::try_from(o.file.row_count).unwrap_or(0))
                .sum(),
            Ordering::Relaxed,
        );
        s.input_bytes.fetch_add(
            inputs
                .iter()
                .map(|f| u64::try_from(f.size_bytes).unwrap_or(0))
                .sum(),
            Ordering::Relaxed,
        );
        s.output_bytes
            .fetch_add(outputs.iter().map(|o| o.bytes).sum(), Ordering::Relaxed);
    }
}

/// An output already in the bucket, waiting for the swap.
struct Published {
    file: NewFile,
    bytes: u64,
}

/// Counts the objects a pass created as orphans when it ends without a
/// committed swap, whether by error or by the task being dropped.
struct OrphanGuard {
    stats: Arc<CompactorStats>,
    tenant: fs_catalog::TenantId,
    hour: chrono::DateTime<chrono::Utc>,
    /// Parquet objects created so far; their sidecars may or may not be.
    created: u64,
    armed: bool,
}

impl Drop for OrphanGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.stats
            .orphaned
            .fetch_add(self.created, Ordering::Relaxed);
        if self.created > 0 {
            warn!(
                role = "compactor",
                tenant = %self.tenant.0,
                hour = %self.hour,
                orphans = self.created,
                "compaction not published; the objects it created are orphans"
            );
        }
    }
}

/// Takes one injected failure when any is left.
fn take_injected(counter: &AtomicU32) -> bool {
    let mut remaining = counter.load(Ordering::SeqCst);
    loop {
        if remaining == 0 {
            return false;
        }
        match counter.compare_exchange(remaining, remaining - 1, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => return true,
            Err(actual) => remaining = actual,
        }
    }
}
