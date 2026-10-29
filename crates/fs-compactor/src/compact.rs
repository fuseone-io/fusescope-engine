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
use fs_catalog::{Catalog, CatalogError, Compaction, FileRecord, LeaseFence, NewFile, Signal};
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use tracing::{info, warn};

use crate::CompactError;
use crate::merge::{L1Merger, MergeConfig};

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
        let mut merger = L1Merger::open(
            Arc::clone(&self.store),
            &self.root,
            tenant,
            hour,
            &owned,
            MergeConfig {
                row_group_rows: self.config.row_group_rows,
                target_bytes: self.config.l1_target_bytes,
            },
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
        drop(merger);
        let compaction = self.swap(tenant, &owned, &outputs, fence).await?;
        guard.armed = false;
        self.count(&owned, &outputs);
        info!(
            role = "compactor",
            tenant = %tenant.0,
            hour = %hour,
            inputs = owned.len(),
            outputs = outputs.len(),
            "compacted"
        );
        Ok(compaction)
    }

    async fn swap(
        &self,
        tenant: fs_catalog::TenantId,
        inputs: &[FileRecord],
        outputs: &[Published],
        fence: &LeaseFence,
    ) -> Result<Compaction, CompactError> {
        if take_injected(&self.inject_swap_failures) {
            return Err(CompactError::Injected("swap"));
        }
        let ids: Vec<_> = inputs.iter().map(|f| f.id).collect();
        let files: Vec<NewFile> = outputs.iter().map(|o| o.file.clone()).collect();
        Ok(self
            .catalog
            .swap_files_fenced(tenant, Signal::Traces, &ids, &[], &files, Some(fence))
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
