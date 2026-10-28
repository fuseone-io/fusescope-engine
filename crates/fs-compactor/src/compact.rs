//! One compaction pass: the oldest closed hour with enough L0 files is
//! merged into L1 files, which are uploaded and swapped in under the
//! leader's fence.
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
use crate::merge::{MergeConfig, Output, merge_to_l1};

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
    /// Most input bytes one pass merges.
    pub max_input_bytes: u64,
    /// Rows per output row group.
    pub row_group_rows: usize,
    /// Make this many swaps fail artificially after the outputs are
    /// uploaded. Zero in production; tests use it to crash before the swap.
    pub inject_swap_failures: u32,
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
}

/// The compaction side of the compactor.
pub struct Compactor {
    catalog: Catalog,
    store: Arc<dyn ObjectStore>,
    root: Path,
    config: CompactionConfig,
    stats: Arc<CompactorStats>,
    inject_swap_failures: AtomicU32,
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
        }
    }

    /// The counters.
    pub fn stats(&self) -> Arc<CompactorStats> {
        Arc::clone(&self.stats)
    }

    /// Compacts the oldest closed hour with at least two L0 files, up to
    /// the configured inputs, and swaps the outputs in under `fence`.
    /// `None` when nothing qualifies.
    ///
    /// # Errors
    /// A refused swap (conflict or lost lease), or a store, Parquet or
    /// catalog failure. The counters record which.
    pub async fn compact_once(
        &self,
        fence: Option<&LeaseFence>,
    ) -> Result<Option<Compaction>, CompactError> {
        let Some(candidate) = self
            .catalog
            .compaction_candidate(Signal::Traces, self.config.hour_close_delay, 2)
            .await?
        else {
            return Ok(None);
        };
        let all = self
            .catalog
            .l0_files_in_hour(candidate.tenant_id, Signal::Traces, candidate.hour)
            .await?;
        let inputs = self.take_inputs(&all);
        if inputs.len() < 2 {
            return Ok(None);
        }
        let result = self
            .compact_inputs(candidate.tenant_id, candidate.hour, &inputs, fence)
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

    /// The oldest files of the hour that fit one pass: at least two.
    fn take_inputs<'a>(&self, all: &'a [FileRecord]) -> Vec<&'a FileRecord> {
        let mut inputs = Vec::new();
        let mut bytes = 0u64;
        for file in all {
            let size = u64::try_from(file.size_bytes).unwrap_or(0);
            if inputs.len() >= self.config.max_inputs_per_pass.max(2)
                || (inputs.len() >= 2 && bytes + size > self.config.max_input_bytes)
            {
                break;
            }
            bytes += size;
            inputs.push(file);
        }
        inputs
    }

    async fn compact_inputs(
        &self,
        tenant: fs_catalog::TenantId,
        hour: chrono::DateTime<chrono::Utc>,
        inputs: &[&FileRecord],
        fence: Option<&LeaseFence>,
    ) -> Result<Compaction, CompactError> {
        let owned: Vec<FileRecord> = inputs.iter().map(|f| (*f).clone()).collect();
        let outputs = merge_to_l1(
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
        for output in &outputs {
            self.put_once(&output.file.path, output.bytes.clone())
                .await?;
            self.put_once(&output.index_path, output.index.clone())
                .await?;
        }
        let swap = self.swap(tenant, &owned, &outputs, fence).await;
        if swap.is_err() {
            self.stats
                .orphaned
                .fetch_add(outputs.len() as u64, Ordering::Relaxed);
            warn!(
                role = "compactor",
                tenant = %tenant.0,
                hour = %hour,
                outputs = outputs.len(),
                "compaction not published; its outputs are orphans"
            );
        }
        let compaction = swap?;
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
        outputs: &[Output],
        fence: Option<&LeaseFence>,
    ) -> Result<Compaction, CompactError> {
        if take_injected(&self.inject_swap_failures) {
            return Err(CompactError::Injected("swap"));
        }
        let ids: Vec<_> = inputs.iter().map(|f| f.id).collect();
        let files: Vec<NewFile> = outputs.iter().map(|o| o.file.clone()).collect();
        Ok(self
            .catalog
            .swap_files_fenced(tenant, Signal::Traces, &ids, &files, fence)
            .await?)
    }

    async fn put_once(&self, path: &str, bytes: Bytes) -> Result<(), CompactError> {
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

    fn count(&self, inputs: &[FileRecord], outputs: &[Output]) {
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
        s.output_bytes.fetch_add(
            outputs.iter().map(|o| o.bytes.len() as u64).sum(),
            Ordering::Relaxed,
        );
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
