//! L1 to L2: the L1 files of a closed day merged into L2 files of the day,
//! zstd level 7, each with its sidecar. A pass counts no attribute key,
//! summary or service graph; only the usage of inputs not counted before.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use fs_catalog::{CatalogError, Compaction, FileRecord, LeaseFence, Level, NewFile, Signal};
use fs_schema::attribute_keys::KeyStats;
use tracing::{info, warn};

use crate::CompactError;
use crate::compact::{Compactor, Published};
use crate::derived::key_table;
use crate::merge::{MergeConfig, Merger};
use crate::objects::OrphanGuard;

/// Rows read from an L1 input at a time: a day merges many inputs, each
/// holding one batch.
const L2_READ_BATCH_ROWS: usize = 8192;

impl Compactor {
    /// Merges the oldest closed day ready for L2, of one tenant and
    /// signal, its L1 files up to the input caps, and swaps the outputs in
    /// under `fence`. A day whose oldest file exceeds the cap is passed
    /// over and the next is tried. `None` when `compact_l2` is off or
    /// nothing qualifies.
    ///
    /// # Errors
    /// A refused swap (conflict or lost lease), or a store, Parquet or
    /// catalog failure.
    pub async fn compact_l2_once(
        &self,
        fence: &LeaseFence,
    ) -> Result<Option<Compaction>, CompactError> {
        if !self.config.compact_l2 {
            return Ok(None);
        }
        let mut skip = 0;
        let (candidate, taken) = loop {
            let Some(candidate) = self
                .catalog
                .l2_inputs(
                    &[Signal::Traces, Signal::Logs],
                    self.config.hour_close_delay,
                    skip,
                )
                .await?
            else {
                return Ok(None);
            };
            let taken = self.taken_l2(&candidate.files);
            if taken >= 1 {
                break (candidate, taken);
            }
            warn!(
                role = "compactor",
                tenant = %candidate.tenant_id.0,
                signal = ?candidate.signal,
                day = %candidate.hour,
                "closed day passed over: its oldest L1 file exceeds compactor.max_l2_input_bytes"
            );
            skip += 1;
        };
        let inputs = candidate.files.get(..taken).unwrap_or(&[]);
        let result = self
            .merge_day(
                candidate.tenant_id,
                candidate.signal,
                candidate.hour,
                inputs,
                fence,
            )
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

    /// The oldest files of a day that fit the L2 caps.
    fn taken_l2(&self, all: &[FileRecord]) -> usize {
        let mut taken = 0;
        let mut bytes = 0u64;
        for file in all {
            if taken >= self.config.max_inputs_per_pass.max(1) {
                break;
            }
            let size = u64::try_from(file.size_bytes).unwrap_or(u64::MAX);
            match bytes.checked_add(size) {
                Some(total) if total <= self.config.max_l2_input_bytes => bytes = total,
                _ => break,
            }
            taken += 1;
        }
        taken
    }

    async fn merge_day(
        &self,
        tenant: fs_catalog::TenantId,
        signal: Signal,
        day: chrono::DateTime<chrono::Utc>,
        inputs: &[FileRecord],
        fence: &LeaseFence,
    ) -> Result<Compaction, CompactError> {
        let (table, _) =
            key_table(signal).ok_or(CompactError::Merge("a signal without L2 files"))?;
        let mut merger = Merger::open(
            Arc::clone(&self.store),
            &self.root,
            tenant,
            signal,
            day,
            inputs,
            self.l2_merge_config(),
            KeyStats::new(table),
        )
        .await?;
        let mut guard = OrphanGuard {
            stats: Arc::clone(&self.stats),
            tenant,
            work: format!("L2 of {day}"),
            created: 0,
            armed: true,
        };
        let mut puts = 0usize;
        let mut outputs: Vec<Published> = Vec::new();
        while let Some(output) = merger.next_output().await? {
            self.put_once(&output.file.path, output.bytes.clone(), &mut puts)
                .await?;
            guard.created += 1;
            if let Some((path, index)) = &output.index {
                self.put_once(path, index.clone(), &mut puts).await?;
            } else if signal == Signal::Traces {
                self.stats.sidecars_over_cap.fetch_add(1, Ordering::Relaxed);
            }
            outputs.push(Published {
                bytes: output.bytes.len() as u64,
                file: output.file,
            });
        }
        let usage = merger.take_usage();
        drop(merger);
        let mut counted: Vec<NewFile> = Vec::new();
        if let Some(counts) = usage {
            let root = self.root.clone();
            let prepared = tokio::task::spawn_blocking(move || {
                crate::usage::usage_files(&root, tenant, &counts)
            })
            .await??;
            for (file, bytes) in prepared {
                self.put_once(&file.path, bytes, &mut puts).await?;
                guard.created += 1;
                counted.push(file);
            }
        }
        if crate::objects::take_injected(&self.inject_swap_failures) {
            return Err(CompactError::Injected("swap"));
        }
        let ids: Vec<_> = inputs.iter().map(|f| f.id).collect();
        let files: Vec<NewFile> = outputs
            .iter()
            .map(|o| o.file.clone())
            .chain(counted.iter().cloned())
            .collect();
        let compaction = self
            .catalog
            .swap_files_fenced(tenant, signal, &ids, &[], &files, Some(fence))
            .await?;
        guard.armed = false;
        self.count(inputs, &outputs);
        self.stats.l2_passes.fetch_add(1, Ordering::Relaxed);
        self.stats
            .usage_contributions
            .fetch_add(counted.len() as u64, Ordering::Relaxed);
        info!(
            role = "compactor",
            tenant = %tenant.0,
            ?signal,
            day = %day,
            inputs = inputs.len(),
            outputs = outputs.len(),
            usage = counted.len(),
            "compacted to L2"
        );
        Ok(compaction)
    }

    fn l2_merge_config(&self) -> MergeConfig {
        MergeConfig {
            level: Level::L2,
            row_group_rows: self.config.row_group_rows,
            read_batch_rows: self.config.row_group_rows.min(L2_READ_BATCH_ROWS),
            target_bytes: self.config.l2_target_bytes,
            output_bytes: self.config.l2_output_bytes,
            max_trace_index_bytes: self.config.max_trace_index_bytes,
            max_services_per_file: self.config.max_services_per_file,
            max_new_keys: 0,
            trace_summary_max_traces: 0,
            trace_summary_max_bytes: 0,
            service_graph_max_spans: 0,
            service_graph_max_bytes: 0,
            usage_max_entries: self.config.usage_max_entries,
            text_index: self.config.text_index,
        }
    }
}
