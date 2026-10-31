//! One compaction pass: the oldest cold hour with enough L0 files of one
//! signal, spans or, when enabled, log rows, is merged into L1 files of
//! that signal, each uploaded as it closes, then swapped in under the
//! leader's fence with the signal's table of attribute keys. Without a
//! fence there is no pass.
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
use fs_catalog::{
    Catalog, CatalogError, Compaction, FileId, FileRecord, LeaseFence, NewFile, Signal,
};
use fs_schema::attribute_keys::{KeyStats, KeyTable};
use fs_schema::service_graph::ServiceGraphBuilder;
use fs_schema::trace_summary::TraceSummaries;
use object_store::ObjectStore;
use object_store::path::Path;
use tracing::{info, warn};

use crate::CompactError;
use crate::derived::{key_table, read_table, table_file};
use crate::merge::{L1Merger, MergeConfig};
use crate::objects::{CountsOrphans, OrphanGuard, create, take_injected};

/// Hard ceiling for the two C6 builders retained by one pass together.
pub const MAX_DERIVED_BUILDER_BYTES: usize = 1024 * 1024 * 1024;

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
    /// Most distinct traces retained to build one pass's summary.
    pub trace_summary_max_traces: usize,
    /// Conservative bytes retained to build one pass's summary.
    pub trace_summary_max_bytes: usize,
    /// Most spans retained to build one pass's service graph.
    pub service_graph_max_spans: usize,
    /// Conservative bytes retained to build one pass's service graph.
    pub service_graph_max_bytes: usize,
    /// Rows per output row group.
    pub row_group_rows: usize,
    /// Also compact the L0 files of log rows. Off, no pass takes a new
    /// hour of them; log L1 files and `log_attribute_keys` tables already
    /// written are read, rebuilt, swept and purged all the same.
    pub compact_logs: bool,
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
    /// `attribute_keys` and `log_attribute_keys` tables rebuilt by
    /// completed passes.
    pub derived: AtomicU64,
    /// Passes aborted because the table of attribute keys before could not
    /// be read; inputs and table stay as they were.
    pub derived_unreadable: AtomicU64,
    /// Entries of keys beyond the tenant's `max_keys_hour` a pass did not
    /// count.
    pub derived_keys_dropped: AtomicU64,
    /// `trace_summary` contribution files published.
    pub trace_summary: AtomicU64,
    /// Completed passes whose trace count crossed the summary cap.
    pub trace_summary_skipped: AtomicU64,
    /// `service_graph_1m` contribution files published.
    pub service_graph: AtomicU64,
    /// Completed passes whose span count crossed the graph cap.
    pub service_graph_skipped: AtomicU64,
}

impl CountsOrphans for CompactorStats {
    fn count_orphans(&self, created: u64) {
        self.orphaned.fetch_add(created, Ordering::Relaxed);
    }
}

/// Partial derived data accumulated while the pass streamed its inputs.
struct DerivedContributions {
    keys: KeyStats,
    summaries: TraceSummaries,
    service_graph: ServiceGraphBuilder,
}

#[derive(Clone, Copy)]
struct DerivedLimit {
    limit: &'static str,
    cap: usize,
    observed: usize,
    estimated_bytes: usize,
}

impl From<fs_schema::trace_summary::LimitExceeded> for DerivedLimit {
    fn from(value: fs_schema::trace_summary::LimitExceeded) -> Self {
        Self {
            limit: value.limit,
            cap: value.cap,
            observed: value.observed,
            estimated_bytes: value.estimated_bytes,
        }
    }
}

impl From<fs_schema::service_graph::LimitExceeded> for DerivedLimit {
    fn from(value: fs_schema::service_graph::LimitExceeded) -> Self {
        Self {
            limit: value.limit,
            cap: value.cap,
            observed: value.observed,
            estimated_bytes: value.estimated_bytes,
        }
    }
}

/// Catalog rows for the derived objects uploaded by a pass.
struct PublishedDerived {
    keys: Option<NewFile>,
    summaries: Option<NewFile>,
    service_graph: Option<NewFile>,
    summary_skipped: Option<DerivedLimit>,
    service_graph_skipped: Option<DerivedLimit>,
}

/// The compaction side of the compactor.
pub struct Compactor {
    catalog: Catalog,
    store: Arc<dyn ObjectStore>,
    root: Path,
    config: CompactionConfig,
    stats: Arc<CompactorStats>,
    inject_swap_failures: AtomicU32,
    /// Hours of a signal already reported as exceeding the input cap.
    reported: std::sync::Mutex<
        std::collections::HashSet<(fs_catalog::TenantId, Signal, chrono::DateTime<chrono::Utc>)>,
    >,
    /// Derived contributions already reported as crossing a cap.
    derived_reported: std::sync::Mutex<
        std::collections::HashSet<(
            fs_catalog::TenantId,
            chrono::DateTime<chrono::Utc>,
            &'static str,
        )>,
    >,
}

impl Compactor {
    /// Compaction over `catalog` and `store`, writing under `root`.
    pub fn new(
        catalog: Catalog,
        store: Arc<dyn ObjectStore>,
        root: Path,
        mut config: CompactionConfig,
        stats: Arc<CompactorStats>,
    ) -> Self {
        (
            config.trace_summary_max_bytes,
            config.service_graph_max_bytes,
        ) = bounded_derived_bytes(
            config.trace_summary_max_bytes,
            config.service_graph_max_bytes,
        );
        let inject = config.inject_swap_failures;
        Self {
            catalog,
            store,
            root,
            config,
            stats,
            inject_swap_failures: AtomicU32::new(inject),
            reported: std::sync::Mutex::new(std::collections::HashSet::new()),
            derived_reported: std::sync::Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// The counters.
    pub fn stats(&self) -> Arc<CompactorStats> {
        Arc::clone(&self.stats)
    }

    /// Compacts the oldest cold hour with at least two L0 files of one
    /// signal, spans before logs within an hour, up to the configured
    /// inputs, and swaps the outputs in under `fence`. An hour whose two
    /// oldest files exceed the input cap is passed over, reported once,
    /// and the next hour of either signal is tried. `None` when nothing
    /// qualifies.
    ///
    /// # Errors
    /// A refused swap (conflict or lost lease), or a store, Parquet or
    /// catalog failure. The counters record which.
    pub async fn compact_once(
        &self,
        fence: &LeaseFence,
    ) -> Result<Option<Compaction>, CompactError> {
        let signals: &[Signal] = if self.config.compact_logs {
            &[Signal::Traces, Signal::Logs]
        } else {
            &[Signal::Traces]
        };
        let mut skip = 0;
        let (candidate, inputs) = loop {
            let Some(candidate) = self
                .catalog
                .compaction_inputs(signals, self.config.hour_close_delay, 2, skip)
                .await?
            else {
                return Ok(None);
            };
            let taken = self.taken(&candidate.files);
            if taken >= 2 {
                break (candidate, taken);
            }
            self.report_over_cap(candidate.tenant_id, candidate.signal, candidate.hour);
            skip += 1;
        };
        let inputs = candidate.files.get(..inputs).unwrap_or(&[]);
        let result = self
            .compact_inputs(
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

    /// Counts and warns once per hour of a signal whose two oldest files
    /// exceed the cap.
    fn report_over_cap(
        &self,
        tenant: fs_catalog::TenantId,
        signal: Signal,
        hour: chrono::DateTime<chrono::Utc>,
    ) {
        let first_time = self
            .reported
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert((tenant, signal, hour));
        if first_time {
            self.stats.skipped.fetch_add(1, Ordering::Relaxed);
            warn!(
                role = "compactor",
                tenant = %tenant.0,
                ?signal,
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
        signal: Signal,
        hour: chrono::DateTime<chrono::Utc>,
        inputs: &[FileRecord],
        fence: &LeaseFence,
    ) -> Result<Compaction, CompactError> {
        let owned: Vec<FileRecord> = inputs.to_vec();
        let keys_of = key_table(signal).ok_or(CompactError::Merge("a signal without L1 files"))?;
        let max_keys_hour = self
            .catalog
            .tenant(tenant)
            .await?
            .ok_or(CompactError::TenantGone(tenant))
            .map(|t| usize::try_from(t.max_keys_hour).unwrap_or(0))?;
        let (table_before, replaced) = self.table_before(tenant, keys_of).await?;
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
            signal,
            hour,
            &owned,
            MergeConfig {
                row_group_rows: self.config.row_group_rows,
                target_bytes: self.config.l1_target_bytes,
                max_new_keys,
                trace_summary_max_traces: self.config.trace_summary_max_traces,
                trace_summary_max_bytes: self.config.trace_summary_max_bytes,
                service_graph_max_spans: self.config.service_graph_max_spans,
                service_graph_max_bytes: self.config.service_graph_max_bytes,
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
            work: format!("compaction of {hour}"),
            created: 0,
            armed: true,
        };
        let mut puts = 0usize;
        while let Some(output) = merger.next_output().await? {
            self.put_once(&output.file.path, output.bytes.clone(), &mut puts)
                .await?;
            guard.created += 1;
            if let Some((path, index)) = &output.index {
                self.put_once(path, index.clone(), &mut puts).await?;
            }
            outputs.push(Published {
                bytes: output.bytes.len() as u64,
                file: output.file,
            });
        }
        let contributions = DerivedContributions {
            keys: merger.take_keys(),
            summaries: merger.take_trace_summaries(),
            service_graph: merger.take_service_graph(),
        };
        drop(merger);
        self.report_dropped_keys(tenant, signal, hour, &contributions.keys);
        let derived = self
            .publish_derived(tenant, hour, contributions, &mut guard, &mut puts)
            .await?;
        let compaction = self
            .swap(tenant, signal, &owned, &replaced, &outputs, &derived, fence)
            .await?;
        guard.armed = false;
        self.count(&owned, &outputs);
        self.count_derived(tenant, hour, &derived);
        info!(
            role = "compactor",
            tenant = %tenant.0,
            ?signal,
            hour = %hour,
            inputs = owned.len(),
            outputs = outputs.len(),
            derived = derived.keys.is_some(),
            trace_summary = derived.summaries.is_some(),
            service_graph = derived.service_graph.is_some(),
            "compacted"
        );
        Ok(compaction)
    }

    /// The tenant's table of keys of the pass's signal before this pass,
    /// read whole, and the rows the pass will replace. Any failure to read
    /// it ends the pass before anything is created, so inputs and table
    /// stay as they are: the history of the table is never replaced by one
    /// pass's view.
    async fn table_before(
        &self,
        tenant: fs_catalog::TenantId,
        (table, files): (KeyTable, Signal),
    ) -> Result<(KeyStats, Vec<(Signal, FileId)>), CompactError> {
        let previous = self.catalog.all_visible_files(tenant, files).await?;
        let mut keys = KeyStats::new(table);
        let mut replaced = Vec::with_capacity(previous.len());
        for file in &previous {
            match read_table(self.store.as_ref(), &file.path, table).await {
                Ok(known) => keys.merge(known),
                Err(e) => {
                    self.stats
                        .derived_unreadable
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        role = "compactor",
                        tenant = %tenant.0,
                        table = table.name(),
                        path = %file.path,
                        error = %e,
                        "attribute keys table unreadable; pass aborted"
                    );
                    return Err(e);
                }
            }
            replaced.push((files, file.id));
        }
        Ok((keys.settled(), replaced))
    }

    fn report_dropped_keys(
        &self,
        tenant: fs_catalog::TenantId,
        signal: Signal,
        hour: chrono::DateTime<chrono::Utc>,
        keys: &KeyStats,
    ) {
        if keys.dropped() == 0 {
            return;
        }
        self.stats
            .derived_keys_dropped
            .fetch_add(keys.dropped(), Ordering::Relaxed);
        warn!(
            role = "compactor",
            tenant = %tenant.0,
            ?signal,
            hour = %hour,
            admitted = keys.new_keys(),
            dropped = keys.dropped(),
            "attribute keys beyond max_keys_hour were not counted"
        );
    }

    async fn publish_derived(
        &self,
        tenant: fs_catalog::TenantId,
        hour: chrono::DateTime<chrono::Utc>,
        contributions: DerivedContributions,
        guard: &mut OrphanGuard<CompactorStats>,
        puts: &mut usize,
    ) -> Result<PublishedDerived, CompactError> {
        let DerivedContributions {
            keys,
            summaries,
            service_graph,
        } = contributions;
        let root = self.root.clone();
        let keys =
            tokio::task::spawn_blocking(move || table_file(&root, tenant, hour, &keys)).await??;
        let keys = self.publish_prepared(keys, guard, puts).await?;

        let summary_skipped = summaries.limit_exceeded().map(Into::into);
        let summaries = if summary_skipped.is_none() {
            let root = self.root.clone();
            tokio::task::spawn_blocking(move || {
                crate::derived::trace_summary_file(&root, tenant, hour, &summaries)
            })
            .await??
        } else {
            None
        };
        let summaries = self.publish_prepared(summaries, guard, puts).await?;

        let root = self.root.clone();
        let (service_graph, service_graph_skipped) = tokio::task::spawn_blocking(move || {
            let (rows, exceeded) = service_graph.finish_with_limit();
            let prepared = match rows {
                Some(rows) => crate::derived::service_graph_file(&root, tenant, hour, &rows)?,
                None => None,
            };
            Ok::<_, CompactError>((prepared, exceeded.map(Into::into)))
        })
        .await??;
        let service_graph = self.publish_prepared(service_graph, guard, puts).await?;
        Ok(PublishedDerived {
            keys,
            summaries,
            service_graph,
            summary_skipped,
            service_graph_skipped,
        })
    }

    fn count_derived(
        &self,
        tenant: fs_catalog::TenantId,
        hour: chrono::DateTime<chrono::Utc>,
        published: &PublishedDerived,
    ) {
        if published.keys.is_some() {
            self.stats.derived.fetch_add(1, Ordering::Relaxed);
        }
        if published.summaries.is_some() {
            self.stats.trace_summary.fetch_add(1, Ordering::Relaxed);
        } else if let Some(limit) = published.summary_skipped {
            self.stats
                .trace_summary_skipped
                .fetch_add(1, Ordering::Relaxed);
            self.report_derived_over_cap(tenant, hour, "trace_summary", limit);
        }
        if published.service_graph.is_some() {
            self.stats.service_graph.fetch_add(1, Ordering::Relaxed);
        } else if let Some(limit) = published.service_graph_skipped {
            self.stats
                .service_graph_skipped
                .fetch_add(1, Ordering::Relaxed);
            self.report_derived_over_cap(tenant, hour, "service_graph_1m", limit);
        }
    }

    fn report_derived_over_cap(
        &self,
        tenant: fs_catalog::TenantId,
        hour: chrono::DateTime<chrono::Utc>,
        signal: &'static str,
        limit: DerivedLimit,
    ) {
        let first_time = self
            .derived_reported
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert((tenant, hour, signal));
        if first_time {
            warn!(
                role = "compactor",
                tenant = %tenant.0,
                hour = %hour,
                signal,
                limit = limit.limit,
                cap = limit.cap,
                observed = limit.observed,
                estimated_bytes = limit.estimated_bytes,
                "derived contribution crossed its cap and was skipped"
            );
        }
    }

    /// Uploads `keys` as the tenant's `attribute_keys` table of this pass:
    /// the file for the swap, or `None` when there is nothing to list.
    async fn publish_prepared(
        &self,
        prepared: Option<(NewFile, Bytes)>,
        guard: &mut OrphanGuard<CompactorStats>,
        puts: &mut usize,
    ) -> Result<Option<NewFile>, CompactError> {
        let Some((file, bytes)) = prepared else {
            return Ok(None);
        };
        self.put_once(&file.path, bytes, puts).await?;
        guard.created += 1;
        Ok(Some(file))
    }

    #[allow(clippy::too_many_arguments)]
    async fn swap(
        &self,
        tenant: fs_catalog::TenantId,
        signal: Signal,
        inputs: &[FileRecord],
        replaced: &[(Signal, FileId)],
        outputs: &[Published],
        derived: &PublishedDerived,
        fence: &LeaseFence,
    ) -> Result<Compaction, CompactError> {
        if take_injected(&self.inject_swap_failures) {
            return Err(CompactError::Injected("swap"));
        }
        let ids: Vec<_> = inputs.iter().map(|f| f.id).collect();
        let files: Vec<NewFile> = outputs
            .iter()
            .map(|o| o.file.clone())
            .chain(derived.keys.clone())
            .chain(derived.summaries.clone())
            .chain(derived.service_graph.clone())
            .collect();
        Ok(self
            .catalog
            .swap_files_fenced(tenant, signal, &ids, replaced, &files, Some(fence))
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
        create(self.store.as_ref(), path, bytes).await
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

fn bounded_derived_bytes(summary: usize, graph: usize) -> (usize, usize) {
    let summary = summary.clamp(
        fs_schema::trace_summary::TRACE_ENTRY_BYTES,
        MAX_DERIVED_BUILDER_BYTES - fs_schema::service_graph::SPAN_ENTRY_BYTES,
    );
    let graph = graph.clamp(
        fs_schema::service_graph::SPAN_ENTRY_BYTES,
        MAX_DERIVED_BUILDER_BYTES - summary,
    );
    (summary, graph)
}

/// An output already in the bucket, waiting for the swap.
struct Published {
    file: NewFile,
    bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_byte_limits_are_bounded_inside_the_compactor() {
        let (summary, graph) = bounded_derived_bytes(usize::MAX, usize::MAX);
        assert_eq!(summary + graph, MAX_DERIVED_BUILDER_BYTES);
        assert!(summary >= fs_schema::trace_summary::TRACE_ENTRY_BYTES);
        assert!(graph >= fs_schema::service_graph::SPAN_ENTRY_BYTES);

        assert_eq!(
            bounded_derived_bytes(0, 0),
            (
                fs_schema::trace_summary::TRACE_ENTRY_BYTES,
                fs_schema::service_graph::SPAN_ENTRY_BYTES,
            )
        );
    }
}
