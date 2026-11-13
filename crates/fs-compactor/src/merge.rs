//! A merge of sorted files of one signal, spans or log rows, into sorted
//! files of the next level: the L0 files of an hour into L1, the L1 files
//! of a day into L2.
//!
//! Every input is sorted by `(service_name, timestamp)` with ties in log
//! order, so the k-way merge of [`crate::kway`] produces the output in
//! that order. An output closes at its target size and is handed over
//! before the next one starts. A null service sorts before every name, as
//! the inputs sorted it.
//!
//! A merge with an output budget holds within it: the inputs their
//! planned chunks and metadata, at most half of it; a row group at most a
//! sixteenth, by the exact size of its rows; and the output closes before
//! a row group, gathered, encoded and appended, would take the total past
//! the budget.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use fs_catalog::{FileId, FileRecord, Level, Signal, TenantId};
use fs_schema::attribute_keys::KeyStats;
use fs_schema::logs::log_schema_v1;
use fs_schema::service_graph::ServiceGraphBuilder;
use fs_schema::spans::span_schema_v1;
use fs_schema::top_level_operations::TopLevelOperations;
use fs_schema::trace_summary::TraceSummaries;
use fs_schema::usage::{UsageCounts, UsageRows, UsageSignal};
use object_store::ObjectStore;
use object_store::path::Path;

use crate::CompactError;
use crate::kway::{Merge, Picks, Reading};
use crate::output::{Output, OutputBuilder};

/// Bytes gathering a row group costs per column besides its rows.
const GROUP_COLUMN_BYTES: usize = 4096;

/// The usage signal of rows of `signal`, `None` for one without usage.
pub(crate) fn usage_signal(signal: Signal) -> Option<UsageSignal> {
    match signal {
        Signal::Traces => Some(UsageSignal::Spans),
        Signal::Logs => Some(UsageSignal::Logs),
        _ => None,
    }
}

/// Bounds of one merge.
#[derive(Debug, Clone, Copy)]
pub struct MergeConfig {
    /// The level of the outputs: L1 from the L0 files of an hour, L2 from
    /// the L1 files of a day. Keys, summaries and the service graph are
    /// built at L1 only.
    pub level: Level,
    /// Rows per output row group.
    pub row_group_rows: usize,
    /// Rows read from an input at a time.
    pub read_batch_rows: usize,
    /// An output closes once its bytes reach this.
    pub target_bytes: usize,
    /// An output closes once what it holds in memory reaches this: its
    /// encoded bytes, its row group in progress and its sidecar.
    pub output_bytes: usize,
    /// Most bytes of trace ids an output collects for its `trace_id`
    /// sidecar; past it the output has none.
    pub max_trace_index_bytes: usize,
    /// Most distinct services an output lists; past it the output lists
    /// none, read as unknown.
    pub max_services_per_file: usize,
    /// Most keys new to the `attribute_keys` table this merge admits: what
    /// is left of the tenant's `max_keys_hour` for the hour being merged.
    pub max_new_keys: usize,
    /// Most distinct traces retained for the pass's summary contribution.
    pub trace_summary_max_traces: usize,
    /// Conservative byte budget for the summary builder.
    pub trace_summary_max_bytes: usize,
    /// Most spans retained to resolve the pass's service graph.
    pub service_graph_max_spans: usize,
    /// Budgets of the text sidecar of each log output.
    pub text_index: fs_index::TextIndexLimits,
    /// Conservative byte budget for the service-graph builder.
    pub service_graph_max_bytes: usize,
    /// Most distinct operations retained for the pass's top-level operations.
    pub top_level_operations_max_operations: usize,
    /// Conservative byte budget for the top-level operations builder.
    pub top_level_operations_max_bytes: usize,
    /// Bytes the usage counts may hold; past them a full count is set
    /// aside, handed out as its own contribution.
    pub usage_max_bytes: usize,
}

/// Merges files of one tenant and signal into outputs of the next level,
/// one at a time: the L0 files of an hour into L1, the L1 files of a day
/// into L2.
pub struct Merger {
    signal: Signal,
    schema: SchemaRef,
    merge: Merge,
    root: Path,
    tenant: TenantId,
    hour: DateTime<Utc>,
    config: MergeConfig,
    /// Set once the inputs are spent.
    done: bool,
    keys: KeyStats,
    /// `hour` in nanoseconds, the budget the merge admits keys on.
    hour_ns: i64,
    summaries: TraceSummaries,
    graph: ServiceGraphBuilder,
    top_level: TopLevelOperations,
    /// The output being written, between steps.
    current: Option<OutputBuilder>,
    /// Most bytes the inputs, the row group and the output held together.
    peak: usize,
    /// Rows picked for a row group the output closed before gathering.
    picks: Option<Picks>,
    /// A row group whose usage counting stopped at a full count, with its
    /// rows as usage counts them, the rows to count and the row to resume
    /// at.
    pending: Option<(RecordBatch, Option<UsageRows>, Vec<bool>, usize)>,
}

/// What a merge hands back at each step.
pub enum Step {
    /// An output, closed.
    Output(Box<Output>),
    /// Usage counts set aside full, a contribution of their own.
    Usage(UsageCounts),
}

impl Merger {
    /// Opens `inputs`, the files of `signal` of `tenant` in the hour, or
    /// for L2 the day, starting at `hour`; outputs go under `root`. `keys` is the tenant's
    /// table of attribute keys of that signal before, which the merge
    /// counts into. Trace summaries and the service graph are built from
    /// spans only. With an output budget the merge takes the oldest
    /// inputs that fit it, [`Merger::taken`].
    ///
    /// # Errors
    /// A signal without L1 files, the oldest input past the budget, or
    /// store or Parquet failures opening an input.
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        root: &Path,
        tenant: TenantId,
        signal: Signal,
        hour: DateTime<Utc>,
        inputs: &[FileRecord],
        config: MergeConfig,
        keys: KeyStats,
    ) -> Result<Self, CompactError> {
        let schema = match signal {
            Signal::Traces => span_schema_v1(),
            Signal::Logs => log_schema_v1(),
            _ => return Err(CompactError::Merge("a signal without L1 files")),
        };
        let usage = usage_signal(signal).map(|s| (s, config.usage_max_bytes));
        let reading = if config.output_bytes == usize::MAX {
            Reading::Rows(config.read_batch_rows)
        } else {
            Reading::Bounded {
                share: config.output_bytes / 2,
                rows: config.read_batch_rows,
            }
        };
        let merge = Merge::open(store, inputs, &schema, reading, usage).await?;
        let hour_ns = hour
            .timestamp_nanos_opt()
            .ok_or(CompactError::Merge("hour outside the nanosecond range"))?;
        Ok(Self {
            signal,
            schema,
            merge,
            root: root.clone(),
            tenant,
            hour,
            config,
            done: false,
            keys,
            hour_ns,
            summaries: TraceSummaries::default(),
            graph: ServiceGraphBuilder::default(),
            top_level: TopLevelOperations::default(),
            current: None,
            peak: 0,
            picks: None,
            pending: None,
        })
    }

    /// How many of the inputs given the merge takes, the oldest.
    pub fn taken(&self) -> usize {
        self.merge.taken()
    }

    /// Most bytes the inputs, the row group and the output held together,
    /// as the merge counts them: what is loaded and encoded, and what
    /// reading a chunk may hold at most while it is read.
    pub fn peak_bytes(&self) -> usize {
        self.peak
    }

    /// Most bytes of usage counts the merge held at once, a full count
    /// being handed out included.
    pub fn usage_peak_bytes(&self) -> usize {
        self.merge.usage_peak
    }

    /// The `attribute_keys` table with every row merged so far counted
    /// in; taken once the inputs are spent.
    pub fn take_keys(&mut self) -> KeyStats {
        std::mem::take(&mut self.keys)
    }

    /// The pass's trace summaries, including which cap skipped them.
    pub fn take_trace_summaries(&mut self) -> TraceSummaries {
        std::mem::take(&mut self.summaries)
    }

    /// The pass's top-level operations, its children resolved against
    /// every span of the pass, including which cap skipped them.
    pub fn take_top_level_operations(&mut self) -> TopLevelOperations {
        let mut operations = std::mem::take(&mut self.top_level);
        operations.finish();
        operations
    }

    /// The pass's service graph builder, including which cap skipped it.
    pub fn take_service_graph(&mut self) -> ServiceGraphBuilder {
        std::mem::take(&mut self.graph)
    }

    /// Usage of the inputs not counted before, once they are spent; `None`
    /// when every input was counted.
    pub fn take_usage(&mut self) -> Option<UsageCounts> {
        self.merge.usage.take()
    }

    /// The next step: an output, closed at its target size, before its
    /// memory would pass the output budget, or at the end of the inputs;
    /// or usage counts set aside full while merging. `None` once the inputs
    /// are spent and every output handed back.
    ///
    /// A row group takes at most a sixteenth of the budget, by the exact
    /// size of its rows, and an output closes before one when what the
    /// inputs may hold, what the output holds and three times the row
    /// group, gathered, encoded and appended, would not fit.
    ///
    /// # Errors
    /// Store, Parquet or Arrow failures, or an input whose columns are not
    /// the signal's schema.
    pub async fn next_step(&mut self) -> Result<Option<Step>, CompactError> {
        if self.done {
            return Ok(self
                .current
                .take()
                .filter(|c| c.rows > 0)
                .map(|c| c.finish(self.tenant))
                .transpose()?
                .map(|o| Step::Output(Box::new(o))));
        }
        let group_bytes = self.config.output_bytes / 16;
        let columns = self.schema.fields().len();
        loop {
            if self.current.is_none() {
                self.current = Some(self.new_output()?);
            }
            let output = self.current.as_ref().map_or(0, OutputBuilder::memory);
            let (group, rows, take, from) = if let Some(pending) = self.pending.take() {
                pending
            } else {
                let picks = if let Some(picks) = self.picks.take() {
                    picks
                } else {
                    let picked = self
                        .merge
                        .pick(&self.schema, self.config.row_group_rows, group_bytes)
                        .await?;
                    let decoding = self.merge.take_decode_peak();
                    self.note(decoding.saturating_add(output));
                    let Some(picks) = picked else {
                        self.done = true;
                        return self.close();
                    };
                    picks
                };
                let gathered = picks
                    .bytes
                    .saturating_add(GROUP_COLUMN_BYTES.saturating_mul(columns));
                let overhead = self
                    .current
                    .as_ref()
                    .map_or(0, |c| c.row_group_overhead(picks.rows(), picks.bytes / 2));
                let room = self
                    .merge
                    .reserved()
                    .saturating_add(output)
                    .saturating_add(gathered.saturating_mul(3))
                    .saturating_add(overhead);
                if room > self.config.output_bytes {
                    if self.current.as_ref().is_some_and(|c| c.rows > 0) {
                        self.picks = Some(picks);
                        return self.close();
                    }
                    return Err(CompactError::OverBudget {
                        path: self.merge.path_of(&picks),
                        floor: gathered,
                    });
                }
                let (group, take) = self.merge.gather(&self.schema, &picks)?;
                drop(picks);
                self.absorb_derived(&group)?;
                let rows = self.merge.usage_rows(&group, &take)?;
                (group, rows, take, 0)
            };
            self.note(
                self.merge
                    .held_bytes()
                    .saturating_add(output)
                    .saturating_add(group.get_array_memory_size())
                    .saturating_add(rows.as_ref().map_or(0, UsageRows::memory_size)),
            );
            if let Some(usage) = &rows {
                if let Some((full, next)) = self.merge.count_usage(usage, from, &take)? {
                    self.pending = Some((group, rows, take, next));
                    return Ok(Some(Step::Usage(full)));
                }
            }
            drop(rows);
            let current = self
                .current
                .as_mut()
                .ok_or(CompactError::Merge("no output being written"))?;
            current.write(&group)?;
            drop(group);
            let full = current.bytes_written() >= self.config.target_bytes;
            let written = current.memory();
            self.note(self.merge.held_bytes().saturating_add(written));
            if full {
                return self.close();
            }
        }
    }

    fn note(&mut self, held: usize) {
        self.peak = self.peak.max(held);
    }

    /// Counts `group` into the keys, and for spans the summaries and the
    /// service graph, at L1.
    fn absorb_derived(&mut self, group: &RecordBatch) -> Result<(), CompactError> {
        if self.config.level != Level::L1 {
            return Ok(());
        }
        self.keys
            .absorb(group, self.hour_ns, self.config.max_new_keys)?;
        if self.signal == Signal::Traces {
            self.summaries.absorb(
                group,
                self.config.trace_summary_max_traces,
                self.config.trace_summary_max_bytes,
            )?;
            self.graph.absorb(
                group,
                self.config.service_graph_max_spans,
                self.config.service_graph_max_bytes,
            )?;
            self.top_level.absorb(
                group,
                self.config.top_level_operations_max_operations,
                self.config.top_level_operations_max_bytes,
            )?;
        }
        Ok(())
    }

    /// The output being written, finished; `None` when it holds no row.
    fn close(&mut self) -> Result<Option<Step>, CompactError> {
        match self.current.take() {
            Some(current) if current.rows > 0 => {
                Ok(Some(Step::Output(Box::new(current.finish(self.tenant)?))))
            }
            _ => Ok(None),
        }
    }

    fn new_output(&self) -> Result<OutputBuilder, CompactError> {
        let id = FileId::new();
        let path = match self.config.level {
            Level::L2 => l2_path(&self.root, self.tenant, self.signal, self.hour, id),
            _ => l1_path(&self.root, self.tenant, self.signal, self.hour, id),
        };
        OutputBuilder::new(&self.schema, self.signal, id, path, &self.config)
    }
}

/// `{root}/t/{tenant}/{traces|logs}/l1/dt=YYYY-MM-DD/hr=HH/{id}.parquet`
pub fn l1_path(
    root: &Path,
    tenant: TenantId,
    signal: Signal,
    hour: DateTime<Utc>,
    id: FileId,
) -> Path {
    let kind = format!("{}/l1", signal.path_name());
    partitioned_path(root, tenant, &kind, hour, id)
}

/// `{root}/t/{tenant}/{traces|logs}/l2/dt=YYYY-MM-DD/{id}.parquet`
pub fn l2_path(
    root: &Path,
    tenant: TenantId,
    signal: Signal,
    day: DateTime<Utc>,
    id: FileId,
) -> Path {
    let relative = format!(
        "t/{}/{}/l2/dt={}/{}.parquet",
        tenant.0,
        signal.path_name(),
        day.format("%Y-%m-%d"),
        id.0
    );
    if root.as_ref().is_empty() {
        Path::from(relative)
    } else {
        Path::from(format!("{root}/{relative}"))
    }
}

/// `{root}/t/{tenant}/derived/{attribute_keys|log_attribute_keys}/dt=YYYY-MM-DD/hr=HH/{id}.parquet`,
/// the table of `keys`, partitioned by the hour of the pass that rebuilt
/// the table.
pub fn attribute_keys_path(
    root: &Path,
    tenant: TenantId,
    keys: Signal,
    hour: DateTime<Utc>,
    id: FileId,
) -> Path {
    partitioned_path(root, tenant, keys.path_name(), hour, id)
}

/// Path of a per-pass trace summary contribution.
pub fn trace_summary_path(root: &Path, tenant: TenantId, hour: DateTime<Utc>, id: FileId) -> Path {
    partitioned_path(root, tenant, Signal::TraceSummary.path_name(), hour, id)
}

/// Path of a per-pass top-level operations contribution.
pub fn top_level_operations_path(
    root: &Path,
    tenant: TenantId,
    hour: DateTime<Utc>,
    id: FileId,
) -> Path {
    partitioned_path(
        root,
        tenant,
        Signal::TopLevelOperations.path_name(),
        hour,
        id,
    )
}

/// Path of a per-pass service graph contribution.
pub fn service_graph_path(root: &Path, tenant: TenantId, hour: DateTime<Utc>, id: FileId) -> Path {
    partitioned_path(root, tenant, Signal::ServiceGraph1m.path_name(), hour, id)
}

pub(crate) fn partitioned_path(
    root: &Path,
    tenant: TenantId,
    kind: &str,
    hour: DateTime<Utc>,
    id: FileId,
) -> Path {
    let relative = format!(
        "t/{}/{kind}/dt={}/hr={:02}/{}.parquet",
        tenant.0,
        hour.format("%Y-%m-%d"),
        hour.format("%H"),
        id.0
    );
    if root.as_ref().is_empty() {
        Path::from(relative)
    } else {
        Path::from(format!("{root}/{relative}"))
    }
}
