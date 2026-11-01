//! The k-way merge of sorted L0 files of one signal, spans or log rows,
//! into sorted L1 files of that signal.
//!
//! Every input is sorted by `(service_name, timestamp)` with ties in log
//! order, so one row group per input is enough to produce the output in
//! that order: the heads of the inputs are kept in a heap keyed by
//! `(service, timestamp, input, row)`, the smallest goes to the output,
//! and an input whose row group is spent loads its next one. An output
//! file closes when it reaches the target size and is handed over before
//! the next one starts, so memory holds one decoded row group per input,
//! the output row group being gathered and the compressed bytes of the one
//! file being written. A null service sorts before every name, as the
//! inputs sorted it.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, SchemaRef, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use fs_catalog::{FileId, FileRecord, Level, Signal, TenantId};
use fs_schema::attribute_keys::KeyStats;
use fs_schema::logs::log_schema_v1;
use fs_schema::parquet::gather;
use fs_schema::service_graph::ServiceGraphBuilder;
use fs_schema::spans::{col, span_schema_v1};
use fs_schema::trace_summary::TraceSummaries;
use fs_schema::usage::{UsageCounts, UsageSignal};
use futures_util::future::BoxFuture;
use futures_util::{FutureExt, StreamExt, TryFutureExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStream};
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

use crate::CompactError;
use crate::output::{Output, OutputBuilder};

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
    /// The output being written, between steps.
    current: Option<OutputBuilder>,
    /// Most bytes the inputs and the output held together, counted before
    /// each row group.
    peak: usize,
    /// A row group whose usage counting stopped at a full count, with the
    /// rows to count and the row to resume at.
    pending: Option<(RecordBatch, Vec<bool>, usize)>,
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
    /// spans only.
    ///
    /// # Errors
    /// A signal without L1 files, or store or Parquet failures opening an
    /// input.
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
        // The inputs hold a quarter of the output budget between them.
        let batch_bytes = (config.output_bytes / 4) / inputs.len().max(1);
        let reading = (config.read_batch_rows, batch_bytes);
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
            current: None,
            peak: 0,
            pending: None,
        })
    }

    /// Most bytes the inputs and the output held together before a row
    /// group, as the merge counts them.
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
    /// A row group holds at most a quarter of the budget, and an output
    /// closes before the next one when what it holds, what the inputs hold
    /// and twice the row group, gathered and encoded, would not fit.
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
        let group_bytes = self.config.output_bytes / 4;
        loop {
            if self.current.is_none() {
                self.current = Some(self.new_output()?);
            }
            let output = self.current.as_ref().map_or(0, OutputBuilder::memory);
            let (group, take, from) = if let Some(pending) = self.pending.take() {
                pending
            } else {
                let held = self.merge.held_bytes();
                self.peak = self.peak.max(held.saturating_add(output));
                let room = held
                    .saturating_add(output)
                    .saturating_add(group_bytes.saturating_mul(2));
                if self.current.as_ref().is_some_and(|c| c.rows > 0)
                    && room > self.config.output_bytes
                {
                    return self.close();
                }
                let Some((group, take)) = self
                    .merge
                    .next_group(&self.schema, self.config.row_group_rows, group_bytes)
                    .await?
                else {
                    self.done = true;
                    return self.close();
                };
                self.absorb_derived(&group)?;
                (group, take, 0)
            };
            if let Some((full, next)) = self.merge.count_usage(&group, from, &take)? {
                self.pending = Some((group, take, next));
                return Ok(Some(Step::Usage(full)));
            }
            let gathered = group.get_array_memory_size();
            self.peak = self.peak.max(
                self.merge
                    .held_bytes()
                    .saturating_add(output)
                    .saturating_add(gathered),
            );
            let current = self
                .current
                .as_mut()
                .ok_or(CompactError::Merge("no output being written"))?;
            current.write(&group)?;
            let full = current.bytes_written() >= self.config.target_bytes;
            drop(group);
            if full {
                return self.close();
            }
        }
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

/// A service name as a sort key: `None` is a null, before every name.
type Service = Option<Arc<str>>;

/// A loaded row group of one input with its sort keys.
struct Slot {
    input: usize,
    batch: RecordBatch,
    services: Vec<Service>,
    timestamps: Vec<i64>,
    /// What the slot holds: the batch and its sort keys.
    bytes: usize,
}

impl Slot {
    fn load(input: usize, batch: RecordBatch, schema: &SchemaRef) -> Result<Self, CompactError> {
        let batch = coerce(batch, schema)?;
        let service = arrow::compute::cast(
            batch.column(schema.index_of(col::SERVICE_NAME)?),
            &DataType::Utf8,
        )?;
        let mut services = Vec::with_capacity(batch.num_rows());
        let mut last: Option<Arc<str>> = None;
        for value in service.as_string::<i32>() {
            let shared = value.map(|value| match &last {
                Some(l) if l.as_ref() == value => Arc::clone(l),
                _ => {
                    let s: Arc<str> = Arc::from(value);
                    last = Some(Arc::clone(&s));
                    s
                }
            });
            services.push(shared);
        }
        let timestamps = batch
            .column(schema.index_of(col::TIMESTAMP)?)
            .as_primitive::<TimestampNanosecondType>()
            .values()
            .to_vec();
        let bytes = batch.get_array_memory_size()
            + services.capacity() * std::mem::size_of::<Service>()
            + timestamps.capacity() * std::mem::size_of::<i64>();
        Ok(Self {
            input,
            batch,
            services,
            timestamps,
            bytes,
        })
    }

    /// What one row holds, rounded up.
    fn row_bytes(&self) -> usize {
        self.bytes.div_ceil(self.batch.num_rows().max(1))
    }
}

/// Casts columns whose type differs from the declared schema, as a file
/// read back without its Arrow metadata would need.
fn coerce(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, CompactError> {
    if batch.schema().fields() == schema.fields() {
        return Ok(batch);
    }
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let index = batch.schema().index_of(field.name())?;
            let column = batch.column(index);
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                Ok(arrow::compute::cast(column, field.data_type())?)
            }
        })
        .collect::<Result<Vec<_>, CompactError>>()?;
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

/// Reads one object of known size through ranged gets.
pub(crate) struct ObjectReader {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) path: Path,
    pub(crate) size: u64,
}

fn to_parquet_err(e: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(e))
}

impl AsyncFileReader for ObjectReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        self.store
            .get_range(&self.path, range)
            .map_err(to_parquet_err)
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>, ParquetError>> {
        async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(to_parquet_err)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>, ParquetError>> {
        async move {
            let size = self.size;
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_and_finish(self, size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

/// The batches of `file`, at most `batch_rows` rows and, when
/// `batch_bytes` bounds them, about `batch_bytes` of decoded memory at a
/// time, one row at least. What a batch decodes to is measured on the
/// first rows of the file, a batch of one row and one of
/// [`PROBE_ROWS`], which tell what any batch costs apart from what each
/// row adds; the footer's sizes are of encoded pages and say nothing of
/// what a dictionary expands to.
///
/// # Errors
/// Store or Parquet failure opening or probing the file.
pub(crate) async fn batches(
    store: Arc<dyn ObjectStore>,
    file: &FileRecord,
    batch_rows: usize,
    batch_bytes: usize,
) -> Result<ParquetRecordBatchStream<ObjectReader>, CompactError> {
    let reader = || ObjectReader {
        store: Arc::clone(&store),
        path: Path::from(file.path.as_str()),
        size: u64::try_from(file.size_bytes).unwrap_or(0),
    };
    let mut rows = batch_rows.max(1);
    if batch_bytes < usize::MAX / 8 {
        let probe = |size: usize| {
            let reader = reader();
            async move {
                let mut stream = ParquetRecordBatchStreamBuilder::new(reader)
                    .await?
                    .with_batch_size(size)
                    .with_row_groups(vec![0])
                    .build()?;
                let first = stream.next().await.transpose()?;
                Ok::<_, CompactError>(first.map(|b| (b.num_rows(), b.get_array_memory_size())))
            }
        };
        if let (Some((1, one)), Some((n, many))) = (probe(1).await?, probe(PROBE_ROWS).await?) {
            let per_row = many
                .saturating_sub(one)
                .div_ceil(n.saturating_sub(1).max(1))
                .max(1);
            let fixed = one.saturating_sub(per_row);
            let room = batch_bytes.saturating_sub(fixed);
            // A quarter more than measured: later rows may be wider.
            rows = (room * 4 / (per_row * 5)).clamp(1, rows);
        }
    }
    Ok(ParquetRecordBatchStreamBuilder::new(reader())
        .await?
        .with_batch_size(rows)
        .build()?)
}

/// Rows of the larger batch read to measure what a row of a file
/// decodes to.
const PROBE_ROWS: usize = 16;

/// The head of one input in the heap: its sort key and where the row is.
type Head = Reverse<(Service, i64, usize, usize, usize)>;

struct Input {
    stream: ParquetRecordBatchStream<ObjectReader>,
    /// Slot of the row group being consumed, if any.
    slot: Option<usize>,
}

struct Merge {
    inputs: Vec<Input>,
    slots: Vec<Option<Slot>>,
    heap: BinaryHeap<Head>,
    /// Usage of the inputs flagged in `uncounted`, within its budget.
    usage: Option<UsageCounts>,
    uncounted: Vec<bool>,
    usage_budget: usize,
    usage_peak: usize,
}

impl Merge {
    async fn open(
        store: Arc<dyn ObjectStore>,
        files: &[FileRecord],
        schema: &SchemaRef,
        (batch_rows, batch_bytes): (usize, usize),
        usage: Option<(UsageSignal, usize)>,
    ) -> Result<Self, CompactError> {
        let uncounted: Vec<bool> = files.iter().map(|f| !f.usage_accounted).collect();
        let counts = usage
            .filter(|_| uncounted.contains(&true))
            .map(|(signal, _)| UsageCounts::new(signal));
        let mut merge = Self {
            inputs: Vec::with_capacity(files.len()),
            slots: Vec::new(),
            heap: BinaryHeap::new(),
            usage: counts,
            uncounted,
            usage_budget: usage.map_or(0, |(_, budget)| budget),
            usage_peak: 0,
        };
        for file in files {
            let stream = batches(Arc::clone(&store), file, batch_rows, batch_bytes).await?;
            merge.inputs.push(Input { stream, slot: None });
        }
        for input in 0..merge.inputs.len() {
            merge.advance(input, 0, schema).await?;
        }
        Ok(merge)
    }

    /// Makes `row` of `input`'s current slot the head, loading the next
    /// row group when the slot is spent and dropping the spent one.
    async fn advance(
        &mut self,
        input: usize,
        row: usize,
        schema: &SchemaRef,
    ) -> Result<(), CompactError> {
        let mut row = row;
        loop {
            if let Some(slot) = self.inputs.get(input).and_then(|i| i.slot) {
                if let Some(loaded) = self.slots.get(slot).and_then(Option::as_ref) {
                    if let (Some(service), Some(ts)) =
                        (loaded.services.get(row), loaded.timestamps.get(row))
                    {
                        self.heap
                            .push(Reverse((service.clone(), *ts, input, row, slot)));
                        return Ok(());
                    }
                }
            }
            let Some(next) = self.inputs.get_mut(input) else {
                return Ok(());
            };
            let Some(batch) = next.stream.next().await.transpose()? else {
                next.slot = None;
                return Ok(());
            };
            if batch.num_rows() == 0 {
                continue;
            }
            let loaded = Slot::load(input, batch, schema)?;
            let slot = self.slots.len();
            next.slot = Some(slot);
            self.slots.push(Some(loaded));
            row = 0;
        }
    }

    /// What the loaded slots hold.
    fn held_bytes(&self) -> usize {
        self.slots.iter().flatten().map(|s| s.bytes).sum()
    }

    /// Counts the rows `take` flags of `group`, from `from`, into the
    /// usage; the counts set aside full and the row to resume at when the
    /// budget stops it.
    fn count_usage(
        &mut self,
        group: &RecordBatch,
        from: usize,
        take: &[bool],
    ) -> Result<Option<(UsageCounts, usize)>, CompactError> {
        let Some(usage) = self.usage.as_mut().filter(|_| take.contains(&true)) else {
            return Ok(None);
        };
        let full = usage.absorb_from(group, from, self.usage_budget, Some(take))?;
        let held =
            usage.estimated_bytes() + full.as_ref().map_or(0, |(full, _)| full.estimated_bytes());
        self.usage_peak = self.usage_peak.max(held);
        Ok(full)
    }

    /// The next output row group in sort order, at most `rows` rows and,
    /// by the bytes per row of the slots it takes from, about `max_bytes`,
    /// one row at least, with which of its rows come from inputs whose
    /// usage was not counted; `None` when every input is spent.
    async fn next_group(
        &mut self,
        schema: &SchemaRef,
        rows: usize,
        max_bytes: usize,
    ) -> Result<Option<(RecordBatch, Vec<bool>)>, CompactError> {
        let mut picked: Vec<(usize, usize)> = Vec::new();
        let mut bytes = 0usize;
        while picked.len() < rows && (picked.is_empty() || bytes < max_bytes) {
            let Some(Reverse((_, _, input, row, slot))) = self.heap.pop() else {
                break;
            };
            bytes = bytes.saturating_add(
                self.slots
                    .get(slot)
                    .and_then(Option::as_ref)
                    .map_or(0, Slot::row_bytes),
            );
            picked.push((slot, row));
            self.advance(input, row + 1, schema).await?;
        }
        if picked.is_empty() {
            return Ok(None);
        }
        let take = picked
            .iter()
            .map(|(slot, _)| {
                self.slots
                    .get(*slot)
                    .and_then(Option::as_ref)
                    .and_then(|s| self.uncounted.get(s.input))
                    .copied()
                    .unwrap_or(false)
            })
            .collect();
        let group = self.gather(schema, &picked)?;
        self.drop_spent();
        Ok(Some((group, take)))
    }

    /// Gathers `(slot, row)` picks into one batch, remapping slots to the
    /// contiguous list `gather` wants.
    fn gather(
        &self,
        schema: &SchemaRef,
        picked: &[(usize, usize)],
    ) -> Result<RecordBatch, CompactError> {
        let used: BTreeSet<usize> = picked.iter().map(|(slot, _)| *slot).collect();
        let order: Vec<usize> = used.into_iter().collect();
        let batches = order
            .iter()
            .map(|slot| {
                self.slots
                    .get(*slot)
                    .and_then(Option::as_ref)
                    .map(|s| s.batch.clone())
                    .ok_or(CompactError::Merge("a pick names a dropped row group"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let indices = picked
            .iter()
            .map(|(slot, row)| {
                order
                    .binary_search(slot)
                    .map(|position| (position, *row))
                    .map_err(|_| CompactError::Merge("a pick names an unknown row group"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(gather(schema, &batches, &indices)?)
    }

    /// Frees row groups no input is reading any more.
    fn drop_spent(&mut self) {
        let current: BTreeSet<usize> = self.inputs.iter().filter_map(|i| i.slot).collect();
        for (index, slot) in self.slots.iter_mut().enumerate() {
            if !current.contains(&index) {
                *slot = None;
            }
        }
    }
}
