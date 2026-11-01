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
    /// Most `(hour, service)` pairs the pass counts usage for; past it the
    /// pass fails rather than drop a count.
    pub usage_max_entries: usize,
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
        let usage = usage_signal(signal).map(|s| (s, config.usage_max_entries));
        let merge = Merge::open(store, inputs, &schema, config.read_batch_rows, usage).await?;
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
        })
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

    /// The next output, closed at the target size or at the end of the
    /// inputs; `None` once the inputs are spent. Only this output's bytes
    /// are held.
    ///
    /// # Errors
    /// Store, Parquet or Arrow failures, or an input whose columns are not
    /// the signal's schema.
    pub async fn next_output(&mut self) -> Result<Option<Output>, CompactError> {
        if self.done {
            return Ok(None);
        }
        let id = FileId::new();
        let path = match self.config.level {
            Level::L2 => l2_path(&self.root, self.tenant, self.signal, self.hour, id),
            _ => l1_path(&self.root, self.tenant, self.signal, self.hour, id),
        };
        let mut current = OutputBuilder::new(&self.schema, self.signal, id, path, &self.config)?;
        let derived = self.config.level == Level::L1;
        while let Some(group) = self
            .merge
            .next_group(&self.schema, self.config.row_group_rows)
            .await?
        {
            if derived {
                self.keys
                    .absorb(&group, self.hour_ns, self.config.max_new_keys)?;
            }
            if derived && self.signal == Signal::Traces {
                self.summaries.absorb(
                    &group,
                    self.config.trace_summary_max_traces,
                    self.config.trace_summary_max_bytes,
                )?;
                self.graph.absorb(
                    &group,
                    self.config.service_graph_max_spans,
                    self.config.service_graph_max_bytes,
                )?;
            }
            current.write(&group)?;
            if current.bytes_written() >= self.config.target_bytes
                || current.memory() >= self.config.output_bytes
            {
                return Ok(Some(current.finish(self.tenant)?));
            }
        }
        self.done = true;
        if current.rows == 0 {
            return Ok(None);
        }
        Ok(Some(current.finish(self.tenant)?))
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
    batch: RecordBatch,
    services: Vec<Service>,
    timestamps: Vec<i64>,
}

impl Slot {
    fn load(batch: RecordBatch, schema: &SchemaRef) -> Result<Self, CompactError> {
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
        Ok(Self {
            batch,
            services,
            timestamps,
        })
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

/// The batches of `file`, `batch_rows` rows at a time.
///
/// # Errors
/// Store or Parquet failure opening the file.
pub(crate) async fn batches(
    store: Arc<dyn ObjectStore>,
    file: &FileRecord,
    batch_rows: usize,
) -> Result<ParquetRecordBatchStream<ObjectReader>, CompactError> {
    let reader = ObjectReader {
        store,
        path: Path::from(file.path.as_str()),
        size: u64::try_from(file.size_bytes).unwrap_or(0),
    };
    Ok(ParquetRecordBatchStreamBuilder::new(reader)
        .await?
        .with_batch_size(batch_rows)
        .build()?)
}

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
    /// Usage of the inputs flagged in `uncounted`, with its cap.
    usage: Option<UsageCounts>,
    uncounted: Vec<bool>,
    usage_cap: usize,
}

impl Merge {
    async fn open(
        store: Arc<dyn ObjectStore>,
        files: &[FileRecord],
        schema: &SchemaRef,
        batch_rows: usize,
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
            usage_cap: usage.map_or(0, |(_, cap)| cap),
        };
        for file in files {
            let stream = batches(Arc::clone(&store), file, batch_rows).await?;
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
            let loaded = Slot::load(batch, schema)?;
            let slot = self.slots.len();
            next.slot = Some(slot);
            self.count_usage(input, &loaded.batch)?;
            self.slots.push(Some(loaded));
            row = 0;
        }
    }

    /// Counts `batch` of `input` into the usage when the input was not
    /// counted before.
    fn count_usage(&mut self, input: usize, batch: &RecordBatch) -> Result<(), CompactError> {
        let (Some(usage), Some(true)) = (self.usage.as_mut(), self.uncounted.get(input).copied())
        else {
            return Ok(());
        };
        usage.absorb(batch)?;
        if usage.len() > self.usage_cap {
            return Err(CompactError::Merge(
                "usage pairs over compactor.usage_max_entries",
            ));
        }
        Ok(())
    }

    /// The next output row group in sort order, or `None` when every
    /// input is spent.
    async fn next_group(
        &mut self,
        schema: &SchemaRef,
        rows: usize,
    ) -> Result<Option<RecordBatch>, CompactError> {
        let mut picked: Vec<(usize, usize)> = Vec::with_capacity(rows);
        while picked.len() < rows {
            let Some(Reverse((_, _, input, row, slot))) = self.heap.pop() else {
                break;
            };
            picked.push((slot, row));
            self.advance(input, row + 1, schema).await?;
        }
        if picked.is_empty() {
            return Ok(None);
        }
        let group = self.gather(schema, &picked)?;
        self.drop_spent();
        Ok(Some(group))
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
