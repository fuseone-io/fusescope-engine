//! The k-way merge of sorted L0 files into sorted L1 files.
//!
//! Every input is sorted by `(service_name, timestamp)` with ties in log
//! order, so one row group per input is enough to produce the output in
//! that order: the heads of the inputs are kept in a heap keyed by
//! `(service, timestamp, input, row)`, the smallest goes to the output,
//! and an input whose row group is spent loads its next one. An output
//! file closes when it reaches the target size and the next one starts, so
//! memory holds one decoded row group per input, the output row group
//! being gathered and the compressed bytes of the file being written.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, SchemaRef, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use fs_catalog::{FileId, FileRecord, Level, NewFile, Signal, TenantId, TimeRange};
use fs_index::TraceIndex;
use fs_schema::parquet::{gather, writer_properties};
use fs_schema::spans::{TRACE_ID_BYTES, col, span_schema_v1, trace_ids};
use futures_util::future::BoxFuture;
use futures_util::{FutureExt, StreamExt, TryFutureExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStream};
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder};
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

use crate::CompactError;

/// One L1 file ready to upload with its catalog row and sidecar.
pub struct Output {
    /// Catalog row.
    pub file: NewFile,
    /// Parquet bytes.
    pub bytes: Bytes,
    /// Encoded `trace_id` sidecar.
    pub index: Bytes,
    /// Where the sidecar goes.
    pub index_path: String,
}

/// Bounds of one merge.
#[derive(Debug, Clone, Copy)]
pub struct MergeConfig {
    /// Rows per output row group.
    pub row_group_rows: usize,
    /// An output closes once its bytes reach this.
    pub target_bytes: usize,
}

/// Merges `inputs`, all L0 files of `tenant` in the hour starting at
/// `hour`, into L1 outputs under `root`.
///
/// # Errors
/// Store, Parquet or Arrow failures, or an input whose columns are not
/// the span schema.
pub async fn merge_to_l1(
    store: Arc<dyn ObjectStore>,
    root: &Path,
    tenant: TenantId,
    hour: DateTime<Utc>,
    inputs: &[FileRecord],
    config: MergeConfig,
) -> Result<Vec<Output>, CompactError> {
    let schema = span_schema_v1();
    let mut merge = Merge::open(store, inputs, &schema, config.row_group_rows).await?;
    let mut outputs = Vec::new();
    let mut current = OutputBuilder::new(&schema, config.row_group_rows)?;
    while let Some(group) = merge.next_group(&schema, config.row_group_rows).await? {
        current.write(&group)?;
        if current.bytes_written() >= config.target_bytes {
            outputs.push(current.finish(root, tenant, hour)?);
            current = OutputBuilder::new(&schema, config.row_group_rows)?;
        }
    }
    if current.rows > 0 {
        outputs.push(current.finish(root, tenant, hour)?);
    }
    Ok(outputs)
}

/// `{root}/t/{tenant}/traces/l1/dt=YYYY-MM-DD/hr=HH/{id}.parquet`
pub fn l1_path(root: &Path, tenant: TenantId, hour: DateTime<Utc>, id: FileId) -> Path {
    let relative = format!(
        "t/{}/traces/l1/dt={}/hr={:02}/{}.parquet",
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

/// A loaded row group of one input with its sort keys.
struct Slot {
    batch: RecordBatch,
    services: Vec<Arc<str>>,
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
            let value = value.unwrap_or("");
            let shared = match &last {
                Some(l) if l.as_ref() == value => Arc::clone(l),
                _ => {
                    let s: Arc<str> = Arc::from(value);
                    last = Some(Arc::clone(&s));
                    s
                }
            };
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
struct ObjectReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    size: u64,
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

/// The head of one input in the heap: its sort key and where the row is.
type Head = Reverse<(Arc<str>, i64, usize, usize, usize)>;

struct Input {
    stream: ParquetRecordBatchStream<ObjectReader>,
    /// Slot of the row group being consumed, if any.
    slot: Option<usize>,
}

struct Merge {
    inputs: Vec<Input>,
    slots: Vec<Option<Slot>>,
    heap: BinaryHeap<Head>,
}

impl Merge {
    async fn open(
        store: Arc<dyn ObjectStore>,
        files: &[FileRecord],
        schema: &SchemaRef,
        batch_rows: usize,
    ) -> Result<Self, CompactError> {
        let mut merge = Self {
            inputs: Vec::with_capacity(files.len()),
            slots: Vec::new(),
            heap: BinaryHeap::new(),
        };
        for file in files {
            let reader = ObjectReader {
                store: Arc::clone(&store),
                path: Path::from(file.path.as_str()),
                size: u64::try_from(file.size_bytes).unwrap_or(0),
            };
            let stream = ParquetRecordBatchStreamBuilder::new(reader)
                .await?
                .with_batch_size(batch_rows)
                .build()?;
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
                            .push(Reverse((Arc::clone(service), *ts, input, row, slot)));
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
            self.slots.push(Some(loaded));
            next.slot = Some(slot);
            row = 0;
        }
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

/// One L1 file being written.
struct OutputBuilder {
    writer: ArrowWriter<Vec<u8>>,
    rows: usize,
    ts_min: i64,
    ts_max: i64,
    services: BTreeSet<String>,
    traces: Vec<[u8; TRACE_ID_BYTES]>,
}

impl OutputBuilder {
    fn new(schema: &SchemaRef, row_group_rows: usize) -> Result<Self, CompactError> {
        let writer = ArrowWriter::try_new(
            Vec::new(),
            Arc::clone(schema),
            Some(writer_properties(row_group_rows)?),
        )?;
        Ok(Self {
            writer,
            rows: 0,
            ts_min: i64::MAX,
            ts_max: i64::MIN,
            services: BTreeSet::new(),
            traces: Vec::new(),
        })
    }

    fn write(&mut self, group: &RecordBatch) -> Result<(), CompactError> {
        let schema = group.schema();
        let ts = group
            .column(schema.index_of(col::TIMESTAMP)?)
            .as_primitive::<TimestampNanosecondType>();
        self.ts_min = self
            .ts_min
            .min(arrow::compute::min(ts).unwrap_or(self.ts_min));
        self.ts_max = self
            .ts_max
            .max(arrow::compute::max(ts).unwrap_or(self.ts_max));
        let service = arrow::compute::cast(
            group.column(schema.index_of(col::SERVICE_NAME)?),
            &DataType::Utf8,
        )?;
        for value in service.as_string::<i32>().iter().flatten() {
            if !self.services.contains(value) {
                self.services.insert(value.to_owned());
            }
        }
        if let Some(ids) = trace_ids(group) {
            self.traces.extend(ids);
        }
        self.writer.write(group)?;
        self.writer.flush()?;
        self.rows += group.num_rows();
        Ok(())
    }

    fn bytes_written(&self) -> usize {
        self.writer.bytes_written()
    }

    fn finish(
        mut self,
        root: &Path,
        tenant: TenantId,
        hour: DateTime<Utc>,
    ) -> Result<Output, CompactError> {
        let bytes = Bytes::from(self.writer.into_inner()?);
        let id = FileId::new();
        let path = l1_path(root, tenant, hour, id);
        let index_path = fs_index::sidecar_path(path.as_ref())
            .ok_or(CompactError::Merge("output path has no sidecar"))?;
        self.traces.sort_unstable();
        self.traces.dedup();
        let index = TraceIndex::new(
            vec![path.to_string()],
            self.traces.iter().map(|t| (*t, 0u32)),
        )?;
        let file = NewFile {
            id,
            tenant_id: tenant,
            signal: Signal::Traces,
            level: Level::L1,
            path: path.to_string(),
            size_bytes: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
            row_count: i64::try_from(self.rows).unwrap_or(i64::MAX),
            range: TimeRange {
                start: Utc.timestamp_nanos(self.ts_min),
                end: Utc.timestamp_nanos(self.ts_max),
            },
            services: self.services.into_iter().collect(),
            summary: None,
        };
        Ok(Output {
            file,
            bytes,
            index: Bytes::from(index.encode()),
            index_path,
        })
    }
}
