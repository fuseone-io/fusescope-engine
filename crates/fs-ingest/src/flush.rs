//! Writes a tenant buffer as one sorted Parquet L0 file, one row group at a
//! time.
//!
//! Every input batch is sorted on its own by `(service_name, timestamp,
//! row)`, so equal keys keep their log order and the sort is a total order.
//! The sorted inputs are then merged, k ways, into row groups of a fixed
//! number of rows; each row group is gathered, written and flushed before
//! the next is built, so the writer holds one row group of pages at a time
//! and the rows it has written live only in the compressed output. Ties
//! between inputs break by input order, which is log order, so a file
//! written from chunks and a file written from the same rows replayed one
//! request at a time are the same bytes.

use std::collections::{BTreeMap, BinaryHeap};
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use fs_catalog::{FileId, Level, NewFile, Signal, TenantId, TimeRange};
use fs_index::TraceIndex;
use fs_schema::logs::log_schema_v1;
use fs_schema::spans::{col, span_schema_v1};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::IngestError;
use crate::buffer::{HotBatch, Stream, trace_set};
use crate::record::{FileLayout, RowSignal};

pub use fs_schema::parquet::{DEFAULT_ROW_GROUP_ROWS, MAX_ROW_GROUP_ROWS};

/// Refuses a row group size outside `1..=MAX_ROW_GROUP_ROWS`.
///
/// # Errors
/// Zero, or more than [`MAX_ROW_GROUP_ROWS`].
pub fn check_row_group_rows(rows: usize) -> Result<(), IngestError> {
    Ok(fs_schema::parquet::check_row_group_rows(rows)?)
}

/// A Parquet file ready to upload plus its catalog entry.
pub struct Prepared {
    /// Catalog row.
    pub file: NewFile,
    /// File bytes.
    pub bytes: Bytes,
    /// The `trace_id` sidecar listing the traces of a span file, encoded,
    /// and where it goes, derived from the file's path. Log files have none.
    pub index: Option<(String, Bytes)>,
    /// The rows as written, one batch per row group in file order, each with
    /// its trace ids: what the hot window keeps for the file after
    /// registration.
    pub rows: Vec<HotBatch>,
    /// Time spent sorting each input batch.
    pub concat: std::time::Duration,
    /// Time spent merging the sorted inputs into row groups.
    pub sort: std::time::Duration,
    /// Time spent encoding Parquet.
    pub encode: std::time::Duration,
    /// Most bytes the writer estimated holding after a row group was written
    /// and before it was flushed: its pages, dictionaries and bloom filters
    /// for that group. It leaves out the compressed output, the sorted
    /// inputs, the merge keys and the row groups kept for the hot window,
    /// which the flush lease covers on top of it.
    pub writer_peak: usize,
}

/// [`prepare_stream`] for span rows.
///
/// # Errors
/// See [`prepare_stream`].
pub fn prepare(
    tenant: TenantId,
    root: &Path,
    id: FileId,
    batches: &[RecordBatch],
    layout: FileLayout,
) -> Result<Prepared, IngestError> {
    prepare_stream(Stream::traces(tenant), root, id, batches, layout)
}

/// Sorts each of `batches`, rows of `stream`, merges them into row groups
/// of the layout's size and encodes each as it completes.
///
/// CPU bound: callers run it on a blocking thread.
///
/// # Errors
/// Arrow or Parquet failures, or a layout this engine does not write.
pub fn prepare_stream(
    stream: Stream,
    root: &Path,
    id: FileId,
    batches: &[RecordBatch],
    layout: FileLayout,
) -> Result<Prepared, IngestError> {
    let schema = schema_of(stream.signal);
    check_layout(&layout, stream.signal)?;
    let row_group_rows = layout.row_group_rows;

    let sort_started = std::time::Instant::now();
    let inputs: Vec<RecordBatch> = batches
        .iter()
        .filter(|b| b.num_rows() > 0)
        .map(sort_batch)
        .collect::<Result<_, _>>()?;
    let keys = MergeKeys::new(&inputs)?;
    let concat = sort_started.elapsed();

    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(
        &mut out,
        Arc::clone(&schema),
        Some(writer_properties(layout, stream.signal)?),
    )?;

    let mut merge = std::time::Duration::ZERO;
    let mut encode = std::time::Duration::ZERO;
    let mut writer_peak = 0;
    let mut rows = Vec::new();
    let (mut ts_min, mut ts_max) = (i64::MAX, i64::MIN);
    let mut heads = keys.heads();
    loop {
        let merge_started = std::time::Instant::now();
        let indices = keys.next_group(&mut heads, row_group_rows);
        if indices.is_empty() {
            break;
        }
        let group = gather(&schema, &inputs, &indices)?;
        merge += merge_started.elapsed();

        let ts = group
            .column(schema.index_of(col::TIMESTAMP)?)
            .as_primitive::<TimestampNanosecondType>();
        ts_min = ts_min.min(arrow::compute::min(ts).unwrap_or(ts_min));
        ts_max = ts_max.max(arrow::compute::max(ts).unwrap_or(ts_max));

        let encode_started = std::time::Instant::now();
        writer.write(&group)?;
        writer_peak = writer_peak.max(writer.memory_size());
        writer.flush()?;
        encode += encode_started.elapsed();

        let traces = trace_set(&group);
        rows.push(HotBatch {
            batch: group,
            traces,
        });
    }
    let encode_started = std::time::Instant::now();
    writer.close()?;
    encode += encode_started.elapsed();
    let bytes = Bytes::from(out);

    let row_count: usize = rows.iter().map(|r| r.batch.num_rows()).sum();
    if row_count == 0 {
        (ts_min, ts_max) = (0, 0);
    }
    let start = Utc.timestamp_nanos(ts_min);
    let path = l0_path_of(root, stream, start, id);
    let index = match stream.signal {
        RowSignal::Traces => {
            let index_path = fs_index::sidecar_path(path.as_ref())
                .ok_or(fs_index::IndexError::BadBody("file path has no sidecar"))?;
            let index = Bytes::from(trace_index(path.as_ref(), &rows)?.encode());
            Some((index_path, index))
        }
        RowSignal::Logs => None,
    };
    let file = NewFile {
        id,
        origin: id,
        tenant_id: stream.tenant,
        signal: catalog_signal(stream.signal),
        level: Level::L0,
        path: path.to_string(),
        size_bytes: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
        row_count: i64::try_from(row_count).unwrap_or(i64::MAX),
        range: TimeRange {
            start,
            end: Utc.timestamp_nanos(ts_max),
        },
        services: keys.services,
        summary: None,
        content_sha256: Some(crate::sha256::digest(&bytes)),
    };
    Ok(Prepared {
        file,
        bytes,
        index,
        rows,
        concat,
        sort: merge,
        encode,
        writer_peak,
    })
}

/// The sidecar of one file: every trace of every row group, file index 0.
fn trace_index(path: &str, rows: &[HotBatch]) -> Result<TraceIndex, IngestError> {
    let pairs = rows
        .iter()
        .filter_map(|r| r.traces.as_ref())
        .flat_map(|set| set.iter().map(|t| (*t, 0u32)));
    Ok(TraceIndex::new(vec![path.to_owned()], pairs)?)
}

/// The writer of `layout` for rows of `signal`; one arm per version in
/// [`FileLayout::written_versions`], so a new version needs its own.
///
/// # Errors
/// A layout this engine does not write for `signal`.
fn writer_properties(
    layout: FileLayout,
    signal: RowSignal,
) -> Result<WriterProperties, IngestError> {
    let rows = layout.row_group_rows;
    Ok(match (signal, layout.version) {
        (RowSignal::Traces, 3) => fs_schema::parquet::writer_properties_layout_v3(rows)?,
        (RowSignal::Traces, 4) => fs_schema::parquet::writer_properties(rows)?,
        (RowSignal::Logs, 4) => fs_schema::parquet::log_writer_properties(rows)?,
        (_, version) => return Err(unwritten_layout(version, signal)),
    })
}

fn unwritten_layout(version: u32, signal: RowSignal) -> IngestError {
    IngestError::Encode(format!(
        "file layout version {version} is not one this engine writes for {signal:?} ({:?})",
        FileLayout::written_versions(signal)
    ))
}

/// The Arrow schema of rows of `signal`.
pub fn schema_of(signal: RowSignal) -> arrow::datatypes::SchemaRef {
    match signal {
        RowSignal::Traces => span_schema_v1(),
        RowSignal::Logs => log_schema_v1(),
    }
}

/// The catalog's signal for files of rows of `signal`.
pub fn catalog_signal(signal: RowSignal) -> Signal {
    match signal {
        RowSignal::Traces => Signal::Traces,
        RowSignal::Logs => Signal::Logs,
    }
}

/// Sort keys of every input, as integers: the rank of the service among all
/// services of the file, the timestamp, then input and row for a total
/// order that is the log order on ties.
struct MergeKeys {
    /// Per input, per row: service rank, null names first.
    ranks: Vec<Vec<u32>>,
    /// Per input: timestamps.
    timestamps: Vec<Vec<i64>>,
    /// Distinct services of the file, sorted.
    services: Vec<String>,
}

/// One row waiting at the head of its input, ordered by its key; the heap is
/// a max-heap, so the key is reversed.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Head(std::cmp::Reverse<(u32, i64, usize, usize)>);

impl MergeKeys {
    fn new(inputs: &[RecordBatch]) -> Result<Self, IngestError> {
        let mut services = BTreeMap::new();
        let mut names: Vec<Vec<Option<String>>> = Vec::with_capacity(inputs.len());
        let mut timestamps = Vec::with_capacity(inputs.len());
        for batch in inputs {
            let schema = batch.schema();
            let service = cast(
                batch.column(schema.index_of("service_name")?),
                &DataType::Utf8,
            )?;
            let service = service.as_string::<i32>();
            let column: Vec<Option<String>> =
                service.iter().map(|s| s.map(str::to_owned)).collect();
            for name in column.iter().flatten() {
                services.entry(name.clone()).or_insert(0u32);
            }
            names.push(column);
            let ts = batch
                .column(schema.index_of(col::TIMESTAMP)?)
                .as_primitive::<TimestampNanosecondType>();
            timestamps.push(ts.values().to_vec());
        }
        for (rank, slot) in services.values_mut().enumerate() {
            *slot = u32::try_from(rank).unwrap_or(u32::MAX);
        }
        let ranks = names
            .into_iter()
            .map(|column| {
                column
                    .iter()
                    .map(|name| {
                        name.as_ref()
                            .map_or(0, |n| services.get(n).map_or(0, |r| r + 1))
                    })
                    .collect()
            })
            .collect();
        Ok(Self {
            ranks,
            timestamps,
            services: services.into_keys().collect(),
        })
    }

    fn key(&self, input: usize, row: usize) -> Option<Head> {
        let rank = *self.ranks.get(input)?.get(row)?;
        let ts = *self.timestamps.get(input)?.get(row)?;
        Some(Head(std::cmp::Reverse((rank, ts, input, row))))
    }

    /// The first row of every non-empty input.
    fn heads(&self) -> BinaryHeap<Head> {
        (0..self.ranks.len())
            .filter_map(|input| self.key(input, 0))
            .collect()
    }

    /// The next `limit` rows in merged order, as `(input, row)` pairs.
    fn next_group(&self, heads: &mut BinaryHeap<Head>, limit: usize) -> Vec<(usize, usize)> {
        let mut out = Vec::new();
        while out.len() < limit {
            let Some(Head(std::cmp::Reverse((_, _, input, row)))) = heads.pop() else {
                break;
            };
            out.push((input, row));
            if let Some(next) = self.key(input, row + 1) {
                heads.push(next);
            }
        }
        out
    }
}

/// Builds one row group from `indices` into `inputs`.
fn gather(
    schema: &arrow::datatypes::SchemaRef,
    inputs: &[RecordBatch],
    indices: &[(usize, usize)],
) -> Result<RecordBatch, IngestError> {
    Ok(fs_schema::parquet::gather(schema, inputs, indices)?)
}

fn sort_batch(batch: &RecordBatch) -> Result<RecordBatch, IngestError> {
    Ok(fs_schema::parquet::sort_batch(batch)?)
}

/// Refuses a layout this engine does not write: the rows wait, bound to
/// their id, for an engine that does, rather than being written as other
/// bytes under the same id.
///
/// # Errors
/// A layout version outside [`FileLayout::written_versions`] for `signal`,
/// or a row group size outside the supported range.
pub fn check_layout(layout: &FileLayout, signal: RowSignal) -> Result<(), IngestError> {
    if !FileLayout::written_versions(signal).contains(&layout.version) {
        return Err(unwritten_layout(layout.version, signal));
    }
    check_row_group_rows(layout.row_group_rows)
}

/// The size the catalog row carries when an object is already in the
/// bucket: an intent with a recorded layout reproduces its bytes exactly,
/// so another size is a corrupt object or a broken encoder and the attempt
/// fails; an intent logged before layouts were recorded (`legacy`) may find
/// an object another encoder wrote, whose size is registered as is.
///
/// # Errors
/// A size mismatch for a non-legacy intent.
pub fn reconcile_existing(existing: u64, prepared: u64, legacy: bool) -> Result<u64, IngestError> {
    if existing == prepared || legacy {
        Ok(existing)
    } else {
        Err(IngestError::Encode(format!(
            "object already in the bucket has {existing} bytes, this layout gives {prepared}"
        )))
    }
}

/// The digest the catalog row carries when an object is already in the
/// bucket, read back from the object: the bytes a retry produces, or, for
/// a `legacy` intent, whatever another encoder wrote.
///
/// # Errors
/// The object's bytes differ from `prepared` for a non-legacy intent, or
/// the bucket fails.
pub async fn verify_existing(
    store: &dyn ObjectStore,
    path: &str,
    prepared: [u8; 32],
    legacy: bool,
) -> Result<[u8; 32], IngestError> {
    let existing = crate::sha256::of_object(store, path).await?;
    if existing == prepared || legacy {
        Ok(existing)
    } else {
        Err(IngestError::ObjectMismatch {
            path: path.to_owned(),
        })
    }
}

/// Outcome of an upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Upload {
    /// This call created the object.
    Created,
    /// An earlier attempt created it; this is its size in the bucket, which
    /// the catalog row must carry even when a later layout gave other bytes.
    Existing {
        /// Size of the object in the bucket.
        size: u64,
    },
}

/// Uploads a prepared file to its path without overwriting: a retried flush
/// finds the object already there, leaves it and reads its size. A store
/// without conditional writes is an error, not a plain put: overwriting is
/// never allowed.
///
/// # Errors
/// Bucket failure other than "already exists", including missing support for
/// conditional writes.
pub async fn upload(store: &dyn ObjectStore, prepared: &Prepared) -> Result<Upload, IngestError> {
    put_once(store, &prepared.file.path, prepared.bytes.clone()).await
}

/// Uploads the file's `trace_id` sidecar under the same create-once rule.
/// Goes after the file and before the catalog row. A sidecar already
/// there must be byte for byte the one these rows produce: a valid one
/// listing other traces would make the querier skip the file. Its size is
/// compared first, so only one of the right size is ever read back.
///
/// # Errors
/// Store failure, or an existing sidecar with other bytes.
pub async fn upload_index(
    store: &dyn ObjectStore,
    prepared: &Prepared,
) -> Result<Option<Upload>, IngestError> {
    let Some((index_path, index)) = &prepared.index else {
        return Ok(None);
    };
    let outcome = put_once(store, index_path, index.clone()).await?;
    if let Upload::Existing { size } = outcome {
        let mismatch = || IngestError::IndexMismatch {
            path: index_path.clone(),
        };
        if size != u64::try_from(index.len()).unwrap_or(u64::MAX) {
            return Err(mismatch());
        }
        let existing = store
            .get(&Path::from(index_path.as_str()))
            .await?
            .bytes()
            .await?;
        if existing != *index {
            return Err(mismatch());
        }
    }
    Ok(Some(outcome))
}

async fn put_once(
    store: &dyn ObjectStore,
    path: &str,
    bytes: Bytes,
) -> Result<Upload, IngestError> {
    let path = Path::from(path);
    let create = PutOptions {
        mode: PutMode::Create,
        ..PutOptions::default()
    };
    match store
        .put_opts(&path, PutPayload::from_bytes(bytes), create)
        .await
    {
        Ok(_) => Ok(Upload::Created),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let meta = store.head(&path).await?;
            Ok(Upload::Existing { size: meta.size })
        }
        Err(e) => Err(e.into()),
    }
}

/// `{root}/t/{tenant}/traces/l0/dt=YYYY-MM-DD/hr=HH/{id}.parquet`
pub fn l0_path(root: &Path, tenant: TenantId, start: DateTime<Utc>, id: FileId) -> Path {
    l0_path_of(root, Stream::traces(tenant), start, id)
}

/// `{root}/t/{tenant}/{traces|logs}/l0/dt=YYYY-MM-DD/hr=HH/{id}.parquet`
pub fn l0_path_of(root: &Path, stream: Stream, start: DateTime<Utc>, id: FileId) -> Path {
    let relative = format!(
        "t/{}/{}/l0/dt={}/hr={:02}/{}.parquet",
        stream.tenant.0,
        catalog_signal(stream.signal).path_name(),
        start.format("%Y-%m-%d"),
        start.format("%H"),
        id.0
    );
    if root.as_ref().is_empty() {
        Path::from(relative)
    } else {
        Path::from(format!("{root}/{relative}"))
    }
}

/// Sorts a span batch by `(service_name, timestamp, row)`.
///
/// # Errors
/// Arrow failures or a missing column.
pub fn sort_spans(batch: &RecordBatch) -> Result<RecordBatch, IngestError> {
    sort_batch(batch)
}

/// Encodes a sorted span batch as a Parquet L0 file with the engine's writer
/// settings, in row groups of [`DEFAULT_ROW_GROUP_ROWS`].
///
/// # Errors
/// Parquet failures.
pub fn encode_parquet(batch: &RecordBatch) -> Result<Bytes, IngestError> {
    let mut out = Vec::with_capacity(batch.get_array_memory_size() / 2);
    let mut writer = ArrowWriter::try_new(
        &mut out,
        batch.schema(),
        Some(writer_properties(
            FileLayout::current(DEFAULT_ROW_GROUP_ROWS),
            RowSignal::Traces,
        )?),
    )?;
    writer.write(batch)?;
    writer.close()?;
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use fs_schema::limits::Limits;
    use fs_schema::spans::SpanBatchBuilder;
    use object_store::local::LocalFileSystem;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use super::*;

    /// 2026-10-24T14:17:03Z.
    const T0: u64 = 1_792_678_623_000_000_000;

    fn batch(service: &str, trace: u8, timestamps: &[u64]) -> RecordBatch {
        let spans = timestamps
            .iter()
            .enumerate()
            .map(|(i, ts)| Span {
                trace_id: vec![trace; 16],
                span_id: vec![u8::try_from(i).unwrap() + 1; 8],
                name: format!("op-{i}"),
                kind: 2,
                start_time_unix_nano: *ts,
                end_time_unix_nano: *ts + 1_000,
                ..Span::default()
            })
            .collect();
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue(service.into())),
                        }),
                        ..KeyValue::default()
                    }],
                    ..Resource::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let mut builder = SpanBatchBuilder::new(Limits::default());
        builder.append_request(&request).unwrap();
        builder.finish().unwrap()
    }

    fn keys_of(batch: &RecordBatch) -> Vec<(String, i64, u8)> {
        let schema = batch.schema();
        let service = cast(
            batch.column(schema.index_of("service_name").unwrap()),
            &DataType::Utf8,
        )
        .unwrap();
        let service = service.as_string::<i32>();
        let ts = batch
            .column(schema.index_of(col::TIMESTAMP).unwrap())
            .as_primitive::<TimestampNanosecondType>();
        let trace = batch
            .column(schema.index_of(col::TRACE_ID).unwrap())
            .as_fixed_size_binary();
        (0..batch.num_rows())
            .map(|i| {
                (
                    service.value(i).to_owned(),
                    ts.value(i),
                    *trace.value(i).first().unwrap(),
                )
            })
            .collect()
    }

    #[test]
    fn l0_paths_partition_by_hour() {
        let tenant = TenantId(uuid::Uuid::nil());
        let id = FileId(uuid::Uuid::nil());
        let start = Utc.with_ymd_and_hms(2026, 10, 24, 14, 17, 3).unwrap();
        assert_eq!(
            l0_path(&Path::from(""), tenant, start, id).to_string(),
            "t/00000000-0000-0000-0000-000000000000/traces/l0/dt=2026-10-24/hr=14/00000000-0000-0000-0000-000000000000.parquet"
        );
        assert!(
            l0_path(&Path::from("engine"), tenant, start, id)
                .to_string()
                .starts_with("engine/t/")
        );
    }

    /// Three unsorted inputs with interleaved services, ties on the key and
    /// a row-group size that splits the file: the row groups come out in
    /// one global order, ties in input order, the file has one row group
    /// per gathered batch, and batching the same rows differently gives
    /// the same bytes.
    #[test]
    fn merge_orders_rows_across_inputs_and_splits_row_groups() {
        let inputs = vec![
            batch("web", 1, &[T0 + 5, T0 + 1, T0 + 3]),
            batch("api", 2, &[T0 + 2, T0 + 2, T0 + 9]),
            batch("api", 3, &[T0 + 2, T0 + 4]),
        ];
        let tenant = TenantId(uuid::Uuid::from_u128(1));
        let id = FileId(uuid::Uuid::from_u128(2));
        let prepared =
            prepare(tenant, &Path::from(""), id, &inputs, FileLayout::current(3)).unwrap();

        assert_eq!(prepared.rows.len(), 3, "eight rows in groups of three");
        let rows: Vec<(String, i64, u8)> = prepared
            .rows
            .iter()
            .flat_map(|r| keys_of(&r.batch))
            .collect();
        let t = |d: u64| i64::try_from(T0 + d).unwrap();
        assert_eq!(
            rows,
            vec![
                ("api".into(), t(2), 2),
                ("api".into(), t(2), 2),
                ("api".into(), t(2), 3),
                ("api".into(), t(4), 3),
                ("api".into(), t(9), 2),
                ("web".into(), t(1), 1),
                ("web".into(), t(3), 1),
                ("web".into(), t(5), 1),
            ],
            "sorted by service and timestamp, ties in input order"
        );
        assert_eq!(prepared.file.services, vec!["api", "web"]);
        assert_eq!(prepared.file.row_count, 8);
        assert_eq!(prepared.file.range.start, Utc.timestamp_nanos(t(1)));
        assert_eq!(prepared.file.range.end, Utc.timestamp_nanos(t(9)));
        assert!(prepared.writer_peak > 0, "the writer's memory was measured");
        assert!(
            prepared.rows.iter().all(|r| r.traces.is_some()),
            "every row group carries its trace set"
        );

        let reader = SerializedFileReader::new(prepared.bytes.clone()).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 3);

        let whole = arrow::compute::concat_batches(&span_schema_v1(), &inputs).unwrap();
        let again = prepare(
            tenant,
            &Path::from(""),
            id,
            &[whole],
            FileLayout::current(3),
        )
        .unwrap();
        assert_eq!(
            again.bytes, prepared.bytes,
            "batching does not change the file"
        );
    }

    fn bloom_columns(bytes: &Bytes) -> Vec<String> {
        let reader = SerializedFileReader::new(bytes.clone()).unwrap();
        reader
            .metadata()
            .row_group(0)
            .columns()
            .iter()
            .filter(|c| c.bloom_filter_offset().is_some())
            .map(|c| c.column_path().string())
            .collect()
    }

    /// Layout 3 has a bloom filter on every column, layout 4 on the three
    /// key columns alone, and a later layout is refused.
    #[test]
    fn layouts_put_blooms_on_their_columns() {
        let inputs = vec![
            batch("web", 1, &[T0 + 5, T0 + 1, T0 + 3]),
            batch("api", 2, &[T0 + 2, T0 + 9]),
        ];
        let tenant = TenantId(uuid::Uuid::from_u128(1));
        let id = FileId(uuid::Uuid::from_u128(2));
        let encode = |version| {
            let layout = FileLayout {
                version,
                row_group_rows: 3,
            };
            prepare(tenant, &Path::from(""), id, &inputs, layout)
        };

        let v3 = encode(3).unwrap();
        let leaves = SerializedFileReader::new(v3.bytes.clone())
            .unwrap()
            .metadata()
            .file_metadata()
            .schema_descr()
            .num_columns();
        assert_eq!(bloom_columns(&v3.bytes).len(), leaves);
        let v4 = encode(4).unwrap();
        assert_eq!(
            bloom_columns(&v4.bytes),
            vec![col::TRACE_ID, col::SPAN_ID, col::SERVICE_NAME]
        );
        assert!(v4.bytes.len() < v3.bytes.len());
        assert!(encode(5).is_err());
    }

    /// Rows that fill every row group exactly: nothing is left for a partial
    /// group, and the writer's memory is still read while it holds a full
    /// group, before the explicit flush.
    #[test]
    fn full_row_groups_are_measured_and_counted() {
        let inputs = vec![
            batch("api", 1, &[T0 + 1, T0 + 2, T0 + 3]),
            batch("api", 2, &[T0 + 4, T0 + 5, T0 + 6]),
        ];
        let tenant = TenantId(uuid::Uuid::from_u128(1));
        let id = FileId(uuid::Uuid::from_u128(2));
        let prepared =
            prepare(tenant, &Path::from(""), id, &inputs, FileLayout::current(3)).unwrap();
        assert_eq!(
            prepared.rows.len(),
            2,
            "six rows in groups of three, no remainder"
        );
        assert!(
            prepared.writer_peak > 0,
            "a full group is measured before it is flushed"
        );
        let reader = SerializedFileReader::new(prepared.bytes.clone()).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 2);
    }

    /// An object already at the path is left alone and its size reported,
    /// so a retry after a crash registers what is in the bucket.
    #[tokio::test]
    async fn the_sidecar_lists_every_trace_of_the_file_and_only_its_path() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let inputs = vec![
            batch("api", 1, &[T0 + 1, T0 + 2]),
            batch("db", 2, &[T0 + 3]),
            batch("api", 1, &[T0 + 4]),
        ];
        let tenant = TenantId(uuid::Uuid::from_u128(1));
        let id = FileId(uuid::Uuid::from_u128(2));
        let prepared = prepare(
            tenant,
            &Path::from("engine"),
            id,
            &inputs,
            FileLayout::current(2),
        )
        .unwrap();
        let (index_path, index_bytes) = prepared.index.clone().unwrap();
        assert_eq!(
            index_path,
            fs_index::sidecar_path(&prepared.file.path).unwrap()
        );
        let index = fs_index::TraceIndex::decode(&index_bytes).unwrap();
        assert_eq!(index.files(), std::slice::from_ref(&prepared.file.path));
        assert_eq!(
            index.len(),
            2,
            "two traces over two row groups, listed once each"
        );
        assert!(index.contains_any(&[[1u8; 16]]));
        assert!(index.contains_any(&[[2u8; 16]]));
        assert!(!index.contains_any(&[[3u8; 16]]));

        assert_eq!(
            upload_index(&store, &prepared).await.unwrap(),
            Some(Upload::Created)
        );
        assert_eq!(
            upload_index(&store, &prepared).await.unwrap(),
            Some(Upload::Existing {
                size: index_bytes.len() as u64
            })
        );
        let stored = store.get(&Path::from(index_path.as_str())).await.unwrap();
        assert_eq!(stored.bytes().await.unwrap(), index_bytes);
    }

    #[tokio::test]
    async fn an_existing_sidecar_with_other_bytes_refuses_the_upload() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let inputs = vec![batch("api", 1, &[T0 + 1]), batch("db", 2, &[T0 + 2])];
        let tenant = TenantId(uuid::Uuid::from_u128(1));
        let id = FileId(uuid::Uuid::from_u128(2));
        let prepared =
            prepare(tenant, &Path::from(""), id, &inputs, FileLayout::current(3)).unwrap();
        let (index_path, _) = prepared.index.clone().unwrap();
        // Valid, internally consistent, and missing trace 2.
        let partial = fs_index::TraceIndex::new(vec![prepared.file.path.clone()], [([1u8; 16], 0)])
            .unwrap()
            .encode();
        store
            .put(&Path::from(index_path.as_str()), partial.clone().into())
            .await
            .unwrap();
        let err = upload_index(&store, &prepared).await.unwrap_err();
        assert!(
            matches!(err, IngestError::IndexMismatch { ref path } if *path == index_path),
            "{err}"
        );
        let stored = store.get(&Path::from(index_path.as_str())).await.unwrap();
        assert_eq!(
            stored.bytes().await.unwrap(),
            partial,
            "nothing overwritten"
        );
    }

    #[tokio::test]
    async fn upload_reports_an_existing_object_and_its_size() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let inputs = vec![batch("api", 1, &[T0 + 1, T0 + 2])];
        let tenant = TenantId(uuid::Uuid::from_u128(1));
        let id = FileId(uuid::Uuid::from_u128(2));
        let prepared =
            prepare(tenant, &Path::from(""), id, &inputs, FileLayout::current(3)).unwrap();
        assert_eq!(upload(&store, &prepared).await.unwrap(), Upload::Created);
        assert_eq!(
            upload(&store, &prepared).await.unwrap(),
            Upload::Existing {
                size: prepared.bytes.len() as u64
            }
        );

        // Another encoder wrote this id first: the size is reported as is;
        // whether it is accepted is the intent's call.
        let other = FileId(uuid::Uuid::from_u128(3));
        let mut foreign = prepare(
            tenant,
            &Path::from(""),
            other,
            &inputs,
            FileLayout::current(3),
        )
        .unwrap();
        let path = Path::from(foreign.file.path.as_str());
        store
            .put(&path, PutPayload::from_static(b"not the same bytes"))
            .await
            .unwrap();
        foreign.bytes = Bytes::from_static(b"xyz");
        assert_eq!(
            upload(&store, &foreign).await.unwrap(),
            Upload::Existing { size: 18 }
        );
    }

    /// Five log rows of two services, the same on every call.
    fn log_batch() -> RecordBatch {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
        let resource_logs = ["web", "api"]
            .into_iter()
            .enumerate()
            .map(|(r, service)| ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue(service.into())),
                        }),
                        ..KeyValue::default()
                    }],
                    ..Resource::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: (0..2 + r as u64)
                        .map(|i| LogRecord {
                            time_unix_nano: T0 + (3 - i) * 1_000,
                            severity_text: "WARN".into(),
                            severity_number: 13,
                            body: Some(AnyValue {
                                value: Some(Value::StringValue(format!("{service} {i}"))),
                            }),
                            trace_id: vec![u8::try_from(i).unwrap() + 1; 16],
                            ..LogRecord::default()
                        })
                        .collect(),
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            })
            .collect();
        let request = ExportLogsServiceRequest { resource_logs };
        let mut builder = fs_schema::logs::LogBatchBuilder::new(Limits::default());
        builder.append_request(&request, &[7; 16]).unwrap();
        builder.finish().unwrap()
    }

    /// Log files exist only from layout 4 on: a log intent of layout 3 was
    /// not written by this engine's cut, and is refused before any byte.
    #[test]
    fn log_layouts_are_versioned_on_their_own() {
        let layout = |version| FileLayout {
            version,
            row_group_rows: 8,
        };
        assert!(check_layout(&layout(3), RowSignal::Traces).is_ok());
        assert!(check_layout(&layout(3), RowSignal::Logs).is_err());
        assert!(check_layout(&layout(4), RowSignal::Logs).is_ok());
        assert!(check_layout(&layout(5), RowSignal::Logs).is_err());
        let stream = Stream::logs(TenantId(uuid::Uuid::from_u128(1)));
        let id = FileId(uuid::Uuid::from_u128(2));
        let refused = prepare_stream(stream, &Path::from(""), id, &[log_batch()], layout(3));
        assert!(refused.is_err());
    }

    /// The bytes of a canonical log input, pinned for every layout version
    /// that writes log files; see the span test below for the rule.
    #[test]
    fn canonical_log_input_gives_the_pinned_bytes_for_each_version() {
        use sha2::{Digest, Sha256};
        let stream = Stream::logs(TenantId(uuid::Uuid::from_u128(0x7e5a)));
        let id = FileId(uuid::Uuid::from_u128(0xf11e));
        let pinned = [(
            4,
            "ad7cf28ab8322a172c46393ed411977da028bfff0c39a2e67be02316c13b1fbb",
        )];
        assert_eq!(
            pinned.map(|(version, _)| version),
            FileLayout::written_versions(RowSignal::Logs)
        );
        for (version, expected) in pinned {
            let layout = FileLayout {
                version,
                row_group_rows: 3,
            };
            let prepared =
                prepare_stream(stream, &Path::from(""), id, &[log_batch()], layout).unwrap();
            assert_eq!(prepared.file.row_count, 5);
            assert!(prepared.index.is_none(), "log files have no trace sidecar");
            let digest: [u8; 32] = Sha256::digest(&prepared.bytes).into();
            let hex = digest.iter().fold(String::new(), |mut out, b| {
                use std::fmt::Write;
                let _ = write!(out, "{b:02x}");
                out
            });
            assert_eq!(
                hex, expected,
                "log layout version {version} bytes changed: bump the version with the digest"
            );
        }
    }

    #[test]
    fn unknown_layout_versions_are_refused() {
        use crate::record::FileLayout;
        assert!(check_layout(&FileLayout::current(8), RowSignal::Traces).is_ok());
        let future = FileLayout {
            version: FileLayout::CURRENT_VERSION + 1,
            row_group_rows: 8,
        };
        assert!(check_layout(&future, RowSignal::Traces).is_err());
        let past = FileLayout {
            version: 2,
            row_group_rows: 8,
        };
        assert!(
            check_layout(&past, RowSignal::Traces).is_err(),
            "version 2 never shipped"
        );
        for rows in [0, MAX_ROW_GROUP_ROWS + 1] {
            let outside = FileLayout {
                version: FileLayout::CURRENT_VERSION,
                row_group_rows: rows,
            };
            assert!(
                check_layout(&outside, RowSignal::Traces).is_err(),
                "{rows} rows per group"
            );
            let inputs = vec![batch("api", 1, &[T0 + 1])];
            let tenant = TenantId(uuid::Uuid::from_u128(1));
            let id = FileId(uuid::Uuid::from_u128(2));
            assert!(
                prepare(
                    tenant,
                    &Path::from(""),
                    id,
                    &inputs,
                    FileLayout::current(rows)
                )
                .is_err()
            );
        }
        assert!(
            check_layout(&FileLayout::current(MAX_ROW_GROUP_ROWS), RowSignal::Traces).is_ok(),
            "the bound itself is allowed"
        );
    }

    /// The bytes of a canonical input, pinned for every layout version this
    /// engine writes. When an upgrade of the Parquet or compression crates,
    /// or a change here, gives other bytes, this fails: bump
    /// `FileLayout::CURRENT_VERSION` and pin its digest, keeping the older
    /// versions' digests, because a retry after a crash must reproduce what
    /// an earlier engine wrote.
    #[test]
    fn canonical_input_gives_the_pinned_bytes_for_each_version() {
        use sha2::{Digest, Sha256};
        let inputs = vec![
            batch("web", 1, &[T0 + 5, T0 + 1, T0 + 3]),
            batch("api", 2, &[T0 + 2, T0 + 2, T0 + 9]),
            batch("api", 3, &[T0 + 2, T0 + 4]),
        ];
        let tenant = TenantId(uuid::Uuid::from_u128(0x7e5a));
        let id = FileId(uuid::Uuid::from_u128(0xf11e));
        let pinned = [
            (
                3,
                "e97e37bb01c1cbf4fdd5d57489da73187111d6cbb18540dc1e583e00be75f68c",
            ),
            (
                4,
                "a4017dff8ea2fca038f436609dbe549af1fc657f25ae1b76f1a3ff39e203b665",
            ),
        ];
        assert_eq!(
            pinned.map(|(version, _)| version),
            FileLayout::written_versions(RowSignal::Traces)
        );
        for (version, expected) in pinned {
            let layout = FileLayout {
                version,
                row_group_rows: 3,
            };
            let prepared = prepare(tenant, &Path::from(""), id, &inputs, layout).unwrap();
            let digest: [u8; 32] = Sha256::digest(&prepared.bytes).into();
            let hex = digest.iter().fold(String::new(), |mut out, b| {
                use std::fmt::Write;
                let _ = write!(out, "{b:02x}");
                out
            });
            assert_eq!(
                hex, expected,
                "layout version {version} bytes changed: bump the version with the digest"
            );
        }
    }

    #[test]
    fn existing_sizes_are_accepted_only_when_equal_or_legacy() {
        assert_eq!(reconcile_existing(10, 10, false).unwrap(), 10);
        assert_eq!(reconcile_existing(10, 10, true).unwrap(), 10);
        assert_eq!(
            reconcile_existing(12, 10, true).unwrap(),
            12,
            "legacy keeps the bucket's size"
        );
        assert!(
            reconcile_existing(12, 10, false).is_err(),
            "a recorded layout must reproduce the bytes"
        );
    }
}
