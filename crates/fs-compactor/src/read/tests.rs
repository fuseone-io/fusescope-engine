use std::fmt::Write as _;

use fs_schema::limits::Limits;
use fs_schema::logs::{LogBatchBuilder, log_schema_v1};
use object_store::local::LocalFileSystem;
use object_store::{ObjectStoreExt, PutPayload};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use parquet::arrow::ArrowWriter;
use parquet::arrow::async_reader::AsyncFileReader;

use super::*;

/// 2026-10-30T00:00:00Z.
const T0: u64 = 1_793_318_400_000_000_000;

fn noise(seed: u64, len: usize) -> String {
    let mut state = (seed + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut out = String::with_capacity(len + 16);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        write!(out, "{state:016x}").unwrap();
    }
    out.truncate(len);
    out
}

/// `narrow` rows of a few bytes, then `wide` rows at the default limits:
/// the largest body and attributes of the largest value, each row its own
/// service and trace.
fn logs(narrow: usize, wide: usize) -> RecordBatch {
    let limits = Limits::default();
    let mut builder = LogBatchBuilder::new(limits);
    for i in 0..narrow + wide {
        let big = i >= narrow;
        let attributes = (0..if big { 4 } else { 1 })
            .map(|a| KeyValue {
                key: format!("k{a}"),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(noise(
                        (i * 8 + a) as u64,
                        if big { limits.max_value_bytes } else { 4 },
                    ))),
                }),
                ..KeyValue::default()
            })
            .collect();
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue(format!("service-{i:05}"))),
                        }),
                        ..KeyValue::default()
                    }],
                    ..Resource::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: T0 + i as u64,
                        trace_id: (i as u128 + 1).to_be_bytes().to_vec(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue(noise(
                                i as u64,
                                if big { limits.max_body_bytes } else { 8 },
                            ))),
                        }),
                        attributes,
                        ..LogRecord::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        builder.append_request(&request, &[7; 16]).unwrap();
    }
    builder.finish().unwrap()
}

async fn stored(batch: &RecordBatch) -> (Arc<dyn ObjectStore>, i64, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let properties = fs_schema::parquet::log_writer_properties(262_144).unwrap();
    let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), Some(properties)).unwrap();
    writer.write(batch).unwrap();
    let bytes = writer.into_inner().unwrap();
    let size = i64::try_from(bytes.len()).unwrap();
    store
        .put(&Path::from("in.parquet"), PutPayload::from(bytes))
        .await
        .unwrap();
    (store, size, dir)
}

#[tokio::test]
async fn a_planned_chunk_holds_no_more_than_its_bound_whatever_rows_came_first() {
    let batch = logs(16, 300);
    let (store, size, _dir) = stored(&batch).await;
    let schema = log_schema_v1();
    let (input, layout) = Input::bounded(Arc::clone(&store), ("in.parquet", size), &schema, &[])
        .await
        .unwrap();
    assert!(layout.floor > 0);
    let budget = layout.floor * 2;
    let chunks = layout.plan(budget, 8192);
    assert!(chunks.len() > 1, "{} chunks", chunks.len());
    assert!(
        chunks
            .iter()
            .all(|c| c.bytes <= budget || c.rows.len() == 1),
        "a chunk past its budget: {chunks:?}"
    );
    // The narrow rows do not lend their width to the wide ones after them.
    let first = chunks.front().unwrap();
    assert!(first.rows.end < 300, "{first:?}");

    let mut input = input.with_chunks(chunks.clone());
    let mut rows = 0;
    let mut planned = chunks.iter();
    while let Some((read, bound)) = input.next().await.unwrap() {
        let chunk = planned.next().unwrap();
        assert_eq!(bound, chunk.bytes);
        let held = read.get_array_memory_size() + read.num_rows() * SLOT_ROW_BYTES;
        assert!(
            held <= bound,
            "{held} bytes decoded, bound {bound}: {chunk:?}"
        );
        assert_eq!(read.num_rows(), chunk.rows.len());
        rows += read.num_rows();
    }
    assert_eq!(rows, batch.num_rows());
}

#[tokio::test]
async fn a_dictionary_is_sized_from_its_page_header() {
    let batch = logs(16, 64);
    let (store, size, _dir) = stored(&batch).await;
    let reader = object_reader(Arc::clone(&store), "in.parquet", size, true);
    let mut probe = reader.clone();
    let metadata = probe
        .get_metadata(Some(
            &ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required),
        ))
        .await
        .unwrap();
    let body = metadata
        .row_group(0)
        .columns()
        .iter()
        .position(|c| c.column_path().string() == fs_schema::logs::col::BODY)
        .unwrap();
    let chunk = metadata.row_group(0).column(body);
    let start = u64::try_from(chunk.dictionary_page_offset().unwrap()).unwrap();
    let first = metadata
        .offset_index()
        .and_then(|index| index.first())
        .and_then(|group| group.get(body))
        .and_then(|column| column.page_locations().first())
        .unwrap()
        .offset;
    let header = store
        .get_range(
            &Path::from("in.parquet"),
            start..start + DICTIONARY_HEADER_BYTES,
        )
        .await
        .unwrap();
    let decompressed = dictionary_page_size(&header).unwrap();
    let compressed = usize::try_from(u64::try_from(first).unwrap() - start).unwrap();
    // Hex bodies compress about two to one.
    assert!(
        decompressed > compressed && decompressed < 4 * compressed,
        "{decompressed} decompressed, {compressed} compressed"
    );
    assert_eq!(dictionary_page_size(&[0x15, 0x02]), None);
    assert_eq!(
        dictionary_page_size(&[0x15, 0x08, 0x15, 0x10]),
        None,
        "a data page"
    );
    assert_eq!(dictionary_page_size(&[0x15, 0x04, 0x15, 0x10]), Some(8));
}

#[test]
fn touching_ranges_are_fetched_together_and_gaps_never() {
    assert_eq!(
        touching_runs(&[10..20, 0..5, 5..8, 18..25, 30..31]),
        vec![0..8, 10..25, 30..31]
    );
}
