//! End to end: OTLP logs request → Arrow batch → Parquet → Arrow batch.
#![allow(
    clippy::too_many_lines,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use arrow::array::{
    Array, AsArray, FixedSizeBinaryArray, MapArray, StringArray, TimestampNanosecondArray,
    UInt8Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatch;
use fs_schema::limits::Limits;
use fs_schema::logs::{LOG_PROMOTED, LogBatchBuilder, col, log_id, log_schema_v1};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const T0: u64 = 1_792_678_623_000_000_000; // 2026-10-24T14:17:03Z

fn kv(key: &str, value: Value) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue { value: Some(value) }),
        ..KeyValue::default()
    }
}

fn s(v: &str) -> Value {
    Value::StringValue(v.into())
}

fn any(value: Value) -> AnyValue {
    AnyValue { value: Some(value) }
}

fn record(time: u64, observed: u64, text: &str) -> LogRecord {
    LogRecord {
        time_unix_nano: time,
        observed_time_unix_nano: observed,
        body: Some(any(s(text))),
        ..LogRecord::default()
    }
}

fn request(service: &str, records: Vec<LogRecord>) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    kv("service.name", s(service)),
                    kv("deployment.environment", s("staging")),
                    kv("host.cpus", Value::IntValue(4)),
                ],
                ..Resource::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "app.logger".into(),
                    version: "2.1".into(),
                    attributes: vec![kv("lib", s("slog"))],
                    ..InstrumentationScope::default()
                }),
                log_records: records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn convert(request: &ExportLogsServiceRequest, seed: &[u8]) -> (RecordBatch, LogBatchBuilder) {
    let mut builder = LogBatchBuilder::default();
    builder.append_request(request, seed).unwrap();
    let batch = builder.finish().unwrap();
    (batch, builder)
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("no column {name}"))
        .as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| panic!("column {name} has another type"))
}

fn dict(batch: &RecordBatch, name: &str, row: usize) -> Option<String> {
    let array = batch
        .column_by_name(name)
        .unwrap()
        .as_dictionary::<Int32Type>();
    if array.is_null(row) {
        return None;
    }
    let values = array.values().as_string::<i32>();
    Some(values.value(array.keys().value(row) as usize).to_owned())
}

fn map_entries(batch: &RecordBatch, name: &str, row: usize) -> Vec<(String, String)> {
    let map: &MapArray = column(batch, name);
    let entry = map.value(row);
    let keys = entry.column(0).as_string::<i32>();
    let values = entry.column(1);
    (0..entry.len())
        .map(|i| {
            let value = arrow::util::display::array_value_to_string(values, i).unwrap();
            (keys.value(i).to_owned(), value)
        })
        .collect()
}

#[test]
fn a_log_record_fills_every_column() {
    let mut log = record(T0, T0 + 5_000, "payment accepted");
    log.severity_number = 9;
    log.severity_text = "INFO".into();
    log.trace_id = vec![7; 16];
    log.span_id = vec![3; 8];
    log.flags = 0xffff_ff01;
    log.attributes = vec![
        kv("order.id", s("o-17")),
        kv("amount", Value::DoubleValue(12.5)),
        kv("retry", Value::IntValue(2)),
        kv("cached", Value::BoolValue(true)),
    ];
    let (batch, builder) = convert(&request("checkout", vec![log]), b"seed");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.schema(), log_schema_v1());
    assert!(builder.stats().is_clean());

    let bucket: &UInt64Array = column(&batch, col::TS_BUCKET_START);
    assert_eq!(bucket.value(0), 1_792_677_600);
    let timestamp: &TimestampNanosecondArray = column(&batch, col::TIMESTAMP);
    assert_eq!(timestamp.value(0), T0 as i64);
    let observed: &TimestampNanosecondArray = column(&batch, col::OBSERVED_TIMESTAMP);
    assert_eq!(observed.value(0), (T0 + 5_000) as i64);
    let trace: &FixedSizeBinaryArray = column(&batch, col::TRACE_ID);
    assert_eq!(trace.value(0), [7; 16]);
    let span: &FixedSizeBinaryArray = column(&batch, col::SPAN_ID);
    assert_eq!(span.value(0), [3; 8]);
    let flags: &UInt32Array = column(&batch, col::TRACE_FLAGS);
    assert_eq!(flags.value(0), 1, "only the W3C trace flags are kept");
    assert_eq!(dict(&batch, col::SEVERITY_TEXT, 0).as_deref(), Some("INFO"));
    let number: &UInt8Array = column(&batch, col::SEVERITY_NUMBER);
    assert_eq!(number.value(0), 9);
    let text: &StringArray = column(&batch, col::BODY);
    assert_eq!(text.value(0), "payment accepted");

    assert_eq!(
        map_entries(&batch, col::ATTRIBUTES_STRING, 0),
        vec![("order.id".to_owned(), "o-17".to_owned())]
    );
    assert_eq!(
        map_entries(&batch, col::ATTRIBUTES_NUMBER, 0),
        vec![
            ("amount".to_owned(), "12.5".to_owned()),
            ("retry".to_owned(), "2.0".to_owned())
        ]
    );
    assert_eq!(
        map_entries(&batch, col::ATTRIBUTES_BOOL, 0),
        vec![("cached".to_owned(), "true".to_owned())]
    );
    assert_eq!(
        map_entries(&batch, col::RESOURCES_STRING, 0),
        vec![
            ("service.name".to_owned(), "checkout".to_owned()),
            ("deployment.environment".to_owned(), "staging".to_owned()),
            ("host.cpus".to_owned(), "4".to_owned())
        ]
    );
    let scope: &StringArray = column(&batch, col::SCOPE_NAME);
    assert_eq!(scope.value(0), "app.logger");
    let version: &StringArray = column(&batch, col::SCOPE_VERSION);
    assert_eq!(version.value(0), "2.1");
    assert_eq!(
        map_entries(&batch, col::SCOPE_STRING, 0),
        vec![("lib".to_owned(), "slog".to_owned())]
    );
    assert_eq!(dict(&batch, "service_name", 0).as_deref(), Some("checkout"));
    assert_eq!(
        dict(&batch, "deployment_environment", 0).as_deref(),
        Some("staging")
    );
    let ids: &FixedSizeBinaryArray = column(&batch, col::ID);
    assert_eq!(ids.value(0), log_id(T0, b"seed", 0));
}

#[test]
fn the_time_falls_back_and_a_record_without_any_is_rejected() {
    let records = vec![
        record(0, T0, "observed only"),
        record(T0 + 1, 0, "event time only"),
        record(0, 0, "no time at all"),
    ];
    let (batch, builder) = convert(&request("api", records), b"seed");
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(builder.stats().logs_rejected, 1);
    let timestamp: &TimestampNanosecondArray = column(&batch, col::TIMESTAMP);
    let observed: &TimestampNanosecondArray = column(&batch, col::OBSERVED_TIMESTAMP);
    assert_eq!(
        (timestamp.value(0), observed.value(0)),
        (T0 as i64, T0 as i64)
    );
    assert_eq!(
        (timestamp.value(1), observed.value(1)),
        ((T0 + 1) as i64, (T0 + 1) as i64)
    );
}

#[test]
fn invalid_trace_context_is_stored_as_absent() {
    let mut zero = record(T0, T0, "zero ids");
    zero.trace_id = vec![0; 16];
    zero.span_id = vec![0; 8];
    let mut short = record(T0, T0, "short ids");
    short.trace_id = vec![1; 15];
    short.span_id = vec![1; 9];
    let mut span_only = record(T0, T0, "span only");
    span_only.span_id = vec![2; 8];
    let (batch, builder) = convert(&request("api", vec![zero, short, span_only]), b"seed");
    assert_eq!(batch.num_rows(), 3);
    assert!(builder.stats().is_clean());
    let trace: &FixedSizeBinaryArray = column(&batch, col::TRACE_ID);
    let span: &FixedSizeBinaryArray = column(&batch, col::SPAN_ID);
    assert_eq!(trace.null_count(), 3);
    assert!(span.is_null(0) && span.is_null(1));
    assert_eq!(span.value(2), [2; 8]);
}

#[test]
fn bodies_render_as_text_and_are_bounded() {
    let structured = Value::KvlistValue(KeyValueList {
        values: vec![
            kv("msg", s("hi")),
            kv(
                "tags",
                Value::ArrayValue(ArrayValue {
                    values: vec![AnyValue {
                        value: Some(Value::IntValue(1)),
                    }],
                }),
            ),
        ],
    });
    let bodies = [
        Some(any(structured)),
        Some(any(Value::BytesValue(vec![0xff, 0x00]))),
        Some(any(Value::IntValue(3))),
        Some(any(Value::BoolValue(false))),
        None,
        Some(any(s("aéé"))),
    ];
    let records = bodies
        .into_iter()
        .map(|b| LogRecord {
            time_unix_nano: T0,
            body: b,
            ..LogRecord::default()
        })
        .collect();
    let mut builder = LogBatchBuilder::new(Limits {
        max_body_bytes: 4,
        ..Limits::default()
    });
    builder
        .append_request(&request("api", records), b"seed")
        .unwrap();
    let batch = builder.finish().unwrap();
    let text: &StringArray = column(&batch, col::BODY);
    let rendered: Vec<&str> = (0..batch.num_rows()).map(|i| text.value(i)).collect();
    assert_eq!(rendered, vec!["{\"ms", "/wA=", "3", "fals", "", "aé"]);
    assert_eq!(builder.stats().bodies_truncated, 3);
    assert_eq!(text.null_count(), 0);
}

#[test]
fn ids_are_deterministic_ordered_by_time_and_distinct() {
    let records = vec![
        record(T0, T0, "a"),
        record(T0, T0, "b"),
        record(T0 + 1_000_000, T0, "c"),
    ];
    let req = request("api", records);
    let (first, _) = convert(&req, b"wal-1:42");
    let (again, _) = convert(&req, b"wal-1:42");
    let (other, _) = convert(&req, b"wal-1:43");
    let ids: &FixedSizeBinaryArray = column(&first, col::ID);
    assert_eq!(ids, column::<FixedSizeBinaryArray>(&again, col::ID));
    let others: &FixedSizeBinaryArray = column(&other, col::ID);
    let all: std::collections::HashSet<&[u8]> = ids.iter().chain(others.iter()).flatten().collect();
    assert_eq!(all.len(), 6);
    let millis = (T0 / 1_000_000).to_be_bytes();
    assert_eq!(ids.value(0)[..6], millis[2..]);
    assert!(ids.value(0) < ids.value(2) && ids.value(1) < ids.value(2));
    assert_eq!(log_id(T0, b"wal-1:42", 1), ids.value(1));
}

#[test]
fn rejected_records_keep_the_ids_of_the_others() {
    let req = request("api", vec![record(T0, T0, "a"), record(T0, T0, "b")]);
    let with_gap = request(
        "api",
        vec![record(T0, T0, "a"), record(0, 0, "x"), record(T0, T0, "b")],
    );
    let (batch, _) = convert(&req, b"seed");
    let (gapped, _) = convert(&with_gap, b"seed");
    let ids: &FixedSizeBinaryArray = column(&gapped, col::ID);
    assert_eq!(ids.value(0), log_id(T0, b"seed", 0));
    assert_eq!(ids.value(1), log_id(T0, b"seed", 2));
    let plain: &FixedSizeBinaryArray = column(&batch, col::ID);
    assert_eq!(plain.value(1), log_id(T0, b"seed", 1));
}

#[test]
fn a_batch_survives_parquet_sorted_with_a_trace_id_bloom() {
    let mut records: Vec<LogRecord> = (0..20u64)
        .map(|i| {
            let mut r = record(T0 + (20 - i) * 1_000, T0, &format!("line {i}"));
            r.trace_id = vec![u8::try_from(i).unwrap() + 1; 16];
            r
        })
        .collect();
    records.push(record(T0, T0, "untraced"));
    let mut builder = LogBatchBuilder::default();
    builder
        .append_request(&request("zeta", records.clone()), b"one")
        .unwrap();
    builder
        .append_request(&request("alpha", records), b"two")
        .unwrap();
    let batch = fs_schema::parquet::sort_batch(&builder.finish().unwrap()).unwrap();

    let mut bytes = Vec::new();
    let props = fs_schema::parquet::log_writer_properties(1024).unwrap();
    let mut writer = ArrowWriter::try_new(&mut bytes, log_schema_v1(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
    let metadata = reader.metadata().clone();
    let read: Vec<RecordBatch> = reader.build().unwrap().map(Result::unwrap).collect();
    let read = arrow::compute::concat_batches(&log_schema_v1(), &read).unwrap();
    assert_eq!(read, batch);
    assert_eq!(dict(&read, "service_name", 0).as_deref(), Some("alpha"));
    assert_eq!(dict(&read, "service_name", 41).as_deref(), Some("zeta"));
    let timestamp: &TimestampNanosecondArray = column(&read, col::TIMESTAMP);
    assert!((1..21).all(|i| timestamp.value(i - 1) <= timestamp.value(i)));

    let group = metadata.row_group(0);
    let with_bloom: Vec<String> = group
        .columns()
        .iter()
        .filter(|c| c.bloom_filter_offset().is_some())
        .map(|c| c.column_path().string())
        .collect();
    assert_eq!(
        with_bloom,
        vec![col::TRACE_ID.to_owned(), "service_name".to_owned()]
    );
}

#[test]
fn the_schema_lists_fixed_columns_then_promoted() {
    let schema = log_schema_v1();
    assert_eq!(schema.metadata()["fusescope.signal"], "logs");
    assert_eq!(schema.metadata()["fusescope.schema_version"], "1");
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let promoted: Vec<&str> = LOG_PROMOTED.iter().map(|p| p.column).collect();
    assert_eq!(promoted, vec!["service_name", "deployment_environment"]);
    assert_eq!(names[names.len() - 2..], promoted[..]);
    assert_eq!(
        names[..3],
        [
            col::TS_BUCKET_START,
            col::TIMESTAMP,
            col::OBSERVED_TIMESTAMP
        ]
    );
    let mut builder = LogBatchBuilder::default();
    assert_eq!(builder.finish().unwrap().num_rows(), 0);
}
