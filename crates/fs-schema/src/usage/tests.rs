use std::cell::Cell;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

use super::*;
use crate::limits::Limits;
use parquet::arrow::ArrowWriter;

thread_local! {
    /// Batches [`UsageRows::new`] measured on this thread.
    pub(super) static SIZED: Cell<usize> = const { Cell::new(0) };
}

/// 2026-10-24T14:17:03Z.
const T0: u64 = 1_792_678_623_000_000_000;

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
        ..KeyValue::default()
    }
}

fn resource(service: &str) -> Resource {
    Resource {
        attributes: vec![kv("service.name", service)],
        ..Resource::default()
    }
}

fn spans(attributes: Vec<KeyValue>, name: &str) -> RecordBatch {
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource("api")),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: vec![1; 16],
                    span_id: vec![2; 8],
                    name: name.into(),
                    kind: 2,
                    start_time_unix_nano: T0,
                    end_time_unix_nano: T0 + 1_000,
                    attributes,
                    ..Span::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let mut builder = crate::spans::SpanBatchBuilder::new(Limits::default());
    builder.append_request(&request).unwrap();
    builder.finish().unwrap()
}

fn logs(bodies: &[&str], service: Option<&str>) -> RecordBatch {
    let records = bodies
        .iter()
        .zip(0u64..)
        .map(|(body, i)| LogRecord {
            time_unix_nano: T0 + i * 3_600_000_000_000,
            severity_text: "INFO".into(),
            severity_number: 9,
            body: Some(AnyValue {
                value: Some(Value::StringValue((*body).into())),
            }),
            ..LogRecord::default()
        })
        .collect();
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(service.map_or_else(Resource::default, resource)),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let mut builder = crate::logs::LogBatchBuilder::new(Limits::default());
    builder.append_request(&request, &[7; 16]).unwrap();
    builder.finish().unwrap()
}

#[test]
fn a_span_counts_its_payload_once_and_no_derived_column() {
    let base = row_bytes(&spans(vec![], "op"), UsageSignal::Spans).unwrap()[0];
    // Pinned: timestamp 8, trace id 16, span id 8, flags 4, name 2, kind 1,
    // duration 8, status code 2, service.name and api 15. Another value is
    // a new version.
    assert_eq!(
        base, 64,
        "usage logical bytes v1 of the canonical span changed"
    );
    let named = row_bytes(&spans(vec![], "operation"), UsageSignal::Spans).unwrap()[0];
    assert_eq!(named - base, 7, "the name by its bytes");
    let attribute = row_bytes(&spans(vec![kv("k", "value")], "op"), UsageSignal::Spans).unwrap()[0];
    assert_eq!(attribute - base, 6, "an attribute by its key and value");
    // http.route is also promoted to its own column: counted once.
    let route = row_bytes(
        &spans(vec![kv("http.route", "/x")], "op"),
        UsageSignal::Spans,
    )
    .unwrap()[0];
    assert_eq!(route - base, 12);
}

#[test]
fn a_log_counts_its_body_and_attributes_after_the_limits() {
    let batch = logs(&["", "hello", &"x".repeat(100_000)], Some("api"));
    let bytes = row_bytes(&batch, UsageSignal::Logs).unwrap();
    // Pinned: two timestamps 16, id 16, flags 4, INFO 4, severity 1,
    // service.name and api 15; the null trace and span ids count nothing.
    assert_eq!(
        bytes[0], 56,
        "usage logical bytes v1 of the canonical log changed"
    );
    assert_eq!(bytes[1] - bytes[0], 5);
    let limit = Limits::default().max_body_bytes as u64;
    assert_eq!(
        bytes[2] - bytes[0],
        limit,
        "the body as truncated by the ingest"
    );
}

#[test]
fn counts_go_by_hour_and_service_and_round_trip() {
    let mut counts = UsageCounts::new(UsageSignal::Logs);
    counts.absorb(&logs(&["a", "bb"], Some("api"))).unwrap();
    counts.absorb(&logs(&["c"], None)).unwrap();
    let hours = counts.hours();
    let first = i64::try_from(T0).unwrap().div_euclid(HOUR_NANOS) * HOUR_NANOS;
    assert_eq!(hours, [first, first + HOUR_NANOS]);
    let first_hour: Vec<_> = counts.of_hour(first).collect();
    assert_eq!(first_hour.len(), 2, "no service and api");
    assert_eq!(first_hour[0].0, None);
    assert_eq!(first_hour[1].0, Some("api"));
    assert_eq!((first_hour[0].1, first_hour[1].1), (1, 1));

    let decoded = decode(bytes::Bytes::from(encode_hour(&counts, first).unwrap())).unwrap();
    assert_eq!(
        decoded,
        first_hour
            .iter()
            .map(|(s, r, b)| (2, s.map(str::to_owned), first, *r, *b))
            .collect::<Vec<_>>()
    );
}

#[test]
fn an_overflow_fails_closed() {
    let mut big = UsageCounts::new(UsageSignal::Spans);
    big.add(0, None, u64::MAX, 1).unwrap();
    let mut one = UsageCounts::new(UsageSignal::Spans);
    one.add(0, None, 1, 1).unwrap();
    assert!(big.merge(one).is_err());
    assert!(
        UsageCounts::new(UsageSignal::Spans)
            .merge(UsageCounts::new(UsageSignal::Logs))
            .is_err()
    );
}

#[test]
fn another_schema_or_bytes_version_is_refused() {
    let mut metadata = schema().metadata().clone();
    metadata.insert("fusescope.usage_bytes_version".into(), "2".into());
    let other = Schema::new_with_metadata(schema().fields().clone(), metadata);
    assert!(check_schema(&other).is_err());
    assert!(check_schema(&schema()).is_ok());
}

/// A contribution of format v1 with `rows`, written in batches of `split`.
fn contribution(rows: &[(u8, Option<&str>, i64)], split: usize) -> bytes::Bytes {
    let mut writer = ArrowWriter::try_new(Vec::new(), schema(), None).unwrap();
    for part in rows.chunks(split) {
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(UInt8Array::from(
                    part.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    part.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(
                    TimestampNanosecondArray::from(part.iter().map(|r| r.2).collect::<Vec<_>>())
                        .with_timezone("UTC"),
                ),
                Arc::new(UInt64Array::from(vec![1u64; part.len()])),
                Arc::new(UInt64Array::from(vec![1u64; part.len()])),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
    }
    bytes::Bytes::from(writer.into_inner().unwrap())
}

#[test]
fn a_contribution_with_rows_outside_the_domain_is_refused() {
    let hour = 1_792_677_600_000_000_000i64;
    let good = contribution(
        &[
            (1, None, hour),
            (1, Some("api"), hour),
            (1, Some("web"), hour),
            (2, Some("api"), hour),
        ],
        1,
    );
    assert_eq!(decode(good).unwrap().len(), 4);
    for (rows, why) in [
        (vec![(3u8, Some("api"), hour)], "signal 3"),
        (vec![(0, Some("api"), hour)], "signal 0"),
        (vec![(1, Some("api"), hour + 1)], "an hour not on the hour"),
        (
            vec![(1, Some("api"), hour), (1, Some("api"), hour)],
            "a key counted twice",
        ),
        (
            vec![(1, None, hour), (1, None, hour)],
            "no service, counted twice",
        ),
        (
            vec![(1, Some("web"), hour), (1, Some("api"), hour)],
            "services out of order",
        ),
        (
            vec![(1, Some("api"), hour), (1, None, hour)],
            "no service after a service",
        ),
        (
            vec![
                (1, Some("api"), hour),
                (1, Some("web"), hour),
                (1, Some("api"), hour),
            ],
            "a key counted twice, apart",
        ),
    ] {
        // One row per batch: the check sees the whole file.
        assert!(decode(contribution(&rows, 1)).is_err(), "{why}");
    }
}

#[test]
fn a_full_count_is_set_aside_before_a_new_key_would_pass_the_budget() {
    let services: Vec<String> = (0..50).map(|i| format!("service-{i:04}")).collect();
    let mut builder = crate::logs::LogBatchBuilder::new(Limits::default());
    for service in &services {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(resource(service)),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: T0,
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("x".into())),
                        }),
                        ..LogRecord::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        builder.append_request(&request, &[7; 16]).unwrap();
    }
    let batch = builder.finish().unwrap();
    let budget = 10 * UsageCounts::entry_bytes("service-0000");
    let mut counts = UsageCounts::new(UsageSignal::Logs);
    let mut full = Vec::new();
    let mut from = 0;
    let before = SIZED.with(Cell::get);
    let rows = UsageRows::new(&batch, UsageSignal::Logs).unwrap();
    while let Some((part, next)) = counts.absorb_from(&rows, from, budget, None).unwrap() {
        // Counting pauses at each full count: nothing else is held.
        assert_eq!(counts.len(), 0);
        assert!(part.estimated_bytes() <= budget);
        assert!(next > from);
        full.push(part);
        from = next;
    }
    assert!(full.len() >= 4, "{} counts set aside", full.len());
    assert_eq!(
        SIZED.with(Cell::get) - before,
        1,
        "the batch is measured once, however often counting resumes"
    );
    assert!(counts.estimated_bytes() <= budget);
    let mut all = UsageCounts::new(UsageSignal::Logs);
    for part in full {
        all.merge(part).unwrap();
    }
    all.merge(counts).unwrap();
    let mut whole = UsageCounts::new(UsageSignal::Logs);
    whole.absorb(&batch).unwrap();
    assert_eq!(all, whole, "nothing lost, nothing counted twice");

    let odd: Vec<bool> = (0..batch.num_rows()).map(|i| i % 2 == 1).collect();
    let mut some = UsageCounts::new(UsageSignal::Logs);
    assert!(
        some.absorb_from(&rows, 0, usize::MAX, Some(&odd))
            .unwrap()
            .is_none()
    );
    assert_eq!(some.len(), services.len() / 2, "only the rows taken");
}

/// A batch of samples of `names`, at `ms` each.
fn samples(names: &[&str], ms: i64) -> RecordBatch {
    use crate::metrics::{SOURCE_ORDER_BYTES, samples_schema_v1};
    use arrow::array::{
        FixedSizeBinaryArray, Float64Array, Int64Array, StringDictionaryBuilder, UInt32Array,
    };
    let n = names.len();
    let mut name = StringDictionaryBuilder::<arrow::datatypes::Int32Type>::new();
    let mut temporality = StringDictionaryBuilder::<arrow::datatypes::Int32Type>::new();
    for each in names {
        name.append_value(each);
        temporality.append_value("Cumulative");
    }
    RecordBatch::try_new(
        samples_schema_v1(),
        vec![
            Arc::new(name.finish()),
            Arc::new(temporality.finish()),
            Arc::new(UInt64Array::from(vec![7u64; n])),
            Arc::new(Int64Array::from(vec![ms; n])),
            Arc::new(Float64Array::from(vec![1.5; n])),
            Arc::new(UInt32Array::from(vec![0u32; n])),
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(std::iter::repeat_n(
                    [9u8; SOURCE_ORDER_BYTES],
                    n,
                ))
                .unwrap(),
            ),
        ],
    )
    .unwrap()
}

/// A sample costs its name, time, value and flags, never its
/// fingerprint, temporality or source order; its service is the one
/// given, its hour that of its time. Samples are written in format v2.
#[test]
fn a_metric_sample_counts_its_name_time_value_and_flags() {
    let ms = 1_792_678_623_000i64;
    let batch = samples(&["cpu", "memory"], ms);
    assert_eq!(
        row_bytes(&batch, UsageSignal::Samples).unwrap(),
        [3 + 20, 6 + 20]
    );
    let services: ArrayRef = Arc::new(StringArray::from(vec![Some("api"), None]));
    let rows = UsageRows::samples(&batch, &services).unwrap();
    let mut counts = UsageCounts::new(UsageSignal::Samples);
    assert!(
        counts
            .absorb_from(&rows, 0, usize::MAX, None)
            .unwrap()
            .is_none()
    );
    let hour = ms.div_euclid(3_600_000) * 3_600_000_000_000;
    assert_eq!(counts.hours(), [hour]);
    let encoded = encode_hour(&counts, hour).unwrap();
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(encoded.clone())).unwrap();
    assert_eq!(schema_version(reader.schema()).unwrap(), SCHEMA_VERSION_V2);
    assert_eq!(
        decode(bytes::Bytes::from(encoded)).unwrap(),
        [
            (3, None, hour, 1, 26),
            (3, Some("api".to_owned()), hour, 1, 23)
        ]
    );
    let short: ArrayRef = Arc::new(StringArray::from(vec![Some("api")]));
    assert!(UsageRows::samples(&batch, &short).is_err());
}

/// Format v1 admits spans and logs only, v2 metric samples too, and
/// neither another signal.
#[test]
fn metric_samples_are_admitted_in_format_v2_only() {
    let hour = 1_792_677_600_000_000_000i64;
    let mut v1 = RowOrder::default();
    let mut v2 = RowOrder::of_version(SCHEMA_VERSION_V2);
    let batch = |signal: u8| {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(UInt8Array::from(vec![signal])),
                Arc::new(StringArray::from(vec![Some("api")])),
                Arc::new(TimestampNanosecondArray::from(vec![hour]).with_timezone("UTC")),
                Arc::new(UInt64Array::from(vec![1u64])),
                Arc::new(UInt64Array::from(vec![1u64])),
            ],
        )
        .unwrap()
    };
    assert!(v1.check(&batch(3)).is_err());
    assert!(v2.check(&batch(3)).is_ok());
    assert!(
        RowOrder::of_version(SCHEMA_VERSION_V2)
            .check(&batch(4))
            .is_err()
    );
    assert!(check_schema(&schema_v2()).is_ok());
}
