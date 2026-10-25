//! End to end: OTLP request → Arrow batch → Parquet → Arrow batch.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::cast_sign_loss,
    clippy::panic
)]

use std::sync::Arc;

use arrow::array::{Array, AsArray, BooleanArray, FixedSizeBinaryArray, ListArray, UInt64Array};
use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatch;
use fs_schema::limits::Limits;
use fs_schema::spans::{PROMOTED, SpanBatchBuilder, col, span_schema_v1};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link, SpanKind};
use opentelemetry_proto::tonic::trace::v1::status::StatusCode;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
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

fn resource(service: &str, extra: &[(&str, Value)]) -> Resource {
    let mut attributes = vec![
        kv("service.name", s(service)),
        kv("deployment.environment.name", s("prod")),
    ];
    attributes.extend(extra.iter().map(|(k, v)| kv(k, v.clone())));
    Resource {
        attributes,
        ..Resource::default()
    }
}

fn span(
    trace: u8,
    id: u8,
    parent: Option<u8>,
    kind: SpanKind,
    start_offset_ns: u64,
    duration_ns: u64,
) -> Span {
    Span {
        trace_id: vec![trace; 16],
        span_id: vec![id; 8],
        parent_span_id: parent.map_or_else(Vec::new, |p| vec![p; 8]),
        name: format!("span-{id}"),
        kind: kind as i32,
        start_time_unix_nano: T0 + start_offset_ns,
        end_time_unix_nano: T0 + start_offset_ns + duration_ns,
        ..Span::default()
    }
}

fn fixture() -> ExportTraceServiceRequest {
    let mut server = span(1, 1, None, SpanKind::Server, 0, 250_000_000);
    server.attributes = vec![
        kv("http.request.method", s("GET")),
        kv("http.route", s("/orders/{id}")),
        kv("http.response.status_code", Value::IntValue(500)),
        kv("retry", Value::BoolValue(false)),
    ];
    server.status = Some(Status {
        code: StatusCode::Error as i32,
        message: "boom".into(),
    });
    server.events = vec![Event {
        time_unix_nano: T0 + 1_000,
        name: "exception".into(),
        attributes: vec![
            kv("exception.type", s("Timeout")),
            kv("exception.escaped", Value::BoolValue(true)),
        ],
        dropped_attributes_count: 0,
    }];
    server.links = vec![
        Link {
            trace_id: vec![9; 16],
            span_id: vec![9; 8],
            trace_state: "a=b".into(),
            ..Link::default()
        },
        Link {
            trace_id: vec![9; 3],
            span_id: vec![9; 8],
            ..Link::default()
        }, // bad id, dropped
    ];

    let mut client = span(1, 2, Some(1), SpanKind::Client, 10_000_000, 100_000_000);
    client.attributes = vec![
        kv("http.method", s("POST")),
        kv("http.status_code", Value::IntValue(201)),
        kv("peer.service", s("payments")),
    ];
    client.flags = 0x100 | 0x200; // parent context known and remote
    client.status = Some(Status {
        code: StatusCode::Ok as i32,
        message: String::new(),
    });

    let mut bad = span(1, 3, None, SpanKind::Internal, 0, 1);
    bad.trace_id = vec![1; 4];

    let mut worker = span(2, 7, None, SpanKind::Consumer, 0, 5_000_000);
    worker.attributes = vec![
        kv("messaging.system", s("kafka")),
        kv("messaging.operation.type", s("process")),
    ];

    ExportTraceServiceRequest {
        resource_spans: vec![
            ResourceSpans {
                resource: Some(resource("api", &[("host.name", s("h1"))])),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "otel-http".into(),
                        version: "1.2".into(),
                        ..InstrumentationScope::default()
                    }),
                    spans: vec![server, client, bad],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            },
            ResourceSpans {
                resource: Some(resource("worker", &[])),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![worker],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            },
        ],
    }
}

fn col_idx(name: &str) -> usize {
    span_schema_v1().index_of(name).unwrap()
}

fn dict_values(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
    let array = batch.column(col_idx(name)).as_dictionary::<Int32Type>();
    let values = array.values().as_string::<i32>();
    array
        .keys()
        .iter()
        .map(|k| k.map(|k| values.value(k as usize).to_owned()))
        .collect()
}

fn convert() -> (RecordBatch, fs_schema::limits::DropStats) {
    let mut builder = SpanBatchBuilder::new(Limits::default());
    builder.append_request(&fixture()).unwrap();
    let stats = builder.stats();
    (builder.finish().unwrap(), stats)
}

#[test]
fn converts_spans_with_schema_v1() {
    let (batch, stats) = convert();
    assert_eq!(batch.schema(), span_schema_v1());
    assert_eq!(batch.num_rows(), 3, "one span has a bad trace id");
    assert_eq!(stats.spans_rejected, 1);
    assert_eq!(stats.links_dropped, 1);
    assert_eq!(stats.attributes_dropped, 0);

    assert_eq!(
        dict_values(&batch, "service_name"),
        vec![
            Some("api".into()),
            Some("api".into()),
            Some("worker".into())
        ]
    );
    assert_eq!(
        dict_values(&batch, "deployment_environment")[0].as_deref(),
        Some("prod")
    );
    assert_eq!(
        dict_values(&batch, "http_method"),
        vec![Some("GET".into()), Some("POST".into()), None]
    );
    assert_eq!(
        dict_values(&batch, "response_status_code"),
        vec![Some("500".into()), Some("201".into()), None]
    );
    assert_eq!(
        dict_values(&batch, "messaging_operation")[2].as_deref(),
        Some("process")
    );
    assert_eq!(
        dict_values(&batch, col::KIND_STRING),
        vec![
            Some("Server".into()),
            Some("Client".into()),
            Some("Consumer".into())
        ]
    );
    assert_eq!(
        dict_values(&batch, col::STATUS_CODE_STRING),
        vec![
            Some("Error".into()),
            Some("Ok".into()),
            Some("Unset".into())
        ]
    );
    assert_eq!(
        dict_values(&batch, col::IS_REMOTE),
        vec![
            Some(String::new()),
            Some("true".into()),
            Some(String::new())
        ]
    );

    let has_error = batch
        .column(col_idx(col::HAS_ERROR))
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(
        has_error.values().iter().collect::<Vec<_>>(),
        vec![true, false, false]
    );

    let duration = batch
        .column(col_idx(col::DURATION_NANO))
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(duration.values(), &[250_000_000, 100_000_000, 5_000_000]);

    let bucket = batch
        .column(col_idx(col::TS_BUCKET_START))
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert!(
        bucket
            .values()
            .iter()
            .all(|b| b % 1800 == 0 && *b <= T0 / 1_000_000_000)
    );

    let parent = batch
        .column(col_idx(col::PARENT_SPAN_ID))
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert!(parent.is_null(0));
    assert_eq!(parent.value(1), &[1u8; 8]);

    let fp = batch
        .column(col_idx(col::RESOURCE_FINGERPRINT))
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(fp.value(0), fp.value(1), "same resource, same fingerprint");
    assert_ne!(fp.value(0), fp.value(2));
}

#[test]
fn nested_events_links_and_maps() {
    let (batch, _) = convert();

    let events = batch
        .column(col_idx(col::EVENTS))
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(events.value_length(0), 1);
    assert_eq!(events.value_length(1), 0);
    let event = events.value(0);
    let event = event.as_struct();
    assert_eq!(event.column(1).as_string::<i32>().value(0), "exception");
    let attrs = event.column(2).as_map();
    assert_eq!(attrs.value_length(0), 2);
    assert_eq!(
        attrs.keys().as_string::<i32>().value(1),
        "exception.escaped"
    );
    assert_eq!(
        attrs.values().as_string::<i32>().value(1),
        "true",
        "event attributes are text"
    );

    let links = batch
        .column(col_idx(col::LINKS))
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(
        links.value_length(0),
        1,
        "the link with a 3 byte trace id was dropped"
    );
    let link = links.value(0);
    let link = link.as_struct();
    assert_eq!(link.column(0).as_fixed_size_binary().value(0), &[9u8; 16]);
    assert_eq!(link.column(2).as_string::<i32>().value(0), "a=b");

    let numbers = batch.column(col_idx(col::ATTRIBUTES_NUMBER)).as_map();
    assert_eq!(numbers.value_length(0), 1);
    assert_eq!(
        numbers.keys().as_string::<i32>().value(0),
        "http.response.status_code"
    );
    assert_eq!(
        numbers
            .values()
            .as_primitive::<arrow::datatypes::Float64Type>()
            .value(0),
        500.0
    );

    let bools = batch.column(col_idx(col::ATTRIBUTES_BOOL)).as_map();
    assert_eq!(bools.value_length(0), 1);
    assert_eq!(bools.value_length(1), 0);

    let resources = batch.column(col_idx(col::RESOURCES_STRING)).as_map();
    assert_eq!(resources.value_length(0), 3);
    assert_eq!(resources.value_length(2), 2);
}

#[test]
fn parquet_round_trip_preserves_schema_and_rows() {
    let (batch, _) = convert();
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(buffer))
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
    let back = arrow::compute::concat_batches(&batch.schema(), &batches).unwrap();

    assert_eq!(back.num_rows(), batch.num_rows());
    assert_eq!(
        back.schema().fields(),
        batch.schema().fields(),
        "parquet must round-trip every field type"
    );
    assert_eq!(back, batch);
}

#[test]
fn limits_are_applied_and_counted() {
    let limits = Limits {
        max_events: 0,
        max_links: 0,
        max_attributes: 1,
        ..Limits::default()
    };
    let mut builder = SpanBatchBuilder::new(limits);
    builder.append_request(&fixture()).unwrap();
    let stats = builder.stats();
    assert_eq!(stats.events_dropped, 1);
    assert_eq!(stats.links_dropped, 2);
    assert!(stats.attributes_dropped > 0);
    let batch = builder.finish().unwrap();
    let strings = batch.column(col_idx(col::ATTRIBUTES_STRING)).as_map();
    let numbers = batch.column(col_idx(col::ATTRIBUTES_NUMBER)).as_map();
    let bools = batch.column(col_idx(col::ATTRIBUTES_BOOL)).as_map();
    for row in 0..batch.num_rows() {
        assert!(
            strings.value_length(row) + numbers.value_length(row) + bools.value_length(row) <= 1
        );
    }
}

#[test]
fn builder_is_reusable_after_finish() {
    let mut builder = SpanBatchBuilder::default();
    builder.append_request(&fixture()).unwrap();
    let first = builder.finish().unwrap();
    assert!(builder.is_empty());
    builder.append_request(&fixture()).unwrap();
    let second = builder.finish().unwrap();
    assert_eq!(first.num_rows(), second.num_rows());
    assert_eq!(first, second);
}

#[test]
fn promoted_registry_matches_schema_tail() {
    let schema = span_schema_v1();
    let tail = &schema.fields()[schema.fields().len() - PROMOTED.len()..];
    for (field, spec) in tail.iter().zip(PROMOTED) {
        assert_eq!(field.name(), spec.column);
        assert!(field.is_nullable());
    }
    let _ = Arc::clone(&schema);
}
