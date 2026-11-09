use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

use super::*;
use crate::limits::Limits;
use crate::spans::SpanBatchBuilder;

const HAS_IS_REMOTE: u32 = 0x100;
const IS_REMOTE: u32 = 0x200;

fn resource(service: Option<&str>, spans: Vec<Span>) -> ResourceSpans {
    ResourceSpans {
        resource: Some(Resource {
            attributes: service
                .map(|service| KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(service.into())),
                    }),
                    ..KeyValue::default()
                })
                .into_iter()
                .collect(),
            ..Resource::default()
        }),
        scope_spans: vec![ScopeSpans {
            spans,
            ..ScopeSpans::default()
        }],
        ..ResourceSpans::default()
    }
}

fn span(name: &str, id: u8, parent: Option<u8>, flags: u32, start: u64) -> Span {
    Span {
        trace_id: vec![1; 16],
        span_id: vec![id; 8],
        parent_span_id: parent.map(|parent| vec![parent; 8]).unwrap_or_default(),
        flags,
        name: name.into(),
        start_time_unix_nano: start,
        end_time_unix_nano: start + 10,
        ..Span::default()
    }
}

fn batch(resource_spans: Vec<ResourceSpans>) -> RecordBatch {
    let mut builder = SpanBatchBuilder::new(Limits::default());
    builder
        .append_request(&ExportTraceServiceRequest { resource_spans })
        .unwrap();
    builder.finish().unwrap()
}

fn sample() -> RecordBatch {
    batch(vec![
        resource(
            Some("api"),
            vec![
                span("GET /", 1, None, 0, 100),
                span("GET /", 2, None, 0, 300),
                span("db", 3, Some(1), HAS_IS_REMOTE, 110),
                span("unknown", 4, Some(1), 0, 120),
            ],
        ),
        resource(
            Some("worker"),
            vec![span("consume", 5, Some(3), HAS_IS_REMOTE | IS_REMOTE, 200)],
        ),
        resource(None, vec![span("orphan", 6, None, 0, 50)]),
    ])
}

fn rows(operations: &TopLevelOperations) -> Vec<(&str, &str, i64)> {
    operations.iter().collect()
}

#[test]
fn roots_and_remote_parents_count_and_nothing_is_inferred() {
    let mut operations = TopLevelOperations::default();
    operations.absorb(&sample(), 10, usize::MAX).unwrap();
    assert_eq!(
        rows(&operations),
        [("api", "GET /", 300), ("worker", "consume", 200)],
        "a local parent and an unknown one do not count, nor a span without a service"
    );
    assert_eq!(operations.range(), Some((100, 300)));
}

#[test]
fn a_contribution_round_trips_and_merges_by_latest_start() {
    let mut operations = TopLevelOperations::default();
    operations.absorb(&sample(), 10, usize::MAX).unwrap();
    let bytes = encode(&operations).unwrap();
    let decoded = decode(bytes.into()).unwrap();
    assert_eq!(rows(&decoded), rows(&operations));

    let mut later = TopLevelOperations::default();
    later
        .absorb(
            &batch(vec![resource(
                Some("api"),
                vec![
                    span("GET /", 7, None, 0, 900),
                    span("POST /", 8, None, 0, 40),
                ],
            )]),
            10,
            usize::MAX,
        )
        .unwrap();
    let mut merged = decoded;
    merged.merge(later);
    assert_eq!(
        rows(&merged),
        [
            ("api", "GET /", 900),
            ("api", "POST /", 40),
            ("worker", "consume", 200)
        ]
    );
}

#[test]
fn committed_format_v1_fixture_remains_readable() {
    let mut expected = TopLevelOperations::default();
    expected.absorb(&sample(), 10, usize::MAX).unwrap();
    let bytes = bytes::Bytes::from_static(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/top_level_operations_v1.parquet"
    )));
    assert_eq!(rows(&decode(bytes).unwrap()), rows(&expected));
}

#[test]
fn crossing_the_operation_cap_emits_no_partial_table() {
    let mut operations = TopLevelOperations::default();
    operations.absorb(&sample(), 1, usize::MAX).unwrap();
    assert!(operations.skipped());
    assert_eq!(operations.len(), 0);
    assert_eq!(operations.range(), None);
    assert_eq!(
        operations.limit_exceeded().map(|limit| limit.limit),
        Some("top_level_operations_max_operations")
    );
    operations
        .absorb(&sample(), usize::MAX, usize::MAX)
        .unwrap();
    assert_eq!(
        operations.len(),
        0,
        "a skipped pass never resumes partially"
    );
}

#[test]
fn byte_cap_is_charged_before_the_first_operation_is_retained() {
    let first = OPERATION_ENTRY_BYTES + "api".len() + "GET /".len();
    let mut operations = TopLevelOperations::default();
    operations.absorb(&sample(), usize::MAX, first - 1).unwrap();
    assert!(operations.skipped());
    assert_eq!(operations.estimated_bytes(), 0);
    assert_eq!(
        operations.limit_exceeded(),
        Some(LimitExceeded {
            limit: "top_level_operations_max_bytes",
            cap: first - 1,
            observed: first,
            estimated_bytes: first,
        })
    );

    let mut operations = TopLevelOperations::default();
    operations
        .absorb(&sample(), usize::MAX, usize::MAX)
        .unwrap();
    assert_eq!(
        operations.estimated_bytes(),
        first + OPERATION_ENTRY_BYTES + "worker".len() + "consume".len()
    );
}

#[test]
fn another_version_or_nullable_column_is_refused() {
    let expected = schema();
    let mut metadata = expected.metadata().clone();
    metadata.insert("fusescope.schema_version".into(), "2".into());
    let future = Schema::new_with_metadata(expected.fields().clone(), metadata);
    assert!(check_schema(&future).is_err());
    let mut fields = expected.fields().to_vec();
    *fields.first_mut().unwrap() =
        Arc::new(Field::new(columns::SERVICE_NAME, DataType::Utf8, true));
    let nullable = Schema::new_with_metadata(fields, expected.metadata().clone());
    assert!(check_schema(&nullable).is_err());
    let mut metadata = expected.metadata().clone();
    metadata.insert("fusescope.signal".into(), "trace_summary".into());
    let other = Schema::new_with_metadata(expected.fields().clone(), metadata);
    assert!(check_schema(&other).is_err());
}
