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
    traced(1, name, id, parent, flags, start)
}

fn traced(trace: u8, name: &str, id: u8, parent: Option<u8>, flags: u32, start: u64) -> Span {
    Span {
        trace_id: vec![trace; 16],
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

/// The operations of `batches` as one pass makes them.
fn pass(batches: &[RecordBatch], max_operations: usize, max_bytes: usize) -> TopLevelOperations {
    let mut operations = TopLevelOperations::default();
    for batch in batches {
        operations.absorb(batch, max_operations, max_bytes).unwrap();
    }
    operations.finish();
    operations
}

#[test]
fn roots_and_children_of_another_service_count_whatever_their_flags() {
    let operations = pass(&[sample()], 10, usize::MAX);
    assert_eq!(
        rows(&operations),
        [("api", "GET /", 300), ("worker", "consume", 200)],
        "children of their own service do not count, nor a span without a service"
    );
    assert_eq!(operations.range(), Some((100, 300)));

    let flags = batch(vec![
        resource(
            Some("api"),
            vec![
                span("root-local", 1, None, HAS_IS_REMOTE, 10),
                span("child-remote", 2, Some(1), HAS_IS_REMOTE | IS_REMOTE, 20),
            ],
        ),
        resource(
            Some("worker"),
            vec![
                span("cross-local", 3, Some(1), HAS_IS_REMOTE, 30),
                span("cross-unknown", 4, Some(2), 0, 40),
                span("orphan-remote", 5, Some(9), HAS_IS_REMOTE | IS_REMOTE, 50),
            ],
        ),
    ]);
    assert_eq!(
        rows(&pass(&[flags], 10, usize::MAX)),
        [
            ("api", "root-local", 10),
            ("worker", "cross-local", 30),
            ("worker", "cross-unknown", 40)
        ],
        "a remote flag under its own service and a missing parent do not count"
    );
}

#[test]
fn a_parent_without_a_service_makes_its_child_top_level() {
    let operations = pass(
        &[batch(vec![
            resource(None, vec![span("root", 1, None, 0, 10)]),
            resource(Some("api"), vec![span("child", 2, Some(1), 0, 20)]),
        ])],
        10,
        usize::MAX,
    );
    assert_eq!(rows(&operations), [("api", "child", 20)]);
}

#[test]
fn a_child_met_before_its_parent_is_resolved_at_the_end_of_the_pass() {
    let child = batch(vec![resource(
        Some("worker"),
        vec![span("consume", 2, Some(1), 0, 20)],
    )]);
    let parent = batch(vec![resource(
        Some("api"),
        vec![span("GET /", 1, None, 0, 10)],
    )]);
    let mut operations = TopLevelOperations::default();
    operations.absorb(&child, 10, usize::MAX).unwrap();
    operations.absorb(&parent, 10, usize::MAX).unwrap();
    operations.finish();
    assert_eq!(
        rows(&operations),
        [("api", "GET /", 10), ("worker", "consume", 20)]
    );
}

#[test]
fn a_parent_is_found_by_trace_and_span_id_together() {
    let operations = pass(
        &[batch(vec![
            resource(Some("api"), vec![traced(1, "root", 1, None, 0, 10)]),
            resource(
                Some("worker"),
                vec![
                    traced(2, "root", 1, None, 0, 20),
                    traced(2, "child", 2, Some(1), 0, 30),
                ],
            ),
        ])],
        10,
        usize::MAX,
    );
    assert_eq!(
        rows(&operations),
        [("api", "root", 10), ("worker", "root", 20)],
        "the api span with the same span id is in another trace"
    );
}

#[test]
fn the_parent_index_is_charged_to_the_contributions_own_cap() {
    let root = span("root", 1, None, 0, 10);
    let chain: Vec<Span> = std::iter::once(root)
        .chain((2..=200u8).map(|id| span("child", id, Some(id - 1), 0, 10)))
        .collect();
    let chain = batch(vec![resource(Some("api"), chain)]);
    let operations = pass(std::slice::from_ref(&chain), usize::MAX, 4096);
    assert!(
        operations.skipped(),
        "each child finds its parent of the same service: no wait, one operation, yet the index fills"
    );
    assert_eq!(
        operations.limit_exceeded().map(|limit| limit.limit),
        Some("top_level_operations_max_bytes")
    );

    let operations = pass(&[chain], usize::MAX, usize::MAX);
    assert_eq!(rows(&operations), [("api", "root", 10)]);
    assert_eq!(
        operations.estimated_bytes(),
        OPERATION_ENTRY_BYTES + "api".len() + "root".len(),
        "finished, the index is released"
    );
}

#[test]
fn a_contribution_round_trips_and_merges_by_latest_start() {
    let operations = pass(&[sample()], 10, usize::MAX);
    let bytes = encode(&operations).unwrap();
    let decoded = decode(bytes.into()).unwrap();
    assert_eq!(rows(&decoded), rows(&operations));

    let later = pass(
        &[batch(vec![resource(
            Some("api"),
            vec![
                span("GET /", 7, None, 0, 900),
                span("POST /", 8, None, 0, 40),
            ],
        )])],
        10,
        usize::MAX,
    );
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
    let bytes = bytes::Bytes::from_static(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/top_level_operations_v1.parquet"
    )));
    assert_eq!(
        rows(&decode(bytes).unwrap()),
        [("api", "GET /", 300), ("worker", "consume", 200)],
        "written before the parent rule, read as written"
    );
}

#[test]
fn crossing_the_operation_cap_emits_no_partial_table() {
    let mut operations = pass(&[sample()], 1, usize::MAX);
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
    operations.finish();
    assert_eq!(
        operations.len(),
        0,
        "a skipped pass never resumes partially"
    );
}

#[test]
fn byte_cap_is_charged_before_anything_is_retained() {
    let mut operations = TopLevelOperations::default();
    operations.absorb(&sample(), usize::MAX, 1).unwrap();
    assert!(operations.skipped());
    assert_eq!(operations.estimated_bytes(), 0);
    let exceeded = operations.limit_exceeded().unwrap();
    assert_eq!(exceeded.limit, "top_level_operations_max_bytes");
    assert_eq!(exceeded.cap, 1);
    assert!(exceeded.observed > 1);

    let operations = pass(&[sample()], usize::MAX, usize::MAX);
    assert_eq!(
        operations.estimated_bytes(),
        2 * OPERATION_ENTRY_BYTES + "api".len() + "GET /".len() + "worker".len() + "consume".len(),
        "finished, only the operations stay charged"
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

#[test]
fn the_growth_bound_covers_what_a_batch_adds() {
    let crossing = batch(vec![
        resource(Some("worker"), vec![span("consume", 5, Some(3), 0, 200)]),
        resource(
            Some("api"),
            vec![
                span("GET /", 1, None, 0, 100),
                span("db", 3, Some(1), 0, 110),
            ],
        ),
    ]);
    let mut operations = TopLevelOperations::default();
    operations
        .absorb(&crossing, usize::MAX, usize::MAX)
        .unwrap();
    let bound = TopLevelOperations::growth_bound(&crossing).unwrap();
    assert!(
        operations.estimated_bytes() <= bound,
        "{} over {bound}",
        operations.estimated_bytes()
    );
}
