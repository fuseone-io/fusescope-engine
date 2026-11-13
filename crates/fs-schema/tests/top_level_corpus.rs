//! Top-level operations against the D7 observation corpus: each case's
//! exports, one compaction pass over all of them, must list what the
//! ClickHouse pipeline wrote for a case sent in one export. Where the
//! pipeline saw parent and child in separate exports it missed the
//! boundary; the pass sees both, and lists those children besides.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::collections::BTreeSet;

use fs_schema::limits::Limits;
use fs_schema::spans::SpanBatchBuilder;
use fs_schema::top_level_operations::TopLevelOperations;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use serde_json::Value as Json;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/top_level_operations_observed.json"
);

/// Cases whose parent and child crossed services in separate exports, and
/// the operation the pass lists that the pipeline did not.
const BOUNDARY_ACROSS_EXPORTS: &[(&str, (&str, &str))] = &[
    ("cross_child_parent_first_export", ("child", "b")),
    ("cross_child_child_first_export", ("child", "b")),
    ("cross_child_local_flag_parent_first_export", ("child", "b")),
    ("cross_child_local_flag_child_first_export", ("child", "b")),
];

fn hex(value: &Json) -> Vec<u8> {
    let text = value.as_str().unwrap();
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

fn span(json: &Json) -> Span {
    Span {
        trace_id: hex(&json["traceId"]),
        span_id: hex(&json["spanId"]),
        parent_span_id: json.get("parentSpanId").map(hex).unwrap_or_default(),
        name: json["name"].as_str().unwrap().to_owned(),
        kind: i32::try_from(json["kind"].as_i64().unwrap()).unwrap(),
        flags: u32::try_from(json["flags"].as_u64().unwrap()).unwrap(),
        start_time_unix_nano: json["startTimeUnixNano"].as_str().unwrap().parse().unwrap(),
        end_time_unix_nano: json["endTimeUnixNano"].as_str().unwrap().parse().unwrap(),
        ..Span::default()
    }
}

fn request(json: &Json) -> ExportTraceServiceRequest {
    let resource_spans = json["resourceSpans"]
        .as_array()
        .unwrap()
        .iter()
        .map(|rs| ResourceSpans {
            resource: Some(Resource {
                attributes: rs["resource"]["attributes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|kv| KeyValue {
                        key: kv["key"].as_str().unwrap().to_owned(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue(
                                kv["value"]["stringValue"].as_str().unwrap().to_owned(),
                            )),
                        }),
                        ..KeyValue::default()
                    })
                    .collect(),
                ..Resource::default()
            }),
            scope_spans: rs["scopeSpans"]
                .as_array()
                .unwrap()
                .iter()
                .map(|ss| ScopeSpans {
                    spans: ss["spans"].as_array().unwrap().iter().map(span).collect(),
                    ..ScopeSpans::default()
                })
                .collect(),
            ..ResourceSpans::default()
        })
        .collect();
    ExportTraceServiceRequest { resource_spans }
}

/// `(name, service suffix)` the pass lists for a case's exports.
fn pass(case: &Json) -> BTreeSet<(String, String)> {
    let mut operations = TopLevelOperations::default();
    let mut prefix = None;
    for export in case["exports"].as_array().unwrap() {
        let mut builder = SpanBatchBuilder::new(Limits::default());
        builder.append_request(&request(export)).unwrap();
        operations
            .absorb(&builder.finish().unwrap(), usize::MAX, usize::MAX)
            .unwrap();
        for rs in export["resourceSpans"].as_array().unwrap() {
            if let Some(kv) = rs["resource"]["attributes"].as_array().unwrap().first() {
                let service = kv["value"]["stringValue"].as_str().unwrap();
                prefix = Some(service[..service.len() - 1].to_owned());
            }
        }
    }
    operations.finish();
    let prefix = prefix.unwrap();
    operations
        .iter()
        .map(|(service, name, _)| {
            (
                name.to_owned(),
                service.strip_prefix(&prefix).unwrap().to_owned(),
            )
        })
        .collect()
}

#[test]
fn the_corpus_records_its_pipeline_one_export_per_batch() {
    let fixture: Json = serde_json::from_str(&std::fs::read_to_string(FIXTURE).unwrap()).unwrap();
    let versions = &fixture["versions"];
    assert!(versions["collector"].as_str().unwrap().contains("@sha256:"));
    assert_ne!(versions["clickhouse"].as_str().unwrap().len(), 0);
    let spacing: f64 = versions["spacing_s"].as_str().unwrap().parse().unwrap();
    assert!(spacing > 1.0, "exports spaced past the 1s batch flush");
    assert!(fixture["cases"].as_array().unwrap().len() >= 30);
}

#[test]
fn a_pass_lists_what_the_pipeline_wrote_and_the_boundaries_it_missed_across_exports() {
    let fixture: Json = serde_json::from_str(&std::fs::read_to_string(FIXTURE).unwrap()).unwrap();
    let mut differing = Vec::new();
    for case in fixture["cases"].as_array().unwrap() {
        let name = case["case"].as_str().unwrap();
        let mut expected: BTreeSet<(String, String)> = case["observed"]
            .as_array()
            .unwrap()
            .iter()
            .map(|pair| {
                (
                    pair[0].as_str().unwrap().to_owned(),
                    pair[1].as_str().unwrap().to_owned(),
                )
            })
            .collect();
        if let Some((_, (op, service))) = BOUNDARY_ACROSS_EXPORTS.iter().find(|(c, _)| *c == name) {
            assert!(case["exports"].as_array().unwrap().len() > 1, "{name}");
            assert!(
                expected.insert(((*op).to_owned(), (*service).to_owned())),
                "{name} is listed already"
            );
        }
        let listed = pass(case);
        if listed != expected {
            differing.push(format!("{name}: pass {listed:?}, expected {expected:?}"));
        }
    }
    assert!(differing.is_empty(), "{}", differing.join("\n"));
}
