//! The derived span columns against the observation corpus: each OTLP span
//! of `fixtures/derived_columns_observed.json` goes through the span builder
//! and the derivation, and must read what the ClickHouse pipeline wrote for
//! it, except the cases v1 storage cannot tell apart.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::collections::BTreeMap;

use arrow::array::{Array, AsArray};
use fs_schema::limits::Limits;
use fs_schema::spans::SpanBatchBuilder;
use fs_schema::spans::derived::{self, Derived};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use serde_json::Value as Json;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/derived_columns_observed.json"
);
/// `client.address` and `http.request.header.host`, the keys semconv names
/// in place of the deprecated `http.host`.
const HOST_KEYS_FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/derived_columns_host_keys_observed.json"
);

/// Cases whose answer depends on the order of a string and a non-string
/// attribute, which v1 keeps in separate maps; the columns they differ in.
const UNORDERED_ACROSS_TYPES: &[(&str, &[&str])] = &[
    ("server_address_then_net_peer_name_int", &["http_host"]),
    (
        "url_full_then_http_url_int",
        &["http_url", "external_http_url"],
    ),
    ("method_stable_then_legacy_int", &["external_http_method"]),
];

fn any_value(json: Option<&Json>) -> Option<AnyValue> {
    let object = json?.as_object()?;
    let value = if let Some(s) = object.get("stringValue") {
        Some(Value::StringValue(s.as_str().unwrap().to_owned()))
    } else if let Some(i) = object.get("intValue") {
        Some(Value::IntValue(i.as_str().unwrap().parse().unwrap()))
    } else if let Some(d) = object.get("doubleValue") {
        Some(Value::DoubleValue(d.as_f64().unwrap()))
    } else if let Some(b) = object.get("boolValue") {
        Some(Value::BoolValue(b.as_bool().unwrap()))
    } else if let Some(array) = object.get("arrayValue") {
        let values = array.get("values").map_or_else(Vec::new, |v| {
            v.as_array()
                .unwrap()
                .iter()
                .filter_map(|v| any_value(Some(v)))
                .collect()
        });
        Some(Value::ArrayValue(
            opentelemetry_proto::tonic::common::v1::ArrayValue { values },
        ))
    } else {
        assert!(object.is_empty(), "unexpected value {object:?}");
        None
    };
    Some(AnyValue { value })
}

fn attributes(json: &Json) -> Vec<KeyValue> {
    json.as_array()
        .unwrap()
        .iter()
        .map(|kv| KeyValue {
            key: kv["key"].as_str().unwrap().to_owned(),
            value: any_value(kv.get("value")),
            ..KeyValue::default()
        })
        .collect()
}

fn span_request(case: &Json) -> ExportTraceServiceRequest {
    let span = Span {
        trace_id: vec![0xd6; 16],
        span_id: vec![0x06; 8],
        name: case["case"].as_str().unwrap().to_owned(),
        kind: i32::try_from(case["span"]["kind"].as_i64().unwrap()).unwrap(),
        start_time_unix_nano: 1_792_677_600_000_000_000,
        end_time_unix_nano: 1_792_677_600_000_500_000,
        attributes: attributes(&case["span"]["attributes"]),
        ..Span::default()
    };
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: attributes(&case["resource"]),
                ..Resource::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![span],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// The four derived columns of `case`'s span, as the engine reads them.
fn engine_reads(case: &Json) -> BTreeMap<&'static str, String> {
    let mut builder = SpanBatchBuilder::new(Limits::default());
    builder.append_request(&span_request(case)).unwrap();
    let batch = builder.finish().unwrap();
    let derivation = derived::derive(&batch, &Derived::ALL).unwrap();
    Derived::ALL
        .iter()
        .zip(derivation.into_arrays())
        .map(|(column, array)| {
            assert_eq!(array.len(), 1);
            (column.name(), array.as_string::<i32>().value(0).to_owned())
        })
        .collect()
}

#[test]
fn the_corpus_records_its_pipeline_and_observes_every_case() {
    let fixture: Json = serde_json::from_str(&std::fs::read_to_string(FIXTURE).unwrap()).unwrap();
    let versions = &fixture["versions"];
    assert!(versions["collector"].as_str().unwrap().contains("@sha256:"));
    assert_ne!(versions["clickhouse"].as_str().unwrap().len(), 0);
    let cases = fixture["cases"].as_array().unwrap();
    assert!(cases.len() >= 200, "{} cases", cases.len());
    for case in cases {
        assert!(
            case["observed"].is_object(),
            "{} was not observed",
            case["case"]
        );
    }
    for kind in [
        "unspecified",
        "internal",
        "server",
        "client",
        "producer",
        "consumer",
    ] {
        let name = format!("kind_{kind}_full");
        assert!(cases.iter().any(|c| c["case"] == name.as_str()), "{name}");
    }
}

/// The cases of `path` the engine reads otherwise than observed, beyond
/// [`UNORDERED_ACROSS_TYPES`], and how many of those it differs in.
fn differences(path: &str) -> (Vec<String>, usize) {
    let fixture: Json = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let mut differing = Vec::new();
    let mut unordered = 0;
    for case in fixture["cases"].as_array().unwrap() {
        let name = case["case"].as_str().unwrap();
        let read = engine_reads(case);
        let differs: Vec<&str> = read
            .iter()
            .filter(|(column, value)| {
                case["observed"][**column].as_str().unwrap() != value.as_str()
            })
            .map(|(column, _)| *column)
            .collect();
        let expected: &[&str] = UNORDERED_ACROSS_TYPES
            .iter()
            .find(|(suffix, _)| name.ends_with(suffix))
            .map_or(&[], |(_, columns)| columns);
        let mut expected: Vec<&str> = expected
            .iter()
            .copied()
            .filter(|c| case["observed"][*c] != read[c].as_str())
            .collect();
        expected.sort_unstable();
        if !expected.is_empty() {
            unordered += 1;
        }
        if differs != expected {
            differing.push(format!(
                "{name}: engine {read:?}, observed {}",
                case["observed"]
            ));
        }
    }
    (differing, unordered)
}

#[test]
fn every_observed_case_reads_as_the_clickhouse_pipeline_wrote_it() {
    let (differing, unordered) = differences(FIXTURE);
    assert!(differing.is_empty(), "{}", differing.join("\n"));
    assert!(
        unordered > 0,
        "the unordered cases still differ, or the list is stale"
    );
}

#[test]
fn client_address_and_the_host_header_read_as_observed() {
    let fixture: Json =
        serde_json::from_str(&std::fs::read_to_string(HOST_KEYS_FIXTURE).unwrap()).unwrap();
    let cases = fixture["cases"].as_array().unwrap();
    assert!(cases.iter().all(|c| c["observed"].is_object()));
    for kind in [
        "unspecified",
        "internal",
        "server",
        "client",
        "producer",
        "consumer",
    ] {
        for key in ["client_address", "header_host"] {
            let name = format!("kind_{kind}_{key}");
            assert!(cases.iter().any(|c| c["case"] == name.as_str()), "{name}");
        }
    }
    let (differing, unordered) = differences(HOST_KEYS_FIXTURE);
    assert!(differing.is_empty(), "{}", differing.join("\n"));
    assert_eq!(unordered, 0);
}

#[test]
fn the_derived_columns_are_named_after_the_backend_and_never_null() {
    let names: Vec<&str> = Derived::ALL.iter().map(|d| d.name()).collect();
    assert_eq!(
        names,
        [
            "http_url",
            "http_host",
            "external_http_method",
            "external_http_url"
        ]
    );
    for d in Derived::ALL {
        let field = d.field();
        assert_eq!(field.data_type(), &arrow::datatypes::DataType::Utf8);
        assert!(!field.is_nullable());
    }
    assert_eq!(
        derived::INPUTS,
        [
            "kind",
            "attributes_string",
            "attributes_number",
            "attributes_bool"
        ]
    );
}

#[test]
fn the_derivation_counts_the_bytes_it_will_make_before_making_them() {
    let fixture: Json = serde_json::from_str(&std::fs::read_to_string(FIXTURE).unwrap()).unwrap();
    let mut builder = SpanBatchBuilder::new(Limits::default());
    for case in fixture["cases"].as_array().unwrap() {
        builder.append_request(&span_request(case)).unwrap();
    }
    let batch = builder.finish().unwrap();
    let derivation = derived::derive(&batch, &Derived::ALL).unwrap();
    let counted = derivation.bytes();
    let made: usize = derivation
        .into_arrays()
        .iter()
        .map(arrow::array::Array::get_buffer_memory_size)
        .sum();
    assert!(counted >= made, "counted {counted}, made {made}");
    assert!(
        counted <= made + 64 * Derived::ALL.len(),
        "counted {counted}, made {made}"
    );
}
