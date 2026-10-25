//! Splits OTLP attribute lists into the typed maps the schema stores.
//!
//! Rules (see `MAPPING.md`, section "Attributes"):
//! - string → `attributes_string`
//! - int and double → `attributes_number` as f64
//! - bool → `attributes_bool`
//! - bytes → `attributes_string`, standard base64
//! - array and key-value list → `attributes_string`, JSON text
//! - empty value → dropped
//!
//! Resource, scope, event and link attributes are stored as string-only maps,
//! so numbers and booleans are rendered as text there.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};

use crate::limits::{DropStats, Limits, truncate_utf8};

/// Attributes split by storage type.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct TypedAttributes {
    /// String, bytes, array and key-value list values.
    pub strings: Vec<(String, String)>,
    /// Integer and floating point values.
    pub numbers: Vec<(String, f64)>,
    /// Boolean values.
    pub bools: Vec<(String, bool)>,
}

impl TypedAttributes {
    /// Total attributes kept across the three maps.
    pub fn len(&self) -> usize {
        self.strings.len() + self.numbers.len() + self.bools.len()
    }

    /// True when no attribute was kept.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Looks up a string attribute by key.
    pub fn string(&self, key: &str) -> Option<&str> {
        self.strings
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// Looks up a numeric attribute by key.
    pub fn number(&self, key: &str) -> Option<f64> {
        self.numbers.iter().find(|(k, _)| k == key).map(|(_, v)| *v)
    }
}

/// Splits `attributes` into typed maps, applying `limits` and counting drops.
pub fn split_typed(
    attributes: &[KeyValue],
    limits: &Limits,
    stats: &mut DropStats,
) -> TypedAttributes {
    let mut out = TypedAttributes::default();
    for kv in attributes {
        if out.len() >= limits.max_attributes || !key_ok(&kv.key, limits) {
            stats.attributes_dropped += 1;
            continue;
        }
        match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
            Some(Value::StringValue(s)) => {
                out.strings
                    .push((kv.key.clone(), bounded(s, limits, stats)));
            }
            Some(Value::BoolValue(b)) => out.bools.push((kv.key.clone(), *b)),
            // i64 → f64 loses precision above 2^53; accepted and documented.
            #[allow(clippy::cast_precision_loss)]
            Some(Value::IntValue(i)) => out.numbers.push((kv.key.clone(), *i as f64)),
            Some(Value::DoubleValue(d)) => out.numbers.push((kv.key.clone(), *d)),
            Some(Value::ArrayValue(_) | Value::KvlistValue(_) | Value::BytesValue(_)) => {
                let rendered = render_complex(kv.value.as_ref().and_then(|v| v.value.as_ref()));
                out.strings
                    .push((kv.key.clone(), bounded(&rendered, limits, stats)));
            }
            // String table references need a table this signal does not carry.
            Some(Value::StringValueStrindex(_)) | None => stats.attributes_dropped += 1,
        }
    }
    out
}

/// Renders every attribute as text, for resource, scope, event and link maps.
pub fn split_strings(
    attributes: &[KeyValue],
    limits: &Limits,
    stats: &mut DropStats,
) -> Vec<(String, String)> {
    let mut out = Vec::with_capacity(attributes.len().min(limits.max_attributes));
    for kv in attributes {
        if out.len() >= limits.max_attributes || !key_ok(&kv.key, limits) {
            stats.attributes_dropped += 1;
            continue;
        }
        match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
            Some(Value::StringValueStrindex(_)) | None => stats.attributes_dropped += 1,
            Some(value) => {
                let rendered = render_any(value);
                out.push((kv.key.clone(), bounded(&rendered, limits, stats)));
            }
        }
    }
    out
}

fn key_ok(key: &str, limits: &Limits) -> bool {
    !key.is_empty() && key.len() <= limits.max_key_bytes
}

fn bounded(value: &str, limits: &Limits, stats: &mut DropStats) -> String {
    let cut = truncate_utf8(value, limits.max_value_bytes);
    if cut.len() < value.len() {
        stats.values_truncated += 1;
    }
    cut.to_owned()
}

/// Text form of any value. Scalars render plainly; bytes as base64; arrays and
/// key-value lists as JSON.
fn render_any(value: &Value) -> String {
    match value {
        Value::StringValue(s) => s.clone(),
        Value::BoolValue(b) => b.to_string(),
        Value::IntValue(i) => i.to_string(),
        Value::DoubleValue(d) => d.to_string(),
        other => render_complex(Some(other)),
    }
}

/// Bytes as base64, arrays and key-value lists as JSON text.
fn render_complex(value: Option<&Value>) -> String {
    match value {
        Some(Value::BytesValue(b)) => BASE64.encode(b),
        Some(other) => to_json(other).to_string(),
        None => String::new(),
    }
}

fn to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::StringValue(s) => serde_json::Value::String(s.clone()),
        Value::BoolValue(b) => serde_json::Value::Bool(*b),
        Value::IntValue(i) => serde_json::Value::from(*i),
        Value::DoubleValue(d) => serde_json::Number::from_f64(*d)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Value::BytesValue(b) => serde_json::Value::String(BASE64.encode(b)),
        Value::ArrayValue(array) => {
            serde_json::Value::Array(array.values.iter().map(any_to_json).collect())
        }
        Value::KvlistValue(list) => serde_json::Value::Object(
            list.values
                .iter()
                .map(|kv| {
                    (
                        kv.key.clone(),
                        kv.value
                            .as_ref()
                            .map_or(serde_json::Value::Null, any_to_json),
                    )
                })
                .collect(),
        ),
        // Unresolvable without the string table.
        Value::StringValueStrindex(_) => serde_json::Value::Null,
    }
}

fn any_to_json(value: &AnyValue) -> serde_json::Value {
    value
        .value
        .as_ref()
        .map_or(serde_json::Value::Null, to_json)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{ArrayValue, KeyValueList};

    fn kv(key: &str, value: Value) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue { value: Some(value) }),
            ..KeyValue::default()
        }
    }

    #[test]
    fn routes_scalars_by_type() {
        let attrs = [
            kv("s", Value::StringValue("x".into())),
            kv("i", Value::IntValue(7)),
            kv("d", Value::DoubleValue(2.5)),
            kv("b", Value::BoolValue(true)),
        ];
        let mut stats = DropStats::default();
        let typed = split_typed(&attrs, &Limits::default(), &mut stats);
        assert_eq!(typed.string("s"), Some("x"));
        assert_eq!(typed.number("i"), Some(7.0));
        assert_eq!(typed.number("d"), Some(2.5));
        assert_eq!(typed.bools, vec![("b".to_owned(), true)]);
        assert!(stats.is_clean());
    }

    #[test]
    fn complex_values_become_text() {
        let array = Value::ArrayValue(ArrayValue {
            values: vec![
                AnyValue {
                    value: Some(Value::IntValue(1)),
                },
                AnyValue {
                    value: Some(Value::StringValue("a".into())),
                },
            ],
        });
        let kvlist = Value::KvlistValue(KeyValueList {
            values: vec![kv("k", Value::BoolValue(false))],
        });
        let attrs = [
            kv("arr", array),
            kv("map", kvlist),
            kv("bin", Value::BytesValue(vec![0xff, 0x00])),
        ];
        let mut stats = DropStats::default();
        let typed = split_typed(&attrs, &Limits::default(), &mut stats);
        assert_eq!(typed.string("arr"), Some(r#"[1,"a"]"#));
        assert_eq!(typed.string("map"), Some(r#"{"k":false}"#));
        assert_eq!(typed.string("bin"), Some("/wA="));
        assert_eq!(typed.numbers.len(), 0);
    }

    #[test]
    fn limits_drop_and_truncate() {
        let limits = Limits {
            max_attributes: 2,
            max_key_bytes: 3,
            max_value_bytes: 4,
            ..Limits::default()
        };
        let attrs = [
            kv("ok", Value::StringValue("123456".into())),
            kv("toolong", Value::StringValue("x".into())),
            kv("", Value::StringValue("x".into())),
            kv("b", Value::BoolValue(true)),
            kv("c", Value::BoolValue(true)),
            KeyValue {
                key: "none".into(),
                value: None,
                ..KeyValue::default()
            },
        ];
        let mut stats = DropStats::default();
        let typed = split_typed(&attrs, &limits, &mut stats);
        assert_eq!(typed.string("ok"), Some("1234"));
        assert_eq!(typed.len(), 2);
        assert_eq!(stats.values_truncated, 1);
        // "toolong" (key), "" (key), "c" (over max), "none" (empty value)
        assert_eq!(stats.attributes_dropped, 4);
    }

    #[test]
    fn string_only_maps_render_everything() {
        let attrs = [
            kv("i", Value::IntValue(3)),
            kv("d", Value::DoubleValue(0.5)),
            kv("b", Value::BoolValue(true)),
        ];
        let mut stats = DropStats::default();
        let strings = split_strings(&attrs, &Limits::default(), &mut stats);
        assert_eq!(
            strings,
            vec![
                ("i".to_owned(), "3".to_owned()),
                ("d".to_owned(), "0.5".to_owned()),
                ("b".to_owned(), "true".to_owned())
            ]
        );
    }
}
