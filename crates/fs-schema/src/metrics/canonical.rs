//! The canonical text of an OTLP attribute value, as a metric label holds
//! it and as the fingerprint hashes it (`MAPPING.md`, "Metrics").
//!
//! - string: as received;
//! - int: decimal;
//! - double: the shortest decimal that reads back as the same value, never
//!   in exponent form, no trailing zeros, `-0` kept; `NaN`, `+Inf`, `-Inf`;
//! - bool: `true` or `false`;
//! - bytes: standard base64;
//! - array: a JSON array, items in order;
//! - key-value list: a JSON object, a key with no value ignored, a
//!   repeated key keeping its last value, keys in byte order, nested lists
//!   alike;
//! - inside JSON, doubles as above, non-finite ones and bytes as JSON
//!   strings, an empty value as `null`, strings escaped as RFC 8259
//!   requires and no further.
//!
//! The text is cut at a byte cap on a UTF-8 boundary, and rendering stops
//! there, so a value is never held past the cap.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use opentelemetry_proto::tonic::common::v1::AnyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value;

/// Bytes of base64 encoded at a time, into a buffer on the stack.
const BASE64_SLICE: usize = 3 * 1024;

/// Where canonical text goes: a capped string, or a count of what would
/// be written.
pub(crate) trait Text: std::fmt::Write {
    /// Appends `s`, cut at the cap.
    fn push(&mut self, s: &str);
    /// True once the cap is reached.
    fn full(&self) -> bool;
    /// True when nothing is kept but the length.
    fn counting(&self) -> bool {
        false
    }
}

/// Text up to a byte cap: what is written past it is dropped, and the text
/// kept ends on a UTF-8 boundary.
pub(crate) struct Capped<'a> {
    out: &'a mut String,
    cap: usize,
    /// Set once a write did not fit.
    pub(crate) truncated: bool,
}

impl<'a> Capped<'a> {
    pub(crate) fn new(out: &'a mut String, cap: usize) -> Self {
        Self {
            out,
            cap,
            truncated: false,
        }
    }
}

impl Text for Capped<'_> {
    fn full(&self) -> bool {
        self.truncated
    }

    fn push(&mut self, s: &str) {
        if self.truncated {
            return;
        }
        let room = self.cap.saturating_sub(self.out.len());
        if s.len() <= room {
            self.out.push_str(s);
            return;
        }
        let mut end = room;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        self.out.push_str(s.get(..end).unwrap_or_default());
        self.truncated = true;
    }
}

impl std::fmt::Write for Capped<'_> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.push(s);
        Ok(())
    }
}

/// The length of a text up to a cap, kept without the text: at least that
/// of the text, a key-value list counted with every entry, repeats too.
struct Counted {
    len: usize,
    cap: usize,
}

impl Text for Counted {
    fn full(&self) -> bool {
        self.len >= self.cap
    }

    fn push(&mut self, s: &str) {
        self.len = self.len.saturating_add(s.len()).min(self.cap);
    }

    fn counting(&self) -> bool {
        true
    }
}

impl std::fmt::Write for Counted {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.push(s);
        Ok(())
    }
}

/// At least the bytes of the canonical text of `value` cut at `cap`, and
/// at most `cap`, found without allocating.
pub(crate) fn rendered_len(value: &Value, cap: usize) -> usize {
    let mut out = Counted { len: 0, cap };
    let _ = render(value, &mut out);
    out.len
}

/// The most bytes rendering `value` holds besides its text: of each
/// key-value list on the way to its deepest, the entries and their sort.
pub(crate) fn render_scratch(value: &Value) -> usize {
    match value {
        Value::ArrayValue(array) => array
            .values
            .iter()
            .filter_map(|v| v.value.as_ref())
            .map(render_scratch)
            .max()
            .unwrap_or(0),
        Value::KvlistValue(list) => {
            let entries = list.values.len().saturating_mul(2 * KV_ENTRY);
            let inner = list
                .values
                .iter()
                .filter_map(|kv| kv.value.as_ref().and_then(|v| v.value.as_ref()))
                .map(render_scratch)
                .max()
                .unwrap_or(0);
            entries.saturating_add(inner)
        }
        _ => 0,
    }
}

/// Bytes of one entry of a key-value list being rendered.
const KV_ENTRY: usize = std::mem::size_of::<(&str, &AnyValue)>();

/// Writes the canonical text of `value`; `None` for a value with none, an
/// empty `AnyValue` or a string table reference.
pub(crate) fn render(value: &Value, out: &mut impl Text) -> Option<()> {
    match value {
        Value::StringValue(s) => out.push(s),
        Value::BoolValue(b) => out.push(if *b { "true" } else { "false" }),
        Value::IntValue(i) => {
            let _ = write!(out, "{i}");
        }
        Value::DoubleValue(d) => double(*d, out),
        Value::BytesValue(b) => base64(b, out),
        Value::ArrayValue(_) | Value::KvlistValue(_) => json(value, out),
        Value::StringValueStrindex(_) => return None,
    }
    Some(())
}

/// The canonical decimal of `d`. Rust's `Display` for `f64` is the shortest
/// text that parses back to the same value and never uses an exponent.
pub(crate) fn double(d: f64, out: &mut impl Text) {
    if d.is_nan() {
        out.push("NaN");
    } else if d.is_infinite() {
        out.push(if d > 0.0 { "+Inf" } else { "-Inf" });
    } else {
        let _ = write!(out, "{d}");
    }
}

fn base64(bytes: &[u8], out: &mut impl Text) {
    let mut text = [0u8; BASE64_SLICE / 3 * 4];
    // In slices of three bytes, which encode to whole base64 quads, so the
    // text is the encoding of the whole and stops at the cap.
    for chunk in bytes.chunks(BASE64_SLICE) {
        if out.full() {
            return;
        }
        let n = BASE64.encode_slice(chunk, &mut text).unwrap_or(0);
        out.push(std::str::from_utf8(text.get(..n).unwrap_or_default()).unwrap_or_default());
    }
}

fn json(value: &Value, out: &mut impl Text) {
    if out.full() {
        return;
    }
    match value {
        Value::StringValue(s) => json_string(s, out),
        Value::BoolValue(b) => out.push(if *b { "true" } else { "false" }),
        Value::IntValue(i) => {
            let _ = write!(out, "{i}");
        }
        Value::DoubleValue(d) if d.is_finite() => double(*d, out),
        Value::DoubleValue(d) => {
            out.push("\"");
            double(*d, out);
            out.push("\"");
        }
        Value::BytesValue(b) => {
            out.push("\"");
            base64(b, out);
            out.push("\"");
        }
        Value::ArrayValue(array) => {
            out.push("[");
            for (i, item) in array.values.iter().enumerate() {
                if out.full() {
                    return;
                }
                if i > 0 {
                    out.push(",");
                }
                json_any(item, out);
            }
            out.push("]");
        }
        Value::KvlistValue(list) if out.counting() => {
            out.push("{");
            for kv in &list.values {
                if let Some(item) = kv.value.as_ref() {
                    if out.full() {
                        return;
                    }
                    out.push(",");
                    json_string(&kv.key, out);
                    out.push(":");
                    json_any(item, out);
                }
            }
            out.push("}");
        }
        Value::KvlistValue(list) => {
            let mut entries: Vec<(&str, &AnyValue)> = Vec::with_capacity(list.values.len());
            for kv in &list.values {
                if let Some(value) = kv.value.as_ref() {
                    entries.push((kv.key.as_str(), value));
                }
            }
            // A stable sort keeps a repeated key's occurrences in order, so
            // the last of each run is the last received.
            entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            out.push("{");
            let mut first = true;
            for (i, (key, item)) in entries.iter().enumerate() {
                if out.full() {
                    return;
                }
                if entries.get(i + 1).is_some_and(|next| next.0 == *key) {
                    continue;
                }
                if !first {
                    out.push(",");
                }
                first = false;
                json_string(key, out);
                out.push(":");
                json_any(item, out);
            }
            out.push("}");
        }
        Value::StringValueStrindex(_) => out.push("null"),
    }
}

fn json_any(value: &AnyValue, out: &mut impl Text) {
    match value.value.as_ref() {
        Some(v) => json(v, out),
        None => out.push("null"),
    }
}

fn json_string(s: &str, out: &mut impl Text) {
    out.push("\"");
    let mut start = 0;
    for (i, c) in s.char_indices() {
        let escaped = match c {
            '"' => "\\\"",
            '\\' => "\\\\",
            '\u{8}' => "\\b",
            '\u{c}' => "\\f",
            '\n' => "\\n",
            '\r' => "\\r",
            '\t' => "\\t",
            c if u32::from(c) < 0x20 => "",
            _ => continue,
        };
        out.push(s.get(start..i).unwrap_or_default());
        if escaped.is_empty() {
            let _ = write!(out, "\\u{:04x}", u32::from(c));
        } else {
            out.push(escaped);
        }
        start = i + c.len_utf8();
        if out.full() {
            return;
        }
    }
    out.push(s.get(start..).unwrap_or_default());
    out.push("\"");
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::{ArrayValue, KeyValue, KeyValueList};

    use super::*;

    fn text(value: &Value, cap: usize) -> (String, bool) {
        let mut out = String::new();
        let mut capped = Capped::new(&mut out, cap);
        render(value, &mut capped).unwrap();
        let truncated = capped.truncated;
        (out, truncated)
    }

    fn any(value: Value) -> AnyValue {
        AnyValue { value: Some(value) }
    }

    fn kv(key: &str, value: Value) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(any(value)),
            ..KeyValue::default()
        }
    }

    #[test]
    fn doubles_are_the_shortest_decimal_that_reads_back() {
        for (d, want) in [
            (1.0, "1"),
            (0.5, "0.5"),
            (0.000_01, "0.00001"),
            (1_000_000.0, "1000000"),
            (-0.0, "-0"),
            (0.1 + 0.2, "0.30000000000000004"),
            (f64::NAN, "NaN"),
            (f64::INFINITY, "+Inf"),
            (f64::NEG_INFINITY, "-Inf"),
        ] {
            assert_eq!(text(&Value::DoubleValue(d), usize::MAX).0, want);
        }
        let smallest = text(&Value::DoubleValue(f64::from_bits(1)), usize::MAX).0;
        assert_eq!(smallest.len(), 326, "5e-324 written out in full");
        assert!(smallest.starts_with("0.000") && smallest.ends_with('5'));
        assert_eq!(smallest.parse::<f64>().unwrap().to_bits(), 1);
        let largest = text(&Value::DoubleValue(f64::MAX), usize::MAX).0;
        assert_eq!(largest.len(), 309);
        assert_eq!(largest.parse::<f64>().unwrap(), f64::MAX);
    }

    #[test]
    fn maps_sort_their_keys_at_every_level_and_keep_the_last_of_a_repeat() {
        let inner = Value::KvlistValue(KeyValueList {
            values: vec![
                kv("z", Value::IntValue(1)),
                kv("a", Value::DoubleValue(f64::NAN)),
            ],
        });
        let outer = Value::KvlistValue(KeyValueList {
            values: vec![
                kv("é", Value::BoolValue(true)),
                kv("b", Value::StringValue("first".into())),
                kv("a", inner),
                kv("b", Value::StringValue("last\n\"".into())),
                kv(
                    "c",
                    Value::ArrayValue(ArrayValue {
                        values: vec![
                            any(Value::IntValue(2)),
                            AnyValue { value: None },
                            any(Value::BytesValue(vec![0xff])),
                            any(Value::DoubleValue(-0.0)),
                        ],
                    }),
                ),
                KeyValue {
                    key: "d".into(),
                    value: None,
                    ..KeyValue::default()
                },
            ],
        });
        assert_eq!(
            text(&outer, usize::MAX).0,
            r#"{"a":{"a":"NaN","z":1},"b":"last\n\"","c":[2,null,"/w==",-0],"é":true}"#
        );
        let control = Value::KvlistValue(KeyValueList {
            values: vec![kv("k\u{1}", Value::StringValue("\u{7f}/".into()))],
        });
        assert_eq!(text(&control, usize::MAX).0, "{\"k\\u0001\":\"\u{7f}/\"}");
    }

    /// The length found without the text is never short of the text,
    /// whatever the cap, and is exact but for a list's repeated keys.
    #[test]
    fn a_counted_length_covers_the_text_without_building_it() {
        let nested = Value::KvlistValue(KeyValueList {
            values: vec![
                kv("b", Value::StringValue("first\u{1}".into())),
                kv("a", Value::BytesValue(vec![1; 5000])),
                kv("b", Value::DoubleValue(f64::NAN)),
                kv(
                    "c",
                    Value::ArrayValue(ArrayValue {
                        values: vec![any(Value::IntValue(-7)), AnyValue { value: None }],
                    }),
                ),
            ],
        });
        let values = [
            Value::StringValue("aé\"".into()),
            Value::IntValue(i64::MIN),
            Value::DoubleValue(f64::from_bits(1)),
            Value::BoolValue(false),
            Value::BytesValue(vec![9; 7000]),
            nested.clone(),
        ];
        for value in &values {
            for cap in [0, 1, 3, 10, 100, 8192, usize::MAX] {
                let (text, _) = text(value, cap);
                let counted = rendered_len(value, cap);
                assert!(counted >= text.len() && counted <= cap, "{value:?} {cap}");
                if !matches!(value, Value::KvlistValue(_)) {
                    assert_eq!(counted, text.len(), "{value:?} {cap}");
                }
            }
        }
        assert_eq!(render_scratch(&Value::IntValue(1)), 0);
        assert_eq!(render_scratch(&nested), 4 * 2 * KV_ENTRY);
    }

    #[test]
    fn text_stops_at_its_cap_on_a_character_boundary() {
        let (cut, truncated) = text(&Value::StringValue("aé".into()), 2);
        assert_eq!((cut.as_str(), truncated), ("a", true));
        let (fits, truncated) = text(&Value::StringValue("aé".into()), 3);
        assert_eq!((fits.as_str(), truncated), ("aé", false));
        let long = Value::ArrayValue(ArrayValue {
            values: (0..100_000).map(|i| any(Value::IntValue(i))).collect(),
        });
        let (head, truncated) = text(&long, 8);
        assert_eq!((head.as_str(), truncated), ("[0,1,2,3", true));
        let bytes = Value::BytesValue(vec![7; 10_000]);
        let (encoded, _) = text(&bytes, usize::MAX);
        assert_eq!(encoded, BASE64.encode(vec![7u8; 10_000]));
    }
}
