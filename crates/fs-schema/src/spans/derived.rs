//! Span columns derived from the stored kind and attributes when they are
//! read: `http_url`, `http_host`, `external_http_method` and
//! `external_http_url`, never stored, and `http_method`,
//! `response_status_code`, `db_name` and `db_operation`, which replace the
//! promoted columns of those names. The rules and their observed sources
//! are in `MAPPING.md`, section "Derived span columns".
//!
//! Each map is folded in its stored order, which is the OTLP order within
//! one value type, so the last key wins within a type. The order between
//! types is not stored: where a column's keys hold values of different
//! types, a string decides, a number only without a string, a bool only
//! without either.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, DictionaryArray, Int32Array, MapArray, StringArray};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Float64Type, Int8Type, Int32Type};
use arrow::record_batch::RecordBatch;

use super::col;
use crate::SchemaError;

/// The stored columns every derived column is computed from.
pub const INPUTS: [&str; 4] = [
    col::KIND,
    col::ATTRIBUTES_STRING,
    col::ATTRIBUTES_NUMBER,
    col::ATTRIBUTES_BOOL,
];

const CLIENT: i8 = 3;
const URL_KEYS: [&str; 2] = ["url.full", "http.url"];
const HOST_KEYS: [&str; 3] = ["server.address", "net.peer.name", "http.host"];
/// The span columns derived from several keys, the last of them winning,
/// with those keys.
const KEYED: [(Derived, &[&str]); 4] = [
    (Derived::HttpMethod, &["http.method", "http.request.method"]),
    (
        Derived::ResponseStatusCode,
        &["http.status_code", "http.response.status_code"],
    ),
    (Derived::DbName, &["db.name", "db.namespace"]),
    (Derived::DbOperation, &["db.operation", "db.operation.name"]),
];

/// One derived span column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Derived {
    /// The last URL attribute.
    HttpUrl,
    /// The last host attribute; for a client span without one, the host of
    /// its first URL.
    HttpHost,
    /// The method of a client span.
    ExternalHttpMethod,
    /// The host of a client span's URL.
    ExternalHttpUrl,
    /// The last HTTP method attribute.
    HttpMethod,
    /// The last HTTP status code attribute; a gRPC status is not one.
    ResponseStatusCode,
    /// The last database name attribute.
    DbName,
    /// The last database operation attribute.
    DbOperation,
}

impl Derived {
    /// The columns the table shows after the stored ones, in that order.
    pub const APPENDED: [Self; 4] = [
        Self::HttpUrl,
        Self::HttpHost,
        Self::ExternalHttpMethod,
        Self::ExternalHttpUrl,
    ];

    /// The columns that take the place of the promoted columns of their
    /// names.
    pub const REPLACED: [Self; 4] = [
        Self::HttpMethod,
        Self::ResponseStatusCode,
        Self::DbName,
        Self::DbOperation,
    ];

    /// Every derived column.
    pub const ALL: [Self; 8] = [
        Self::HttpUrl,
        Self::HttpHost,
        Self::ExternalHttpMethod,
        Self::ExternalHttpUrl,
        Self::HttpMethod,
        Self::ResponseStatusCode,
        Self::DbName,
        Self::DbOperation,
    ];

    /// The column name, the backend's.
    pub fn name(self) -> &'static str {
        match self {
            Self::HttpUrl => "http_url",
            Self::HttpHost => "http_host",
            Self::ExternalHttpMethod => "external_http_method",
            Self::ExternalHttpUrl => "external_http_url",
            Self::HttpMethod => "http_method",
            Self::ResponseStatusCode => "response_status_code",
            Self::DbName => "db_name",
            Self::DbOperation => "db_operation",
        }
    }

    /// True for a column that takes the place of a stored one.
    pub fn replaces(self) -> bool {
        Self::REPLACED.contains(&self)
    }

    /// The derived column called `name`.
    pub fn of(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|d| d.name() == name)
    }

    /// The column's field: text, `''` where nothing derives it; for a
    /// column that replaces a promoted one, that column's dictionary type
    /// and nullability, though never null.
    pub fn field(self) -> Field {
        if self.replaces() {
            let dictionary =
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
            return Field::new(self.name(), dictionary, true);
        }
        Field::new(self.name(), DataType::Utf8, false)
    }
}

/// The values of the derived columns of one batch, borrowed from it where
/// they are stored text, with the bytes their arrays will take.
#[derive(Debug)]
pub struct Derivation<'a> {
    columns: Vec<Column<'a>>,
    bytes: usize,
}

/// One derived column's values: text, or for a column that replaces a
/// dictionary-encoded one, its keys into its distinct values.
#[derive(Debug)]
enum Column<'a> {
    Text(Vec<Cow<'a, str>>),
    Dictionary {
        keys: Vec<i32>,
        values: Vec<Cow<'a, str>>,
    },
}

impl<'a> Column<'a> {
    fn of(column: Derived, values: Vec<Cow<'a, str>>) -> Result<Self, SchemaError> {
        if !column.replaces() {
            return Ok(Self::Text(values));
        }
        let mut distinct: HashMap<Cow<'a, str>, i32> = HashMap::new();
        let mut keys = Vec::with_capacity(values.len());
        let mut unique = Vec::new();
        for value in values {
            let next = i32::try_from(unique.len())
                .map_err(|_| SchemaError::Layout("derived dictionary keys"))?;
            let key = *distinct.entry(value.clone()).or_insert_with(|| {
                unique.push(value);
                next
            });
            keys.push(key);
        }
        Ok(Self::Dictionary {
            keys,
            values: unique,
        })
    }

    /// The bytes the column's arrays allocate, exactly.
    fn bytes(&self) -> usize {
        match self {
            Self::Text(values) => array_bytes(values),
            Self::Dictionary { keys, values } => {
                keys.len() * std::mem::size_of::<i32>() + array_bytes(values)
            }
        }
    }

    fn into_array(self) -> Result<ArrayRef, SchemaError> {
        Ok(match self {
            Self::Text(values) => string_array(&values),
            Self::Dictionary { keys, values } => Arc::new(DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(keys),
                string_array(&values),
            )?),
        })
    }
}

impl Derivation<'_> {
    /// The bytes [`Derivation::into_arrays`] allocates, exactly.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// The derived arrays, in the order they were asked for.
    ///
    /// # Errors
    /// Arrow refuses a dictionary, which the derivation's keys rule out.
    pub fn into_arrays(self) -> Result<Vec<ArrayRef>, SchemaError> {
        self.columns.into_iter().map(Column::into_array).collect()
    }
}

/// Derives `wanted` for every row of `batch`, which holds at least the
/// [`INPUTS`] with their span schema types.
///
/// # Errors
/// [`SchemaError::Layout`] when an input is missing or of another type.
pub fn derive<'a>(
    batch: &'a RecordBatch,
    wanted: &[Derived],
) -> Result<Derivation<'a>, SchemaError> {
    let kind = batch
        .column_by_name(col::KIND)
        .and_then(|c| c.as_primitive_opt::<Int8Type>())
        .ok_or(SchemaError::Layout(col::KIND))?;
    let strings = map(batch, col::ATTRIBUTES_STRING)?;
    let numbers = map(batch, col::ATTRIBUTES_NUMBER)?;
    let bools = map(batch, col::ATTRIBUTES_BOOL)?;
    let string_values = strings
        .values()
        .as_string_opt::<i32>()
        .ok_or(SchemaError::Layout(col::ATTRIBUTES_STRING))?;
    let number_values = numbers
        .values()
        .as_primitive_opt::<Float64Type>()
        .ok_or(SchemaError::Layout(col::ATTRIBUTES_NUMBER))?;
    let bool_values = bools
        .values()
        .as_boolean_opt()
        .ok_or(SchemaError::Layout(col::ATTRIBUTES_BOOL))?;
    let mut columns: Vec<Vec<Cow<'a, str>>> = wanted
        .iter()
        .map(|_| Vec::with_capacity(batch.num_rows()))
        .collect();
    for row in 0..batch.num_rows() {
        let mut span =
            fold_strings(entries(strings, row).map(|(k, i)| (k, string_values.value(i))));
        for ((_, keys), slot) in KEYED.iter().zip(span.keyed.iter_mut()) {
            if slot.is_none() {
                *slot = last_typed(
                    keys,
                    entries(numbers, row).map(|(k, i)| (k, number_text(number_values.value(i)))),
                )
                .or_else(|| {
                    last_typed(
                        keys,
                        entries(bools, row).map(|(k, i)| (k, bool_values.value(i).to_string())),
                    )
                })
                .map(Cow::Owned);
            }
        }
        let derived = span.finish(kind.value(row) == CLIENT);
        for (out, column) in columns.iter_mut().zip(wanted) {
            out.push(derived.value(*column));
        }
    }
    let columns = columns
        .into_iter()
        .zip(wanted)
        .map(|(values, column)| Column::of(*column, values))
        .collect::<Result<Vec<_>, _>>()?;
    let bytes = columns.iter().map(Column::bytes).sum();
    Ok(Derivation { columns, bytes })
}

/// The host of `url`, without user info, port or IPv6 brackets, its case
/// kept; `''` for a URL with no authority (no `scheme://`, a relative
/// path); `url` itself when it does not parse: a scheme that does not start
/// with a letter, a port that is not a number.
pub fn url_host(url: &str) -> &str {
    let first_segment = url.split('/').next().unwrap_or_default();
    let Some((scheme, rest)) = url.split_once(':').filter(|_| first_segment.contains(':')) else {
        return "";
    };
    let scheme_ok = scheme
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
    if !scheme_ok {
        return url;
    }
    let Some(rest) = rest.strip_prefix("//") else {
        return "";
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let (host, port) = match host_port.strip_prefix('[') {
        Some(bracketed) => match bracketed.split_once(']') {
            Some((host, port)) => (host, port),
            None => return url,
        },
        None => match host_port.rsplit_once(':') {
            Some((host, _)) => (host, &host_port[host.len()..]),
            None => (host_port, ""),
        },
    };
    if port_ok(port) { host } else { url }
}

/// Renders a number attribute as text: an integral value without a fraction.
pub(crate) fn number_text(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < 1e15 {
        // Integral and within the exactly representable range; the cast is exact.
        #[allow(clippy::cast_possible_truncation)]
        return (n as i64).to_string();
    }
    n.to_string()
}

#[derive(Default)]
struct Fold<'a> {
    url: &'a str,
    first_url: &'a str,
    host: &'a str,
    /// The values of the [`KEYED`] columns, in that order.
    keyed: [Option<Cow<'a, str>>; 4],
}

struct Span<'a> {
    url: &'a str,
    host: &'a str,
    keyed: [Cow<'a, str>; 4],
    client: bool,
}

impl<'a> Fold<'a> {
    fn finish(self, client: bool) -> Span<'a> {
        let host = if client && self.host.is_empty() {
            url_host(self.first_url)
        } else {
            self.host
        };
        Span {
            url: self.url,
            host,
            keyed: self.keyed.map(Option::unwrap_or_default),
            client,
        }
    }
}

impl<'a> Span<'a> {
    fn value(&self, column: Derived) -> Cow<'a, str> {
        match column {
            Derived::HttpUrl => Cow::Borrowed(self.url),
            Derived::HttpHost => Cow::Borrowed(self.host),
            Derived::ExternalHttpMethod if self.client => self.keyed(Derived::HttpMethod),
            Derived::ExternalHttpUrl if self.client => Cow::Borrowed(url_host(self.url)),
            Derived::ExternalHttpMethod | Derived::ExternalHttpUrl => Cow::Borrowed(""),
            keyed => self.keyed(keyed),
        }
    }

    fn keyed(&self, column: Derived) -> Cow<'a, str> {
        KEYED
            .iter()
            .position(|(c, _)| *c == column)
            .and_then(|at| self.keyed.get(at))
            .cloned()
            .unwrap_or_default()
    }
}

fn fold_strings<'a>(entries: impl Iterator<Item = (&'a str, &'a str)>) -> Fold<'a> {
    let mut fold = Fold::default();
    for (key, value) in entries {
        if URL_KEYS.contains(&key) {
            fold.url = value;
            if fold.first_url.is_empty() {
                fold.first_url = value;
            }
        } else if HOST_KEYS.contains(&key) {
            fold.host = value;
        } else if let Some(at) = KEYED.iter().position(|(_, keys)| keys.contains(&key)) {
            if let Some(slot) = fold.keyed.get_mut(at) {
                *slot = Some(Cow::Borrowed(value));
            }
        }
    }
    fold
}

fn last_typed<'a>(
    keys: &[&str],
    entries: impl Iterator<Item = (&'a str, String)>,
) -> Option<String> {
    entries
        .filter(|(key, _)| keys.contains(key))
        .last()
        .map(|(_, value)| value)
}

fn map<'a>(batch: &'a RecordBatch, name: &'static str) -> Result<&'a MapArray, SchemaError> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_map_opt())
        .ok_or(SchemaError::Layout(name))
}

/// The keys of `row`'s entries in `map`, with each entry's index into the
/// map's values.
fn entries(map: &MapArray, row: usize) -> impl Iterator<Item = (&str, usize)> {
    let keys = map.keys().as_string::<i32>();
    let offsets = map.value_offsets();
    let bound = |at: usize| {
        offsets
            .get(at)
            .and_then(|o| usize::try_from(*o).ok())
            .unwrap_or(0)
    };
    let range = bound(row)..bound(row + 1);
    range.map(move |i| (keys.value(i), i))
}

fn port_ok(port: &str) -> bool {
    port.is_empty()
        || port
            .strip_prefix(':')
            .is_some_and(|p| p.chars().all(|c| c.is_ascii_digit()))
}

fn array_bytes(values: &[Cow<'_, str>]) -> usize {
    let data: usize = values.iter().map(|v| v.len()).sum();
    data + (values.len() + 1) * std::mem::size_of::<i32>()
}

fn string_array(values: &[Cow<'_, str>]) -> ArrayRef {
    let mut offsets = Vec::with_capacity(values.len() + 1);
    let mut data = Vec::with_capacity(values.iter().map(|v| v.len()).sum());
    offsets.push(0i32);
    for value in values {
        data.extend_from_slice(value.as_bytes());
        // A batch's text stays within i32 offsets, as its stored maps do.
        offsets.push(i32::try_from(data.len()).unwrap_or(i32::MAX));
    }
    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
    Arc::new(StringArray::new(offsets, Buffer::from_vec(data), None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limits::Limits;
    use crate::spans::SpanBatchBuilder;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    fn attribute(key: &str, value: Value) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue { value: Some(value) }),
            ..KeyValue::default()
        }
    }

    /// `column` of one span of `kind` with `attributes`, in their order.
    fn read(column: Derived, kind: i32, attributes: Vec<KeyValue>) -> String {
        let span = Span {
            trace_id: vec![1; 16],
            span_id: vec![1; 8],
            kind,
            start_time_unix_nano: 1,
            end_time_unix_nano: 2,
            attributes,
            ..Span::default()
        };
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![span],
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            }],
        };
        let mut builder = SpanBatchBuilder::new(Limits::default());
        builder.append_request(&request).unwrap();
        let batch = builder.finish().unwrap();
        let arrays = derive(&batch, &[column]).unwrap().into_arrays().unwrap();
        let text = arrow::compute::cast(arrays.first().unwrap(), &DataType::Utf8).unwrap();
        text.as_string::<i32>().value(0).to_owned()
    }

    fn text(v: &str) -> Value {
        Value::StringValue(v.into())
    }

    #[test]
    fn across_types_a_string_decides_whatever_the_order() {
        let later_number = vec![
            attribute("http.method", text("GET")),
            attribute("http.request.method", Value::IntValue(5)),
        ];
        assert_eq!(read(Derived::HttpMethod, 2, later_number), "GET");
        let earlier_number = vec![
            attribute("http.method", Value::IntValue(5)),
            attribute("http.request.method", text("POST")),
        ];
        assert_eq!(read(Derived::HttpMethod, 2, earlier_number), "POST");
        let empty_string = vec![
            attribute("db.name", Value::IntValue(7)),
            attribute("db.namespace", text("")),
        ];
        assert_eq!(
            read(Derived::DbName, 2, empty_string),
            "",
            "an empty string is a string"
        );
    }

    #[test]
    fn without_a_string_a_number_decides_then_a_bool() {
        let numbers = vec![
            attribute("http.status_code", Value::IntValue(404)),
            attribute("http.response.status_code", Value::IntValue(200)),
        ];
        assert_eq!(
            read(Derived::ResponseStatusCode, 2, numbers),
            "200",
            "one type: the last wins"
        );
        let mixed = vec![
            attribute("db.operation", Value::BoolValue(true)),
            attribute("db.operation.name", Value::DoubleValue(1.5)),
        ];
        assert_eq!(read(Derived::DbOperation, 2, mixed), "1.5");
        assert_eq!(
            read(
                Derived::DbOperation,
                2,
                vec![attribute("db.operation", Value::BoolValue(true))]
            ),
            "true"
        );
    }

    #[test]
    fn a_grpc_status_feeds_no_response_status_code() {
        assert_eq!(
            read(
                Derived::ResponseStatusCode,
                3,
                vec![attribute("rpc.grpc.status_code", Value::IntValue(2))]
            ),
            ""
        );
        let after = vec![
            attribute("http.status_code", Value::IntValue(500)),
            attribute("rpc.grpc.status_code", Value::IntValue(2)),
        ];
        assert_eq!(read(Derived::ResponseStatusCode, 3, after), "500");
    }

    #[test]
    fn a_client_spans_external_method_is_its_http_method() {
        let both = vec![
            attribute("http.request.method", text("PATCH")),
            attribute("http.method", text("DELETE")),
        ];
        assert_eq!(read(Derived::HttpMethod, 3, both.clone()), "DELETE");
        assert_eq!(read(Derived::ExternalHttpMethod, 3, both.clone()), "DELETE");
        assert_eq!(read(Derived::ExternalHttpMethod, 2, both), "");
    }

    #[test]
    fn a_url_host_keeps_case_and_drops_user_info_port_and_brackets() {
        assert_eq!(url_host("https://User:pw@UP.D6.test:1/x"), "UP.D6.test");
        assert_eq!(url_host("http://[::1]:8080/x"), "::1");
        assert_eq!(url_host("http://127.0.0.1:8080/ip"), "127.0.0.1");
        assert_eq!(url_host("https://a.d6.test/p?q=1"), "a.d6.test");
        assert_eq!(url_host("https://h.d6.test?q=1"), "h.d6.test");
    }

    #[test]
    fn a_url_without_an_authority_has_no_host() {
        for url in [
            "noscheme.d6.test/path",
            "/relative/path?q=1",
            "https://",
            "",
            "mailto:a@b",
        ] {
            assert_eq!(url_host(url), "", "{url}");
        }
    }

    #[test]
    fn a_url_that_does_not_parse_is_its_own_host() {
        for url in [
            "1http://x/",
            "http://h:port/",
            ":x",
            "http://[::1/x",
            "http://h:8a/",
        ] {
            assert_eq!(url_host(url), url, "{url}");
        }
        assert_eq!(url_host("http://ep.d6.test:/x"), "ep.d6.test");
    }

    #[test]
    fn numbers_render_integral_values_without_a_fraction() {
        assert_eq!(number_text(5.0), "5");
        assert_eq!(number_text(1.5), "1.5");
        assert_eq!(number_text(-3.0), "-3");
    }
}
