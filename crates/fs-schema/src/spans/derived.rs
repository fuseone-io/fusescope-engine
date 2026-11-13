//! Span columns derived from the stored kind and attributes when they are
//! read, never stored: `http_url`, `http_host`, `external_http_method` and
//! `external_http_url`. The rules and their observed sources are in
//! `MAPPING.md`, section "Derived span columns".
//!
//! Each map is folded in its stored order, which is the OTLP order within
//! one value type; the order between types is not stored, so string values
//! decide wherever a key also appears with another type.

use std::borrow::Cow;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, MapArray, StringArray};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Float64Type, Int8Type};
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
const METHOD_KEYS: [&str; 2] = ["http.request.method", "http.method"];

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
}

impl Derived {
    /// Every derived column, in the order the table shows them.
    pub const ALL: [Self; 4] = [
        Self::HttpUrl,
        Self::HttpHost,
        Self::ExternalHttpMethod,
        Self::ExternalHttpUrl,
    ];

    /// The column name, the backend's.
    pub fn name(self) -> &'static str {
        match self {
            Self::HttpUrl => "http_url",
            Self::HttpHost => "http_host",
            Self::ExternalHttpMethod => "external_http_method",
            Self::ExternalHttpUrl => "external_http_url",
        }
    }

    /// The derived column called `name`.
    pub fn of(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|d| d.name() == name)
    }

    /// The column's field: text, `''` where nothing derives it.
    pub fn field(self) -> Field {
        Field::new(self.name(), DataType::Utf8, false)
    }
}

/// The values of the derived columns of one batch, borrowed from it where
/// they are stored text, with the bytes their arrays will take.
#[derive(Debug)]
pub struct Derivation<'a> {
    columns: Vec<Vec<Cow<'a, str>>>,
    bytes: usize,
}

impl Derivation<'_> {
    /// The bytes [`Derivation::into_arrays`] allocates, exactly.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// The derived arrays, in the order they were asked for.
    pub fn into_arrays(self) -> Vec<ArrayRef> {
        self.columns.iter().map(|c| string_array(c)).collect()
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
        if span.method.is_none() {
            span.method = last_typed(
                entries(numbers, row).map(|(k, i)| (k, number_text(number_values.value(i)))),
            )
            .or_else(|| {
                last_typed(entries(bools, row).map(|(k, i)| (k, bool_values.value(i).to_string())))
            })
            .map(Cow::Owned);
        }
        let derived = span.finish(kind.value(row) == CLIENT);
        for (out, column) in columns.iter_mut().zip(wanted) {
            out.push(derived.value(*column));
        }
    }
    let bytes = columns.iter().map(|values| array_bytes(values)).sum();
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
    method: Option<Cow<'a, str>>,
}

struct Span<'a> {
    url: &'a str,
    host: &'a str,
    method: Cow<'a, str>,
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
            method: self.method.unwrap_or_default(),
            client,
        }
    }
}

impl<'a> Span<'a> {
    fn value(&self, column: Derived) -> Cow<'a, str> {
        match column {
            Derived::HttpUrl => Cow::Borrowed(self.url),
            Derived::HttpHost => Cow::Borrowed(self.host),
            Derived::ExternalHttpMethod if self.client => self.method.clone(),
            Derived::ExternalHttpUrl if self.client => Cow::Borrowed(url_host(self.url)),
            Derived::ExternalHttpMethod | Derived::ExternalHttpUrl => Cow::Borrowed(""),
        }
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
        } else if METHOD_KEYS.contains(&key) {
            fold.method = Some(Cow::Borrowed(value));
        }
    }
    fold
}

fn last_typed<'a>(entries: impl Iterator<Item = (&'a str, String)>) -> Option<String> {
    entries
        .filter(|(key, _)| METHOD_KEYS.contains(key))
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
