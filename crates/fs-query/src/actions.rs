//! Typed actions over Flight `DoAction`, for the hot paths where SQL only
//! gets in the way.
//!
//! An action body is JSON; a field the action does not know is refused,
//! so a misspelt bound cannot widen a lookup. Its results are one Arrow
//! IPC stream, split in order across the `Result` bodies: concatenated,
//! the bodies are the stream, with the schema in the first and the
//! end-of-stream marker in the last. The `x-fusescope-partial` header
//! applies as for `DoGet`. Bounds are inclusive, in nanoseconds since the
//! epoch, and optional.
//!
//! `trace.lookup`: `{"trace_id": "<32 hex digits>", "start_unix_nano": n,
//! "end_unix_nano": n}`; the spans of that trace ordered by `timestamp`,
//! every column of the spans table.
//!
//! `metadata.services`: `{"start_unix_nano": n, "end_unix_nano": n,
//! "limit": n}`; the distinct `service_name` of the tenant's spans,
//! sorted; more services than the limit is an error, not a truncation.
//!
//! `metadata.operations`: `{"service_name": "api", "start_unix_nano": n,
//! "end_unix_nano": n, "limit": n}`; the span `name`s of that service with
//! how many spans carry each, most frequent first.
//!
//! `metadata.keys`: `{"signal": "traces", "prefix": "http.", "limit": n}`;
//! the rows of the `attribute_keys` table whose key starts with `prefix`,
//! most frequent first.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::QueryError;

/// Action type of a trace lookup.
pub const TRACE_LOOKUP: &str = "trace.lookup";
/// Action type of the service listing.
pub const METADATA_SERVICES: &str = "metadata.services";
/// Action type of the operation listing of one service.
pub const METADATA_OPERATIONS: &str = "metadata.operations";
/// Action type of the attribute key listing.
pub const METADATA_KEYS: &str = "metadata.keys";
/// Rows a listing answers at most when its body names no `limit`.
pub const DEFAULT_LIST_LIMIT: usize = 1000;

/// Inclusive bounds on `timestamp`, nanoseconds since the epoch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Range {
    /// Lower bound.
    pub start_ns: Option<i64>,
    /// Upper bound.
    pub end_ns: Option<i64>,
}

/// Body of a [`TRACE_LOOKUP`] action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceLookup {
    /// The trace id as 32 hex digits.
    pub trace_id: String,
    /// Inclusive lower bound on `timestamp`, nanoseconds since the epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_unix_nano: Option<u64>,
    /// Inclusive upper bound on `timestamp`, nanoseconds since the epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_unix_nano: Option<u64>,
}

/// A trace lookup with its fields decoded and checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceQuery {
    /// The trace id.
    pub trace_id: [u8; 16],
    /// Inclusive lower bound on `timestamp`, nanoseconds since the epoch.
    pub start_ns: Option<i64>,
    /// Inclusive upper bound on `timestamp`, nanoseconds since the epoch.
    pub end_ns: Option<i64>,
}

impl TraceLookup {
    /// Decodes a JSON body.
    ///
    /// # Errors
    /// Not JSON, not the shape of a lookup, or a field the action does
    /// not know.
    pub fn parse(body: &[u8]) -> Result<Self, QueryError> {
        parse(body)
    }

    /// Decodes the trace id and checks the range.
    ///
    /// # Errors
    /// A trace id that is not 32 hex digits, a bound beyond `i64`, or a
    /// start after the end.
    pub fn query(&self) -> Result<TraceQuery, QueryError> {
        let trace_id = decode_hex_16(&self.trace_id)
            .ok_or_else(|| QueryError::InvalidAction("trace_id must be 32 hex digits".into()))?;
        let range = range_of(self.start_unix_nano, self.end_unix_nano)?;
        Ok(TraceQuery {
            trace_id,
            start_ns: range.start_ns,
            end_ns: range.end_ns,
        })
    }
}

/// Body of a [`METADATA_SERVICES`] action.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServicesLookup {
    /// Inclusive lower bound on `timestamp`, nanoseconds since the epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_unix_nano: Option<u64>,
    /// Inclusive upper bound on `timestamp`, nanoseconds since the epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_unix_nano: Option<u64>,
    /// Most services to answer; [`DEFAULT_LIST_LIMIT`] when absent. More
    /// than that is refused, not truncated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// A service listing with its fields checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServicesQuery {
    /// The bounds.
    pub range: Range,
    /// Most services to answer.
    pub limit: usize,
}

impl ServicesQuery {
    /// Every service within `range`, up to the default limit.
    pub fn within(range: Range) -> Self {
        Self {
            range,
            limit: DEFAULT_LIST_LIMIT,
        }
    }
}

impl ServicesLookup {
    /// Decodes a JSON body.
    ///
    /// # Errors
    /// Not JSON, not the shape of a lookup, or a field the action does
    /// not know.
    pub fn parse(body: &[u8]) -> Result<Self, QueryError> {
        parse(body)
    }

    /// Checks the range and the limit.
    ///
    /// # Errors
    /// A bound beyond `i64`, a start after the end, or a limit of zero.
    pub fn query(&self) -> Result<ServicesQuery, QueryError> {
        Ok(ServicesQuery {
            range: range_of(self.start_unix_nano, self.end_unix_nano)?,
            limit: limit_of(self.limit)?,
        })
    }
}

/// Body of a [`METADATA_OPERATIONS`] action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationsLookup {
    /// The service whose operations to list.
    pub service_name: String,
    /// Inclusive lower bound on `timestamp`, nanoseconds since the epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_unix_nano: Option<u64>,
    /// Inclusive upper bound on `timestamp`, nanoseconds since the epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_unix_nano: Option<u64>,
    /// Most rows to answer; [`DEFAULT_LIST_LIMIT`] when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// An operation listing with its fields checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationsQuery {
    /// The service.
    pub service_name: String,
    /// The bounds.
    pub range: Range,
    /// Most rows to answer.
    pub limit: usize,
}

impl OperationsLookup {
    /// Decodes a JSON body.
    ///
    /// # Errors
    /// Not JSON, not the shape of a lookup, or a field the action does
    /// not know.
    pub fn parse(body: &[u8]) -> Result<Self, QueryError> {
        parse(body)
    }

    /// Checks the service, the range and the limit.
    ///
    /// # Errors
    /// An empty service, a bound beyond `i64`, a start after the end, or
    /// a limit of zero.
    pub fn query(&self) -> Result<OperationsQuery, QueryError> {
        if self.service_name.is_empty() {
            return Err(QueryError::InvalidAction("service_name is empty".into()));
        }
        Ok(OperationsQuery {
            service_name: self.service_name.clone(),
            range: range_of(self.start_unix_nano, self.end_unix_nano)?,
            limit: limit_of(self.limit)?,
        })
    }
}

/// Body of a [`METADATA_KEYS`] action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeysLookup {
    /// The signal whose keys to list; only `traces` has a table so far.
    pub signal: String,
    /// Keep the keys starting with this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Most rows to answer; [`DEFAULT_LIST_LIMIT`] when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// A key listing with its fields checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeysQuery {
    /// Keep the keys starting with this.
    pub prefix: Option<String>,
    /// Most rows to answer.
    pub limit: usize,
}

impl KeysLookup {
    /// Decodes a JSON body.
    ///
    /// # Errors
    /// Not JSON, not the shape of a lookup, or a field the action does
    /// not know.
    pub fn parse(body: &[u8]) -> Result<Self, QueryError> {
        parse(body)
    }

    /// Checks the signal and the limit.
    ///
    /// # Errors
    /// A signal without an `attribute_keys` table, or a limit of zero.
    pub fn query(&self) -> Result<KeysQuery, QueryError> {
        if self.signal != "traces" {
            return Err(QueryError::InvalidAction(format!(
                "signal {:?} has no attribute_keys table",
                self.signal
            )));
        }
        Ok(KeysQuery {
            prefix: self.prefix.clone(),
            limit: limit_of(self.limit)?,
        })
    }
}

fn parse<T: DeserializeOwned>(body: &[u8]) -> Result<T, QueryError> {
    serde_json::from_slice(body).map_err(|e| QueryError::InvalidAction(e.to_string()))
}

fn range_of(start: Option<u64>, end: Option<u64>) -> Result<Range, QueryError> {
    let bound = |ns: Option<u64>, name: &str| -> Result<Option<i64>, QueryError> {
        ns.map(|n| {
            i64::try_from(n)
                .map_err(|_| QueryError::InvalidAction(format!("{name} is out of range")))
        })
        .transpose()
    };
    let start_ns = bound(start, "start_unix_nano")?;
    let end_ns = bound(end, "end_unix_nano")?;
    if let (Some(start), Some(end)) = (start_ns, end_ns) {
        if start > end {
            return Err(QueryError::InvalidAction(
                "start_unix_nano is after end_unix_nano".into(),
            ));
        }
    }
    Ok(Range { start_ns, end_ns })
}

fn limit_of(limit: Option<usize>) -> Result<usize, QueryError> {
    match limit {
        Some(0) => Err(QueryError::InvalidAction("limit is zero".into())),
        Some(limit) => Ok(limit),
        None => Ok(DEFAULT_LIST_LIMIT),
    }
}

fn decode_hex_16(text: &str) -> Option<[u8; 16]> {
    let digits = text.as_bytes();
    if digits.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (byte, pair) in out.iter_mut().zip(digits.chunks_exact(2)) {
        let hi = (*pair.first()? as char).to_digit(16)?;
        let lo = (*pair.get(1)? as char).to_digit(16)?;
        *byte = u8::try_from(hi << 4 | lo).ok()?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lookup_decodes_its_trace_id_and_checks_its_range() {
        let lookup = TraceLookup::parse(
            br#"{"trace_id":"000102030405060708090a0b0c0d0e0F","start_unix_nano":5,"end_unix_nano":9}"#,
        )
        .unwrap();
        let query = lookup.query().unwrap();
        assert_eq!(query.trace_id[0..3], [0, 1, 2]);
        assert_eq!(query.trace_id[15], 15);
        assert_eq!((query.start_ns, query.end_ns), (Some(5), Some(9)));

        let open =
            TraceLookup::parse(br#"{"trace_id":"00000000000000000000000000000000"}"#).unwrap();
        assert_eq!(open.query().unwrap().start_ns, None);

        for bad in [
            &br#"{"trace_id":"0102"}"#[..],
            br#"{"trace_id":"zz000000000000000000000000000000"}"#,
            br#"{"trace_id":"00000000000000000000000000000000","start_unix_nano":2,"end_unix_nano":1}"#,
            br#"{"trace_id":"00000000000000000000000000000000","end_unix_nano":18446744073709551615}"#,
        ] {
            let err = TraceLookup::parse(bad).unwrap().query().unwrap_err();
            assert!(matches!(err, QueryError::InvalidAction(_)), "{err}");
        }
        for bad in [
            &b"not json"[..],
            br#"{"trace_id":"00000000000000000000000000000000","end_unix_nanos":1}"#,
        ] {
            assert!(matches!(
                TraceLookup::parse(bad).unwrap_err(),
                QueryError::InvalidAction(_)
            ));
        }
    }

    #[test]
    fn the_listings_check_their_fields_and_default_their_limit() {
        let services = ServicesLookup::parse(br#"{"end_unix_nano":9,"limit":2}"#).unwrap();
        let query = services.query().unwrap();
        assert_eq!((query.range.end_ns, query.limit), (Some(9), 2));
        assert!(ServicesLookup::parse(br#"{"end_unix_nanos":9}"#).is_err());
        assert_eq!(
            ServicesLookup::parse(b"{}").unwrap().query().unwrap(),
            ServicesQuery::within(Range::default())
        );
        assert!(
            ServicesLookup::parse(br#"{"limit":0}"#)
                .unwrap()
                .query()
                .is_err()
        );

        let operations = OperationsLookup::parse(br#"{"service_name":"api","limit":3}"#).unwrap();
        let query = operations.query().unwrap();
        assert_eq!((query.service_name.as_str(), query.limit), ("api", 3));
        assert_eq!(
            OperationsLookup::parse(br#"{"service_name":"api"}"#)
                .unwrap()
                .query()
                .unwrap()
                .limit,
            DEFAULT_LIST_LIMIT
        );
        for bad in [
            &br#"{"service_name":""}"#[..],
            br#"{"service_name":"api","limit":0}"#,
            br#"{"service_name":"api","start_unix_nano":2,"end_unix_nano":1}"#,
        ] {
            let err = OperationsLookup::parse(bad).unwrap().query().unwrap_err();
            assert!(matches!(err, QueryError::InvalidAction(_)), "{err}");
        }
        assert!(
            OperationsLookup::parse(b"{}").is_err(),
            "service_name is required"
        );

        let keys = KeysLookup::parse(br#"{"signal":"traces","prefix":"http."}"#).unwrap();
        let query = keys.query().unwrap();
        assert_eq!(query.prefix.as_deref(), Some("http."));
        assert_eq!(query.limit, DEFAULT_LIST_LIMIT);
        let err = KeysLookup::parse(br#"{"signal":"logs"}"#)
            .unwrap()
            .query()
            .unwrap_err();
        assert!(matches!(err, QueryError::InvalidAction(_)), "{err}");
        assert!(
            KeysLookup::parse(br#"{"prefix":"x"}"#).is_err(),
            "signal is required"
        );
    }
}
