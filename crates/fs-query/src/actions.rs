//! Typed actions over Flight `DoAction`, for the hot paths where SQL only
//! gets in the way.
//!
//! An action body is JSON. Its results are one Arrow IPC stream, split in
//! order across the `Result` bodies: concatenated, the bodies are the
//! stream, with the schema in the first and the end-of-stream marker in
//! the last. The `x-fusescope-partial` header applies as for `DoGet`.
//!
//! `trace.lookup`: `{"trace_id": "<32 hex digits>", "start_unix_nano": n,
//! "end_unix_nano": n}`, the bounds optional, inclusive, in nanoseconds
//! since the epoch; answers the spans of that trace ordered by `timestamp`,
//! every column of the spans table.

use serde::{Deserialize, Serialize};

use crate::QueryError;

/// Action type of a trace lookup.
pub const TRACE_LOOKUP: &str = "trace.lookup";

/// Body of a [`TRACE_LOOKUP`] action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Not JSON, or not the shape of a lookup.
    pub fn parse(body: &[u8]) -> Result<Self, QueryError> {
        serde_json::from_slice(body).map_err(|e| QueryError::InvalidAction(e.to_string()))
    }

    /// Decodes the trace id and checks the range.
    ///
    /// # Errors
    /// A trace id that is not 32 hex digits, a bound beyond `i64`, or a
    /// start after the end.
    pub fn query(&self) -> Result<TraceQuery, QueryError> {
        let trace_id = decode_hex_16(&self.trace_id)
            .ok_or_else(|| QueryError::InvalidAction("trace_id must be 32 hex digits".into()))?;
        let bound = |ns: Option<u64>, name: &str| -> Result<Option<i64>, QueryError> {
            ns.map(|n| {
                i64::try_from(n)
                    .map_err(|_| QueryError::InvalidAction(format!("{name} is out of range")))
            })
            .transpose()
        };
        let start_ns = bound(self.start_unix_nano, "start_unix_nano")?;
        let end_ns = bound(self.end_unix_nano, "end_unix_nano")?;
        if let (Some(start), Some(end)) = (start_ns, end_ns) {
            if start > end {
                return Err(QueryError::InvalidAction(
                    "start_unix_nano is after end_unix_nano".into(),
                ));
            }
        }
        Ok(TraceQuery {
            trace_id,
            start_ns,
            end_ns,
        })
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
        assert!(matches!(
            TraceLookup::parse(b"not json").unwrap_err(),
            QueryError::InvalidAction(_)
        ));
    }
}
