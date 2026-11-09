//! Stable machine-readable reasons on a gRPC status, for a client that
//! must tell one cause from another under the same code. The reason travels
//! as a `google.rpc.ErrorInfo` inside the `google.rpc.Status` of the
//! `grpc-status-details-bin` trailer, as gRPC clients read it; the message
//! stays human text and is never the discriminator.

use bytes::Bytes;
use prost::Message;
use tonic::{Code, Status};

/// `ErrorInfo.domain` of every reason the engine sets.
pub const DOMAIN: &str = "fusescope-engine";
/// `ErrorInfo.reason` of a query whose answer passed `query.max_rows`.
pub const TOO_MANY_ROWS: &str = "TOO_MANY_ROWS";
/// `Any.type_url` of an `ErrorInfo`.
pub const ERROR_INFO_TYPE_URL: &str = "type.googleapis.com/google.rpc.ErrorInfo";

/// `google.rpc.Status`.
#[derive(Clone, PartialEq, Message)]
pub struct RpcStatus {
    /// The gRPC code.
    #[prost(int32, tag = "1")]
    pub code: i32,
    /// The status message.
    #[prost(string, tag = "2")]
    pub message: String,
    /// Typed details.
    #[prost(message, repeated, tag = "3")]
    pub details: Vec<ProtoAny>,
}

/// `google.protobuf.Any`.
#[derive(Clone, PartialEq, Message)]
pub struct ProtoAny {
    /// What `value` encodes.
    #[prost(string, tag = "1")]
    pub type_url: String,
    /// The encoded message.
    #[prost(bytes = "vec", tag = "2")]
    pub value: Vec<u8>,
}

/// `google.rpc.ErrorInfo`.
#[derive(Clone, PartialEq, Message)]
pub struct ErrorInfo {
    /// The stable reason.
    #[prost(string, tag = "1")]
    pub reason: String,
    /// Who sets the reason.
    #[prost(string, tag = "2")]
    pub domain: String,
    /// Nothing is set: values could carry tenant data.
    #[prost(map = "string, string", tag = "3")]
    pub metadata: std::collections::HashMap<String, String>,
}

/// A status of `code` and `message` whose details carry `reason`.
pub fn with_reason(code: Code, message: impl Into<String>, reason: &str) -> Status {
    let message = message.into();
    let info = ErrorInfo {
        reason: reason.to_owned(),
        domain: DOMAIN.to_owned(),
        metadata: std::collections::HashMap::new(),
    };
    let status = RpcStatus {
        code: code as i32,
        message: message.clone(),
        details: vec![ProtoAny {
            type_url: ERROR_INFO_TYPE_URL.to_owned(),
            value: info.encode_to_vec(),
        }],
    };
    Status::with_details(code, message, Bytes::from(status.encode_to_vec()))
}

/// The engine reason a status carries, if any.
pub fn reason_of(status: &Status) -> Option<String> {
    let decoded = RpcStatus::decode(status.details()).ok()?;
    decoded
        .details
        .iter()
        .filter(|any| any.type_url == ERROR_INFO_TYPE_URL)
        .filter_map(|any| ErrorInfo::decode(any.value.as_slice()).ok())
        .find(|info| info.domain == DOMAIN)
        .map(|info| info.reason)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reason_round_trips_through_the_status_details() {
        let status = with_reason(Code::ResourceExhausted, "too many rows", TOO_MANY_ROWS);
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(status.message(), "too many rows");
        assert_eq!(reason_of(&status).as_deref(), Some(TOO_MANY_ROWS));
        let decoded = RpcStatus::decode(status.details()).unwrap();
        assert_eq!(decoded.code, Code::ResourceExhausted as i32);
        assert_eq!(decoded.details.len(), 1);
    }

    #[test]
    fn a_plain_status_has_no_reason() {
        assert_eq!(reason_of(&Status::resource_exhausted("over budget")), None);
    }
}
