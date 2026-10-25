//! WAL record framing: which tenant and signal a raw OTLP payload belongs to.
//!
//! The payload is the protobuf exactly as received, so replay runs the same
//! conversion code as live traffic and produces identical rows.

use bytes::Bytes;
use fs_catalog::TenantId;

use crate::IngestError;

/// Record format version.
const VERSION: u8 = 1;

/// Signal carried by a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordSignal {
    /// `ExportTraceServiceRequest`.
    Traces = 1,
}

impl RecordSignal {
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Traces),
            _ => None,
        }
    }
}

/// Header plus payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    /// Owner.
    pub tenant: TenantId,
    /// Payload type.
    pub signal: RecordSignal,
    /// Raw protobuf.
    pub payload: Bytes,
}

/// Bytes of the header: version, signal, tenant uuid.
pub const HEADER_LEN: usize = 1 + 1 + 16;

impl WalRecord {
    /// Serialises header and payload into one buffer.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + self.payload.len());
        out.push(VERSION);
        out.push(self.signal as u8);
        out.extend_from_slice(self.tenant.0.as_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    /// Parses a record written by [`WalRecord::encode`].
    ///
    /// # Errors
    /// Short buffer, unknown version or signal.
    pub fn decode(bytes: &[u8]) -> Result<Self, IngestError> {
        let (header, payload) = bytes
            .split_at_checked(HEADER_LEN)
            .ok_or_else(|| IngestError::Record(format!("short record: {} bytes", bytes.len())))?;
        let version = header.first().copied().unwrap_or(0);
        if version != VERSION {
            return Err(IngestError::Record(format!(
                "unsupported version {version}"
            )));
        }
        let signal = RecordSignal::from_byte(header.get(1).copied().unwrap_or(0))
            .ok_or_else(|| IngestError::Record("unknown signal".into()))?;
        let uuid = header
            .get(2..18)
            .and_then(|b| uuid::Uuid::from_slice(b).ok())
            .ok_or_else(|| IngestError::Record("bad tenant id".into()))?;
        Ok(Self {
            tenant: TenantId(uuid),
            signal,
            payload: Bytes::copy_from_slice(payload),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let record = WalRecord {
            tenant: TenantId::new(),
            signal: RecordSignal::Traces,
            payload: Bytes::from_static(b"otlp"),
        };
        let bytes = record.encode();
        assert_eq!(bytes.len(), HEADER_LEN + 4);
        assert_eq!(WalRecord::decode(&bytes).unwrap(), record);
    }

    #[test]
    fn rejects_garbage() {
        assert!(WalRecord::decode(b"short").is_err());
        let mut bad_version = WalRecord {
            tenant: TenantId::new(),
            signal: RecordSignal::Traces,
            payload: Bytes::new(),
        }
        .encode();
        *bad_version.first_mut().unwrap() = 9;
        assert!(WalRecord::decode(&bad_version).is_err());
        let mut bad_signal = WalRecord {
            tenant: TenantId::new(),
            signal: RecordSignal::Traces,
            payload: Bytes::new(),
        }
        .encode();
        *bad_signal.get_mut(1).unwrap() = 7;
        assert!(WalRecord::decode(&bad_signal).is_err());
    }
}
