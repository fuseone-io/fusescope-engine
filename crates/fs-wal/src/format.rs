//! On-disk encoding of segment headers and record headers.

/// Magic bytes at the start of every segment. The last byte is the format version.
pub(crate) const MAGIC: [u8; 8] = *b"FSWAL\0\0\x01";

/// Bytes of the segment header: magic plus the segment id.
pub const HEADER_LEN: u64 = 16;

/// Bytes of a record header: payload length plus crc32c.
pub const RECORD_HEADER_LEN: usize = 8;

/// Encodes the segment header.
pub(crate) fn encode_header(segment_id: u64) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&MAGIC);
    out[8..].copy_from_slice(&segment_id.to_le_bytes());
    out
}

/// Decodes a segment header, returning the segment id it claims.
pub(crate) fn decode_header(buf: &[u8; 16]) -> Option<u64> {
    let (magic, id) = buf.split_at(8);
    if magic != MAGIC {
        return None;
    }
    id.try_into().ok().map(u64::from_le_bytes)
}

/// Encodes a record header for `payload`.
pub(crate) fn encode_record_header(payload: &[u8]) -> Option<[u8; RECORD_HEADER_LEN]> {
    let len = u32::try_from(payload.len()).ok()?;
    let mut out = [0u8; RECORD_HEADER_LEN];
    out[..4].copy_from_slice(&len.to_le_bytes());
    out[4..].copy_from_slice(&crc32c::crc32c(payload).to_le_bytes());
    Some(out)
}

/// Splits a record header into `(payload_len, crc)`.
pub(crate) fn decode_record_header(buf: [u8; RECORD_HEADER_LEN]) -> (u32, u32) {
    let (len, crc) = buf.split_at(4);
    let len = len.try_into().map_or(0, u32::from_le_bytes);
    let crc = crc.try_into().map_or(0, u32::from_le_bytes);
    (len, crc)
}

/// True when `payload` matches `crc`.
pub(crate) fn verify(payload: &[u8], crc: u32) -> bool {
    crc32c::crc32c(payload) == crc
}

/// File name of a segment.
pub(crate) fn segment_file_name(id: u64) -> String {
    format!("{id:020}.wal")
}

/// Segment id from a file name, or `None` for files that are not segments.
pub(crate) fn parse_segment_file_name(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".wal")?;
    if stem.len() != 20 || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trip() {
        let encoded = encode_header(42);
        assert_eq!(decode_header(&encoded), Some(42));
        let mut bad = encoded;
        bad[0] = b'X';
        assert_eq!(decode_header(&bad), None);
    }

    #[test]
    fn record_header_round_trip() {
        let payload = b"hello";
        let header = encode_record_header(payload).unwrap();
        let (len, crc) = decode_record_header(header);
        assert_eq!(len, 5);
        assert!(verify(payload, crc));
        assert!(!verify(b"hellp", crc));
    }

    #[test]
    fn file_names_are_sortable_and_strict() {
        assert_eq!(segment_file_name(7), "00000000000000000007.wal");
        assert_eq!(parse_segment_file_name("00000000000000000007.wal"), Some(7));
        assert_eq!(parse_segment_file_name("7.wal"), None);
        assert_eq!(parse_segment_file_name("00000000000000000007.tmp"), None);
        assert_eq!(parse_segment_file_name("0000000000000000000a.wal"), None);
        assert!(segment_file_name(9) < segment_file_name(10));
    }
}
