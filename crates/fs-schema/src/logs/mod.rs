//! Log schema and the OTLP logs to Arrow converter.

mod builder;
mod schema;

pub use builder::LogBatchBuilder;
pub use schema::{LOG_PROMOTED, SCHEMA_VERSION, col, log_schema_v1};

use twox_hash::XxHash3_64;

/// Bytes of a log id.
pub const LOG_ID_BYTES: usize = 16;

/// The id of the `ordinal`th record of a request whose origin is `seed`, for a
/// record at `unix_nano`.
///
/// ULID layout: the first 6 bytes are the big-endian Unix milliseconds, the
/// other 10 a hash of `seed` and `ordinal`, so ids sort by time as bytes and a
/// replay of the same request from the same origin produces the same ids.
pub fn log_id(unix_nano: u64, seed: &[u8], ordinal: u64) -> [u8; LOG_ID_BYTES] {
    let mut input = Vec::with_capacity(seed.len() + 8);
    input.extend_from_slice(seed);
    input.extend_from_slice(&ordinal.to_le_bytes());
    let high = XxHash3_64::oneshot_with_seed(0, &input).to_be_bytes();
    let low = XxHash3_64::oneshot_with_seed(1, &input).to_be_bytes();
    let millis = (unix_nano / 1_000_000).to_be_bytes();
    let mut id = [0u8; LOG_ID_BYTES];
    let (time, random) = id.split_at_mut(6);
    time.copy_from_slice(millis.get(2..).unwrap_or_default());
    let (first, last) = random.split_at_mut(8);
    first.copy_from_slice(&high);
    last.copy_from_slice(low.get(6..).unwrap_or_default());
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_seed_and_ordinal_are_not_ambiguous() {
        assert_ne!(log_id(0, b"a", 1), log_id(0, b"a\x01", 0));
        assert_eq!(log_id(1_000_000, b"", 0)[..6], [0, 0, 0, 0, 0, 1]);
    }
}
