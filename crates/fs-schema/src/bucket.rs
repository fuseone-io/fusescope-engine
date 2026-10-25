//! Time bucketing shared by every signal.

/// Width of `ts_bucket_start` buckets in seconds (30 minutes).
pub const BUCKET_SECONDS: u64 = 1800;

/// Start of the 30 minute bucket containing a nanosecond Unix timestamp.
///
/// The Go query builder emits predicates on this column, and the engine maps
/// them to partition pruning.
pub fn ts_bucket_start(unix_nano: u64) -> u64 {
    let seconds = unix_nano / 1_000_000_000;
    seconds - (seconds % BUCKET_SECONDS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_align_to_half_hours() {
        // 2026-10-24T14:17:03Z
        let ts = 1_792_678_623_000_000_000_u64;
        let start = ts_bucket_start(ts);
        assert_eq!(start % BUCKET_SECONDS, 0);
        assert!(start <= ts / 1_000_000_000);
        assert!(start + BUCKET_SECONDS > ts / 1_000_000_000);
    }

    #[test]
    fn zero_and_boundaries() {
        assert_eq!(ts_bucket_start(0), 0);
        assert_eq!(ts_bucket_start(1_800_000_000_000), 1800);
        assert_eq!(ts_bucket_start(1_799_999_999_999), 0);
    }
}
