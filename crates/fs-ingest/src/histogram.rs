//! Fixed-bucket latency histogram, lock free, rendered as a Prometheus
//! histogram by the metrics page.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Upper bounds of the buckets, in seconds; the last bucket is unbounded.
pub const BOUNDS: [f64; 10] = [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0];

/// Counts of observations per bucket, plus their sum and count.
#[derive(Debug, Default)]
pub struct Histogram {
    buckets: [AtomicU64; 11],
    sum_nanos: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    /// Records one duration.
    pub fn observe(&self, d: Duration) {
        let secs = d.as_secs_f64();
        let index = BOUNDS
            .iter()
            .position(|b| secs <= *b)
            .unwrap_or(BOUNDS.len());
        if let Some(bucket) = self.buckets.get(index) {
            bucket.fetch_add(1, Ordering::Relaxed);
        }
        self.sum_nanos.fetch_add(
            u64::try_from(d.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Cumulative counts per bucket, in `BOUNDS` order with the unbounded
    /// bucket last.
    pub fn cumulative(&self) -> [u64; 11] {
        let mut out = [0u64; 11];
        let mut acc = 0;
        for (slot, bucket) in out.iter_mut().zip(&self.buckets) {
            acc += bucket.load(Ordering::Relaxed);
            *slot = acc;
        }
        out
    }

    /// Sum of observations, nanoseconds.
    pub fn sum_nanos(&self) -> u64 {
        self.sum_nanos.load(Ordering::Relaxed)
    }

    /// Number of observations.
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observations_land_in_their_bucket() {
        let h = Histogram::default();
        h.observe(Duration::from_micros(500));
        h.observe(Duration::from_millis(3));
        h.observe(Duration::from_secs(2));
        let c = h.cumulative();
        assert_eq!(c[0], 1, "0.5 ms is within 1 ms");
        assert_eq!(c[1], 1);
        assert_eq!(c[2], 2, "3 ms is within 5 ms");
        assert_eq!(c[9], 2);
        assert_eq!(c[10], 3, "2 s only fits the unbounded bucket");
        assert_eq!(h.count(), 3);
        assert_eq!(h.sum_nanos(), 2_003_500_000);
    }
}
