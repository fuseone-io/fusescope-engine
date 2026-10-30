//! Fixed-bucket latency histogram, lock free, rendered as a Prometheus
//! histogram by the metrics page.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Upper bounds of the default buckets, in seconds, for steps measured in
/// milliseconds; the last bucket is unbounded.
pub const BOUNDS: [f64; 10] = [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0];

/// Counts of observations per bucket, plus their sum and count.
#[derive(Debug)]
pub struct Histogram {
    bounds: &'static [f64],
    buckets: Box<[AtomicU64]>,
    sum_nanos: AtomicU64,
    count: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self::with_bounds(&BOUNDS)
    }
}

impl Histogram {
    /// A histogram over `bounds`, ascending upper bounds in seconds, plus
    /// an unbounded last bucket.
    pub fn with_bounds(bounds: &'static [f64]) -> Self {
        Self {
            bounds,
            buckets: (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect(),
            sum_nanos: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// The upper bounds, in seconds, without the unbounded bucket.
    pub fn bounds(&self) -> &'static [f64] {
        self.bounds
    }

    /// Records one duration.
    pub fn observe(&self, d: Duration) {
        let secs = d.as_secs_f64();
        let index = self
            .bounds
            .iter()
            .position(|b| secs <= *b)
            .unwrap_or(self.bounds.len());
        if let Some(bucket) = self.buckets.get(index) {
            bucket.fetch_add(1, Ordering::Relaxed);
        }
        self.sum_nanos.fetch_add(
            u64::try_from(d.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Cumulative counts per bucket, in `bounds` order with the unbounded
    /// bucket last.
    pub fn cumulative(&self) -> Vec<u64> {
        let mut acc = 0;
        self.buckets
            .iter()
            .map(|bucket| {
                acc += bucket.load(Ordering::Relaxed);
                acc
            })
            .collect()
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
        assert_eq!(
            h.cumulative(),
            [1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 3],
            "0.5 ms within 1 ms, 3 ms within 5 ms, 2 s only in the unbounded bucket"
        );
        assert_eq!(h.count(), 3);
        assert_eq!(h.sum_nanos(), 2_003_500_000);

        let wide = Histogram::with_bounds(&[1.0, 30.0]);
        wide.observe(Duration::from_secs(2));
        assert_eq!(wide.cumulative(), [0, 1, 1]);
        assert_eq!(wide.bounds(), [1.0, 30.0]);
    }
}
