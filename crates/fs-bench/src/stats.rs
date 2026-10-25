//! Timing helpers and percentile summaries.

use std::time::{Duration, Instant};

/// A set of latency samples.
#[derive(Debug, Default, Clone)]
pub struct Samples {
    values: Vec<Duration>,
}

impl Samples {
    /// Records one sample.
    pub fn push(&mut self, d: Duration) {
        self.values.push(d);
    }

    /// Mutable access to the raw samples.
    pub fn values_mut(&mut self) -> &mut Vec<Duration> {
        &mut self.values
    }

    /// Consumes the set.
    pub fn into_values(self) -> Vec<Duration> {
        self.values
    }

    /// Percentile `p` in `[0, 100]`, by nearest rank.
    pub fn percentile(&self, p: f64) -> Duration {
        if self.values.is_empty() {
            return Duration::ZERO;
        }
        let mut sorted = self.values.clone();
        sorted.sort_unstable();
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
        sorted
            .get(rank.clamp(1, sorted.len()) - 1)
            .copied()
            .unwrap_or(Duration::ZERO)
    }
}

/// Human duration: µs, ms or s.
pub fn fmt(d: Duration) -> String {
    let us = d.as_micros();
    if us < 1_000 {
        format!("{us}µs")
    } else if us < 1_000_000 {
        format!("{:.2}ms", d.as_secs_f64() * 1e3)
    } else {
        format!("{:.2}s", d.as_secs_f64())
    }
}

/// Times a closure.
pub fn timed<T>(f: impl FnOnce() -> T) -> (T, Duration) {
    let start = Instant::now();
    let out = f();
    (out, start.elapsed())
}

/// Throughput in MiB/s.
#[allow(clippy::cast_precision_loss)]
pub fn mib_per_sec(bytes: usize, d: Duration) -> f64 {
    if d.is_zero() {
        return 0.0;
    }
    bytes as f64 / (1024.0 * 1024.0) / d.as_secs_f64()
}

/// Items per second.
#[allow(clippy::cast_precision_loss)]
pub fn per_sec(count: usize, d: Duration) -> f64 {
    if d.is_zero() {
        return 0.0;
    }
    count as f64 / d.as_secs_f64()
}
