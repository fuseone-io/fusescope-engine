//! Token bucket per tenant, in bytes per second.

use std::collections::HashMap;
use std::time::Instant;

use fs_catalog::TenantId;

struct Bucket {
    tokens: f64,
    updated: Instant,
}

/// Byte-rate limiter keyed by tenant. Burst equals one second of rate.
pub struct RateLimiter {
    rate: f64,
    buckets: HashMap<TenantId, Bucket>,
}

impl RateLimiter {
    /// Limiter allowing `bytes_per_sec` per tenant.
    pub fn new(bytes_per_sec: u64) -> Self {
        // u64 → f64 is fine for any realistic rate.
        #[allow(clippy::cast_precision_loss)]
        let rate = bytes_per_sec as f64;
        Self {
            rate,
            buckets: HashMap::new(),
        }
    }

    /// Takes `bytes` for `tenant` at `now`. Returns false when over the limit.
    pub fn allow(&mut self, tenant: TenantId, bytes: usize, now: Instant) -> bool {
        let bucket = self.buckets.entry(tenant).or_insert(Bucket {
            tokens: self.rate,
            updated: now,
        });
        let elapsed = now.saturating_duration_since(bucket.updated).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.rate).min(self.rate);
        bucket.updated = now;
        #[allow(clippy::cast_precision_loss)]
        let cost = bytes as f64;
        if bucket.tokens >= cost {
            bucket.tokens -= cost;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn enforces_rate_with_refill() {
        let t = TenantId::new();
        let other = TenantId::new();
        let mut limiter = RateLimiter::new(1000);
        let start = Instant::now();
        assert!(limiter.allow(t, 600, start));
        assert!(limiter.allow(t, 400, start));
        assert!(!limiter.allow(t, 1, start), "bucket drained");
        assert!(limiter.allow(other, 1000, start), "tenants are independent");
        assert!(limiter.allow(t, 500, start + Duration::from_millis(500)));
        assert!(!limiter.allow(t, 1, start + Duration::from_millis(500)));
        assert!(
            limiter.allow(t, 1000, start + Duration::from_secs(10)),
            "burst caps at one second"
        );
        assert!(!limiter.allow(t, 1, start + Duration::from_secs(10)));
    }
}
