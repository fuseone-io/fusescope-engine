//! Bearer token authentication with a short positive and negative cache.
//! A revocation or a rotation reaches the ingester within the positive TTL:
//! an answer cached before it lives out its TTL, since nothing tells the
//! cache of the change. An answer cached after a rotation never outlives
//! the old token's scheduled end, measured on the catalog's clock.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use fs_catalog::{Catalog, TenantId, TokenScope, hash_token, looks_like_token};
use tonic::Status;
use tonic::metadata::MetadataMap;

/// Negative results are cached briefly so a flood of bad tokens does not
/// become a flood of catalog queries.
const NEGATIVE_TTL: Duration = Duration::from_secs(5);

/// What the catalog answered for a token hash.
#[derive(Debug, Clone, Copy)]
enum Lookup {
    Tenant(TenantId),
    Unknown,
}

impl Lookup {
    fn tenant(self) -> Option<TenantId> {
        match self {
            Lookup::Tenant(t) => Some(t),
            Lookup::Unknown => None,
        }
    }
}

type Cache = HashMap<[u8; 32], (Lookup, Instant)>;

/// Resolves requests to tenants.
pub struct Authenticator {
    catalog: Catalog,
    ttl: Duration,
    anonymous: Option<TenantId>,
    cache: Mutex<Cache>,
}

impl Authenticator {
    /// Creates an authenticator caching positive answers for `ttl`.
    pub fn new(catalog: Catalog, ttl: Duration, anonymous: Option<TenantId>) -> Self {
        Self {
            catalog,
            ttl,
            anonymous,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Tenant for a request, or the gRPC status to answer with.
    ///
    /// # Errors
    /// `UNAUTHENTICATED` for missing, malformed, unknown or revoked tokens;
    /// `UNAVAILABLE` when the catalog cannot be reached.
    pub async fn authenticate(&self, metadata: &MetadataMap) -> Result<TenantId, Status> {
        let Some(token) = bearer(metadata) else {
            return self
                .anonymous
                .ok_or_else(|| Status::unauthenticated("missing bearer token"));
        };
        if !looks_like_token(token) {
            return Err(Status::unauthenticated("malformed token"));
        }
        let hash = hash_token(token);
        if let Some(cached) = self.cached(&hash) {
            return cached
                .tenant()
                .ok_or_else(|| Status::unauthenticated("unknown or revoked token"));
        }
        let asked = Instant::now();
        let found = self
            .catalog
            .resolve_token_until(token, TokenScope::Ingest)
            .await
            .map_err(|e| Status::unavailable(format!("token lookup failed: {e}")))?;
        let (resolved, not_after) = match found {
            Some((tenant, left)) => (
                Lookup::Tenant(tenant),
                left.map(|left| asked.checked_add(left).unwrap_or(asked)),
            ),
            None => (Lookup::Unknown, None),
        };
        self.remember(hash, resolved, not_after);
        resolved
            .tenant()
            .ok_or_else(|| Status::unauthenticated("unknown or revoked token"))
    }

    /// Forgets cached answers, for example after a revocation.
    pub fn clear(&self) {
        if let Ok(mut cache) = self.cache.lock() {
            cache.clear();
        }
    }

    fn cached(&self, hash: &[u8; 32]) -> Option<Lookup> {
        let cache = self.cache.lock().ok()?;
        let (value, expires) = cache.get(hash)?;
        (Instant::now() < *expires).then_some(*value)
    }

    /// Caches `value` for the TTL, a positive answer never past
    /// `not_after`, the instant a rotation stops the token, counted from
    /// before the lookup so no wait since then extends it.
    fn remember(&self, hash: [u8; 32], value: Lookup, not_after: Option<Instant>) {
        let ttl = if matches!(value, Lookup::Tenant(_)) {
            self.ttl
        } else {
            NEGATIVE_TTL
        };
        if let Ok(mut cache) = self.cache.lock() {
            let now = Instant::now();
            let Some(expires) = expiry(now, ttl, not_after) else {
                return;
            };
            if cache.len() > 10_000 {
                cache.retain(|_, (_, expires)| now < *expires);
            }
            cache.insert(hash, (value, expires));
        }
    }
}

/// When an answer stored at `now` expires: after `ttl`, never past
/// `not_after`; `None` when `not_after` has already come and the answer
/// must not be stored at all.
fn expiry(now: Instant, ttl: Duration, not_after: Option<Instant>) -> Option<Instant> {
    let expires = now.checked_add(ttl).unwrap_or(now);
    match not_after {
        Some(deadline) if deadline <= now => None,
        Some(deadline) => Some(expires.min(deadline)),
        None => Some(expires),
    }
}

/// The token of an `authorization: Bearer <token>` header.
fn bearer(metadata: &MetadataMap) -> Option<&str> {
    let value = metadata.get("authorization")?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| token.trim())
        .filter(|t| !t.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    #[test]
    fn an_answer_never_outlives_the_scheduled_end() {
        let asked = Instant::now();
        let ttl = Duration::from_secs(60);
        let end = asked + Duration::from_secs(10);
        assert_eq!(expiry(asked, ttl, None), Some(asked + ttl));
        assert_eq!(expiry(asked, ttl, Some(end)), Some(end));
        let after_a_wait = asked + Duration::from_secs(9);
        assert_eq!(
            expiry(after_a_wait, ttl, Some(end)),
            Some(end),
            "a wait before storing does not move the end"
        );
        assert_eq!(
            expiry(end, ttl, Some(end)),
            None,
            "stored at the end: not at all"
        );
        assert_eq!(expiry(end + Duration::from_secs(1), ttl, Some(end)), None);
        let short = Duration::from_secs(5);
        assert_eq!(expiry(asked, short, Some(end)), Some(asked + short));
    }

    #[test]
    fn extracts_bearer_tokens() {
        let mut md = MetadataMap::new();
        assert_eq!(bearer(&md), None);
        md.insert("authorization", MetadataValue::from_static("Bearer abc"));
        assert_eq!(bearer(&md), Some("abc"));
        md.insert("authorization", MetadataValue::from_static("bearer   xyz "));
        assert_eq!(bearer(&md), Some("xyz"));
        md.insert("authorization", MetadataValue::from_static("Basic abc"));
        assert_eq!(bearer(&md), None);
        md.insert("authorization", MetadataValue::from_static("Bearer "));
        assert_eq!(bearer(&md), None);
    }
}
