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
        let (resolved, left) = match found {
            Some((tenant, left)) => (
                Lookup::Tenant(tenant),
                left.map(|left| left.saturating_sub(asked.elapsed())),
            ),
            None => (Lookup::Unknown, None),
        };
        self.remember(hash, resolved, left);
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

    /// Caches `value` for the TTL, a positive answer for no longer than
    /// `left`, the time a rotation left the token.
    fn remember(&self, hash: [u8; 32], value: Lookup, left: Option<Duration>) {
        let mut ttl = if matches!(value, Lookup::Tenant(_)) {
            self.ttl
        } else {
            NEGATIVE_TTL
        };
        if let Some(left) = left {
            ttl = ttl.min(left);
        }
        if let Ok(mut cache) = self.cache.lock() {
            if cache.len() > 10_000 {
                cache.retain(|_, (_, expires)| Instant::now() < *expires);
            }
            cache.insert(hash, (value, Instant::now() + ttl));
        }
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
