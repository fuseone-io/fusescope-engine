//! Who a Flight SQL request comes from, and the ticket bound to it.
//!
//! In `token` mode a static query token from the catalog names the tenant,
//! and the ticket is the tenant id followed by the SQL, as it has always
//! been. In `jwt` mode the backend's JWT names the tenant and the subject
//! acting for it, the tenant must exist and not be deleted in the catalog,
//! and the ticket is `FST2`, the tenant id, the subject's length as two
//! big-endian bytes, the subject and the SQL: `DoGet` executes it only for
//! a token of the same tenant and subject. It carries no MAC, because a
//! caller that writes its own ticket can only name itself. A `jwt`
//! querier refuses the old ticket, so queriers move to `jwt` before the
//! backend sends JWTs.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use fs_auth::JwtVerifier;
use fs_catalog::{Catalog, TenantId, TokenScope};
use tonic::Status;
use tonic::metadata::MetadataMap;
use tracing::debug;

use crate::querier::QueryStats;

const TICKET_V2: &[u8; 4] = b"FST2";
const UUID_BYTES: usize = 16;
/// Tenants whose existence is remembered; past it the cache starts over.
const MAX_CACHED_TENANTS: usize = 10_000;

/// How Flight SQL clients authenticate.
#[derive(Clone)]
pub enum QueryAuth {
    /// Static query tokens from the catalog.
    Token,
    /// The backend's tenant JWTs; a tenant found in the catalog is trusted
    /// for `tenant_ttl` before it is looked up again.
    Jwt {
        /// Verifies the tokens.
        verifier: Arc<JwtVerifier>,
        /// How long a tenant found alive is trusted.
        tenant_ttl: Duration,
    },
}

impl std::fmt::Debug for QueryAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryAuth::Token => f.write_str("Token"),
            QueryAuth::Jwt { tenant_ttl, .. } => f
                .debug_struct("Jwt")
                .field("tenant_ttl", tenant_ttl)
                .finish_non_exhaustive(),
        }
    }
}

/// Who a request comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    /// The tenant it acts for.
    pub tenant: TenantId,
    /// Who in the backend asked; `None` for a static token.
    pub subject: Option<String>,
}

/// Resolves callers and remembers tenants found alive.
pub(crate) struct Authenticator {
    catalog: Catalog,
    auth: QueryAuth,
    alive: Mutex<HashMap<TenantId, Instant>>,
    stats: Arc<QueryStats>,
}

impl Authenticator {
    pub(crate) fn new(catalog: Catalog, auth: QueryAuth, stats: Arc<QueryStats>) -> Self {
        Self {
            catalog,
            auth,
            alive: Mutex::new(HashMap::new()),
            stats,
        }
    }

    /// The caller of a request carrying `authorization: Bearer <token>`.
    pub(crate) async fn caller(&self, metadata: &MetadataMap) -> Result<Caller, Status> {
        let result = self.resolve(metadata).await;
        if let Err(status) = &result {
            if status.code() == tonic::Code::Unauthenticated {
                self.stats.auth_refused.fetch_add(1, Ordering::Relaxed);
                debug!(reason = status.message(), "flight request refused");
            }
        }
        result
    }

    async fn resolve(&self, metadata: &MetadataMap) -> Result<Caller, Status> {
        let token = metadata
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split_once(' '))
            .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
            .map(|(_, t)| t.trim())
            .ok_or_else(|| Status::unauthenticated("missing bearer token"))?;
        match &self.auth {
            QueryAuth::Token => {
                let tenant = self
                    .catalog
                    .resolve_token(token, TokenScope::Query)
                    .await
                    .map_err(|e| Status::unavailable(format!("token lookup failed: {e}")))?
                    .ok_or_else(|| Status::unauthenticated("unknown or revoked token"))?;
                Ok(Caller {
                    tenant,
                    subject: None,
                })
            }
            QueryAuth::Jwt {
                verifier,
                tenant_ttl,
            } => {
                let claims = verifier
                    .verify(token)
                    .map_err(|e| Status::unauthenticated(e.to_string()))?;
                let tenant = TenantId(claims.tenant);
                self.ensure_alive(tenant, *tenant_ttl).await?;
                Ok(Caller {
                    tenant,
                    subject: Some(claims.subject),
                })
            }
        }
    }

    async fn ensure_alive(&self, tenant: TenantId, ttl: Duration) -> Result<(), Status> {
        let now = Instant::now();
        let known = self
            .alive
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&tenant)
            .is_some_and(|until| *until > now);
        if known {
            return Ok(());
        }
        let found = self
            .catalog
            .tenant(tenant)
            .await
            .map_err(|e| Status::unavailable(format!("tenant lookup failed: {e}")))?;
        if found.is_none_or(|t| t.deleted_at.is_some()) {
            return Err(Status::unauthenticated("unknown or deleted tenant"));
        }
        let mut alive = self.alive.lock().unwrap_or_else(PoisonError::into_inner);
        if alive.len() >= MAX_CACHED_TENANTS {
            alive.clear();
        }
        alive.insert(tenant, now + ttl);
        Ok(())
    }
}

/// The ticket for `sql` planned for `caller`.
pub(crate) fn encode_ticket(caller: &Caller, sql: &str) -> bytes::Bytes {
    let mut out = Vec::with_capacity(TICKET_V2.len() + UUID_BYTES + 2 + sql.len());
    if let Some(subject) = &caller.subject {
        out.extend_from_slice(TICKET_V2);
        out.extend_from_slice(caller.tenant.0.as_bytes());
        let len = u16::try_from(subject.len()).unwrap_or(u16::MAX);
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(
            subject
                .as_bytes()
                .get(..usize::from(len))
                .unwrap_or_default(),
        );
    } else {
        out.extend_from_slice(caller.tenant.0.as_bytes());
    }
    out.extend_from_slice(sql.as_bytes());
    bytes::Bytes::from(out)
}

/// The SQL of `ticket` if it was issued to `caller`.
pub(crate) fn redeem_ticket(ticket: &[u8], caller: &Caller) -> Result<String, Status> {
    let bad = || Status::invalid_argument("bad ticket");
    let (tenant, subject, sql) = if caller.subject.is_some() {
        let rest = ticket
            .strip_prefix(TICKET_V2)
            .ok_or_else(|| Status::permission_denied("ticket was not issued for a tenant token"))?;
        let (tenant, rest) = rest.split_at_checked(UUID_BYTES).ok_or_else(bad)?;
        let (len, rest) = rest.split_first_chunk::<2>().ok_or_else(bad)?;
        let (subject, sql) = rest
            .split_at_checked(usize::from(u16::from_be_bytes(*len)))
            .ok_or_else(bad)?;
        let subject = std::str::from_utf8(subject).map_err(|_| bad())?;
        (tenant, Some(subject), sql)
    } else {
        let (tenant, sql) = ticket.split_at_checked(UUID_BYTES).ok_or_else(bad)?;
        (tenant, None, sql)
    };
    let tenant = TenantId(uuid::Uuid::from_slice(tenant).map_err(|_| bad())?);
    if tenant != caller.tenant {
        return Err(Status::permission_denied(
            "ticket does not belong to this tenant",
        ));
    }
    if subject != caller.subject.as_deref() {
        return Err(Status::permission_denied(
            "ticket was issued to another subject",
        ));
    }
    std::str::from_utf8(sql)
        .map(str::to_owned)
        .map_err(|_| bad())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caller(tenant: u128, subject: Option<&str>) -> Caller {
        Caller {
            tenant: TenantId(uuid::Uuid::from_u128(tenant)),
            subject: subject.map(str::to_owned),
        }
    }

    #[test]
    fn a_ticket_is_redeemed_only_by_its_tenant_and_subject() {
        let token = caller(1, None);
        let ticket = encode_ticket(&token, "select 1");
        assert_eq!(ticket.len(), 16 + 8, "the old layout, unchanged");
        assert_eq!(redeem_ticket(&ticket, &token).unwrap(), "select 1");
        assert!(redeem_ticket(&ticket, &caller(2, None)).is_err());

        let alice = caller(1, Some("user:alice"));
        let ticket = encode_ticket(&alice, "select 2");
        assert_eq!(redeem_ticket(&ticket, &alice).unwrap(), "select 2");
        for other in [caller(1, Some("user:bob")), caller(2, Some("user:alice"))] {
            assert_eq!(
                redeem_ticket(&ticket, &other).unwrap_err().code(),
                tonic::Code::PermissionDenied,
                "{other:?}"
            );
        }
        let old = encode_ticket(&token, "select 1");
        assert_eq!(
            redeem_ticket(&old, &alice).unwrap_err().code(),
            tonic::Code::PermissionDenied,
            "an old ticket is not redeemed with a JWT"
        );
        for garbage in [&b"FST2"[..], b"FST2short", b"short"] {
            assert!(redeem_ticket(garbage, &alice).is_err());
        }
    }
}
