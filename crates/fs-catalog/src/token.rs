//! Ingest tokens: random secrets handed to collectors, stored hashed.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use crate::CatalogError;
use crate::catalog::Catalog;
use crate::types::TenantId;

/// Prefix of every token, so leaked strings are recognisable in scanners.
pub const TOKEN_PREFIX: &str = "fst_";

/// Bytes of entropy in a token.
const TOKEN_BYTES: usize = 32;

/// What a token may do. Stored as text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
pub enum TokenScope {
    /// Send telemetry to the ingester.
    Ingest,
    /// Run queries against the querier.
    Query,
}

impl TokenScope {
    /// Lowercase name, as stored.
    pub fn name(self) -> &'static str {
        match self {
            TokenScope::Ingest => "ingest",
            TokenScope::Query => "query",
        }
    }
}

impl std::str::FromStr for TokenScope {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ingest" => Ok(TokenScope::Ingest),
            "query" => Ok(TokenScope::Query),
            other => Err(format!(
                "unknown token scope {other:?}, expected ingest or query"
            )),
        }
    }
}

/// A token row, never carrying the secret.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct TenantToken {
    /// The first 8 bytes of the token's sha256 in hex: how an operator
    /// names it without the secret.
    pub id: String,
    /// Owner.
    pub tenant_id: TenantId,
    /// Human label, for example the collector it was issued to.
    pub name: String,
    /// What it may do.
    pub scope: TokenScope,
    /// Issue time.
    pub created_at: DateTime<Utc>,
    /// When it stops being accepted: in the past once revoked, in the
    /// future while a rotation lets it overlap with its successor.
    pub revoked_at: Option<DateTime<Utc>>,
}

/// The columns of a [`TenantToken`], `t` being `engine_tenant_token`.
pub(crate) const TOKEN_COLUMNS: &str = "encode(substring(t.token_hash from 1 for 8), 'hex') as id, \
     t.tenant_id, t.name, t.scope, t.created_at, t.revoked_at";

/// Longest overlap a rotation may give the old token.
pub const MAX_ROTATION_OVERLAP: std::time::Duration = std::time::Duration::from_secs(7 * 86_400);

/// Generates a fresh token: `fst_` plus 32 random bytes in URL-safe base64.
///
/// # Errors
/// The operating system refused to provide randomness.
pub fn generate_token() -> Result<String, getrandom::Error> {
    let mut bytes = [0u8; TOKEN_BYTES];
    getrandom::fill(&mut bytes)?;
    Ok(format!("{TOKEN_PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes)))
}

/// Hash stored in the catalog and used for lookups.
pub fn hash_token(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

/// True when `token` has the shape this crate issues. Cheap pre-check before
/// hashing and querying, so random strings do not reach the database.
pub fn looks_like_token(token: &str) -> bool {
    token.len() == TOKEN_PREFIX.len() + 43
        && token.starts_with(TOKEN_PREFIX)
        && token[TOKEN_PREFIX.len()..]
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_well_formed_and_unique() {
        let a = generate_token().unwrap();
        let b = generate_token().unwrap();
        assert!(looks_like_token(&a), "{a}");
        assert!(looks_like_token(&b), "{b}");
        assert_ne!(a, b);
        assert_ne!(hash_token(&a), hash_token(&b));
        assert_eq!(hash_token(&a), hash_token(&a));
    }

    #[test]
    fn rejects_malformed_tokens() {
        assert!(!looks_like_token(""));
        assert!(!looks_like_token("fst_short"));
        assert!(!looks_like_token(&format!("xyz_{}", "a".repeat(43))));
        assert!(!looks_like_token(&format!("fst_{}", "a".repeat(42))));
        assert!(!looks_like_token(&format!("fst_{}!", "a".repeat(42))));
        assert!(looks_like_token(&format!("fst_{}", "a".repeat(43))));
    }
}

/// The 8 bytes a token id names, or `None` when it is not 16 hex digits.
fn id_bytes(id: &str) -> Option<[u8; 8]> {
    if id.len() != 16 || !id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; 8];
    for (n, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(id.get(2 * n..2 * n + 2)?, 16).ok()?;
    }
    Some(out)
}

impl Catalog {
    /// The tenant of a live token with `scope` and when it stops being
    /// accepted, if a rotation scheduled that; `None` when the token is
    /// unknown, revoked, of another scope, or its tenant deleted.
    ///
    /// # Errors
    /// Database failure.
    pub async fn resolve_token_until(
        &self,
        token: &str,
        scope: TokenScope,
    ) -> Result<Option<(TenantId, Option<DateTime<Utc>>)>, CatalogError> {
        if !looks_like_token(token) {
            return Ok(None);
        }
        let hash = hash_token(token);
        Ok(sqlx::query_as(
            "select t.tenant_id, t.revoked_at from engine_tenant_token t
             join engine_tenant n on n.id = t.tenant_id
             where t.token_hash = $1 and t.scope = $2
               and (t.revoked_at is null or t.revoked_at > now()) and n.deleted_at is null",
        )
        .bind(hash.as_slice())
        .bind(scope)
        .fetch_optional(self.pool())
        .await?)
    }

    /// Revokes the token named `id` now, a scheduled revocation brought
    /// forward included. `None` when no live token has that id.
    ///
    /// # Errors
    /// `id` names more than one token, or database failure.
    pub async fn revoke_token_by_id(&self, id: &str) -> Result<Option<TenantToken>, CatalogError> {
        let Some(prefix) = id_bytes(id) else {
            return Ok(None);
        };
        let mut tx = self.pool().begin().await?;
        let Some(hash) = live_hash(&mut tx, id, &prefix).await? else {
            return Ok(None);
        };
        let row = sqlx::query_as::<_, TenantToken>(&format!(
            "update engine_tenant_token t set revoked_at = now() where t.token_hash = $1
             returning {TOKEN_COLUMNS}"
        ))
        .bind(&hash)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Some(row))
    }

    /// Issues a successor of the token named `id`, same tenant, name and
    /// scope, and schedules the old one to stop `overlap` from now, clamped
    /// to [`MAX_ROTATION_OVERLAP`] and never later than it was already
    /// due. Returns the new secret, shown once, the new row and the old.
    /// `None` when no live token has that id.
    ///
    /// # Errors
    /// `id` names more than one token, or database failure.
    pub async fn rotate_token(
        &self,
        id: &str,
        overlap: std::time::Duration,
    ) -> Result<Option<(String, TenantToken, TenantToken)>, CatalogError> {
        let Some(prefix) = id_bytes(id) else {
            return Ok(None);
        };
        let overlap = i64::try_from(overlap.min(MAX_ROTATION_OVERLAP).as_secs()).unwrap_or(0);
        let mut tx = self.pool().begin().await?;
        let Some(hash) = live_hash(&mut tx, id, &prefix).await? else {
            return Ok(None);
        };
        let old = sqlx::query_as::<_, TenantToken>(&format!(
            "update engine_tenant_token t
             set revoked_at = least(coalesce(t.revoked_at, 'infinity'), now() + make_interval(secs => $2))
             where t.token_hash = $1
             returning {TOKEN_COLUMNS}"
        ))
        .bind(&hash)
        .bind(overlap)
        .fetch_one(&mut *tx)
        .await?;
        let secret = generate_token()?;
        let new = sqlx::query_as::<_, TenantToken>(&format!(
            "insert into engine_tenant_token as t (token_hash, tenant_id, name, scope)
             values ($1, $2, $3, $4)
             returning {TOKEN_COLUMNS}"
        ))
        .bind(hash_token(&secret).as_slice())
        .bind(old.tenant_id)
        .bind(&old.name)
        .bind(old.scope)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Some((secret, new, old)))
    }
}

/// The hash of the one live token whose hash starts with `prefix`, locked.
async fn live_hash(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: &str,
    prefix: &[u8; 8],
) -> Result<Option<Vec<u8>>, CatalogError> {
    let rows: Vec<(Vec<u8>,)> = sqlx::query_as(
        "select token_hash from engine_tenant_token
         where substring(token_hash from 1 for 8) = $1
           and (revoked_at is null or revoked_at > now())
         for update",
    )
    .bind(prefix.as_slice())
    .fetch_all(&mut **tx)
    .await?;
    match rows.as_slice() {
        [] => Ok(None),
        [(hash,)] => Ok(Some(hash.clone())),
        _ => Err(CatalogError::AmbiguousToken { id: id.to_owned() }),
    }
}

#[cfg(test)]
mod id_tests {
    use super::*;

    #[test]
    fn a_token_id_is_sixteen_hex_digits() {
        assert_eq!(
            id_bytes("00ff10a0b0c0d0e0"),
            Some([0x00, 0xff, 0x10, 0xa0, 0xb0, 0xc0, 0xd0, 0xe0])
        );
        for bad in [
            "",
            "00ff",
            "00ff10a0b0c0d0e0ff",
            "zzff10a0b0c0d0e0",
            "00ff10a0b0c0d0é",
        ] {
            assert_eq!(id_bytes(bad), None, "{bad}");
        }
    }
}
