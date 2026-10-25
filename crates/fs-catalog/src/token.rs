//! Ingest tokens: random secrets handed to collectors, stored hashed.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use crate::types::TenantId;

/// Prefix of every token, so leaked strings are recognisable in scanners.
pub const TOKEN_PREFIX: &str = "fst_";

/// Bytes of entropy in a token.
const TOKEN_BYTES: usize = 32;

/// A token row, never carrying the secret.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct TenantToken {
    /// Owner.
    pub tenant_id: TenantId,
    /// Human label, for example the collector it was issued to.
    pub name: String,
    /// Issue time.
    pub created_at: DateTime<Utc>,
    /// Revocation time, when revoked.
    pub revoked_at: Option<DateTime<Utc>>,
}

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
