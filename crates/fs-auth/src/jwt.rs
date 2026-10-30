//! Tenant JWTs from the backend: compact JWS, `EdDSA` over Ed25519 only.
//!
//! A token is three base64url segments without padding. The header names
//! `alg` `EdDSA` and the `kid` of a key in the configured set, and carries
//! no `crit`. The claims hold `iss` and `aud` as configured, `sub` (who in
//! the backend asked, for the audit), `fusescope_tenant` (the tenant's
//! UUID), `iat` and `exp` as integer seconds, and optionally `nbf`. A token
//! lives at most the configured `max_ttl` from `iat` to `exp`, and the
//! clock may differ by `leeway` either way. Anything else is refused, the
//! signature being checked before any claim is read.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::Value;
use thiserror::Error;
use tokio::sync::watch;
use tokio_rustls::rustls::crypto::ring::default_provider;
use tokio_rustls::rustls::pki_types::{SignatureVerificationAlgorithm, alg_id};

use crate::jwks::{Jwks, JwksStats};
use crate::reload::Reload;

/// Longest token read; a backend token is a few hundred bytes.
pub const MAX_TOKEN_BYTES: usize = 8 * 1024;
/// Longest `sub` accepted.
pub const MAX_SUBJECT_BYTES: usize = 256;
/// The claim naming the tenant.
pub const TENANT_CLAIM: &str = "fusescope_tenant";

/// What a token must say besides its signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JwtRules {
    /// Required `iss`.
    pub issuer: String,
    /// Required in `aud`.
    pub audience: String,
    /// Longest `exp - iat`.
    pub max_ttl: Duration,
    /// Clock difference tolerated on `exp`, `iat` and `nbf`.
    pub leeway: Duration,
}

/// The claims of a verified token the engine uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claims {
    /// The tenant the backend acts for.
    pub tenant: uuid::Uuid,
    /// Who in the backend asked.
    pub subject: String,
    /// `exp`, seconds since the epoch.
    pub expires: u64,
}

/// Why a token or the key set was refused.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum JwtError {
    /// Not three base64url segments of JSON, or larger than the bound.
    #[error("malformed token")]
    Malformed,
    /// A header other than `alg` `EdDSA` with a `kid` and no `crit`.
    #[error("unsupported token header")]
    Header,
    /// No key in the set has the token's `kid`.
    #[error("unknown signing key")]
    UnknownKey,
    /// The signature does not verify.
    #[error("bad signature")]
    Signature,
    /// A claim is missing, wrong or out of time.
    #[error("token claim {0} refused")]
    Claim(&'static str),
    /// The key set file is unreadable or holds something else.
    #[error("jwt keys {}: {reason}", path.display())]
    Keys {
        /// The file.
        path: PathBuf,
        /// What is wrong with it.
        reason: String,
    },
}

/// Verifies tenant JWTs against a reloaded key set.
pub struct JwtVerifier {
    rules: JwtRules,
    keys: Arc<Jwks>,
    ed25519: &'static dyn SignatureVerificationAlgorithm,
}

impl std::fmt::Debug for JwtVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtVerifier")
            .field("rules", &self.rules)
            .finish_non_exhaustive()
    }
}

impl JwtVerifier {
    /// Reads the key set at `keys`, read again every `reload_interval`.
    ///
    /// # Errors
    /// The file is unreadable or not a set of Ed25519 signing keys.
    pub fn load(
        keys: PathBuf,
        rules: JwtRules,
        reload_interval: Duration,
    ) -> Result<Arc<Self>, JwtError> {
        let ed25519 = default_provider()
            .signature_verification_algorithms
            .all
            .iter()
            .copied()
            .find(|alg| {
                alg.public_key_alg_id() == alg_id::ED25519
                    && alg.signature_alg_id() == alg_id::ED25519
            })
            .ok_or_else(|| JwtError::Keys {
                path: keys.clone(),
                reason: "the crypto provider has no Ed25519".to_owned(),
            })?;
        Ok(Arc::new(Self {
            rules,
            keys: Arc::new(Jwks::load(keys, reload_interval)?),
            ed25519,
        }))
    }

    /// Verifies `token` now.
    ///
    /// # Errors
    /// See [`JwtVerifier::verify_at`].
    pub fn verify(&self, token: &str) -> Result<Claims, JwtError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        self.verify_at(token, now)
    }

    /// Verifies `token` as of `now`, seconds since the epoch.
    ///
    /// # Errors
    /// The token is malformed, its header unsupported, its key unknown, its
    /// signature bad, or a claim missing, wrong or out of time.
    pub fn verify_at(&self, token: &str, now: u64) -> Result<Claims, JwtError> {
        if token.len() > MAX_TOKEN_BYTES {
            return Err(JwtError::Malformed);
        }
        let mut segments = token.split('.');
        let (Some(header), Some(payload), Some(signature), None) = (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
        ) else {
            return Err(JwtError::Malformed);
        };
        let header: Value = json(header)?;
        if header.get("alg").and_then(Value::as_str) != Some("EdDSA")
            || header.get("crit").is_some()
        {
            return Err(JwtError::Header);
        }
        let kid = header
            .get("kid")
            .and_then(Value::as_str)
            .ok_or(JwtError::Header)?;
        let keys = self.keys.current();
        let key = keys.get(kid).ok_or(JwtError::UnknownKey)?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| JwtError::Malformed)?;
        let signed = &token[..header_and_payload_len(token)];
        self.ed25519
            .verify_signature(key, signed.as_bytes(), &signature)
            .map_err(|_| JwtError::Signature)?;
        self.claims(&json(payload)?, now)
    }

    fn claims(&self, claims: &Value, now: u64) -> Result<Claims, JwtError> {
        let leeway = self.rules.leeway.as_secs();
        let string = |name: &'static str| {
            claims
                .get(name)
                .and_then(Value::as_str)
                .ok_or(JwtError::Claim(name))
        };
        let seconds = |name: &'static str| {
            claims
                .get(name)
                .and_then(Value::as_u64)
                .ok_or(JwtError::Claim(name))
        };
        if string("iss")? != self.rules.issuer {
            return Err(JwtError::Claim("iss"));
        }
        let audience = match claims.get("aud") {
            Some(Value::String(one)) => one == &self.rules.audience,
            Some(Value::Array(many)) => many
                .iter()
                .any(|aud| aud.as_str() == Some(&self.rules.audience)),
            _ => false,
        };
        if !audience {
            return Err(JwtError::Claim("aud"));
        }
        let issued = seconds("iat")?;
        let expires = seconds("exp")?;
        if issued > now.saturating_add(leeway) {
            return Err(JwtError::Claim("iat"));
        }
        if expires.saturating_add(leeway) < now {
            return Err(JwtError::Claim("exp"));
        }
        if expires < issued || expires - issued > self.rules.max_ttl.as_secs() {
            return Err(JwtError::Claim("exp"));
        }
        if claims.get("nbf").is_some() && seconds("nbf")? > now.saturating_add(leeway) {
            return Err(JwtError::Claim("nbf"));
        }
        let subject = string("sub")?;
        if subject.is_empty() || subject.len() > MAX_SUBJECT_BYTES {
            return Err(JwtError::Claim("sub"));
        }
        let tenant = uuid::Uuid::parse_str(string(TENANT_CLAIM)?)
            .map_err(|_| JwtError::Claim(TENANT_CLAIM))?;
        Ok(Claims {
            tenant,
            subject: subject.to_owned(),
            expires,
        })
    }

    /// Reads the key set again and publishes it if it changed.
    ///
    /// # Errors
    /// The changed file does not load; the last good set stays, marked stale.
    pub fn reload(&self) -> Result<Reload, JwtError> {
        self.keys.reload()
    }

    /// Reloads the key set on its interval until `shutdown` flips.
    pub async fn run(self: Arc<Self>, shutdown: watch::Receiver<bool>) {
        Arc::clone(&self.keys).run(shutdown).await;
    }

    /// The key set's reload interval.
    pub fn reload_interval(&self) -> Duration {
        self.keys.interval()
    }

    /// Counters of the key set's reload.
    pub fn stats(&self) -> Arc<JwksStats> {
        Arc::clone(&self.keys.stats)
    }

    /// Whether the last reload failed.
    pub fn stale(&self) -> bool {
        self.keys.stats.stale.load(Ordering::Relaxed)
    }
}

fn header_and_payload_len(token: &str) -> usize {
    token.rfind('.').unwrap_or(0)
}

fn json(segment: &str) -> Result<Value, JwtError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(segment)
        .map_err(|_| JwtError::Malformed)?;
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| JwtError::Malformed)?;
    if value.is_object() {
        Ok(value)
    } else {
        Err(JwtError::Malformed)
    }
}
