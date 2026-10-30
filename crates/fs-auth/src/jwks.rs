//! The backend's signing keys: a JWK set of Ed25519 public keys by `kid`,
//! read again on an interval like the TLS files.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Deserialize;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::jwt::JwtError;
use crate::reload::Reload;

const ED25519_KEY_BYTES: usize = 32;
/// Largest key set file read; a set of a few keys is well under it.
const MAX_JWKS_BYTES: u64 = 1 << 20;

/// Ed25519 public keys by `kid`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct KeySet {
    keys: HashMap<String, [u8; ED25519_KEY_BYTES]>,
}

impl KeySet {
    pub(crate) fn get(&self, kid: &str) -> Option<&[u8; ED25519_KEY_BYTES]> {
        self.keys.get(kid)
    }

    /// Parses a JWK set holding only Ed25519 signing keys, refusing any
    /// other key, a repeated `kid` or an empty set rather than skipping.
    pub(crate) fn parse(bytes: &[u8]) -> Result<Self, String> {
        #[derive(Deserialize)]
        struct Set {
            keys: Vec<Jwk>,
        }
        #[derive(Deserialize)]
        struct Jwk {
            kty: String,
            crv: Option<String>,
            kid: Option<String>,
            x: Option<String>,
            alg: Option<String>,
            #[serde(rename = "use")]
            usage: Option<String>,
        }
        let set: Set = serde_json::from_slice(bytes).map_err(|e| format!("not a JWK set: {e}"))?;
        if set.keys.is_empty() {
            return Err("no key".to_owned());
        }
        let mut keys = HashMap::new();
        for (n, jwk) in set.keys.into_iter().enumerate() {
            let at = |reason: &str| format!("key {} {reason}", n + 1);
            if jwk.kty != "OKP" || jwk.crv.as_deref() != Some("Ed25519") {
                return Err(at("is not an Ed25519 key (kty OKP, crv Ed25519)"));
            }
            if jwk.alg.as_deref().is_some_and(|alg| alg != "EdDSA")
                || jwk.usage.as_deref().is_some_and(|usage| usage != "sig")
            {
                return Err(at("is not for EdDSA signatures"));
            }
            let kid = jwk
                .kid
                .filter(|kid| !kid.is_empty())
                .ok_or_else(|| at("has no kid"))?;
            let x = URL_SAFE_NO_PAD
                .decode(jwk.x.unwrap_or_default())
                .ok()
                .and_then(|x| <[u8; ED25519_KEY_BYTES]>::try_from(x).ok())
                .ok_or_else(|| at("has no 32-byte x"))?;
            if keys.insert(kid.clone(), x).is_some() {
                return Err(format!("kid {kid:?} appears twice"));
            }
        }
        Ok(Self { keys })
    }
}

/// Counters of the key set's reload.
#[derive(Debug, Default)]
pub struct JwksStats {
    /// Changed key sets that loaded.
    pub reloads: AtomicU64,
    /// Reloads that failed; the last good set stays.
    pub reload_failures: AtomicU64,
    /// The last reload failed.
    pub stale: AtomicBool,
}

/// The key set file, its current contents and their reload.
pub(crate) struct Jwks {
    path: PathBuf,
    interval: Duration,
    current: Mutex<Arc<KeySet>>,
    seen: Mutex<Vec<u8>>,
    pub(crate) stats: Arc<JwksStats>,
}

impl Jwks {
    pub(crate) fn load(path: PathBuf, interval: Duration) -> Result<Self, JwtError> {
        let bytes = read(&path)?;
        let keys = KeySet::parse(&bytes).map_err(|reason| invalid(&path, reason))?;
        Ok(Self {
            path,
            interval,
            current: Mutex::new(Arc::new(keys)),
            seen: Mutex::new(bytes),
            stats: Arc::new(JwksStats::default()),
        })
    }

    pub(crate) fn current(&self) -> Arc<KeySet> {
        Arc::clone(&self.current.lock().unwrap_or_else(PoisonError::into_inner))
    }

    pub(crate) fn reload(&self) -> Result<Reload, JwtError> {
        let result = self.try_reload();
        match &result {
            Ok(Reload::Unchanged) => {}
            Ok(Reload::Reloaded) => {
                self.stats.reloads.fetch_add(1, Ordering::Relaxed);
                self.stats.stale.store(false, Ordering::Relaxed);
                info!(keys = %self.path.display(), "jwt keys reloaded");
            }
            Err(e) => {
                self.stats.reload_failures.fetch_add(1, Ordering::Relaxed);
                self.stats.stale.store(true, Ordering::Relaxed);
                warn!(error = %e, "jwt key reload failed; the last good set stays");
            }
        }
        result
    }

    fn try_reload(&self) -> Result<Reload, JwtError> {
        let bytes = read(&self.path)?;
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        if *seen == bytes {
            return Ok(Reload::Unchanged);
        }
        let keys = KeySet::parse(&bytes);
        *seen = bytes;
        drop(seen);
        let keys = keys.map_err(|reason| invalid(&self.path, reason))?;
        *self.current.lock().unwrap_or_else(PoisonError::into_inner) = Arc::new(keys);
        Ok(Reload::Reloaded)
    }

    pub(crate) fn interval(&self) -> Duration {
        self.interval
    }

    pub(crate) async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut tick = tokio::time::interval(self.interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    let jwks = Arc::clone(&self);
                    if let Err(e) = tokio::task::spawn_blocking(move || jwks.reload()).await {
                        warn!(error = %e, "jwt key reload task failed");
                    }
                }
                _ = shutdown.changed() => break,
            }
        }
    }
}

fn read(path: &Path) -> Result<Vec<u8>, JwtError> {
    let size = std::fs::metadata(path)
        .map_err(|e| invalid(path, e.to_string()))?
        .len();
    if size > MAX_JWKS_BYTES {
        return Err(invalid(
            path,
            format!("{size} bytes, over {MAX_JWKS_BYTES}"),
        ));
    }
    std::fs::read(path).map_err(|e| invalid(path, e.to_string()))
}

fn invalid(path: &Path, reason: String) -> JwtError {
    JwtError::Keys {
        path: path.to_owned(),
        reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const X: &str = "11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo";

    fn key(fields: &str) -> String {
        format!(r#"{{"keys":[{{{fields}}}]}}"#)
    }

    #[test]
    fn only_ed25519_signing_keys_with_distinct_kids_are_accepted() {
        let good = key(&format!(
            r#""kty":"OKP","crv":"Ed25519","kid":"k1","x":"{X}","alg":"EdDSA","use":"sig""#
        ));
        assert!(KeySet::parse(good.as_bytes()).unwrap().get("k1").is_some());
        for bad in [
            r#"{"keys":[]}"#.to_owned(),
            "not json".to_owned(),
            key(&format!(r#""kty":"EC","crv":"P-256","kid":"k1","x":"{X}""#)),
            key(&format!(
                r#""kty":"OKP","crv":"X25519","kid":"k1","x":"{X}""#
            )),
            key(&format!(r#""kty":"OKP","crv":"Ed25519","x":"{X}""#)),
            key(&format!(
                r#""kty":"OKP","crv":"Ed25519","kid":"k1","x":"{X}","alg":"RS256""#
            )),
            key(&format!(
                r#""kty":"OKP","crv":"Ed25519","kid":"k1","x":"{X}","use":"enc""#
            )),
            key(r#""kty":"OKP","crv":"Ed25519","kid":"k1","x":"AAAA""#),
            format!(
                r#"{{"keys":[{{"kty":"OKP","crv":"Ed25519","kid":"k1","x":"{X}"}},{{"kty":"OKP","crv":"Ed25519","kid":"k1","x":"{X}"}}]}}"#
            ),
        ] {
            assert!(KeySet::parse(bad.as_bytes()).is_err(), "{bad}");
        }
    }
}
