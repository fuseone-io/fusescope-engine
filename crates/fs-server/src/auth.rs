//! The `[auth]` section: how Flight SQL clients authenticate. `token`, the
//! default, takes the catalog's static query tokens; `jwt` takes only the
//! backend's tenant JWTs, verified against the key set in `jwt_keys`.

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde::Deserialize;

use crate::ServerError;

/// How Flight SQL clients authenticate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    /// Static query tokens from the catalog.
    #[default]
    Token,
    /// The backend's tenant JWTs only.
    Jwt,
}

/// Authentication settings.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthSection {
    /// `token` or `jwt`.
    #[serde(default)]
    pub mode: AuthMode,
    /// JWK set of the backend's Ed25519 signing keys.
    #[serde(default)]
    pub jwt_keys: String,
    /// Required `iss`.
    #[serde(default)]
    pub jwt_issuer: String,
    /// Required in `aud`.
    #[serde(default = "default_audience")]
    pub jwt_audience: String,
    /// Longest a token may live from `iat` to `exp`.
    #[serde(default = "default_max_ttl")]
    pub jwt_max_ttl_secs: u64,
    /// Clock difference tolerated with the backend.
    #[serde(default = "default_leeway")]
    pub jwt_leeway_secs: u64,
    /// How often the key set is read again.
    #[serde(default = "default_reload")]
    pub jwt_reload_interval_secs: u64,
    /// How long a tenant found alive in the catalog is trusted.
    #[serde(default = "default_tenant_cache")]
    pub tenant_cache_secs: u64,
}

impl Default for AuthSection {
    fn default() -> Self {
        Self {
            mode: AuthMode::Token,
            jwt_keys: String::new(),
            jwt_issuer: String::new(),
            jwt_audience: default_audience(),
            jwt_max_ttl_secs: default_max_ttl(),
            jwt_leeway_secs: default_leeway(),
            jwt_reload_interval_secs: default_reload(),
            tenant_cache_secs: default_tenant_cache(),
        }
    }
}

fn default_audience() -> String {
    "fusescope-engine".to_owned()
}

fn default_max_ttl() -> u64 {
    300
}

fn default_leeway() -> u64 {
    30
}

fn default_reload() -> u64 {
    60
}

fn default_tenant_cache() -> u64 {
    10
}

impl AuthSection {
    /// Refuses `jwt` without its key set and issuer, `token` with them
    /// named, and bounds outside reason.
    ///
    /// # Errors
    /// The section is incomplete, contradictory or out of bounds.
    pub fn validate(&self) -> Result<(), ServerError> {
        let invalid = |m: &str| Err(ServerError::InvalidConfig(m.to_owned()));
        let named = !self.jwt_keys.is_empty() || !self.jwt_issuer.is_empty();
        match self.mode {
            AuthMode::Jwt if self.jwt_keys.is_empty() || self.jwt_issuer.is_empty() => {
                return invalid("auth.mode = \"jwt\" requires auth.jwt_keys and auth.jwt_issuer");
            }
            AuthMode::Token if named => {
                return invalid(
                    "auth.jwt_keys or auth.jwt_issuer is set but auth.mode is \"token\"; set mode = \"jwt\" or remove them",
                );
            }
            _ => {}
        }
        if self.jwt_audience.is_empty()
            || !(1..=3600).contains(&self.jwt_max_ttl_secs)
            || self.jwt_leeway_secs > 300
            || !(1..=86_400).contains(&self.jwt_reload_interval_secs)
            || self.tenant_cache_secs > 3600
        {
            return invalid(
                "auth.jwt_audience must be set, auth.jwt_max_ttl_secs between 1 and 3600, auth.jwt_leeway_secs at most 300, auth.jwt_reload_interval_secs between 1 and 86400 and auth.tenant_cache_secs at most 3600",
            );
        }
        Ok(())
    }

    /// The verifier's inputs, in `jwt` mode.
    pub fn jwt(&self) -> Option<(PathBuf, fs_auth::JwtRules, Duration)> {
        (self.mode == AuthMode::Jwt).then(|| {
            (
                PathBuf::from(&self.jwt_keys),
                fs_auth::JwtRules {
                    issuer: self.jwt_issuer.clone(),
                    audience: self.jwt_audience.clone(),
                    max_ttl: Duration::from_secs(self.jwt_max_ttl_secs),
                    leeway: Duration::from_secs(self.jwt_leeway_secs),
                },
                Duration::from_secs(self.jwt_reload_interval_secs),
            )
        })
    }

    /// How long a tenant found alive is trusted.
    pub fn tenant_cache(&self) -> Duration {
        Duration::from_secs(self.tenant_cache_secs)
    }
}

/// Renders the key set's counters.
pub(crate) fn render(out: &mut String, stats: &fs_auth::JwksStats) {
    let load = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed);
    crate::metrics::counter(
        out,
        "fusescope_jwt_key_reloads_total",
        "Changed JWT key sets that loaded",
        load(&stats.reloads),
    );
    crate::metrics::counter(
        out,
        "fusescope_jwt_key_reload_failures_total",
        "JWT key set reloads that failed and kept the last good set",
        load(&stats.reload_failures),
    );
    crate::metrics::gauge(
        out,
        "fusescope_jwt_keys_stale",
        "1 when the last JWT key set reload failed",
        u64::from(stats.stale.load(Ordering::Relaxed)),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn section(toml: &str) -> AuthSection {
        toml::from_str(toml).unwrap()
    }

    #[test]
    fn token_by_default_and_jwt_refused_half_configured() {
        let token = AuthSection::default();
        token.validate().unwrap();
        assert!(token.jwt().is_none());
        let jwt = section(
            "mode = \"jwt\"\njwt_keys = \"/keys/jwks.json\"\njwt_issuer = \"fusescope-backend\"\n",
        );
        jwt.validate().unwrap();
        let (keys, rules, reload) = jwt.jwt().unwrap();
        assert_eq!(keys, PathBuf::from("/keys/jwks.json"));
        assert_eq!(rules.audience, "fusescope-engine");
        assert_eq!(rules.max_ttl, Duration::from_secs(300));
        assert_eq!(reload, Duration::from_secs(60));
        for (bad, why) in [
            ("mode = \"jwt\"\n", "requires auth.jwt_keys"),
            (
                "mode = \"jwt\"\njwt_keys = \"k\"\n",
                "requires auth.jwt_keys",
            ),
            ("jwt_keys = \"k\"\n", "auth.mode is \"token\""),
            (
                "mode = \"jwt\"\njwt_keys = \"k\"\njwt_issuer = \"i\"\njwt_max_ttl_secs = 3601\n",
                "between 1 and 3600",
            ),
            (
                "mode = \"jwt\"\njwt_keys = \"k\"\njwt_issuer = \"i\"\njwt_audience = \"\"\n",
                "jwt_audience must be set",
            ),
        ] {
            let err = section(bad).validate().unwrap_err().to_string();
            assert!(err.contains(why), "{bad}: {err}");
        }
        assert!(toml::from_str::<AuthSection>("mode = \"oauth\"\n").is_err());
    }
}
