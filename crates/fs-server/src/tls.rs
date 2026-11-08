//! The `[tls]` section: mTLS of the Flight SQL listener, the internal hot
//! window listener, the OTLP ingest listener and the clients of the first
//! two. Off unless `enabled = true`; development keeps it off, staging and
//! production turn it on.

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde::Deserialize;

use crate::ServerError;

/// mTLS settings.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsSection {
    /// Require mTLS on every listener and use it as a client.
    #[serde(default)]
    pub enabled: bool,
    /// PEM certificate chain, leaf first; served and presented as a client.
    #[serde(default)]
    pub cert: String,
    /// PEM private key of the leaf.
    #[serde(default)]
    pub key: String,
    /// PEM bundle of the CAs a Flight SQL client, the backend, must chain to.
    #[serde(default)]
    pub public_client_ca: String,
    /// PEM bundle of the CAs a hot-window client, a querier, must chain to.
    /// Shares no CA with `public_client_ca`.
    #[serde(default)]
    pub hot_client_ca: String,
    /// PEM bundle of the CAs an OTLP client, a collector, must chain to.
    /// Shares no CA with the other two; required where an ingester runs.
    #[serde(default)]
    pub ingest_client_ca: String,
    /// How often the three files are read again.
    #[serde(default = "default_reload_interval")]
    pub reload_interval_secs: u64,
    /// Longest a handshake may take.
    #[serde(default = "default_handshake_timeout")]
    pub handshake_timeout_secs: u64,
    /// Handshakes in progress per listener.
    #[serde(default = "default_max_handshakes")]
    pub max_handshakes: usize,
}

impl Default for TlsSection {
    fn default() -> Self {
        Self {
            enabled: false,
            cert: String::new(),
            key: String::new(),
            public_client_ca: String::new(),
            hot_client_ca: String::new(),
            ingest_client_ca: String::new(),
            reload_interval_secs: default_reload_interval(),
            handshake_timeout_secs: default_handshake_timeout(),
            max_handshakes: default_max_handshakes(),
        }
    }
}

fn default_reload_interval() -> u64 {
    60
}

fn default_handshake_timeout() -> u64 {
    10
}

fn default_max_handshakes() -> usize {
    1024
}

impl TlsSection {
    /// The files, when TLS is on.
    pub fn files(&self) -> Option<fs_auth::TlsFiles> {
        self.enabled.then(|| fs_auth::TlsFiles {
            cert: PathBuf::from(&self.cert),
            key: PathBuf::from(&self.key),
            public_client_ca: PathBuf::from(&self.public_client_ca),
            hot_client_ca: PathBuf::from(&self.hot_client_ca),
            ingest_client_ca: (!self.ingest_client_ca.is_empty())
                .then(|| PathBuf::from(&self.ingest_client_ca)),
        })
    }

    /// The limits of the listeners and the reload.
    pub fn limits(&self) -> fs_auth::TlsLimits {
        fs_auth::TlsLimits {
            reload_interval: Duration::from_secs(self.reload_interval_secs),
            handshake_timeout: Duration::from_secs(self.handshake_timeout_secs),
            max_handshakes: self.max_handshakes,
        }
    }

    /// Refuses a half-configured section: on without its four files, or
    /// off with files named, which would look protected and is not.
    ///
    /// # Errors
    /// The section is incomplete, contradictory or out of bounds.
    pub fn validate(&self) -> Result<(), ServerError> {
        let invalid = |m: &str| Err(ServerError::InvalidConfig(m.to_owned()));
        let named = [
            &self.cert,
            &self.key,
            &self.public_client_ca,
            &self.hot_client_ca,
        ]
        .map(|p| !p.is_empty());
        if !self.enabled && !self.ingest_client_ca.is_empty() {
            return invalid(
                "tls.ingest_client_ca is set but tls.enabled is false; enable it or remove it",
            );
        }
        if self.enabled && named.contains(&false) {
            return invalid(
                "tls.enabled requires tls.cert, tls.key, tls.public_client_ca and tls.hot_client_ca",
            );
        }
        if !self.enabled && named.contains(&true) {
            return invalid(
                "tls.cert, tls.key, tls.public_client_ca or tls.hot_client_ca is set but tls.enabled is false; enable it or remove them",
            );
        }
        if self.enabled && self.public_client_ca == self.hot_client_ca {
            return invalid("tls.public_client_ca and tls.hot_client_ca must be different bundles");
        }
        if self.enabled
            && [&self.public_client_ca, &self.hot_client_ca].contains(&&self.ingest_client_ca)
        {
            return invalid(
                "tls.ingest_client_ca must be a bundle apart from tls.public_client_ca and tls.hot_client_ca",
            );
        }
        let day = 86_400;
        if !(1..=day).contains(&self.reload_interval_secs)
            || !(1..=day).contains(&self.handshake_timeout_secs)
            || !(1..=fs_auth::MAX_HANDSHAKES).contains(&self.max_handshakes)
        {
            return invalid(
                "tls.reload_interval_secs and tls.handshake_timeout_secs must be between 1 and 86400 and tls.max_handshakes between 1 and 65536",
            );
        }
        Ok(())
    }

    /// Refuses an ingester whose OTLP listener would serve plain text while
    /// `[tls]` is on.
    ///
    /// # Errors
    /// `[tls]` is on, `ingests` and `ingest_client_ca` is not set.
    pub fn validate_ingester(&self, ingests: bool) -> Result<(), ServerError> {
        if self.enabled && ingests && self.ingest_client_ca.is_empty() {
            return Err(ServerError::InvalidConfig(
                "tls.enabled requires tls.ingest_client_ca where an ingester runs: its OTLP listener would serve plain text".into(),
            ));
        }
        Ok(())
    }
}

/// Renders the TLS counters.
pub(crate) fn render(out: &mut String, stats: &fs_auth::TlsStats) {
    let load = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed);
    for (name, help, value) in [
        (
            "fusescope_tls_reloads_total",
            "Changed TLS files that loaded and serve new connections",
            load(&stats.reloads),
        ),
        (
            "fusescope_tls_reload_failures_total",
            "TLS reloads that failed and kept the last good config",
            load(&stats.reload_failures),
        ),
    ] {
        crate::metrics::counter(out, name, help, value);
    }
    let per_listener = |count: fn(&fs_auth::ListenerStats) -> &std::sync::atomic::AtomicU64| {
        fs_auth::Listener::ALL.map(|l| (l.name(), load(count(stats.listener(l)))))
    };
    crate::metrics::labelled_counter(
        out,
        "fusescope_tls_handshake_failures_total",
        "TLS handshakes that failed or timed out, refused client certificates included",
        "listener",
        &per_listener(|s| &s.handshake_failures),
    );
    crate::metrics::labelled_counter(
        out,
        "fusescope_tls_handshakes_refused_total",
        "Connections closed because tls.max_handshakes were in progress",
        "listener",
        &per_listener(|s| &s.handshakes_refused),
    );
    crate::metrics::gauge(
        out,
        "fusescope_tls_stale",
        "1 when the last TLS reload failed and new connections get an older config",
        u64::from(stats.stale.load(Ordering::Relaxed)),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn section(toml: &str) -> TlsSection {
        toml::from_str(toml).unwrap()
    }

    #[test]
    fn off_by_default_and_refused_half_configured() {
        let off = TlsSection::default();
        off.validate().unwrap();
        assert!(off.files().is_none());
        let on = section(
            "enabled = true\ncert = \"/tls/tls.crt\"\nkey = \"/tls/tls.key\"\npublic_client_ca = \"/tls/backend-ca.crt\"\nhot_client_ca = \"/tls/querier-ca.crt\"\n",
        );
        on.validate().unwrap();
        assert!(on.files().unwrap().ingest_client_ca.is_none());
        on.validate_ingester(false).unwrap();
        let err = on.validate_ingester(true).unwrap_err().to_string();
        assert!(err.contains("tls.ingest_client_ca"), "{err}");
        let ingesting = TlsSection {
            ingest_client_ca: "/tls/collector-ca.crt".into(),
            ..on.clone()
        };
        ingesting.validate().unwrap();
        ingesting.validate_ingester(true).unwrap();
        assert_eq!(
            ingesting.files().unwrap().ingest_client_ca,
            Some(PathBuf::from("/tls/collector-ca.crt"))
        );
        off.validate_ingester(true).unwrap();
        assert_eq!(
            on.files().unwrap().hot_client_ca,
            PathBuf::from("/tls/querier-ca.crt")
        );
        let at = |n: usize| TlsSection {
            max_handshakes: n,
            ..on.clone()
        };
        at(fs_auth::MAX_HANDSHAKES).validate().unwrap();
        assert!(at(fs_auth::MAX_HANDSHAKES + 1).validate().is_err());
        assert!(at(usize::MAX).validate().is_err());
        assert_eq!(on.limits().reload_interval, Duration::from_secs(60));
        assert_eq!(on.limits().max_handshakes, 1024);
        for (bad, why) in [
            (
                "enabled = true\ncert = \"a\"\nkey = \"b\"\n",
                "tls.enabled requires",
            ),
            ("enabled = true\n", "tls.enabled requires"),
            ("cert = \"a\"\n", "tls.enabled is false"),
            (
                "enabled = true\ncert = \"a\"\nkey = \"b\"\npublic_client_ca = \"c\"\nhot_client_ca = \"d\"\nreload_interval_secs = 0\n",
                "between 1 and 86400",
            ),
            (
                "enabled = true\ncert = \"a\"\nkey = \"b\"\npublic_client_ca = \"c\"\nhot_client_ca = \"d\"\nmax_handshakes = 0\n",
                "max_handshakes between 1 and 65536",
            ),
            (
                "enabled = true\ncert = \"a\"\nkey = \"b\"\npublic_client_ca = \"c\"\nhot_client_ca = \"c\"\n",
                "different bundles",
            ),
            ("ingest_client_ca = \"e\"\n", "tls.enabled is false"),
            (
                "enabled = true\ncert = \"a\"\nkey = \"b\"\npublic_client_ca = \"c\"\nhot_client_ca = \"d\"\ningest_client_ca = \"d\"\n",
                "a bundle apart",
            ),
            (
                "enabled = true\ncert = \"a\"\nkey = \"b\"\npublic_client_ca = \"c\"\nhot_client_ca = \"d\"\ningest_client_ca = \"c\"\n",
                "a bundle apart",
            ),
        ] {
            let err = section(bad).validate().unwrap_err().to_string();
            assert!(err.contains(why), "{bad}: {err}");
        }
    }
}
