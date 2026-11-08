//! Reading and validating the identity and the client CAs.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore, ServerConfig};
use tonic::transport::Identity;
use tracing::warn;

use crate::validity::Validity;

/// The PEM files of a process's TLS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsFiles {
    /// Certificate chain, leaf first, served and presented as a client.
    pub cert: PathBuf,
    /// Private key of the leaf.
    pub key: PathBuf,
    /// CAs a client of the public Flight SQL listener must chain to: the
    /// backend's.
    pub public_client_ca: PathBuf,
    /// CAs a client of the internal hot-window listener must chain to: the
    /// queriers'. It shares no CA with `public_client_ca`, so a certificate
    /// that opens one listener never opens the other.
    pub hot_client_ca: PathBuf,
    /// CAs a client of the OTLP ingest listener must chain to: the
    /// collectors'. It shares no CA with the other two; a process without
    /// it serves no ingest listener over TLS.
    pub ingest_client_ca: Option<PathBuf>,
}

/// Why TLS could not be loaded or a connection set up.
#[derive(Debug, Error)]
pub enum TlsError {
    /// A file could not be read.
    #[error("tls: reading {}: {source}", path.display())]
    Read {
        /// The file.
        path: PathBuf,
        /// Underlying error.
        #[source]
        source: std::io::Error,
    },
    /// A file holds nothing usable of what it is for.
    #[error("tls: {}: {reason}", path.display())]
    Invalid {
        /// The file.
        path: PathBuf,
        /// What is wrong with it.
        reason: String,
    },
    /// An endpoint address could not be made into a TLS endpoint.
    #[error("tls endpoint {address}: {reason}")]
    Endpoint {
        /// Address as given.
        address: String,
        /// What failed.
        reason: String,
    },
}

/// The contents of the files, read at one moment.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Pems {
    cert: Vec<u8>,
    key: Vec<u8>,
    public_client_ca: Vec<u8>,
    hot_client_ca: Vec<u8>,
    ingest_client_ca: Option<Vec<u8>>,
}

impl fmt::Debug for Pems {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pems")
            .field("cert", &self.cert.len())
            .field("key", &"***")
            .field("public_client_ca", &self.public_client_ca.len())
            .field("hot_client_ca", &self.hot_client_ca.len())
            .field(
                "ingest_client_ca",
                &self.ingest_client_ca.as_ref().map(Vec::len),
            )
            .finish()
    }
}

impl Pems {
    pub(crate) fn read(files: &TlsFiles) -> Result<Self, TlsError> {
        Ok(Self {
            cert: read(&files.cert)?,
            key: read(&files.key)?,
            public_client_ca: read(&files.public_client_ca)?,
            hot_client_ca: read(&files.hot_client_ca)?,
            ingest_client_ca: files.ingest_client_ca.as_deref().map(read).transpose()?,
        })
    }
}

/// What one load produced: each listener's config and the clients'
/// identity.
pub(crate) struct Loaded {
    pub(crate) public: Arc<ServerConfig>,
    pub(crate) hot: Arc<ServerConfig>,
    pub(crate) ingest: Option<Arc<ServerConfig>>,
    pub(crate) identity: Identity,
}

/// Builds every side from `pems`, refusing a certificate or CA that does
/// not parse, a key that does not match the certificate, an empty file, a
/// client CA that is not a self-signed root, any two client CA bundles
/// that share a CA key, a certificate of the chain outside its validity,
/// or a bundle none of whose CAs is within its validity.
pub(crate) fn load(pems: &Pems, files: &TlsFiles) -> Result<Loaded, TlsError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let now = now();
    let chain = certificates(&pems.cert, &files.cert)?;
    for (n, cert) in chain.iter().enumerate() {
        let validity = Validity::of(cert).ok_or_else(|| {
            invalid(
                &files.cert,
                format!(
                    "certificate {} of the chain has no readable validity",
                    n + 1
                ),
            )
        })?;
        if let Some(refusal) = validity.refusal(now) {
            return Err(invalid(
                &files.cert,
                format!("certificate {} of the chain {refusal}", n + 1),
            ));
        }
    }
    let key = PrivateKeyDer::from_pem_slice(&pems.key)
        .map_err(|e| invalid(&files.key, format!("no private key: {e}")))?;
    let (public_cas, public_keys) = bundle(
        &pems.public_client_ca,
        &files.public_client_ca,
        &provider,
        now,
    )?;
    let (hot_cas, hot_keys) = bundle(&pems.hot_client_ca, &files.hot_client_ca, &provider, now)?;
    disjoint(
        (&hot_keys, &files.hot_client_ca),
        (&public_keys, &files.public_client_ca),
        "a backend certificate would open the hot window",
    )?;
    let ingest = match (&pems.ingest_client_ca, &files.ingest_client_ca) {
        (Some(pem), Some(path)) => {
            let (cas, keys) = bundle(pem, path, &provider, now)?;
            disjoint(
                (&keys, path),
                (&public_keys, &files.public_client_ca),
                "a collector certificate would open Flight SQL",
            )?;
            disjoint(
                (&keys, path),
                (&hot_keys, &files.hot_client_ca),
                "a collector certificate would open the hot window",
            )?;
            Some((cas, path))
        }
        _ => None,
    };
    let pair = |e: rustls::Error| {
        invalid(
            &files.cert,
            format!("with key {}: {e}", files.key.display()),
        )
    };
    let server = |cas: Vec<CertificateDer<'static>>, path: &Path| {
        let mut roots = RootCertStore::empty();
        for ca in cas {
            roots.add(ca).map_err(|e| invalid(path, e.to_string()))?;
        }
        let verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
                .build()
                .map_err(|e| invalid(path, e.to_string()))?;
        let mut config = ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(pair)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain.clone(), key.clone_key())
            .map_err(pair)?;
        config.alpn_protocols = vec![b"h2".to_vec()];
        Ok::<_, TlsError>(Arc::new(config))
    };
    let public = server(public_cas, &files.public_client_ca)?;
    let hot = server(hot_cas, &files.hot_client_ca)?;
    let ingest = ingest.map(|(cas, path)| server(cas, path)).transpose()?;
    ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(pair)?
        .with_root_certificates(RootCertStore::empty())
        .with_client_auth_cert(chain, key)
        .map_err(pair)?;
    Ok(Loaded {
        public,
        hot,
        ingest,
        identity: Identity::from_pem(&pems.cert, &pems.key),
    })
}

/// A client CA bundle and the keys of its roots. A CA outside its
/// validity admits no one, so it is only reported unless every CA of the
/// bundle is.
fn bundle(
    pem: &[u8],
    path: &Path,
    provider: &rustls::crypto::CryptoProvider,
    now: i64,
) -> Result<(Vec<CertificateDer<'static>>, Vec<Vec<u8>>), TlsError> {
    let cas = certificates(pem, path)?;
    let keys = crate::anchors::root_keys(&cas, provider).map_err(|reason| invalid(path, reason))?;
    let mut refused = Vec::new();
    for (n, ca) in cas.iter().enumerate() {
        let validity = Validity::of(ca).ok_or_else(|| {
            invalid(
                path,
                format!(
                    "certificate {} of the bundle has no readable validity",
                    n + 1
                ),
            )
        })?;
        if let Some(refusal) = validity.refusal(now) {
            refused.push(format!("certificate {} {refusal}", n + 1));
        }
    }
    if refused.len() == cas.len() {
        return Err(invalid(
            path,
            format!("no CA of the bundle is valid now: {}", refused.join(", ")),
        ));
    }
    for refusal in &refused {
        warn!(path = %path.display(), "tls: a CA of the bundle admits no one: {refusal}");
    }
    Ok((cas, keys))
}

/// Seconds since the Unix epoch on the system clock.
fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// Refuses `bundle` when it shares a CA key with `other`, naming `bundle`.
fn disjoint(
    (keys, path): (&[Vec<u8>], &Path),
    (other_keys, other): (&[Vec<u8>], &Path),
    consequence: &str,
) -> Result<(), TlsError> {
    if keys.iter().any(|key| other_keys.contains(key)) {
        return Err(invalid(
            path,
            format!("shares a CA key with {}: {consequence}", other.display()),
        ));
    }
    Ok(())
}

fn certificates(pem: &[u8], path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let certs = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| invalid(path, e.to_string()))?;
    if certs.is_empty() {
        return Err(invalid(path, "no certificate".to_owned()));
    }
    Ok(certs)
}

fn read(path: &Path) -> Result<Vec<u8>, TlsError> {
    std::fs::read(path).map_err(|source| TlsError::Read {
        path: path.to_owned(),
        source,
    })
}

fn invalid(path: &Path, reason: String) -> TlsError {
    TlsError::Invalid {
        path: path.to_owned(),
        reason,
    }
}
