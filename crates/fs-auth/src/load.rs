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

/// The PEM files of a process's TLS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsFiles {
    /// Certificate chain, leaf first, served and presented as a client.
    pub cert: PathBuf,
    /// Private key of the leaf.
    pub key: PathBuf,
    /// CAs a connecting client's certificate must chain to.
    pub client_ca: PathBuf,
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

/// The contents of the three files, read at one moment.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Pems {
    cert: Vec<u8>,
    key: Vec<u8>,
    client_ca: Vec<u8>,
}

impl fmt::Debug for Pems {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pems")
            .field("cert", &self.cert.len())
            .field("key", &"***")
            .field("client_ca", &self.client_ca.len())
            .finish()
    }
}

impl Pems {
    pub(crate) fn read(files: &TlsFiles) -> Result<Self, TlsError> {
        Ok(Self {
            cert: read(&files.cert)?,
            key: read(&files.key)?,
            client_ca: read(&files.client_ca)?,
        })
    }
}

/// What one load produced: the listeners' config and the clients' identity.
pub(crate) struct Loaded {
    pub(crate) server: Arc<ServerConfig>,
    pub(crate) identity: Identity,
}

/// Builds both sides from `pems`, refusing a certificate or CA that does
/// not parse, a key that does not match the certificate, or an empty file.
pub(crate) fn load(pems: &Pems, files: &TlsFiles) -> Result<Loaded, TlsError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let chain = certificates(&pems.cert, &files.cert)?;
    let key = PrivateKeyDer::from_pem_slice(&pems.key)
        .map_err(|e| invalid(&files.key, format!("no private key: {e}")))?;
    let mut roots = RootCertStore::empty();
    for ca in certificates(&pems.client_ca, &files.client_ca)? {
        roots
            .add(ca)
            .map_err(|e| invalid(&files.client_ca, e.to_string()))?;
    }
    let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
        .build()
        .map_err(|e| invalid(&files.client_ca, e.to_string()))?;
    let pair = |e: rustls::Error| {
        invalid(
            &files.cert,
            format!("with key {}: {e}", files.key.display()),
        )
    };
    let mut server = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(pair)?
        .with_client_cert_verifier(verifier)
        .with_single_cert(chain.clone(), key.clone_key())
        .map_err(pair)?;
    server.alpn_protocols = vec![b"h2".to_vec()];
    ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(pair)?
        .with_root_certificates(RootCertStore::empty())
        .with_client_auth_cert(chain, key)
        .map_err(pair)?;
    Ok(Loaded {
        server: Arc::new(server),
        identity: Identity::from_pem(&pems.cert, &pems.key),
    })
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
