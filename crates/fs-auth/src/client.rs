//! Clients of the engine's TLS listeners.

use tokio::sync::watch;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

use crate::load::TlsError;

/// A client's side of a process's TLS: the identity it presents, followed
/// across reloads, and the operating system's trust store for the server.
#[derive(Clone)]
pub struct ClientTls {
    identity: watch::Receiver<Identity>,
    pinned: Option<Vec<Certificate>>,
}

impl std::fmt::Debug for ClientTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientTls")
            .field("system_roots", &self.pinned.is_none())
            .finish_non_exhaustive()
    }
}

impl ClientTls {
    pub(crate) fn new(identity: watch::Receiver<Identity>) -> Self {
        Self {
            identity,
            pinned: None,
        }
    }

    /// The same identity trusting only the CAs in `ca_pem` for the server,
    /// in place of the system store. For tests, whose CAs are not in it;
    /// no configuration reaches it.
    #[must_use]
    pub fn trusting_only(mut self, ca_pem: &[u8]) -> Self {
        self.pinned = Some(vec![Certificate::from_pem(ca_pem)]);
        self
    }

    fn config(&self) -> ClientTlsConfig {
        let config = ClientTlsConfig::new().identity(self.identity.borrow().clone());
        match &self.pinned {
            Some(roots) => config.ca_certificates(roots.iter().cloned()),
            None => config.with_native_roots(),
        }
    }
}

/// An endpoint for `address` (`host:port`): `https` with the current
/// identity under `tls`, plain `http` without it. The host is the name
/// the server's certificate must carry.
///
/// # Errors
/// The address is not a valid URI authority.
pub fn endpoint(address: &str, tls: Option<&ClientTls>) -> Result<Endpoint, TlsError> {
    let fail = |e: tonic::transport::Error| TlsError::Endpoint {
        address: address.to_owned(),
        reason: e.to_string(),
    };
    match tls {
        Some(tls) => Endpoint::from_shared(format!("https://{address}"))
            .and_then(|endpoint| endpoint.tls_config(tls.config()))
            .map_err(fail),
        None => Endpoint::from_shared(format!("http://{address}")).map_err(fail),
    }
}

/// `error` and its sources on one line: a refused handshake shows only as
/// "transport error" at the top, its reason further down.
pub fn error_chain(error: &dyn std::error::Error) -> String {
    let mut out = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}
