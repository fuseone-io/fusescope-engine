//! TLS listeners: the handshake with the config current when a connection
//! arrives, off the accept loop.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::server::TlsStream;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::server::Router;
use tracing::{debug, warn};

use crate::reload::{TlsLimits, TlsStats};

/// Pause after a failed `accept`, such as running out of file descriptors,
/// so the loop does not spin.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(50);

/// A listener's side of a process's TLS.
#[derive(Clone)]
pub struct ServerTls {
    config: watch::Receiver<Arc<ServerConfig>>,
    stats: Arc<TlsStats>,
    limits: TlsLimits,
}

impl std::fmt::Debug for ServerTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerTls")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl ServerTls {
    pub(crate) fn new(
        config: watch::Receiver<Arc<ServerConfig>>,
        stats: Arc<TlsStats>,
        limits: TlsLimits,
    ) -> Self {
        Self {
            config,
            stats,
            limits,
        }
    }

    /// Connections from `listener` that completed a handshake, a client
    /// certificate included, each with the config current when it arrived.
    /// Handshakes run concurrently, at most `max_handshakes` at a time and
    /// each within `handshake_timeout`; the accept loop stops once the
    /// stream is dropped.
    pub fn incoming(
        &self,
        listener: TcpListener,
    ) -> ReceiverStream<std::io::Result<TlsStream<TcpStream>>> {
        let (tx, rx) = mpsc::channel(1);
        let tls = self.clone();
        tokio::spawn(async move { tls.accept(listener, tx).await });
        ReceiverStream::new(rx)
    }

    async fn accept(
        self,
        listener: TcpListener,
        tx: mpsc::Sender<std::io::Result<TlsStream<TcpStream>>>,
    ) {
        let permits = Arc::new(Semaphore::new(self.limits.max_handshakes));
        loop {
            let accepted = tokio::select! {
                accepted = listener.accept() => accepted,
                () = tx.closed() => break,
            };
            let (tcp, peer) = match accepted {
                Ok(pair) => pair,
                Err(e) => {
                    warn!(error = %e, "accept failed");
                    tokio::time::sleep(ACCEPT_BACKOFF).await;
                    continue;
                }
            };
            let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                self.stats
                    .handshakes_refused
                    .fetch_add(1, Ordering::Relaxed);
                debug!(%peer, "tls handshakes at their limit; connection closed");
                continue;
            };
            let acceptor = TlsAcceptor::from(Arc::clone(&self.config.borrow()));
            let stats = Arc::clone(&self.stats);
            let timeout = self.limits.handshake_timeout;
            let tx = tx.clone();
            tokio::spawn(async move {
                let _permit = permit;
                match tokio::time::timeout(timeout, acceptor.accept(tcp)).await {
                    Ok(Ok(stream)) => {
                        let _ = tx.send(Ok(stream)).await;
                    }
                    Ok(Err(e)) => {
                        stats.handshake_failures.fetch_add(1, Ordering::Relaxed);
                        debug!(%peer, error = %e, "tls handshake refused");
                    }
                    Err(_) => {
                        stats.handshake_failures.fetch_add(1, Ordering::Relaxed);
                        debug!(%peer, ?timeout, "tls handshake timed out");
                    }
                }
            });
        }
    }
}

/// Serves `router` on `listener` until `signal`: over mTLS with `tls`, in
/// plain text without it.
///
/// # Errors
/// Transport failure.
pub async fn serve(
    router: Router,
    listener: TcpListener,
    tls: Option<&ServerTls>,
    signal: impl Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    match tls {
        Some(tls) => {
            router
                .serve_with_incoming_shutdown(tls.incoming(listener), signal)
                .await
        }
        None => {
            router
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), signal)
                .await
        }
    }
}
