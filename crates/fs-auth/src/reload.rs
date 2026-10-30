//! The loaded TLS of a process and its reload from the files.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::watch;
use tokio_rustls::rustls::ServerConfig;
use tonic::transport::Identity;
use tracing::{info, warn};

use crate::client::ClientTls;
use crate::incoming::ServerTls;
use crate::load::{Pems, TlsError, TlsFiles, load};

/// Bounds of the TLS listeners and the reload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TlsLimits {
    /// How often the files are read again.
    pub reload_interval: Duration,
    /// Longest a handshake may take before the connection is dropped.
    pub handshake_timeout: Duration,
    /// Handshakes in progress per listener; connections beyond it are
    /// closed at once.
    pub max_handshakes: usize,
}

/// Counters of the TLS listeners and the reload.
#[derive(Debug, Default)]
pub struct TlsStats {
    /// Changed files that loaded and now serve new connections.
    pub reloads: AtomicU64,
    /// Reloads that failed; the last good config stays.
    pub reload_failures: AtomicU64,
    /// Handshakes that failed or timed out, a refused client certificate
    /// included.
    pub handshake_failures: AtomicU64,
    /// Connections closed because `max_handshakes` were in progress.
    pub handshakes_refused: AtomicU64,
    /// The last reload failed: new connections get an older config than
    /// the files describe.
    pub stale: AtomicBool,
}

/// Outcome of a successful [`Tls::reload`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reload {
    /// The files are as last read.
    Unchanged,
    /// The files changed and new connections get them.
    Reloaded,
}

/// A process's TLS: the config new connections get and the files it
/// comes from.
pub struct Tls {
    files: TlsFiles,
    limits: TlsLimits,
    server: watch::Sender<Arc<ServerConfig>>,
    identity: watch::Sender<Identity>,
    seen: Mutex<Pems>,
    stats: Arc<TlsStats>,
}

impl std::fmt::Debug for Tls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tls")
            .field("files", &self.files)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl Tls {
    /// Reads and validates the files.
    ///
    /// # Errors
    /// A file is unreadable or empty, a certificate or CA does not parse,
    /// or the key does not match the certificate.
    pub fn load(files: TlsFiles, limits: TlsLimits) -> Result<Arc<Self>, TlsError> {
        let pems = Pems::read(&files)?;
        let loaded = load(&pems, &files)?;
        Ok(Arc::new(Self {
            files,
            limits,
            server: watch::Sender::new(loaded.server),
            identity: watch::Sender::new(loaded.identity),
            seen: Mutex::new(pems),
            stats: Arc::new(TlsStats::default()),
        }))
    }

    /// What a listener needs: the current config, followed.
    pub fn server(&self) -> ServerTls {
        ServerTls::new(
            self.server.subscribe(),
            Arc::clone(&self.stats),
            self.limits,
        )
    }

    /// What a client needs: the current identity, followed.
    pub fn client(&self) -> ClientTls {
        ClientTls::new(self.identity.subscribe())
    }

    /// Counters, shared with the listeners.
    pub fn stats(&self) -> Arc<TlsStats> {
        Arc::clone(&self.stats)
    }

    /// Reads the files again and publishes them if they changed. Contents
    /// that failed once are not tried again until they change.
    ///
    /// # Errors
    /// The changed files do not load; the last good config stays and is
    /// marked stale.
    pub fn reload(&self) -> Result<Reload, TlsError> {
        let result = self.try_reload();
        match &result {
            Ok(Reload::Unchanged) => {}
            Ok(Reload::Reloaded) => {
                self.stats.reloads.fetch_add(1, Ordering::Relaxed);
                self.stats.stale.store(false, Ordering::Relaxed);
                info!(cert = %self.files.cert.display(), "tls reloaded; new connections use it");
            }
            Err(e) => {
                self.stats.reload_failures.fetch_add(1, Ordering::Relaxed);
                self.stats.stale.store(true, Ordering::Relaxed);
                warn!(error = %e, "tls reload failed; the last good config stays");
            }
        }
        result
    }

    fn try_reload(&self) -> Result<Reload, TlsError> {
        let pems = Pems::read(&self.files)?;
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        if *seen == pems {
            return Ok(Reload::Unchanged);
        }
        let loaded = load(&pems, &self.files);
        *seen = pems;
        drop(seen);
        let loaded = loaded?;
        self.server.send_replace(loaded.server);
        self.identity.send_replace(loaded.identity);
        Ok(Reload::Reloaded)
    }

    /// Reloads every `reload_interval` until `shutdown` flips. The files
    /// are read on the blocking pool.
    pub async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut tick = tokio::time::interval(self.limits.reload_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    let tls = Arc::clone(&self);
                    if let Err(e) = tokio::task::spawn_blocking(move || tls.reload()).await {
                        warn!(error = %e, "tls reload task failed");
                    }
                }
                _ = shutdown.changed() => break,
            }
        }
    }
}
