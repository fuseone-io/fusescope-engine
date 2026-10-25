//! Ingester lifecycle: WAL replay, gRPC server, flusher, shutdown.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use fs_catalog::{Catalog, TenantId};
use fs_schema::spans::SpanBatchBuilder;
use fs_wal::{Event, Position, Wal};
use object_store::ObjectStore;
use object_store::path::Path;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer;
use prost::Message;
use tokio::sync::{Mutex, Notify, watch};
use tonic::Status;
use tracing::{error, info, warn};

use crate::auth::Authenticator;
use crate::buffer::Buffers;
use crate::config::IngestConfig;
use crate::ratelimit::RateLimiter;
use crate::record::{RecordSignal, WalRecord};
use crate::server::TraceReceiver;
use crate::{IngestError, flush};

/// Counters exposed for metrics and tests.
#[derive(Debug, Default)]
pub struct IngestStats {
    /// Export requests accepted.
    pub requests: AtomicU64,
    /// Spans buffered.
    pub spans: AtomicU64,
    /// Spans rejected for malformed ids.
    pub spans_rejected: AtomicU64,
    /// Requests refused by the throughput limiter.
    pub rate_limited: AtomicU64,
    /// Requests refused because buffers or WAL were full.
    pub over_capacity: AtomicU64,
    /// Files written to the bucket.
    pub files_flushed: AtomicU64,
    /// Flush attempts that failed.
    pub flush_failures: AtomicU64,
}

/// Result of converting one request.
pub(crate) struct Converted {
    pub(crate) accepted: usize,
    pub(crate) rejected: usize,
}

/// State shared by the gRPC handler and the flusher.
pub struct Shared {
    pub(crate) config: IngestConfig,
    pub(crate) auth: Authenticator,
    pub(crate) limiter: Option<StdMutex<RateLimiter>>,
    pub(crate) stats: IngestStats,
    wal: Arc<StdMutex<Wal>>,
    buffers: Mutex<Buffers>,
    flush_now: Notify,
    catalog: Catalog,
    store: Arc<dyn ObjectStore>,
    root: Path,
}

impl Shared {
    /// Refuses when the tenant's buffer or the WAL on disk is over its cap.
    pub(crate) async fn check_capacity(&self, tenant: TenantId) -> Result<(), Status> {
        let buffered = self.buffers.lock().await.bytes_of(tenant);
        if buffered >= self.config.max_buffer_bytes {
            self.stats.over_capacity.fetch_add(1, Ordering::Relaxed);
            self.flush_now.notify_one();
            return Err(Status::resource_exhausted(
                "tenant buffer full, flush pending",
            ));
        }
        let wal_bytes = {
            let wal = self.wal.clone();
            tokio::task::spawn_blocking(move || {
                wal.lock().map_or(0, |w| {
                    w.segments()
                        .map_or(0, |s| s.iter().map(|x| x.bytes).sum::<u64>())
                })
            })
            .await
            .unwrap_or(0)
        };
        if wal_bytes >= self.config.wal_max_bytes {
            self.stats.over_capacity.fetch_add(1, Ordering::Relaxed);
            self.flush_now.notify_one();
            return Err(Status::unavailable("write-ahead log full"));
        }
        Ok(())
    }

    /// Appends and syncs on a blocking thread.
    pub(crate) async fn append_wal(&self, record: Vec<u8>) -> Result<Position, IngestError> {
        let wal = self.wal.clone();
        let position = tokio::task::spawn_blocking(move || {
            let mut wal = wal.lock().map_err(|_| fs_wal::WalError::EmptyRecord)?;
            let position = wal.append(&record)?;
            wal.sync()?;
            Ok::<_, fs_wal::WalError>(position)
        })
        .await??;
        Ok(position)
    }

    /// Converts a request and adds it to the tenant buffer.
    pub(crate) async fn convert_and_buffer(
        &self,
        tenant: TenantId,
        message: &ExportTraceServiceRequest,
        position: Position,
        now: Instant,
    ) -> Result<Converted, IngestError> {
        let mut builder = SpanBatchBuilder::new(self.config.limits);
        builder.append_request(message)?;
        let stats = builder.stats();
        let batch = builder.finish()?;
        let accepted = batch.num_rows();
        let rejected = usize::try_from(stats.spans_rejected).unwrap_or(usize::MAX);

        let mut buffers = self.buffers.lock().await;
        buffers.push_batch(tenant, batch, position, now);
        let due = buffers.get_mut(tenant).rows >= self.config.flush_rows
            || buffers.get_mut(tenant).bytes >= self.config.flush_bytes;
        drop(buffers);
        if due {
            self.flush_now.notify_one();
        }
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        self.stats
            .spans
            .fetch_add(accepted as u64, Ordering::Relaxed);
        self.stats
            .spans_rejected
            .fetch_add(stats.spans_rejected, Ordering::Relaxed);
        Ok(Converted { accepted, rejected })
    }

    /// Flushes one tenant. On failure the rows go back to the buffer.
    async fn flush_tenant(&self, tenant: TenantId) -> Result<bool, IngestError> {
        let Some(taken) = self.buffers.lock().await.take(tenant) else {
            return Ok(false);
        };
        let root = self.root.clone();
        let batches = taken.batches.clone();
        let prepared =
            tokio::task::spawn_blocking(move || flush::prepare(tenant, &root, &batches)).await;
        let result = match prepared {
            Ok(Ok(prepared)) => match flush::upload(self.store.as_ref(), &prepared).await {
                Ok(()) => self
                    .catalog
                    .register_file(&prepared.file)
                    .await
                    .map(|_| prepared.file)
                    .map_err(IngestError::from),
                Err(e) => Err(e),
            },
            Ok(Err(e)) => Err(e),
            Err(e) => Err(IngestError::from(e)),
        };
        match result {
            Ok(file) => {
                self.stats.files_flushed.fetch_add(1, Ordering::Relaxed);
                info!(tenant = %tenant.0, path = %file.path, rows = file.row_count, bytes = file.size_bytes, services = file.services.len(), "flushed l0");
                // Keep serving the rows until queriers have surely seen the file.
                self.buffers.lock().await.remember_persisted(
                    tenant,
                    file.id,
                    taken.batches,
                    Instant::now(),
                );
                self.truncate_wal().await;
                Ok(true)
            }
            Err(e) => {
                self.stats.flush_failures.fetch_add(1, Ordering::Relaxed);
                warn!(tenant = %tenant.0, error = %e, rows = taken.rows, "flush failed, keeping rows buffered");
                self.buffers.lock().await.get_mut(tenant).restore(taken);
                Err(e)
            }
        }
    }

    /// Drops WAL segments every tenant has flushed past.
    async fn truncate_wal(&self) {
        let earliest = self.buffers.lock().await.earliest_wal();
        let wal = self.wal.clone();
        let result = tokio::task::spawn_blocking(move || {
            let mut wal = wal.lock().map_err(|_| fs_wal::WalError::EmptyRecord)?;
            let cut = earliest.unwrap_or_else(|| wal.position());
            wal.truncate_before(cut)
        })
        .await;
        match result {
            Ok(Ok(removed)) if removed > 0 => info!(segments = removed, "wal truncated"),
            Ok(Ok(_)) => {}
            Ok(Err(e)) => warn!(error = %e, "wal truncate failed"),
            Err(e) => warn!(error = %e, "wal truncate task failed"),
        }
    }

    /// Live and recently persisted rows of `tenant`, for the hot window.
    pub(crate) async fn snapshot(&self, tenant: TenantId) -> crate::buffer::Snapshot {
        self.buffers.lock().await.snapshot(tenant)
    }

    async fn flush_due(&self, everything: bool) {
        let now = Instant::now();
        let tenants = {
            let mut buffers = self.buffers.lock().await;
            buffers.sweep_persisted(self.config.persisted_retain, now);
            if everything {
                buffers.all()
            } else {
                buffers.due(
                    self.config.flush_rows,
                    self.config.flush_bytes,
                    self.config.flush_interval,
                    now,
                )
            }
        };
        for tenant in tenants {
            if let Err(e) = self.flush_tenant(tenant).await {
                error!(tenant = %tenant.0, error = %e, "flush error");
            }
        }
    }
}

/// Rebuilds buffers from the WAL. Returns `(records, damaged)`; damaged
/// records are counted and skipped.
fn replay(
    limits: fs_schema::limits::Limits,
    wal: &mut Wal,
    buffers: &mut Buffers,
) -> Result<(usize, usize), IngestError> {
    let mut records = 0usize;
    let mut damaged = 0usize;
    let now = Instant::now();
    for event in wal.replay()? {
        match event {
            Event::Record(entry) => {
                records += 1;
                let record = match WalRecord::decode(&entry.payload) {
                    Ok(r) => r,
                    Err(e) => {
                        damaged += 1;
                        warn!(position = ?entry.position, error = %e, "skipping undecodable wal record");
                        continue;
                    }
                };
                match record.signal {
                    RecordSignal::Traces => {
                        let Ok(message) = ExportTraceServiceRequest::decode(record.payload) else {
                            damaged += 1;
                            continue;
                        };
                        let mut builder = SpanBatchBuilder::new(limits);
                        builder.append_request(&message)?;
                        let batch = builder.finish()?;
                        buffers.push_batch(record.tenant, batch, entry.position, now);
                    }
                }
            }
            Event::Damaged { position, damage } => {
                damaged += 1;
                warn!(?position, ?damage, "wal damage during replay");
            }
        }
    }
    Ok((records, damaged))
}

impl Buffers {
    fn push_batch(
        &mut self,
        tenant: TenantId,
        batch: arrow::record_batch::RecordBatch,
        position: Position,
        now: Instant,
    ) {
        self.get_mut(tenant).push(batch, position, now);
    }
}

/// The ingester role.
pub struct Ingester {
    shared: Arc<Shared>,
}

impl Ingester {
    /// Wires the pieces together and replays the WAL into memory.
    ///
    /// # Errors
    /// WAL replay failure.
    pub fn new(
        config: IngestConfig,
        catalog: Catalog,
        store: Arc<dyn ObjectStore>,
        root: Path,
        mut wal: Wal,
    ) -> Result<Self, IngestError> {
        let auth = Authenticator::new(
            catalog.clone(),
            config.token_cache_ttl,
            config.anonymous_tenant,
        );
        if config.anonymous_tenant.is_some() {
            warn!(
                "anonymous ingest is enabled; every unauthenticated request is accepted for one tenant"
            );
        }
        let limiter = config
            .tenant_bytes_per_sec
            .map(|r| StdMutex::new(RateLimiter::new(r)));
        let mut buffers = Buffers::default();
        let (records, damaged) = replay(config.limits, &mut wal, &mut buffers)?;
        info!(
            records,
            damaged,
            rows = buffers.total_rows(),
            "wal replayed"
        );
        let shared = Shared {
            config,
            auth,
            limiter,
            stats: IngestStats::default(),
            wal: Arc::new(StdMutex::new(wal)),
            buffers: Mutex::new(buffers),
            flush_now: Notify::new(),
            catalog,
            store,
            root,
        };
        Ok(Self {
            shared: Arc::new(shared),
        })
    }

    /// Counters.
    pub fn stats(&self) -> &IngestStats {
        &self.shared.stats
    }

    /// Flushes every tenant now. Used by tests and at shutdown.
    pub async fn flush_all(&self) {
        self.shared.flush_due(true).await;
    }

    /// Serves OTLP (and the hot window when configured) until `shutdown`
    /// flips, then flushes everything.
    ///
    /// Bound addresses are sent on `bound` and `hot_bound` once listening, so
    /// callers (and tests binding port 0) learn the ports.
    ///
    /// # Errors
    /// Bind or transport failure.
    pub async fn run(
        &self,
        mut shutdown: watch::Receiver<bool>,
        bound: Option<tokio::sync::oneshot::Sender<std::net::SocketAddr>>,
        hot_bound: Option<tokio::sync::oneshot::Sender<std::net::SocketAddr>>,
    ) -> Result<(), IngestError> {
        let listener = tokio::net::TcpListener::bind(self.shared.config.listen)
            .await
            .map_err(|source| IngestError::Bind {
                addr: self.shared.config.listen.to_string(),
                source,
            })?;
        let addr = listener.local_addr().map_err(|source| IngestError::Bind {
            addr: "local_addr".into(),
            source,
        })?;
        info!(%addr, "otlp grpc listening");
        if let Some(tx) = bound {
            let _ = tx.send(addr);
        }

        let service = TraceServiceServer::new(TraceReceiver::new(self.shared.clone()))
            .max_decoding_message_size(self.shared.config.max_message_bytes);
        let mut server_shutdown = shutdown.clone();
        let server = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    let _ = server_shutdown.changed().await;
                },
            );

        let hot = crate::hot::serve(
            self.shared.clone(),
            self.shared.config.hot_listen,
            shutdown.clone(),
            hot_bound,
        );

        let shared = self.shared.clone();
        let flusher = async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = tick.tick() => shared.flush_due(false).await,
                    () = shared.flush_now.notified() => shared.flush_due(false).await,
                    _ = shutdown.changed() => break,
                }
            }
        };

        let (transport, hot_result, ()) = tokio::join!(server, hot, flusher);
        transport?;
        hot_result?;
        info!("otlp server stopped, flushing buffers");
        self.flush_all().await;
        let wal = self.shared.wal.clone();
        tokio::task::spawn_blocking(move || {
            wal.lock()
                .map_err(|_| fs_wal::WalError::EmptyRecord)
                .and_then(|mut w| w.sync())
        })
        .await??;
        Ok(())
    }
}
