//! Ingester lifecycle: WAL replay, gRPC server, flusher, shutdown.
//!
//! # Flush protocol
//!
//! A flush must produce the same file whether it runs once or is retried after
//! a crash at any point, with other requests arriving meanwhile:
//!
//! 1. Under the flush gate (a write lock no request holds while it appends to
//!    the WAL and buffers), take the tenant's live rows and read the WAL
//!    position: every record before it is in the buffers, so the rows taken
//!    are exactly the tenant's records in `[first, end)`.
//! 2. Append a [`FlushIntent`] naming the file id and that range, and sync.
//! 3. Encode, upload without overwriting, register idempotently.
//! 4. Move the rows to the persisted state and checkpoint the WAL.
//!
//! Replay groups records under the intents found in the log, so a crash
//! anywhere after step 2 reproduces the same file and the same catalog row;
//! a crash before step 2 simply flushes the rows again under a new intent.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use fs_catalog::{Catalog, FileId, TenantId};
use fs_schema::spans::SpanBatchBuilder;
use fs_wal::{Event, Position, Wal};
use object_store::ObjectStore;
use object_store::path::Path;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer;
use prost::Message;
use tokio::sync::{Mutex, Notify, RwLock, watch};
use tonic::Status;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::auth::Authenticator;
use crate::buffer::{Buffers, Snapshot, TenantBuffer};
use crate::config::IngestConfig;
use crate::ratelimit::RateLimiter;
use crate::record::{FlushIntent, RecordSignal, WalRecord};
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
    /// Flushes resumed from an intent after a restart.
    pub flushes_recovered: AtomicU64,
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
    instance: Uuid,
    buffers: Mutex<Buffers>,
    /// Requests hold it shared across WAL append and buffer push; a flush
    /// holds it exclusively while it takes a buffer and reads the WAL position.
    gate: RwLock<()>,
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
        let wal_bytes = self.wal.lock().map_or(0, |w| w.bytes());
        if wal_bytes >= self.config.wal_max_bytes {
            self.stats.over_capacity.fetch_add(1, Ordering::Relaxed);
            self.flush_now.notify_one();
            return Err(Status::unavailable("write-ahead log full"));
        }
        Ok(())
    }

    /// Appends and syncs on a blocking thread.
    async fn append_wal(&self, record: Vec<u8>) -> Result<Position, IngestError> {
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

    /// Logs a request, converts it and adds it to the tenant buffer, all while
    /// holding the flush gate so a concurrent flush sees either both effects or
    /// neither.
    pub(crate) async fn ingest(
        &self,
        tenant: TenantId,
        message: &ExportTraceServiceRequest,
        record: Vec<u8>,
        now: Instant,
    ) -> Result<Converted, IngestError> {
        let _shared = self.gate.read().await;
        let position = self.append_wal(record).await?;

        let mut builder = SpanBatchBuilder::new(self.config.limits);
        builder.append_request(message)?;
        let stats = builder.stats();
        let batch = builder.finish()?;
        let accepted = batch.num_rows();
        let rejected = usize::try_from(stats.spans_rejected).unwrap_or(usize::MAX);

        let due = {
            let mut buffers = self.buffers.lock().await;
            buffers.get_mut(tenant).push(batch, position, now);
            let live = buffers.get_mut(tenant);
            live.rows >= self.config.flush_rows || live.bytes >= self.config.flush_bytes
        };
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

    /// Steps 1 and 2 of the flush protocol, then materialisation.
    async fn flush_tenant(&self, tenant: TenantId) -> Result<bool, IngestError> {
        let (file_id, intent, batches) = {
            let _exclusive = self.gate.write().await;
            let end = self
                .wal
                .lock()
                .map(|w| w.position())
                .map_err(|_| fs_wal::WalError::EmptyRecord)?;
            let mut buffers = self.buffers.lock().await;
            let Some(first) = buffers.live_first_wal(tenant) else {
                return Ok(false);
            };
            let file_id = FileId::derived(self.instance, tenant, first.segment, first.offset);
            let Some(batches) = buffers.begin_flush(tenant, file_id) else {
                return Ok(false);
            };
            (
                file_id,
                FlushIntent {
                    tenant,
                    file: file_id,
                    first,
                    end,
                },
                batches,
            )
        };
        if let Err(e) = self.append_wal(intent.to_record().encode()).await {
            self.stats.flush_failures.fetch_add(1, Ordering::Relaxed);
            warn!(tenant = %tenant.0, error = %e, "flush intent could not be logged; keeping rows buffered");
            self.buffers.lock().await.abort_flush(tenant, file_id);
            return Err(e);
        }
        self.materialize(tenant, file_id, batches)
            .await
            .map(|()| true)
    }

    /// Steps 3 and 4: encode, upload, register, persist, checkpoint. On failure
    /// the rows return to the live buffer; the intent in the WAL still binds
    /// them to `file_id` if the process dies before the retry.
    async fn materialize(
        &self,
        tenant: TenantId,
        file_id: FileId,
        batches: Vec<RecordBatch>,
    ) -> Result<(), IngestError> {
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let root = self.root.clone();
        let prepared =
            tokio::task::spawn_blocking(move || flush::prepare(tenant, &root, file_id, &batches))
                .await;
        let result = match prepared {
            Ok(Ok(prepared)) => match flush::upload(self.store.as_ref(), &prepared).await {
                Ok(created) => self
                    .catalog
                    .register_file(&prepared.file)
                    .await
                    .map(|_| (prepared.file, created))
                    .map_err(IngestError::from),
                Err(e) => Err(e),
            },
            Ok(Err(e)) => Err(e),
            Err(e) => Err(IngestError::from(e)),
        };
        match result {
            Ok((file, created)) => {
                self.stats.files_flushed.fetch_add(1, Ordering::Relaxed);
                info!(
                    tenant = %tenant.0,
                    path = %file.path,
                    rows = file.row_count,
                    bytes = file.size_bytes,
                    services = file.services.len(),
                    created,
                    "flushed l0"
                );
                self.buffers
                    .lock()
                    .await
                    .finish_flush(tenant, file_id, Instant::now());
                if self.config.checkpoint_after_flush {
                    self.checkpoint_wal().await;
                }
                Ok(())
            }
            Err(e) => {
                self.stats.flush_failures.fetch_add(1, Ordering::Relaxed);
                warn!(tenant = %tenant.0, error = %e, rows, "flush failed, keeping rows buffered");
                self.buffers.lock().await.abort_flush(tenant, file_id);
                Err(e)
            }
        }
    }

    /// Finishes flushes that an intent in the WAL had started before a restart.
    async fn recover_flushing(&self) {
        let pending = self.buffers.lock().await.flushing();
        for (tenant, file, batches) in pending {
            self.stats.flushes_recovered.fetch_add(1, Ordering::Relaxed);
            info!(tenant = %tenant.0, file = %file.0, rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>(), "resuming flush from intent");
            if let Err(e) = self.materialize(tenant, file, batches).await {
                error!(tenant = %tenant.0, file = %file.0, error = %e, "recovered flush failed; rows stay buffered under the same intent");
            }
        }
    }

    /// Records that everything before the earliest still-buffered record is
    /// materialised: replay after a restart starts there, and segments before
    /// it are deleted.
    async fn checkpoint_wal(&self) {
        let earliest = self.buffers.lock().await.earliest_wal();
        let wal = self.wal.clone();
        let result = tokio::task::spawn_blocking(move || {
            let mut wal = wal.lock().map_err(|_| fs_wal::WalError::EmptyRecord)?;
            let cut = earliest.unwrap_or_else(|| wal.position());
            wal.checkpoint(cut).map(|()| cut)
        })
        .await;
        match result {
            Ok(Ok(cut)) => debug!(?cut, "wal checkpoint"),
            Ok(Err(e)) => warn!(error = %e, "wal checkpoint failed; replay may duplicate rows"),
            Err(e) => warn!(error = %e, "wal checkpoint task failed"),
        }
    }

    /// Rows of `tenant` in every state, plus the id the live rows will get.
    pub(crate) async fn snapshot(&self, tenant: TenantId) -> (Snapshot, Option<FileId>) {
        let snapshot = self.buffers.lock().await.snapshot(tenant);
        let live_file = snapshot
            .live_first_wal
            .map(|p| FileId::derived(self.instance, tenant, p.segment, p.offset));
        (snapshot, live_file)
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

/// Everything replay found: rows with their positions, and flush intents.
struct Replayed {
    rows: Vec<(TenantId, Position, RecordBatch)>,
    intents: Vec<FlushIntent>,
    records: usize,
    damaged: usize,
}

/// Reads the WAL from the checkpoint. Damaged records are counted and skipped.
fn replay(limits: fs_schema::limits::Limits, wal: &mut Wal) -> Result<Replayed, IngestError> {
    let mut out = Replayed {
        rows: Vec::new(),
        intents: Vec::new(),
        records: 0,
        damaged: 0,
    };
    for event in wal.replay()? {
        match event {
            Event::Record(entry) => {
                out.records += 1;
                let record = match WalRecord::decode(&entry.payload) {
                    Ok(r) => r,
                    Err(e) => {
                        out.damaged += 1;
                        warn!(position = ?entry.position, error = %e, "skipping undecodable wal record");
                        continue;
                    }
                };
                match record.signal {
                    RecordSignal::Traces => {
                        let Ok(message) = ExportTraceServiceRequest::decode(record.payload) else {
                            out.damaged += 1;
                            continue;
                        };
                        let mut builder = SpanBatchBuilder::new(limits);
                        builder.append_request(&message)?;
                        out.rows
                            .push((record.tenant, entry.position, builder.finish()?));
                    }
                    RecordSignal::FlushIntent => match FlushIntent::from_payload(&record.payload) {
                        Ok(intent) => out.intents.push(intent),
                        Err(e) => {
                            out.damaged += 1;
                            warn!(position = ?entry.position, error = %e, "skipping undecodable flush intent");
                        }
                    },
                }
            }
            Event::Damaged { position, damage } => {
                out.damaged += 1;
                warn!(?position, ?damage, "wal damage during replay");
            }
        }
    }
    Ok(out)
}

/// Rebuilds buffers: rows covered by an intent go back to the flushing state
/// under that file id, everything else is live.
fn rebuild(replayed: Replayed) -> Buffers {
    let now = Instant::now();
    let mut buffers = Buffers::default();
    let mut groups: Vec<(FlushIntent, TenantBuffer)> = replayed
        .intents
        .into_iter()
        .map(|i| (i, TenantBuffer::default()))
        .collect();
    for (tenant, position, batch) in replayed.rows {
        match groups
            .iter_mut()
            .find(|(intent, _)| intent.covers(tenant, position))
        {
            Some((_, buffer)) => buffer.push(batch, position, now),
            None => buffers.get_mut(tenant).push(batch, position, now),
        }
    }
    for (intent, buffer) in groups {
        buffers.insert_flushing(intent.tenant, intent.file, buffer);
    }
    buffers
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
        let replayed = replay(config.limits, &mut wal)?;
        let (records, damaged, intents) =
            (replayed.records, replayed.damaged, replayed.intents.len());
        let buffers = rebuild(replayed);
        info!(
            records,
            damaged,
            intents,
            rows = buffers.total_rows(),
            "wal replayed"
        );
        let instance = wal.instance_id();
        let shared = Shared {
            config,
            auth,
            limiter,
            stats: IngestStats::default(),
            wal: Arc::new(StdMutex::new(wal)),
            instance,
            buffers: Mutex::new(buffers),
            gate: RwLock::new(()),
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

    /// Finishes flushes left by a previous run, then flushes every tenant.
    /// Used by tests and at shutdown.
    pub async fn flush_all(&self) {
        self.shared.recover_flushing().await;
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
        self.shared.recover_flushing().await;

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
