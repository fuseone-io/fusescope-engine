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
//! After step 2 a failure of any later step keeps the rows bound to the same
//! id and retries them unchanged: an id has exactly one possible content.

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
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::auth::Authenticator;
use crate::buffer::{Buffers, Reservation, Snapshot, TenantBuffer};
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
    /// Retries of a flush whose earlier attempt failed after the intent.
    pub flush_retries: AtomicU64,
    /// Hot-window snapshot latency, gate wait included.
    pub hot_scan: crate::histogram::Histogram,
    /// Checkpoint latency, gate wait and I/O included.
    pub checkpoint: crate::histogram::Histogram,
}

/// Per-stage flush timings and buffer memory, for sizing the flush path.
#[derive(Debug, Default)]
pub struct FlushStats {
    /// Flush attempts started, successful or not.
    pub attempts: AtomicU64,
    /// Flushes completed. Stage times below are added only on completion, so
    /// `stage / flushes` is the cost of a successful flush.
    pub flushes: AtomicU64,
    /// Rows written.
    pub rows: AtomicU64,
    /// Parquet bytes written.
    pub parquet_bytes: AtomicU64,
    /// Nanoseconds concatenating request batches.
    pub concat_nanos: AtomicU64,
    /// Nanoseconds sorting.
    pub sort_nanos: AtomicU64,
    /// Nanoseconds encoding Parquet.
    pub encode_nanos: AtomicU64,
    /// Nanoseconds uploading.
    pub upload_nanos: AtomicU64,
    /// Nanoseconds registering in the catalog.
    pub register_nanos: AtomicU64,
    /// Nanoseconds from taking the rows to the checkpoint.
    pub total_nanos: AtomicU64,
    /// Bytes in live buffers, sampled every flusher tick.
    pub live_bytes: AtomicU64,
    /// Bytes bound to flushes in progress, sampled every flusher tick.
    pub flushing_bytes: AtomicU64,
    /// Bytes kept for the hot window after registration, sampled every flusher tick.
    pub persisted_bytes: AtomicU64,
    /// Chunks built by the coalescer.
    pub coalesces: AtomicU64,
    /// Request batches merged into chunks.
    pub coalesced_batches: AtomicU64,
    /// Nanoseconds spent merging.
    pub coalesce_nanos: AtomicU64,
    /// Working memory held by chunks being built; the buffers' own counter.
    pub scratch_bytes: Arc<AtomicU64>,
    /// Chunks being built right now.
    pub coalesce_active: AtomicU64,
    /// Time to build one chunk.
    pub coalesce_time: crate::histogram::Histogram,
}

/// Permits of the flush semaphore for a configured `max_flushes`.
fn flush_permits(max_flushes: usize) -> usize {
    max_flushes.clamp(1, u32::MAX as usize / 4)
}

/// Stage durations of one flush attempt, in nanoseconds.
#[derive(Debug, Default)]
struct StageTiming {
    concat: u64,
    sort: u64,
    encode: u64,
    upload: u64,
    register: u64,
}

fn nanos(d: std::time::Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
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
    pub(crate) stats: Arc<IngestStats>,
    flush_stats: Arc<FlushStats>,
    wal: Arc<StdMutex<Wal>>,
    writer: crate::walwriter::WalWriter,
    instance: Uuid,
    buffers: Mutex<Buffers>,
    /// Requests hold it shared across WAL append and buffer push; a flush
    /// holds it exclusively while it takes a buffer and reads the WAL position.
    gate: RwLock<()>,
    flush_now: Notify,
    coalesce_now: Notify,
    conversions: tokio::sync::Semaphore,
    coalesces: Arc<tokio::sync::Semaphore>,
    flushes: Arc<tokio::sync::Semaphore>,
    inject_register_failures: std::sync::atomic::AtomicU32,
    push_delay_pending: std::sync::atomic::AtomicBool,
    retry_delay_pending: std::sync::atomic::AtomicBool,
    catalog: Catalog,
    store: Arc<dyn ObjectStore>,
    root: Path,
}

impl Shared {
    /// Admits `bytes` of converted rows against the caps, atomically with
    /// every other admission and push, and reserves them until the rows are
    /// in a buffer. A request that would cross a cap is refused: the caps are
    /// ceilings, not watermarks.
    async fn admit(&self, tenant: TenantId, bytes: usize) -> Result<Reservation, IngestError> {
        let mut buffers = self.buffers.lock().await;
        let reserved = buffers.reserved_of(tenant);
        if buffers.bytes_of(tenant) + reserved + bytes > self.config.max_buffer_bytes {
            self.stats.over_capacity.fetch_add(1, Ordering::Relaxed);
            self.flush_now.notify_one();
            return Err(IngestError::Capacity("tenant buffer full, flush pending"));
        }
        if buffers.tenant_total(tenant) + reserved + bytes > self.config.max_tenant_bytes {
            // Persisted rows count and are not evicted: queriers with a stale
            // file list still need them. Admission waits instead.
            self.stats.over_capacity.fetch_add(1, Ordering::Relaxed);
            return Err(IngestError::Capacity(
                "tenant memory full, hot-window retention pending",
            ));
        }
        if buffers.total_bytes() + buffers.reserved_total() + buffers.scratch_total() + bytes
            > self.config.max_memory_bytes
        {
            self.stats.over_capacity.fetch_add(1, Ordering::Relaxed);
            return Err(IngestError::Capacity(
                "ingester memory full, hot-window retention pending",
            ));
        }
        // Never lock the WAL here: the writer holds it through every fsync.
        let wal_bytes = self.writer.stats().wal_bytes.load(Ordering::Relaxed);
        if wal_bytes >= self.config.wal_max_bytes {
            self.stats.over_capacity.fetch_add(1, Ordering::Relaxed);
            self.flush_now.notify_one();
            return Err(IngestError::WalFull);
        }
        Ok(buffers.reserve(tenant, bytes))
    }

    /// Appends through the group-commit writer; resolves once synced. The
    /// position stays tracked as travelling until the handle is dropped.
    async fn append_wal(
        &self,
        tenant: TenantId,
        record: Vec<u8>,
    ) -> Result<crate::walwriter::Tracked, IngestError> {
        self.writer.append(tenant, record).await
    }

    /// Converts a request, admits it against the caps, logs it and adds it to
    /// the tenant buffer. The flush gate is held from the append to the push
    /// only, so a concurrent flush, checkpoint or hot scan sees either both
    /// the log record and the rows or neither, and waits for no conversion.
    /// Conversion comes first: a request the engine cannot convert or hold is
    /// refused before it reaches the WAL. Conversions run at most
    /// `max_conversions` at a time, which bounds the memory of batches built
    /// but not yet admitted.
    pub(crate) async fn ingest(
        &self,
        tenant: TenantId,
        message: &ExportTraceServiceRequest,
        record: Vec<u8>,
        now: Instant,
    ) -> Result<Converted, IngestError> {
        let slot = self
            .conversions
            .acquire()
            .await
            .map_err(|_| IngestError::Backpressure("conversions closed"))?;
        let mut builder = SpanBatchBuilder::new(self.config.limits);
        builder.append_request(message)?;
        let stats = builder.stats();
        let batch = builder.finish()?;
        let accepted = batch.num_rows();
        let rejected = usize::try_from(stats.spans_rejected).unwrap_or(usize::MAX);

        let reservation = self.admit(tenant, batch.get_array_memory_size()).await?;
        drop(slot);
        let _shared = self.gate.read().await;
        let tracked = self.append_wal(tenant, record).await?;
        if let Some(delay) = self.config.inject_push_delay {
            if self.push_delay_pending.swap(false, Ordering::Relaxed) {
                tokio::time::sleep(delay).await;
            }
        }

        let (due, coalesce) = {
            let mut buffers = self.buffers.lock().await;
            let was_due = self.flush_is_due(buffers.get_mut(tenant));
            buffers.get_mut(tenant).push(batch, tracked.position, now);
            // Both released under the lock: no admission runs between the push
            // and the release, and no coalesce takes a run the position
            // belongs in before the batch is there.
            drop(reservation);
            drop(tracked);
            let live = buffers.get_mut(tenant);
            // Edges, not levels: the flusher is woken once per crossing, not
            // once per request while the backlog lasts.
            (
                self.flush_is_due(live) && !was_due,
                live.request_batches() == self.config.coalesce_batches,
            )
        };
        if due {
            self.flush_now.notify_one();
        }
        if coalesce {
            self.coalesce_now.notify_one();
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

    /// The live buffer crossed a flush threshold.
    fn flush_is_due(&self, live: &TenantBuffer) -> bool {
        live.rows >= self.config.flush_rows || live.bytes >= self.config.flush_bytes
    }

    /// Steps 1 and 2 of the flush protocol: cut the live rows under the gate
    /// and log the intent. Returns the file id and the rows to materialise,
    /// or `None` when nothing was live.
    async fn cut_tenant(
        &self,
        tenant: TenantId,
    ) -> Result<Option<(FileId, Vec<RecordBatch>)>, IngestError> {
        let (file_id, intent, batches) = {
            let _exclusive = self.gate.write().await;
            let end = self
                .wal
                .lock()
                .map(|w| w.position())
                .map_err(|_| fs_wal::WalError::EmptyRecord)?;
            let mut buffers = self.buffers.lock().await;
            let Some(first) = buffers.live_first_wal(tenant) else {
                return Ok(None);
            };
            let file_id = FileId::derived(self.instance, tenant, first.segment, first.offset);
            let Some(batches) = buffers.begin_flush(tenant, file_id) else {
                return Ok(None);
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
        if let Err(e) = self.append_wal(tenant, intent.to_record().encode()).await {
            self.stats.flush_failures.fetch_add(1, Ordering::Relaxed);
            warn!(tenant = %tenant.0, error = %e, "flush intent could not be logged; keeping rows buffered");
            self.buffers.lock().await.abort_flush(tenant, file_id);
            return Err(e);
        }
        Ok(Some((file_id, batches)))
    }

    /// Steps 3 and 4: encode, upload, register, persist, checkpoint. On failure
    /// the rows stay bound to `file_id` and are retried as they are.
    async fn materialize(
        &self,
        tenant: TenantId,
        file_id: FileId,
        batches: Vec<RecordBatch>,
    ) -> Result<(), IngestError> {
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let fs = &self.flush_stats;
        fs.attempts.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let mut timing = StageTiming::default();
        let result = self
            .write_and_register(tenant, file_id, batches, &mut timing)
            .await;
        match result {
            Ok((file, created, rows, held)) => {
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
                    .finish_flush(tenant, file_id, rows, Instant::now());
                // Counted as persisted from here on.
                drop(held);
                if self.config.checkpoint_after_flush {
                    self.checkpoint_wal().await;
                }
                // Stage times are published only for completed flushes, so the
                // per-flush averages are not inflated by retries.
                fs.concat_nanos.fetch_add(timing.concat, Ordering::Relaxed);
                fs.sort_nanos.fetch_add(timing.sort, Ordering::Relaxed);
                fs.encode_nanos.fetch_add(timing.encode, Ordering::Relaxed);
                fs.upload_nanos.fetch_add(timing.upload, Ordering::Relaxed);
                fs.register_nanos
                    .fetch_add(timing.register, Ordering::Relaxed);
                fs.flushes.fetch_add(1, Ordering::Relaxed);
                fs.rows.fetch_add(
                    u64::try_from(file.row_count).unwrap_or(0),
                    Ordering::Relaxed,
                );
                fs.parquet_bytes.fetch_add(
                    u64::try_from(file.size_bytes).unwrap_or(0),
                    Ordering::Relaxed,
                );
                fs.total_nanos
                    .fetch_add(nanos(started.elapsed()), Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                // The intent is durable: the rows stay bound to this id and are
                // retried as they are. Returning them to the live buffer would let
                // later rows join a file whose object may already exist.
                self.stats.flush_failures.fetch_add(1, Ordering::Relaxed);
                warn!(tenant = %tenant.0, file = %file_id.0, error = %e, rows, "flush failed; will retry with the same id");
                self.buffers
                    .lock()
                    .await
                    .flush_failed(tenant, file_id, Instant::now());
                Err(e)
            }
        }
    }

    /// Encode, upload and register, filling `timing` as it goes. Returns the
    /// catalog row, whether this attempt created the object, the rows as
    /// written and the scratch lease that counts them until they are
    /// persisted.
    async fn write_and_register(
        &self,
        tenant: TenantId,
        file_id: FileId,
        batches: Vec<RecordBatch>,
        timing: &mut StageTiming,
    ) -> Result<
        (
            fs_catalog::NewFile,
            bool,
            RecordBatch,
            crate::buffer::Scratch,
        ),
        IngestError,
    > {
        // Preparing holds the merged rows and the sorted rows at once, each at
        // most the size of the inputs: reserved under the cap before the work
        // starts, refused with a backoff when the cap has no room, and sized
        // to what is really held once known.
        let inputs: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
        let mut held = self
            .buffers
            .lock()
            .await
            .try_hold_scratch(inputs.saturating_mul(2), self.config.max_memory_bytes)
            .ok_or(IngestError::Backpressure("flush scratch"))?;
        let root = self.root.clone();
        let prepared =
            tokio::task::spawn_blocking(move || flush::prepare(tenant, &root, file_id, &batches))
                .await??;
        held.resize(prepared.rows.get_array_memory_size() + prepared.bytes.len());
        timing.concat = nanos(prepared.concat);
        timing.sort = nanos(prepared.sort);
        timing.encode = nanos(prepared.encode);
        let upload_started = Instant::now();
        let created = flush::upload(self.store.as_ref(), &prepared).await?;
        timing.upload = nanos(upload_started.elapsed());
        held.resize(prepared.rows.get_array_memory_size());
        let register_started = Instant::now();
        self.register(&prepared.file).await?;
        timing.register = nanos(register_started.elapsed());
        Ok((prepared.file, created, prepared.rows, held))
    }

    /// Catalog registration, with the test-only failure injection in front.
    async fn register(&self, file: &fs_catalog::NewFile) -> Result<(), IngestError> {
        let remaining = self.inject_register_failures.load(Ordering::Relaxed);
        if remaining > 0 {
            self.inject_register_failures
                .store(remaining - 1, Ordering::Relaxed);
            return Err(IngestError::Injected("catalog registration"));
        }
        self.catalog.register_file(file).await?;
        Ok(())
    }

    /// Retries flushes whose earlier attempt failed after the intent, or
    /// resumes those an earlier run left behind. `force` ignores the backoff
    /// and waits for a flush permit instead of stopping when none is free.
    /// The pass works from the entries due when it started and tries each at
    /// most once, so a flush that fails during a forced pass waits for its
    /// backoff instead of holding the pass. A permit is taken before an
    /// entry is marked, so an entry in progress always has its attempt
    /// running, and retries share the permits of new cuts: together they
    /// never exceed `max_flushes`.
    async fn retry_flushing(self: &Arc<Self>, force: bool) {
        let due = self.buffers.lock().await.due_flushes(Instant::now(), force);
        for (tenant, file) in due {
            let Some(permit) = self.flush_permit(force).await else {
                return;
            };
            let taken = self.buffers.lock().await.take_flushing(tenant, file);
            let Some(batches) = taken else {
                continue;
            };
            self.stats.flush_retries.fetch_add(1, Ordering::Relaxed);
            debug!(tenant = %tenant.0, file = %file.0, rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>(), "retrying flush");
            let delay = self
                .config
                .inject_retry_delay
                .filter(|_| self.retry_delay_pending.swap(false, Ordering::Relaxed));
            self.spawn_materialize(tenant, file, batches, permit, delay);
        }
    }

    /// A flush permit: awaited when `wait`, otherwise only if one is free.
    async fn flush_permit(&self, wait: bool) -> Option<tokio::sync::OwnedSemaphorePermit> {
        if wait {
            Arc::clone(&self.flushes).acquire_owned().await.ok()
        } else {
            Arc::clone(&self.flushes).try_acquire_owned().ok()
        }
    }

    /// Materialises one cut on its own task, holding `permit` until it ends,
    /// after `delay` when a test asks for one.
    fn spawn_materialize(
        self: &Arc<Self>,
        tenant: TenantId,
        file_id: FileId,
        batches: Vec<RecordBatch>,
        permit: tokio::sync::OwnedSemaphorePermit,
        delay: Option<Duration>,
    ) {
        let shared = Arc::clone(self);
        tokio::spawn(async move {
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            if let Err(e) = shared.materialize(tenant, file_id, batches).await {
                error!(tenant = %tenant.0, error = %e, "flush error");
            }
            drop(permit);
            shared.flush_now.notify_one();
        });
    }

    /// Records that everything before the earliest still-buffered record is
    /// materialised: replay after a restart starts there, and segments before
    /// it are deleted.
    /// Advances the checkpoint to the earliest row still in memory, or to the
    /// end of the log when nothing is. The cut is chosen under the exclusive
    /// gate, so no request is between its append and its push: every record
    /// accepted after the cut is above it and replay still finds it.
    async fn checkpoint_wal(&self) {
        let started = Instant::now();
        let cut = {
            let _exclusive = self.gate.write().await;
            let earliest = self.buffers.lock().await.earliest_wal();
            let Some(position) = earliest.or_else(|| self.wal.lock().ok().map(|w| w.position()))
            else {
                warn!("wal lock poisoned; skipping checkpoint");
                return;
            };
            position
        };
        let wal = self.wal.clone();
        let writer_stats = self.writer.stats();
        let result = tokio::task::spawn_blocking(move || {
            let mut wal = wal.lock().map_err(|_| fs_wal::WalError::EmptyRecord)?;
            let outcome = wal.checkpoint(cut).map(|()| cut);
            // Segments were removed: publish the new size while still holding
            // the lock, or a full WAL stays "full" until the next append.
            writer_stats.wal_bytes.store(wal.bytes(), Ordering::Relaxed);
            outcome
        })
        .await;
        match result {
            Ok(Ok(cut)) => debug!(?cut, "wal checkpoint"),
            Ok(Err(e)) => warn!(error = %e, "wal checkpoint failed; replay may duplicate rows"),
            Err(e) => warn!(error = %e, "wal checkpoint task failed"),
        }
        self.stats.checkpoint.observe(started.elapsed());
    }

    /// Merges complete request batches into chunks, as many groups at a time
    /// as `max_coalesces` allows, each on the blocking pool. A group is a
    /// run of adjacent batches of one tenant below the writer's stable bound
    /// for that tenant: nothing with a lower position is still on its way to
    /// a buffer, so the chunk keeps the rows in log order, the order replay
    /// produces. Its inputs are reserved inside the memory cap before the
    /// merge starts and stay served meanwhile. Returns once every permit is
    /// in use or no group is left; a finishing task brings the flusher back
    /// for more.
    async fn coalesce_due(self: &Arc<Self>) {
        let min = self.config.coalesce_batches;
        let max_bytes = self.config.coalesce_bytes;
        loop {
            let Ok(permit) = Arc::clone(&self.coalesces).try_acquire_owned() else {
                return;
            };
            let taken = {
                let mut buffers = self.buffers.lock().await;
                // The merge's inputs are reserved inside the cap, never on top
                // of it: with no headroom nothing is merged until a flush.
                let headroom = self.config.max_memory_bytes.saturating_sub(
                    buffers.total_bytes() + buffers.reserved_total() + buffers.scratch_total(),
                );
                let candidates = buffers.coalesce_candidates(min);
                candidates.into_iter().find_map(|tenant| {
                    let bound = self.writer.stable_bound(tenant);
                    buffers
                        .take_for_coalesce(tenant, bound, min, max_bytes, headroom)
                        .map(|taken| (tenant, taken))
                })
            };
            let Some((tenant, taken)) = taken else {
                return;
            };
            let shared = Arc::clone(self);
            tokio::spawn(async move {
                shared.coalesce_group(tenant, taken).await;
                drop(permit);
                shared.coalesce_now.notify_one();
            });
        }
    }

    /// Builds the chunk of one group and puts it in place. On any failure
    /// the batches stay as they are.
    async fn coalesce_group(&self, tenant: TenantId, taken: crate::buffer::Taken) {
        let crate::buffer::Taken {
            id,
            batches,
            scratch,
        } = taken;
        let count = batches.len();
        let fs = &self.flush_stats;
        fs.coalesce_active.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let merged = tokio::task::spawn_blocking(move || {
            arrow::compute::concat_batches(&fs_schema::spans::span_schema_v1(), &batches)
        })
        .await;
        match merged {
            Ok(Ok(chunk)) => {
                if self.buffers.lock().await.finish_coalesce(tenant, id, chunk) {
                    fs.coalesces.fetch_add(1, Ordering::Relaxed);
                    fs.coalesced_batches
                        .fetch_add(count as u64, Ordering::Relaxed);
                    fs.coalesce_nanos
                        .fetch_add(nanos(started.elapsed()), Ordering::Relaxed);
                    fs.coalesce_time.observe(started.elapsed());
                }
            }
            Ok(Err(e)) => {
                warn!(tenant = %tenant.0, error = %e, "coalesce failed; batches stay as they are");
                self.buffers.lock().await.cancel_coalesce(tenant, id);
            }
            Err(e) => {
                warn!(tenant = %tenant.0, error = %e, "coalesce task failed; batches stay as they are");
                self.buffers.lock().await.cancel_coalesce(tenant, id);
            }
        }
        fs.coalesce_active.fetch_sub(1, Ordering::Relaxed);
        drop(scratch);
    }

    /// Rows of `tenant` in every state, plus the id the live rows will get.
    /// Taken under the exclusive gate: no request is between its append and
    /// its push, so the smallest live position, and with it the announced id,
    /// is the one the next flush derives from.
    pub(crate) async fn snapshot(&self, tenant: TenantId) -> (Snapshot, Option<FileId>) {
        let started = Instant::now();
        let snapshot = {
            let _exclusive = self.gate.write().await;
            self.buffers.lock().await.snapshot(tenant)
        };
        self.stats.hot_scan.observe(started.elapsed());
        let live_file = snapshot
            .live_first_wal
            .map(|p| FileId::derived(self.instance, tenant, p.segment, p.offset));
        (snapshot, live_file)
    }

    /// Cuts every due tenant (every tenant with live rows when `everything`)
    /// and materialises each cut on its own task, at most `max_flushes` at a
    /// time, so the flusher keeps coalescing while files are written. A
    /// tenant left without a permit stays due for the next round. With
    /// `everything` the call returns only once every flush, including those
    /// started earlier, has finished.
    async fn flush_due(self: &Arc<Self>, everything: bool) {
        let now = Instant::now();
        let tenants = {
            let mut buffers = self.buffers.lock().await;
            buffers.sweep_persisted(self.config.persisted_retain, now);
            let (live, flushing, persisted) = buffers.memory();
            self.flush_stats
                .live_bytes
                .store(live as u64, Ordering::Relaxed);
            self.flush_stats
                .flushing_bytes
                .store(flushing as u64, Ordering::Relaxed);
            self.flush_stats
                .persisted_bytes
                .store(persisted as u64, Ordering::Relaxed);
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
        self.retry_flushing(everything).await;
        for tenant in tenants {
            let Some(permit) = self.flush_permit(everything).await else {
                return;
            };
            match self.cut_tenant(tenant).await {
                Ok(Some((file_id, batches))) => {
                    self.spawn_materialize(tenant, file_id, batches, permit, None);
                }
                Ok(None) => {}
                Err(e) => error!(tenant = %tenant.0, error = %e, "flush error"),
            }
        }
        if everything {
            // Every permit at once: nothing is still being written.
            let all = flush_permits(self.config.max_flushes);
            let _drained = Arc::clone(&self.flushes)
                .acquire_many_owned(u32::try_from(all).unwrap_or(u32::MAX))
                .await;
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
fn rebuild(replayed: Replayed, scratch: Arc<AtomicU64>) -> Buffers {
    let now = Instant::now();
    let mut buffers = Buffers::with_scratch_counter(scratch);
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
        let flush_stats = Arc::new(FlushStats::default());
        let buffers = rebuild(replayed, Arc::clone(&flush_stats.scratch_bytes));
        info!(
            records,
            damaged,
            intents,
            rows = buffers.total_rows(),
            "wal replayed"
        );
        let instance = wal.instance_id();
        let config_injected = config.inject_register_failures;
        let config_conversions = config.max_conversions;
        let config_coalesces = config.max_coalesces;
        let config_flushes = config.max_flushes;
        let push_delay_pending = config.inject_push_delay.is_some();
        let retry_delay_pending = config.inject_retry_delay.is_some();
        let wal = Arc::new(StdMutex::new(wal));
        let writer = crate::walwriter::WalWriter::spawn(Arc::clone(&wal), config.group_commit);
        let shared = Shared {
            config,
            auth,
            limiter,
            stats: Arc::new(IngestStats::default()),
            flush_stats,
            wal,
            writer,
            instance,
            buffers: Mutex::new(buffers),
            gate: RwLock::new(()),
            flush_now: Notify::new(),
            coalesce_now: Notify::new(),
            conversions: tokio::sync::Semaphore::new(
                config_conversions.clamp(1, tokio::sync::Semaphore::MAX_PERMITS),
            ),
            coalesces: Arc::new(tokio::sync::Semaphore::new(
                config_coalesces.clamp(1, tokio::sync::Semaphore::MAX_PERMITS),
            )),
            flushes: Arc::new(tokio::sync::Semaphore::new(flush_permits(config_flushes))),
            inject_register_failures: std::sync::atomic::AtomicU32::new(config_injected),
            push_delay_pending: std::sync::atomic::AtomicBool::new(push_delay_pending),
            retry_delay_pending: std::sync::atomic::AtomicBool::new(retry_delay_pending),
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

    /// Shared handle to the counters, for a metrics exporter.
    pub fn shared_stats(&self) -> Arc<IngestStats> {
        Arc::clone(&self.shared.stats)
    }

    /// Flush timings and buffer memory.
    pub fn flush_stats(&self) -> Arc<FlushStats> {
        Arc::clone(&self.shared.flush_stats)
    }

    /// Group-commit counters of the WAL writer.
    pub fn writer_stats(&self) -> Arc<crate::walwriter::WriterStats> {
        self.shared.writer.stats()
    }

    /// Bytes the WAL held on disk after the last group commit.
    pub fn wal_bytes(&self) -> u64 {
        self.shared.writer.stats().wal_bytes.load(Ordering::Relaxed)
    }

    /// Retries or resumes every pending flush, then flushes every tenant.
    /// Used by tests and at shutdown.
    pub async fn flush_all(&self) {
        self.shared.flush_due(true).await;
    }

    /// Advances the WAL checkpoint to the earliest row still in memory. Runs
    /// after every flush; exposed for tests and operators.
    pub async fn checkpoint(&self) {
        self.shared.checkpoint_wal().await;
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
        let recovered = self.shared.buffers.lock().await.idle_flushing();
        if recovered > 0 {
            self.shared
                .stats
                .flushes_recovered
                .fetch_add(recovered as u64, Ordering::Relaxed);
            info!(flushes = recovered, "resuming flushes from intents");
            self.shared.retry_flushing(true).await;
        }

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
                    _ = tick.tick() => {
                        shared.coalesce_due().await;
                        shared.flush_due(false).await;
                    }
                    () = shared.coalesce_now.notified() => shared.coalesce_due().await,
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
