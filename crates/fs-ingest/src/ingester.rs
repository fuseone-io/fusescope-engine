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
//! 4. Move the rows to the persisted state, confirm the registration, so
//!    the compactor's delay counts from after the retention began, and
//!    checkpoint the WAL: after the flush and again once the confirmation
//!    lands, which is what lets the checkpoint pass the intent.
//!
//! Replay groups records under the intents found in the log, so a crash
//! anywhere after step 2 reproduces the same file and the same catalog row;
//! a crash before step 2 simply flushes the rows again under a new intent.
//! The checkpoint stays behind the intent of a file not yet confirmed, so a
//! crash before the confirmation replays the flush and confirms it. The
//! confirmation runs off the flusher: a flush returns once its rows are
//! retained, and the confirmer attempts each confirmation on its own permit,
//! with a timeout and backoff, giving an attempt back when it is cancelled.
//! After step 2 a failure of any later step keeps the rows bound to the same
//! id and retries them unchanged: an id has exactly one possible content.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use fs_catalog::{Catalog, CatalogError, FileId, TenantId};
use fs_schema::logs::LogBatchBuilder;
use fs_schema::spans::SpanBatchBuilder;
use fs_wal::{Event, Position, Wal};
use object_store::ObjectStore;
use object_store::path::Path;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsServiceServer;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer;
use prost::Message;
use tokio::sync::{Mutex, Notify, RwLock, watch};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::auth::Authenticator;
use crate::buffer::{Bound, Buffers, ConfirmAttempt, Reservation, Snapshot, Stream, TenantBuffer};
use crate::config::IngestConfig;
use crate::ratelimit::RateLimiter;
use crate::record::{FileLayout, FlushIntent, RecordSignal, WalRecord};
use crate::server::{LogsReceiver, TraceReceiver};
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
    /// Log records buffered.
    pub logs: AtomicU64,
    /// Log records rejected for carrying no time.
    pub logs_rejected: AtomicU64,
    /// Log bodies truncated to the body limit.
    pub bodies_truncated: AtomicU64,
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
    /// Hot-window batches a scan for given traces skipped by their trace set.
    pub hot_batches_skipped: AtomicU64,
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
    /// Nanoseconds sorting each input batch.
    pub concat_nanos: AtomicU64,
    /// Nanoseconds merging the sorted inputs into row groups.
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
    /// Injected registration failures being held by the test delay right
    /// now, so a test can act inside that window.
    pub registrations_held: AtomicU64,
    /// Injected intent failures being held by the test delay right now:
    /// the rows are cut and their intent is not durable.
    pub intents_held: AtomicU64,
    /// Most bytes the Parquet writer held for one row group, ever.
    pub writer_peak_bytes: AtomicU64,
    /// Sum over flushes of the writer's peak bytes; `/ flushes` is the mean.
    pub writer_bytes: AtomicU64,
    /// Row groups written.
    pub row_groups: AtomicU64,
    /// Bytes of `trace_id` sidecars created.
    pub trace_index_bytes: AtomicU64,
    /// Flushes given a fresh id because the orphan sweep marked their path
    /// or the object at their path held other bytes.
    pub superseded: AtomicU64,
    /// Retried flushes that met an object with other bytes at their path.
    pub replay_mismatch: AtomicU64,
    /// Registration confirmations that failed and wait for a retry.
    pub confirm_failures: AtomicU64,
    /// Registered files whose confirmation has not landed.
    pub unconfirmed: AtomicU64,
}

/// Permits of the flush semaphore for a configured `max_flushes`.
/// Takes one injected failure when any is left: exactly as many as
/// configured fire, whatever the number of concurrent attempts.
fn take_injected(counter: &std::sync::atomic::AtomicU32) -> bool {
    let mut remaining = counter.load(Ordering::SeqCst);
    loop {
        if remaining == 0 {
            return false;
        }
        match counter.compare_exchange(remaining, remaining - 1, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => return true,
            Err(actual) => remaining = actual,
        }
    }
}

fn flush_permits(max_flushes: usize) -> usize {
    max_flushes.clamp(1, u32::MAX as usize / 4)
}

/// Permits of the confirmation semaphore for a configured `max_confirms`,
/// within what the runtime can track.
fn confirm_permits(max_confirms: usize) -> usize {
    max_confirms.clamp(1, tokio::sync::Semaphore::MAX_PERMITS)
}

/// One flush of a cut: its id, its rows and the scratch held for them.
type Cut = (FileId, Vec<RecordBatch>, crate::buffer::Scratch);

/// One flush attempt handed to its task: the rows as cut, the scratch lease
/// held for them and the layout the intent recorded.
struct Attempt {
    stream: Stream,
    file: FileId,
    /// The id the rows were first assigned; see `Flushing::origin`.
    origin: FileId,
    batches: Vec<RecordBatch>,
    held: crate::buffer::Scratch,
    /// The intent's layout; `None` for an intent logged before layouts were
    /// recorded, which uses the current layout and trusts the object it finds.
    layout: Option<FileLayout>,
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
    confirm_now: Notify,
    conversions: tokio::sync::Semaphore,
    coalesces: Arc<tokio::sync::Semaphore>,
    flushes: Arc<tokio::sync::Semaphore>,
    confirms: Arc<tokio::sync::Semaphore>,
    inject_register_failures: std::sync::atomic::AtomicU32,
    inject_intent_failures: std::sync::atomic::AtomicU32,
    inject_confirm_failures: std::sync::atomic::AtomicU32,
    inject_index_failures: std::sync::atomic::AtomicU32,
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
    async fn admit(
        &self,
        stream: Stream,
        bytes: usize,
        rows: usize,
        hours: &[DateTime<Utc>],
    ) -> Result<Reservation, IngestError> {
        let mut buffers = self.buffers.lock().await;
        if buffers.live_hours_with(stream, hours) > self.config.max_live_hours {
            self.stats.over_capacity.fetch_add(1, Ordering::Relaxed);
            buffers.press(stream);
            self.flush_now.notify_one();
            return Err(IngestError::Capacity(
                "tenant rows span too many hours, flush pending",
            ));
        }
        // Every cap sees the batch with the bound of the trace set it will
        // get, which its bytes carry from the push until a chunk replaces it.
        let cost = Buffers::batch_cost(bytes, rows);
        if buffers.bytes_of(stream) + buffers.reserved_of(stream) + cost
            > self.config.max_buffer_bytes
        {
            self.stats.over_capacity.fetch_add(1, Ordering::Relaxed);
            self.flush_now.notify_one();
            return Err(IngestError::Capacity("tenant buffer full, flush pending"));
        }
        // The tenant's memory is one budget across its signals.
        let tenant_total: usize = Stream::of(stream.tenant)
            .into_iter()
            .map(|s| buffers.tenant_total(s) + buffers.reserved_of(s))
            .sum();
        if tenant_total + cost > self.config.max_tenant_bytes {
            // Persisted rows count and are not evicted: queriers with a stale
            // file list still need them. Admission waits instead.
            self.stats.over_capacity.fetch_add(1, Ordering::Relaxed);
            return Err(IngestError::Capacity(
                "tenant memory full, hot-window retention pending",
            ));
        }
        // Admitted rows must always be writable: besides the batch itself,
        // room for the scratch its flush will reserve stays free.
        if Buffers::admission_cost(bytes, rows) > buffers.headroom(self.config.max_memory_bytes) {
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
        Ok(buffers.reserve(stream, cost, hours))
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
        self.push_converted(Stream::traces(tenant), batch, slot, record, now)
            .await?;
        self.stats
            .spans
            .fetch_add(accepted as u64, Ordering::Relaxed);
        self.stats
            .spans_rejected
            .fetch_add(stats.spans_rejected, Ordering::Relaxed);
        let rejected = usize::try_from(stats.spans_rejected).unwrap_or(usize::MAX);
        Ok(Converted { accepted, rejected })
    }

    /// [`Self::ingest`] for a logs request, whose rows take their ids from
    /// `seed`, the seed `record` carries.
    pub(crate) async fn ingest_logs(
        &self,
        tenant: TenantId,
        message: &ExportLogsServiceRequest,
        seed: &[u8],
        record: Vec<u8>,
        now: Instant,
    ) -> Result<Converted, IngestError> {
        let slot = self
            .conversions
            .acquire()
            .await
            .map_err(|_| IngestError::Backpressure("conversions closed"))?;
        let mut builder = LogBatchBuilder::new(self.config.limits);
        builder.append_request(message, seed)?;
        let stats = builder.stats();
        let batch = builder.finish()?;
        let accepted = batch.num_rows();
        self.push_converted(Stream::logs(tenant), batch, slot, record, now)
            .await?;
        self.stats
            .logs
            .fetch_add(accepted as u64, Ordering::Relaxed);
        self.stats
            .logs_rejected
            .fetch_add(stats.logs_rejected, Ordering::Relaxed);
        self.stats
            .bodies_truncated
            .fetch_add(stats.bodies_truncated, Ordering::Relaxed);
        let rejected = usize::try_from(stats.logs_rejected).unwrap_or(usize::MAX);
        Ok(Converted { accepted, rejected })
    }

    /// Admits a converted batch against the caps, logs its record and adds
    /// it to the stream's buffer. The conversion `slot` is given back once
    /// the batch is admitted. The flush gate is held from the append to the
    /// push only, so a concurrent flush, checkpoint or hot scan sees either
    /// both the log record and the rows or neither, and waits for no
    /// conversion. Conversion comes first: a request the engine cannot
    /// convert or hold is refused before it reaches the WAL.
    async fn push_converted(
        &self,
        stream: Stream,
        batch: RecordBatch,
        slot: tokio::sync::SemaphorePermit<'_>,
        record: Vec<u8>,
        now: Instant,
    ) -> Result<(), IngestError> {
        let hours: Vec<_> = crate::hours::hours_in(std::slice::from_ref(&batch))?
            .into_iter()
            .collect();
        if hours.len() > self.config.max_hours_per_batch {
            return Err(IngestError::TooManyHours {
                hours: hours.len(),
                limit: self.config.max_hours_per_batch,
            });
        }

        let reservation = self
            .admit(
                stream,
                batch.get_array_memory_size(),
                batch.num_rows(),
                &hours,
            )
            .await?;
        drop(slot);
        let _shared = self.gate.read().await;
        let tracked = self.append_wal(stream.tenant, record).await?;
        if let Some(delay) = self.config.inject_push_delay {
            if self.push_delay_pending.swap(false, Ordering::Relaxed) {
                tokio::time::sleep(delay).await;
            }
        }

        let (due, coalesce) = {
            let mut buffers = self.buffers.lock().await;
            let was_due = self.flush_is_due(buffers.get_mut(stream));
            buffers
                .get_mut(stream)
                .push_with_hours(batch, hours, tracked.position, now);
            // Both released under the lock: no admission runs between the push
            // and the release, and no coalesce takes a run the position
            // belongs in before the batch is there.
            drop(reservation);
            drop(tracked);
            let live = buffers.get_mut(stream);
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
        Ok(())
    }

    /// The live buffer crossed a flush threshold.
    fn flush_is_due(&self, live: &TenantBuffer) -> bool {
        live.rows >= self.config.flush_rows || live.bytes >= self.config.flush_bytes
    }

    /// Steps 1 and 2 of the flush protocol: cut the live rows under the gate
    /// and log the intents: one flush per hour the rows fall in, at most
    /// `max_hours_per_cut` of them, oldest first, each under the id derived
    /// from the rows' first position and its hour and under an intent
    /// naming that hour, so no file ever holds two hours, the hot window
    /// can announce every id beforehand, and rows left live, by the bound
    /// or by an intent that could not be logged, are cut later under an
    /// intent of their own hour that never claims the rows of another.
    /// Returns the flushes whose intent is durable, or `None` when nothing
    /// was live.
    async fn cut_stream(&self, stream: Stream) -> Result<Option<Vec<Cut>>, IngestError> {
        let tenant = stream.tenant;
        let (intents, parts) = {
            let _exclusive = self.gate.write().await;
            let end = self
                .wal
                .lock()
                .map(|w| w.position())
                .map_err(|_| fs_wal::WalError::EmptyRecord)?;
            let mut buffers = self.buffers.lock().await;
            let Some(first) = buffers.live_first_wal(stream) else {
                return Ok(None);
            };
            let layout = FileLayout::current(self.config.row_group_rows);
            let all_hours = buffers.live_hours(stream);
            let hours: Vec<_> = all_hours
                .iter()
                .copied()
                .take(
                    self.config
                        .max_hours_per_cut
                        .clamp(1, crate::config::MAX_HOURS_PER_CUT),
                )
                .collect();
            let intent = |file, hour: Option<DateTime<Utc>>| FlushIntent {
                tenant,
                file,
                first,
                end,
                layout: Some(layout),
                hour: hour.map(|h| h.timestamp()),
                signal: stream.signal,
            };
            let file_at = |hour: Option<DateTime<Utc>>| match hour {
                Some(hour) => {
                    FileId::derived_at(self.instance, tenant, first.segment, first.offset, hour)
                }
                None => FileId::derived(self.instance, tenant, first.segment, first.offset),
            };
            if all_hours.len() <= 1 {
                let hour = hours.first().copied();
                let file = file_at(hour);
                let Some((batches, held)) =
                    buffers.begin_flush(stream, file, layout, (first, end), hour)
                else {
                    return Ok(None);
                };
                (vec![intent(file, hour)], vec![(file, batches, held)])
            } else {
                let parts: Vec<_> = hours.iter().map(|h| (file_at(Some(*h)), *h)).collect();
                let Some(cut) = buffers.begin_flush_split(stream, layout, (first, end), &parts)?
                else {
                    return Ok(None);
                };
                let intents = cut.iter().map(|p| intent(p.file, Some(p.hour))).collect();
                (
                    intents,
                    cut.into_iter()
                        .map(|p| (p.file, p.batches, p.held))
                        .collect(),
                )
            }
        };
        let mut logged = Vec::with_capacity(parts.len());
        let mut first_error = None;
        for (intent, part) in intents.into_iter().zip(parts) {
            let logged_intent = if take_injected(&self.inject_intent_failures) {
                if let Some(delay) = self.config.inject_intent_delay {
                    self.flush_stats.intents_held.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(delay).await;
                    self.flush_stats.intents_held.fetch_sub(1, Ordering::SeqCst);
                }
                Err(IngestError::Injected("flush intent"))
            } else {
                self.append_wal(tenant, intent.to_record().encode()).await
            };
            match logged_intent {
                Ok(tracked) => {
                    drop(tracked);
                    logged.push(part);
                }
                Err(e) => {
                    self.stats.flush_failures.fetch_add(1, Ordering::Relaxed);
                    warn!(tenant = %tenant.0, file = %part.0.0, error = %e, "flush intent could not be logged; keeping rows buffered");
                    self.buffers.lock().await.abort_flush(stream, part.0);
                    first_error.get_or_insert(e);
                }
            }
        }
        // The pressure of a refusal outlives a cut whose intent failed: the
        // rows are live again and the next pass cuts them.
        if first_error.is_none() {
            self.buffers.lock().await.unpress(stream);
        }
        match (logged.is_empty(), first_error) {
            (true, Some(e)) => Err(e),
            (true, None) => Ok(None),
            (false, _) => Ok(Some(logged)),
        }
    }

    /// Steps 3 and 4: encode, upload, register, persist, checkpoint. On failure
    /// the rows stay bound to `file_id` and are retried as they are.
    async fn materialize(
        &self,
        stream: Stream,
        file_id: FileId,
        origin: FileId,
        batches: Vec<RecordBatch>,
        held: crate::buffer::Scratch,
        layout: Option<FileLayout>,
    ) -> Result<(), IngestError> {
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let fs = &self.flush_stats;
        fs.attempts.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let mut timing = StageTiming::default();
        let result = self
            .write_and_register(stream, file_id, origin, batches, layout, &mut timing)
            .await;
        // The lease and the margin trade places under the buffer lock, in
        // both outcomes, so no admission ever sees the room without the
        // rows that will take it back.
        match result {
            Ok((file, created, rows, writer_peak)) => {
                self.stats.files_flushed.fetch_add(1, Ordering::Relaxed);
                info!(
                    tenant = %stream.tenant.0,
                    signal = ?stream.signal,
                    path = %file.path,
                    rows = file.row_count,
                    bytes = file.size_bytes,
                    services = file.services.len(),
                    created,
                    "flushed l0"
                );
                let row_groups = rows.len();
                {
                    let mut buffers = self.buffers.lock().await;
                    buffers.finish_flush(stream, file_id, rows, Instant::now());
                    self.publish_unconfirmed(&buffers);
                    drop(held);
                }
                self.confirm_now.notify_one();
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
                let peak = u64::try_from(writer_peak).unwrap_or(u64::MAX);
                fs.writer_peak_bytes.fetch_max(peak, Ordering::Relaxed);
                fs.writer_bytes.fetch_add(peak, Ordering::Relaxed);
                fs.row_groups
                    .fetch_add(row_groups as u64, Ordering::Relaxed);
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
                warn!(tenant = %stream.tenant.0, signal = ?stream.signal, file = %file_id.0, error = %e, rows, "flush failed; will retry");
                // The entry is still in progress here, so no retry can take
                // it under the old id while it changes id.
                let file_id = match &e {
                    IngestError::Catalog(CatalogError::ObjectDoomed { .. }) => {
                        self.supersede(stream, file_id).await
                    }
                    IngestError::ObjectMismatch { .. } => {
                        self.flush_stats
                            .replay_mismatch
                            .fetch_add(1, Ordering::Relaxed);
                        self.supersede(stream, file_id).await
                    }
                    _ => file_id,
                };
                {
                    let mut buffers = self.buffers.lock().await;
                    buffers.flush_failed(stream, file_id, Instant::now());
                    drop(held);
                }
                Err(e)
            }
        }
    }

    /// Gives the rows of a flush the orphan sweep is deleting a fresh id:
    /// a path once marked is never written again, since a delete still in
    /// flight when the sweep lost its lease could remove the new object.
    /// The new intent for the same records goes to the log before the
    /// rows take the id, so a restart retries under the new id too; a log
    /// failure keeps the old id, whose next attempt is refused again,
    /// since the catalog keeps a deleted path refused as a tombstone.
    /// Called while the flush is still in progress, so no retry takes the
    /// old id meanwhile. Returns the id the rows have afterwards.
    async fn supersede(&self, stream: Stream, old: FileId) -> FileId {
        let tenant = stream.tenant;
        let range = {
            let buffers = self.buffers.lock().await;
            buffers.flushing_range(stream, old)
        };
        let Some(bound) = range else {
            return old;
        };
        let new = FileId::new();
        let intent = FlushIntent {
            tenant,
            file: new,
            first: bound.range.0,
            end: bound.range.1,
            layout: bound.layout,
            hour: bound.hour.map(|h| h.timestamp()),
            signal: stream.signal,
        };
        if let Err(e) = self.append_wal(tenant, intent.to_record().encode()).await {
            warn!(tenant = %tenant.0, file = %old.0, error = %e, "superseding intent could not be logged; keeping the id");
            return old;
        }
        if self.buffers.lock().await.rename_flushing(stream, old, new) {
            self.flush_stats.superseded.fetch_add(1, Ordering::Relaxed);
            info!(tenant = %tenant.0, old = %old.0, new = %new.0, "flush takes a fresh id: its object was marked as an orphan");
            new
        } else {
            old
        }
    }

    /// Encode, upload and register, filling `timing` as it goes. The caller
    /// keeps the attempt's scratch lease whole throughout. Returns the catalog
    /// row, whether this attempt created the object and the rows as written.
    async fn write_and_register(
        &self,
        stream: Stream,
        file_id: FileId,
        origin: FileId,
        batches: Vec<RecordBatch>,
        layout: Option<FileLayout>,
        timing: &mut StageTiming,
    ) -> Result<
        (
            fs_catalog::NewFile,
            bool,
            Vec<crate::buffer::HotBatch>,
            usize,
        ),
        IngestError,
    > {
        let legacy = layout.is_none();
        let layout = layout.unwrap_or_else(|| FileLayout::current(self.config.row_group_rows));
        flush::check_layout(&layout)?;
        let root = self.root.clone();
        let prepared = tokio::task::spawn_blocking(move || {
            flush::prepare_stream(stream, &root, file_id, &batches, layout)
        })
        .await??;
        timing.concat = nanos(prepared.concat);
        timing.sort = nanos(prepared.sort);
        timing.encode = nanos(prepared.encode);
        // A path the orphan sweep marked or deleted is never written again,
        // not even as an orphan: the registration would refuse it anyway.
        if let Some(path) = self.catalog.object_refused(&prepared.file.path).await? {
            return Err(IngestError::Catalog(CatalogError::ObjectDoomed { path }));
        }
        let upload_started = Instant::now();
        let outcome = flush::upload(self.store.as_ref(), &prepared).await?;
        self.upload_index(&prepared).await?;
        timing.upload = nanos(upload_started.elapsed());
        let flush::Prepared {
            mut file,
            bytes,
            rows,
            writer_peak,
            ..
        } = prepared;
        file.origin = origin;
        // The object is in the bucket: its bytes go now; the lease waits for
        // the outcome.
        drop(bytes);
        let created = match outcome {
            flush::Upload::Created => true,
            flush::Upload::Existing { size } => {
                let ours = u64::try_from(file.size_bytes).unwrap_or(0);
                let registered = flush::reconcile_existing(size, ours, legacy)?;
                let prepared = file.content_sha256.unwrap_or([0; 32]);
                let digest =
                    flush::verify_existing(self.store.as_ref(), &file.path, prepared, legacy)
                        .await?;
                if registered != ours || digest != prepared {
                    warn!(tenant = %stream.tenant.0, file = %file_id.0, existing = size, prepared = ours, "legacy intent: registering the object another encoder wrote");
                    file.size_bytes = i64::try_from(registered).unwrap_or(i64::MAX);
                    file.content_sha256 = Some(digest);
                }
                false
            }
        };
        let register_started = Instant::now();
        self.register(&file).await?;
        timing.register = nanos(register_started.elapsed());
        Ok((file, created, rows, writer_peak))
    }

    /// Sidecar upload, with the test-only failure injection in front. A
    /// sidecar already there with other bytes fails the attempt, so the
    /// file is never registered behind an index that would hide it.
    async fn upload_index(&self, prepared: &flush::Prepared) -> Result<(), IngestError> {
        if take_injected(&self.inject_index_failures) {
            return Err(IngestError::Injected("trace index upload"));
        }
        let outcome = flush::upload_index(self.store.as_ref(), prepared).await?;
        if let (Some(flush::Upload::Created), Some((_, index))) = (outcome, &prepared.index) {
            self.flush_stats
                .trace_index_bytes
                .fetch_add(u64::try_from(index.len()).unwrap_or(0), Ordering::Relaxed);
        }
        Ok(())
    }

    /// Catalog registration, with the test-only failure injection in front.
    async fn register(&self, file: &fs_catalog::NewFile) -> Result<(), IngestError> {
        if take_injected(&self.inject_register_failures) {
            if let Some(delay) = self.config.inject_register_delay {
                self.flush_stats
                    .registrations_held
                    .fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(delay).await;
                self.flush_stats
                    .registrations_held
                    .fetch_sub(1, Ordering::SeqCst);
            }
            return Err(IngestError::Injected("catalog registration"));
        }
        self.catalog.register_file(file).await?;
        Ok(())
    }

    /// One attempt at confirming the registration of `file`, whose rows
    /// are retained in the persisted state, within `confirm_timeout`. A
    /// failure leaves the checkpoint behind the file's intent and schedules
    /// a retry with backoff; `shutdown` cancels the attempt, which gives the
    /// entry back untouched.
    async fn confirm(
        &self,
        stream: Stream,
        file: FileId,
        attempt: ConfirmAttempt,
        mut shutdown: Option<watch::Receiver<bool>>,
    ) {
        let call = async {
            if take_injected(&self.inject_confirm_failures) {
                if let Some(delay) = self.config.inject_confirm_delay {
                    tokio::time::sleep(delay).await;
                }
                return Err(IngestError::Injected("catalog confirmation"));
            }
            self.catalog
                .confirm_file(file)
                .await
                .map_err(IngestError::from)
        };
        let cancelled = async {
            match shutdown.as_mut() {
                Some(shutdown) => {
                    let _ = shutdown.changed().await;
                }
                None => std::future::pending().await,
            }
        };
        let outcome = tokio::select! {
            outcome = tokio::time::timeout(self.config.confirm_timeout, call) => outcome,
            () = cancelled => return,
        };
        let mut buffers = self.buffers.lock().await;
        let confirmed = matches!(outcome, Ok(Ok(())));
        match outcome {
            Ok(Ok(())) => buffers.confirmed(file),
            Ok(Err(e)) => {
                buffers.confirm_failed(file, Instant::now());
                self.flush_stats
                    .confirm_failures
                    .fetch_add(1, Ordering::Relaxed);
                warn!(tenant = %stream.tenant.0, file = %file.0, error = %e, "registration not confirmed; will retry");
            }
            Err(_) => {
                buffers.confirm_failed(file, Instant::now());
                self.flush_stats
                    .confirm_failures
                    .fetch_add(1, Ordering::Relaxed);
                warn!(tenant = %stream.tenant.0, file = %file.0, "confirmation timed out; will retry");
            }
        }
        self.publish_unconfirmed(&buffers);
        drop(buffers);
        drop(attempt);
        if confirmed && self.config.checkpoint_after_flush {
            self.checkpoint_wal().await;
        }
    }

    /// Starts an attempt for every confirmation due while a permit is free,
    /// each on its own task in `attempts`: a permit is taken before an
    /// entry, so an entry in progress always has its call running, and the
    /// pass itself never waits on the catalog.
    async fn confirm_due(
        self: &Arc<Self>,
        shutdown: Option<&watch::Receiver<bool>>,
        attempts: &mut JoinSet<()>,
    ) {
        loop {
            let Ok(permit) = self.confirms.clone().try_acquire_owned() else {
                break;
            };
            let taken = self.buffers.lock().await.take_confirmation(Instant::now());
            let Some((stream, file, attempt)) = taken else {
                break;
            };
            let shared = self.clone();
            let shutdown = shutdown.cloned();
            attempts.spawn(async move {
                shared.confirm(stream, file, attempt, shutdown).await;
                drop(permit);
            });
        }
    }

    /// Confirms everything due, refilling the permits as attempts finish,
    /// until nothing is due or `confirm_timeout` has passed: a stop leaves
    /// nothing to the replay that a reachable catalog could take now.
    async fn confirm_remaining(self: &Arc<Self>) {
        let mut attempts = JoinSet::new();
        let _ = tokio::time::timeout(self.config.confirm_timeout, async {
            loop {
                self.confirm_due(None, &mut attempts).await;
                if attempts.join_next().await.is_none() {
                    break;
                }
            }
        })
        .await;
        attempts.shutdown().await;
    }

    fn publish_unconfirmed(&self, buffers: &Buffers) {
        self.flush_stats
            .unconfirmed
            .store(buffers.unconfirmed_count() as u64, Ordering::Relaxed);
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
        for (stream, file) in due {
            let Some(permit) = self.flush_permit(force).await else {
                return;
            };
            let taken = self.buffers.lock().await.take_flushing(stream, file);
            let Some((batches, held, layout, origin)) = taken else {
                continue;
            };
            self.stats.flush_retries.fetch_add(1, Ordering::Relaxed);
            debug!(tenant = %stream.tenant.0, signal = ?stream.signal, file = %file.0, rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>(), "retrying flush");
            let delay = self
                .config
                .inject_retry_delay
                .filter(|_| self.retry_delay_pending.swap(false, Ordering::Relaxed));
            let attempt = Attempt {
                stream,
                file,
                origin,
                batches,
                held,
                layout,
            };
            self.spawn_materialize(vec![attempt], permit, delay);
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
    /// Materialises `attempts`, the parts of one cut, one after the other
    /// under `permit`.
    fn spawn_materialize(
        self: &Arc<Self>,
        attempts: Vec<Attempt>,
        permit: tokio::sync::OwnedSemaphorePermit,
        delay: Option<Duration>,
    ) {
        let shared = Arc::clone(self);
        tokio::spawn(async move {
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            for attempt in attempts {
                let Attempt {
                    stream,
                    file,
                    origin,
                    batches,
                    held,
                    layout,
                } = attempt;
                if let Err(e) = shared
                    .materialize(stream, file, origin, batches, held, layout)
                    .await
                {
                    error!(tenant = %stream.tenant.0, signal = ?stream.signal, error = %e, "flush error");
                }
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
                let headroom = buffers.headroom(self.config.max_memory_bytes);
                let candidates = buffers.coalesce_candidates(min, max_bytes);
                // The tenant's bound covers its records of every signal, so
                // it is never above the stream's own.
                candidates.into_iter().find_map(|stream| {
                    let bound = self.writer.stable_bound(stream.tenant);
                    buffers
                        .take_for_coalesce(stream, bound, min, max_bytes, headroom)
                        .map(|taken| (stream, taken))
                })
            };
            let Some((stream, taken)) = taken else {
                return;
            };
            let shared = Arc::clone(self);
            tokio::spawn(async move {
                shared.coalesce_group(stream, taken).await;
                drop(permit);
                shared.coalesce_now.notify_one();
            });
        }
    }

    /// Builds the chunk of one group and puts it in place. On any failure
    /// the batches stay as they are.
    async fn coalesce_group(&self, stream: Stream, taken: crate::buffer::Taken) {
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
            let chunk = arrow::compute::concat_batches(&flush::schema_of(stream.signal), &batches)?;
            let traces = crate::buffer::trace_set(&chunk);
            Ok::<_, arrow::error::ArrowError>((chunk, traces))
        })
        .await;
        match merged {
            Ok(Ok((chunk, traces))) => {
                if self
                    .buffers
                    .lock()
                    .await
                    .finish_coalesce(stream, id, chunk, traces)
                {
                    fs.coalesces.fetch_add(1, Ordering::Relaxed);
                    fs.coalesced_batches
                        .fetch_add(count as u64, Ordering::Relaxed);
                    fs.coalesce_nanos
                        .fetch_add(nanos(started.elapsed()), Ordering::Relaxed);
                    fs.coalesce_time.observe(started.elapsed());
                }
            }
            Ok(Err(e)) => {
                warn!(tenant = %stream.tenant.0, error = %e, "coalesce failed; batches stay as they are");
                self.buffers.lock().await.cancel_coalesce(stream, id);
            }
            Err(e) => {
                warn!(tenant = %stream.tenant.0, error = %e, "coalesce task failed; batches stay as they are");
                self.buffers.lock().await.cancel_coalesce(stream, id);
            }
        }
        fs.coalesce_active.fetch_sub(1, Ordering::Relaxed);
        drop(scratch);
    }

    /// Rows of `tenant` in every state, plus every id the live rows can be
    /// registered under: one per hour they fall in, as the cut derives
    /// them. Taken under the exclusive gate: no request is between its
    /// append and its push, so the smallest live position, and with it the
    /// announced ids, is the one the next flush derives from.
    pub(crate) async fn snapshot(&self, stream: Stream) -> (Snapshot, Vec<FileId>) {
        let tenant = stream.tenant;
        let started = Instant::now();
        let snapshot = {
            let _exclusive = self.gate.write().await;
            self.buffers.lock().await.snapshot(stream)
        };
        self.stats.hot_scan.observe(started.elapsed());
        let mut live_files = Vec::new();
        if let Some(p) = snapshot.live_first_wal {
            if snapshot.live_hours.is_empty() {
                live_files.push(FileId::derived(self.instance, tenant, p.segment, p.offset));
            }
            live_files.extend(
                snapshot.live_hours.iter().map(|hour| {
                    FileId::derived_at(self.instance, tenant, p.segment, p.offset, *hour)
                }),
            );
        }
        (snapshot, live_files)
    }

    /// Cuts every due tenant (every tenant with live rows when `everything`)
    /// and materialises each cut on its own task, at most `max_flushes` at a
    /// time, so the flusher keeps coalescing while files are written. A
    /// tenant left without a permit stays due for the next round. With
    /// `everything` the call returns only once every flush, including those
    /// started earlier, has finished. Returns how many flushes were cut.
    async fn flush_due(self: &Arc<Self>, everything: bool) -> usize {
        let mut cuts = 0;
        let now = Instant::now();
        let streams = {
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
        for stream in streams {
            let Some(permit) = self.flush_permit(everything).await else {
                return cuts;
            };
            match self.cut_stream(stream).await {
                Ok(Some(parts)) => {
                    cuts += parts.len();
                    let attempts = parts
                        .into_iter()
                        .map(|(file, batches, held)| Attempt {
                            stream,
                            file,
                            origin: file,
                            batches,
                            held,
                            layout: Some(FileLayout::current(self.config.row_group_rows)),
                        })
                        .collect();
                    self.spawn_materialize(attempts, permit, None);
                }
                Ok(None) => {}
                Err(e) => {
                    error!(tenant = %stream.tenant.0, signal = ?stream.signal, error = %e, "flush error");
                }
            }
        }
        if everything {
            // Every permit at once: nothing is still being written.
            let all = flush_permits(self.config.max_flushes);
            let _drained = Arc::clone(&self.flushes)
                .acquire_many_owned(u32::try_from(all).unwrap_or(u32::MAX))
                .await;
        }
        cuts
    }

    /// True while some tenant has live rows.
    async fn has_live(&self) -> bool {
        !self.buffers.lock().await.all().is_empty()
    }
}

/// Everything replay found: rows with their positions, and flush intents.
struct Replayed {
    rows: Vec<(Stream, Position, RecordBatch)>,
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
    // Any record this engine cannot read and any damage the log reports may
    // hide a flush intent; skipping it would send the intent's rows back to
    // live and write them as other bytes under an id already taken. The
    // only damage ever tolerated, bytes after the last commit marker, was
    // cut when the log opened: no sync completed for them, so nothing was
    // acknowledged and no flush ever depended on them.
    for event in wal.replay()? {
        match event {
            Event::Record(entry) => {
                out.records += 1;
                let record = WalRecord::decode(&entry.payload).map_err(|e| {
                    IngestError::Record(format!(
                        "wal record at {:?} cannot be read by this engine ({e}); it was written by a newer one",
                        entry.position
                    ))
                })?;
                match record.signal {
                    RecordSignal::Traces => {
                        let Ok(message) = ExportTraceServiceRequest::decode(record.payload) else {
                            out.damaged += 1;
                            continue;
                        };
                        let mut builder = SpanBatchBuilder::new(limits);
                        builder.append_request(&message)?;
                        let stream = Stream::traces(record.tenant);
                        out.rows.push((stream, entry.position, builder.finish()?));
                    }
                    RecordSignal::Logs => {
                        let Some((seed, request)) =
                            crate::record::split_log_payload(&record.payload)
                        else {
                            out.damaged += 1;
                            continue;
                        };
                        let Ok(message) = ExportLogsServiceRequest::decode(request) else {
                            out.damaged += 1;
                            continue;
                        };
                        let mut builder = LogBatchBuilder::new(limits);
                        builder.append_request(&message, seed)?;
                        let stream = Stream::logs(record.tenant);
                        out.rows.push((stream, entry.position, builder.finish()?));
                    }
                    // An intent this engine cannot read was written by a newer
                    // one. Skipping it would send its rows back to live and
                    // write them as other bytes under an id already taken, so
                    // the replay stops here: the log needs an engine that reads it.
                    RecordSignal::FlushIntent => match FlushIntent::from_record(&record) {
                        Ok(intent) => out.intents.push(intent),
                        Err(e) => {
                            return Err(IngestError::Record(format!(
                                "flush intent at {:?} cannot be read by this engine ({e}); it was written by a newer one",
                                entry.position
                            )));
                        }
                    },
                }
            }
            Event::Damaged { position, damage } => {
                return Err(IngestError::Record(format!(
                    "wal damage at {position:?} ({damage:?}); records past it cannot be trusted and may include a flush intent"
                )));
            }
        }
    }
    Ok(out)
}

/// Rebuilds buffers: rows covered by an intent go back to the flushing state
/// under that file id, everything else is live.
/// Rebuilds buffers: rows covered by an intent go back to the flushing
/// state under it, filtered to the intent's hour when it has one; rows no
/// intent covers, or of an hour no intent of their range names, are live.
///
/// # Errors
/// Rows without the span timestamp column under an intent with an hour.
fn rebuild(replayed: Replayed, scratch: Arc<AtomicU64>) -> Result<Buffers, IngestError> {
    let now = Instant::now();
    let mut buffers = Buffers::with_scratch_counter(scratch);
    // Several intents for the same records are one flush under the id of
    // the last logged, an earlier id having been given up, and the id of
    // the first logged is the origin: the one the hot window announced
    // from the start, taken from the log rather than derived again.
    let mut latest: Vec<(FlushIntent, FileId)> = Vec::new();
    for intent in replayed.intents {
        let same_range = |earlier: &FlushIntent| {
            earlier.tenant == intent.tenant
                && earlier.first == intent.first
                && earlier.end == intent.end
                && earlier.hour == intent.hour
        };
        let origin = latest
            .iter()
            .find(|(earlier, _)| same_range(earlier))
            .map_or(intent.file, |(_, origin)| *origin);
        latest.retain(|(earlier, _)| !same_range(earlier));
        latest.push((intent, origin));
    }
    let mut groups: Vec<(FlushIntent, FileId, TenantBuffer)> = latest
        .into_iter()
        .map(|(i, origin)| (i, origin, TenantBuffer::default()))
        .collect();
    for (stream, position, batch) in replayed.rows {
        let covering: Vec<usize> = groups
            .iter()
            .enumerate()
            .filter(|(_, (intent, _, _))| intent.covers(stream.tenant, stream.signal, position))
            .map(|(i, _)| i)
            .collect();
        let whole = covering.iter().find(|i| {
            groups
                .get(**i)
                .is_some_and(|(intent, _, _)| intent.hour.is_none())
        });
        if let Some(i) = whole {
            if let Some((_, _, buffer)) = groups.get_mut(*i) {
                buffer.push(batch, position, now);
            }
            continue;
        }
        let (targets, hours): (Vec<usize>, Vec<DateTime<Utc>>) = covering
            .into_iter()
            .filter_map(|i| {
                groups
                    .get(i)
                    .and_then(|(intent, _, _)| intent.hour_start())
                    .map(|hour| (i, hour))
            })
            .unzip();
        if hours.is_empty() {
            buffers.get_mut(stream).push(batch, position, now);
            continue;
        }
        let split = crate::hours::partition(&batch, &hours)?;
        for (i, piece) in targets.into_iter().zip(split.by_hour) {
            if let Some((_, _, buffer)) = groups.get_mut(i) {
                buffer.push(piece, position, now);
            }
        }
        buffers.get_mut(stream).push(split.rest, position, now);
    }
    for (intent, origin, buffer) in groups {
        let bound = Bound {
            file: intent.file,
            range: (intent.first, intent.end),
            layout: intent.layout,
            hour: intent.hour_start(),
        };
        let stream = Stream {
            tenant: intent.tenant,
            signal: intent.signal,
        };
        buffers.insert_flushing(stream, bound, buffer, origin);
    }
    Ok(buffers)
}

/// The ingester role.
pub struct Ingester {
    shared: Arc<Shared>,
    hot_tls: Option<fs_auth::ServerTls>,
}

impl Ingester {
    /// Serves the hot window over mTLS with `tls` instead of plain text.
    #[must_use]
    pub fn with_hot_tls(mut self, tls: fs_auth::ServerTls) -> Self {
        self.hot_tls = Some(tls);
        self
    }

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
        let buffers = rebuild(replayed, Arc::clone(&flush_stats.scratch_bytes))?;
        info!(
            records,
            damaged,
            intents,
            rows = buffers.total_rows(),
            "wal replayed"
        );
        let instance = wal.instance_id();
        let config_injected = config.inject_register_failures;
        let config_confirm_injected = config.inject_confirm_failures;
        let config_intent_injected = config.inject_intent_failures;
        let config_confirms = config.max_confirms;
        let config_index_injected = config.inject_index_failures;
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
            confirm_now: Notify::new(),
            coalesce_now: Notify::new(),
            conversions: tokio::sync::Semaphore::new(
                config_conversions.clamp(1, tokio::sync::Semaphore::MAX_PERMITS),
            ),
            coalesces: Arc::new(tokio::sync::Semaphore::new(
                config_coalesces.clamp(1, tokio::sync::Semaphore::MAX_PERMITS),
            )),
            flushes: Arc::new(tokio::sync::Semaphore::new(flush_permits(config_flushes))),
            confirms: Arc::new(tokio::sync::Semaphore::new(confirm_permits(
                config_confirms,
            ))),
            inject_register_failures: std::sync::atomic::AtomicU32::new(config_injected),
            inject_intent_failures: std::sync::atomic::AtomicU32::new(config_intent_injected),
            inject_confirm_failures: std::sync::atomic::AtomicU32::new(config_confirm_injected),
            inject_index_failures: std::sync::atomic::AtomicU32::new(config_index_injected),
            push_delay_pending: std::sync::atomic::AtomicBool::new(push_delay_pending),
            retry_delay_pending: std::sync::atomic::AtomicBool::new(retry_delay_pending),
            catalog,
            store,
            root,
        };
        Ok(Self {
            shared: Arc::new(shared),
            hot_tls: None,
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
        // A cut takes at most `max_hours_per_cut` hours, so a pass can leave
        // rows live; passes repeat while they make progress, and stop on a
        // pass that cut nothing rather than spin on a failing intent.
        while self.shared.flush_due(true).await > 0 && self.shared.has_live().await {}
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
        let logs = LogsServiceServer::new(LogsReceiver::new(self.shared.clone()))
            .max_decoding_message_size(self.shared.config.max_message_bytes);
        let mut server_shutdown = shutdown.clone();
        let server = tonic::transport::Server::builder()
            .add_service(service)
            .add_service(logs)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    let _ = server_shutdown.changed().await;
                },
            );

        let hot = crate::hot::serve(
            self.shared.clone(),
            self.shared.config.hot_listen,
            self.hot_tls.as_ref(),
            shutdown.clone(),
            hot_bound,
        );

        let shared = self.shared.clone();
        let mut flusher_shutdown = shutdown.clone();
        let flusher = async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        shared.coalesce_due().await;
                        let _ = shared.flush_due(false).await;
                    }
                    () = shared.coalesce_now.notified() => shared.coalesce_due().await,
                    () = shared.flush_now.notified() => {
                        let _ = shared.flush_due(false).await;
                    }
                    _ = flusher_shutdown.changed() => break,
                }
            }
        };

        // Every wake refills the permits, a finished attempt included, so a
        // backlog drains at `max_confirms` calls at a time. The stop cancels
        // the calls still running and waits for each to give its entry back.
        let shared = self.shared.clone();
        let confirmer = async move {
            let mut tick = tokio::time::interval(shared.config.confirm_interval);
            let mut attempts = JoinSet::new();
            loop {
                tokio::select! {
                    _ = tick.tick() => {}
                    () = shared.confirm_now.notified() => {}
                    Some(_) = attempts.join_next(), if !attempts.is_empty() => {}
                    _ = shutdown.changed() => break,
                }
                shared.confirm_due(Some(&shutdown), &mut attempts).await;
            }
            attempts.shutdown().await;
        };

        let (transport, hot_result, (), ()) = tokio::join!(server, hot, flusher, confirmer);
        transport?;
        hot_result?;
        info!("otlp server stopped, flushing buffers");
        self.flush_all().await;
        self.shared.confirm_remaining().await;
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use fs_catalog::{FileId, TenantId};
    use fs_wal::Position;

    use fs_schema::limits::Limits;
    use fs_schema::spans::SpanBatchBuilder;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    use super::{Replayed, confirm_permits, rebuild};
    use crate::buffer::Stream;
    use crate::record::{FlushIntent, RowSignal};

    /// 2026-10-22T14:00:00Z.
    const H14: u64 = 1_792_677_600_000_000_000;
    const HOUR: u64 = 3_600_000_000_000;

    fn span_batch(timestamps: &[u64]) -> RecordBatch {
        let spans = timestamps
            .iter()
            .enumerate()
            .map(|(i, ts)| Span {
                trace_id: vec![1; 16],
                span_id: vec![u8::try_from(i).unwrap() + 1; 8],
                name: format!("op-{i}"),
                kind: 2,
                start_time_unix_nano: *ts,
                end_time_unix_nano: *ts + 1_000,
                ..Span::default()
            })
            .collect();
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("api".into())),
                        }),
                        ..KeyValue::default()
                    }],
                    ..Resource::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let mut builder = SpanBatchBuilder::new(Limits::default());
        builder.append_request(&request).unwrap();
        builder.finish().unwrap()
    }

    /// Intents of one hour each over the same records put each row under
    /// the intent of its hour; a row of an hour no intent names is live.
    #[test]
    fn a_replayed_split_flush_puts_each_row_under_the_intent_of_its_hour() {
        let tenant = TenantId(uuid::Uuid::from_u128(7));
        let h14 = FileId(uuid::Uuid::from_u128(0x14));
        let h15 = FileId(uuid::Uuid::from_u128(0x15));
        let intent = |file: FileId, hour: u64| FlushIntent {
            tenant,
            file,
            first: at(10),
            end: at(30),
            layout: None,
            hour: Some(i64::try_from(hour / 1_000_000_000).unwrap()),
            signal: RowSignal::Traces,
        };
        let replayed = Replayed {
            rows: vec![
                (
                    Stream::traces(tenant),
                    at(10),
                    span_batch(&[H14 + 1, H14 + HOUR + 1, H14 + 2]),
                ),
                (
                    Stream::traces(tenant),
                    at(20),
                    span_batch(&[H14 + HOUR + 2, H14 + 2 * HOUR]),
                ),
            ],
            intents: vec![intent(h14, H14), intent(h15, H14 + HOUR)],
            records: 2,
            damaged: 0,
        };
        let mut buffers = rebuild(replayed, Arc::new(AtomicU64::new(0))).unwrap();
        let rows =
            |batches: &[RecordBatch]| batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        let (b14, _l14, _, _) = buffers.take_flushing(Stream::traces(tenant), h14).unwrap();
        assert_eq!(rows(&b14), 2);
        let (b15, _l15, _, _) = buffers.take_flushing(Stream::traces(tenant), h15).unwrap();
        assert_eq!(rows(&b15), 2);
        assert_eq!(
            buffers
                .flushing_range(Stream::traces(tenant), h15)
                .unwrap()
                .hour,
            Some(
                chrono::DateTime::from_timestamp(
                    i64::try_from((H14 + HOUR) / 1_000_000_000).unwrap(),
                    0
                )
                .unwrap()
            )
        );
        assert_eq!(
            buffers
                .snapshot(Stream::traces(tenant))
                .live
                .iter()
                .map(|b| b.batch.num_rows())
                .sum::<usize>(),
            1,
            "the row of 16:00 belongs to no intent"
        );
        assert_eq!(buffers.live_first_wal(Stream::traces(tenant)), Some(at(20)));
    }

    #[test]
    fn confirm_permits_stay_within_what_the_runtime_can_track() {
        let most = tokio::sync::Semaphore::MAX_PERMITS;
        assert_eq!(confirm_permits(0), 1);
        assert_eq!(confirm_permits(4), 4);
        assert_eq!(confirm_permits(most), most);
        assert_eq!(confirm_permits(most + 1), most);
        assert_eq!(confirm_permits(usize::MAX), most);
        let _ = tokio::sync::Semaphore::new(confirm_permits(usize::MAX));
    }

    fn batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let values = Int64Array::from_iter_values(0..i64::try_from(rows).unwrap());
        RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap()
    }

    fn at(offset: u64) -> Position {
        Position { segment: 1, offset }
    }

    /// The origin of a replayed flush is the id of the first intent logged
    /// for its records, whatever that id is, and the flush runs under the
    /// id of the last.
    #[test]
    fn a_replayed_flush_takes_its_origin_from_the_first_intent_logged() {
        let tenant = TenantId(uuid::Uuid::from_u128(7));
        // Deliberately not the id a fresh cut would derive from the position.
        let first = FileId(uuid::Uuid::from_u128(0xf1a5));
        let second = FileId(uuid::Uuid::from_u128(0xf1a6));
        let intent = |file: FileId| FlushIntent {
            tenant,
            file,
            first: at(10),
            end: at(30),
            layout: None,
            hour: None,
            signal: RowSignal::Traces,
        };
        let replayed = Replayed {
            rows: vec![
                (Stream::traces(tenant), at(10), batch(2)),
                (Stream::traces(tenant), at(20), batch(3)),
            ],
            intents: vec![intent(first), intent(second)],
            records: 2,
            damaged: 0,
        };
        let mut buffers = rebuild(replayed, Arc::new(AtomicU64::new(0))).unwrap();
        assert!(
            buffers
                .flushing_range(Stream::traces(tenant), first)
                .is_none(),
            "the first id was given up"
        );
        let (batches, _lease, _, origin) = buffers
            .take_flushing(Stream::traces(tenant), second)
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
        assert_eq!(
            origin, first,
            "announced by the id the rows were first given"
        );
    }
}
