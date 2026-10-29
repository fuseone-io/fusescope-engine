//! Prometheus text exposition of the engine's counters, on a plain HTTP/1.1
//! listener. Deliberately dependency-free: the page is a handful of lines and
//! the only client is a scraper.

use std::fmt::Write as _;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::ServerError;

/// Counters registered by the roles as they start.
pub struct Metrics {
    started: Instant,
    ingest: OnceLock<Arc<fs_ingest::IngestStats>>,
    writer: OnceLock<Arc<fs_ingest::walwriter::WriterStats>>,
    flush: OnceLock<Arc<fs_ingest::FlushStats>>,
    wal_bytes: OnceLock<Arc<dyn Fn() -> u64 + Send + Sync>>,
    query: OnceLock<Arc<fs_query::QueryStats>>,
    compactor: OnceLock<Arc<fs_compactor::Stats>>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            ingest: OnceLock::new(),
            writer: OnceLock::new(),
            flush: OnceLock::new(),
            wal_bytes: OnceLock::new(),
            query: OnceLock::new(),
            compactor: OnceLock::new(),
        }
    }
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish_non_exhaustive()
    }
}

impl Metrics {
    /// Registers the ingester's counters and WAL size gauge.
    pub fn register_ingest(
        &self,
        stats: Arc<fs_ingest::IngestStats>,
        wal_bytes: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) {
        let _ = self.ingest.set(stats);
        let _ = self.wal_bytes.set(wal_bytes);
    }

    /// Registers the WAL writer's group-commit counters.
    pub fn register_writer(&self, stats: Arc<fs_ingest::walwriter::WriterStats>) {
        let _ = self.writer.set(stats);
    }

    /// Registers the flush timings and buffer gauges.
    pub fn register_flush(&self, stats: Arc<fs_ingest::FlushStats>) {
        let _ = self.flush.set(stats);
    }

    /// Registers the querier's counters.
    pub fn register_query(&self, stats: Arc<fs_query::QueryStats>) {
        let _ = self.query.set(stats);
    }

    /// Registers the compactor's counters.
    pub fn register_compactor(&self, stats: Arc<fs_compactor::Stats>) {
        let _ = self.compactor.set(stats);
    }

    /// Renders the exposition page.
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(4096);
        self.render_ingest(&mut out);
        self.render_flush(&mut out);
        self.render_query(&mut out);
        self.render_compactor(&mut out);
        if let Some(f) = self.wal_bytes.get() {
            gauge(
                &mut out,
                "fusescope_ingest_wal_bytes",
                "Bytes the write-ahead log holds on disk",
                f(),
            );
        }
        gauge(
            &mut out,
            "fusescope_uptime_seconds",
            "Seconds since the process started",
            self.started.elapsed().as_secs(),
        );
        out
    }

    fn render_ingest(&self, out: &mut String) {
        if let Some(i) = self.ingest.get() {
            counter(
                out,
                "fusescope_ingest_requests_total",
                "OTLP export requests accepted",
                i.requests.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_ingest_spans_total",
                "Spans buffered",
                i.spans.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_ingest_spans_rejected_total",
                "Spans rejected for malformed ids",
                i.spans_rejected.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_ingest_rate_limited_total",
                "Requests refused by the throughput limiter",
                i.rate_limited.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_ingest_over_capacity_total",
                "Requests refused because buffers or WAL were full",
                i.over_capacity.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_hot_scan_batches_skipped_total",
                "Hot-window batches skipped by their trace set",
                i.hot_batches_skipped.load(Ordering::Relaxed),
            );
            histogram(
                out,
                "fusescope_hot_scan_seconds",
                "Hot-window snapshot latency, gate wait included",
                &i.hot_scan,
            );
            histogram(
                out,
                "fusescope_checkpoint_seconds",
                "WAL checkpoint latency, gate wait and I/O included",
                &i.checkpoint,
            );
            counter(
                out,
                "fusescope_ingest_files_flushed_total",
                "Parquet files written and registered",
                i.files_flushed.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_ingest_flush_failures_total",
                "Flush attempts that failed",
                i.flush_failures.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_ingest_flush_retries_total",
                "Flush retries after a failed attempt",
                i.flush_retries.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_ingest_flushes_recovered_total",
                "Flushes resumed from an intent after a restart",
                i.flushes_recovered.load(Ordering::Relaxed),
            );
        }
        if let Some(w) = self.writer.get() {
            counter(
                out,
                "fusescope_wal_records_total",
                "Records appended to the WAL",
                w.records.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_wal_batches_total",
                "Group-commit batches synced",
                w.batches.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_wal_backpressure_total",
                "Appends refused because the writer queue was full",
                w.backpressure.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_wal_sync_seconds_total",
                "Seconds spent in fsync",
                w.sync_nanos.load(Ordering::Relaxed) / 1_000_000_000,
            );
        }
    }

    fn render_flush(&self, out: &mut String) {
        let Some(f) = self.flush.get() else { return };
        let ns = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed);
        counter(
            out,
            "fusescope_flush_attempts_total",
            "Flush attempts, successful or not",
            ns(&f.attempts),
        );
        counter(
            out,
            "fusescope_flush_total",
            "Flushes completed",
            ns(&f.flushes),
        );
        counter(
            out,
            "fusescope_flush_rows_total",
            "Rows written by flushes",
            ns(&f.rows),
        );
        counter(
            out,
            "fusescope_flush_parquet_bytes_total",
            "Parquet bytes written by flushes",
            ns(&f.parquet_bytes),
        );
        counter(
            out,
            "fusescope_flush_concat_nanoseconds_total",
            "Nanoseconds sorting each input batch, completed flushes",
            ns(&f.concat_nanos),
        );
        counter(
            out,
            "fusescope_flush_sort_nanoseconds_total",
            "Nanoseconds merging sorted inputs into row groups, completed flushes",
            ns(&f.sort_nanos),
        );
        counter(
            out,
            "fusescope_flush_encode_nanoseconds_total",
            "Nanoseconds encoding Parquet",
            ns(&f.encode_nanos),
        );
        counter(
            out,
            "fusescope_flush_upload_nanoseconds_total",
            "Nanoseconds uploading",
            ns(&f.upload_nanos),
        );
        counter(
            out,
            "fusescope_flush_register_nanoseconds_total",
            "Nanoseconds registering in the catalog",
            ns(&f.register_nanos),
        );
        counter(
            out,
            "fusescope_flush_nanoseconds_total",
            "Nanoseconds from taking rows to checkpoint",
            ns(&f.total_nanos),
        );
        counter(
            out,
            "fusescope_coalesce_total",
            "Chunks built by merging request batches in log order.",
            f.coalesces.load(Ordering::Relaxed),
        );
        counter(
            out,
            "fusescope_coalesce_batches_total",
            "Request batches merged into chunks.",
            f.coalesced_batches.load(Ordering::Relaxed),
        );
        counter(
            out,
            "fusescope_coalesce_nanoseconds_total",
            "Time spent merging request batches.",
            f.coalesce_nanos.load(Ordering::Relaxed),
        );
        self.render_buffers(out);
    }

    fn render_buffers(&self, out: &mut String) {
        let Some(f) = self.flush.get() else { return };
        let ns = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed);
        gauge(
            out,
            "fusescope_buffer_live_bytes",
            "Bytes in live buffers",
            ns(&f.live_bytes),
        );
        gauge(
            out,
            "fusescope_buffer_flushing_bytes",
            "Bytes bound to flushes in progress",
            ns(&f.flushing_bytes),
        );
        gauge(
            out,
            "fusescope_buffer_persisted_bytes",
            "Bytes kept for the hot window after registration",
            ns(&f.persisted_bytes),
        );
        gauge(
            out,
            "fusescope_buffer_scratch_bytes",
            "Working memory held by chunks being built",
            ns(&f.scratch_bytes),
        );
        gauge(
            out,
            "fusescope_coalesce_active",
            "Chunks being built right now",
            ns(&f.coalesce_active),
        );
        gauge(
            out,
            "fusescope_flush_writer_peak_bytes",
            "Most bytes the Parquet writer held for one row group",
            ns(&f.writer_peak_bytes),
        );
        counter(
            out,
            "fusescope_flush_writer_bytes_total",
            "Sum over flushes of the writer's peak bytes",
            ns(&f.writer_bytes),
        );
        counter(
            out,
            "fusescope_flush_row_groups_total",
            "Parquet row groups written",
            ns(&f.row_groups),
        );
        counter(
            out,
            "fusescope_flush_trace_index_bytes_total",
            "Bytes of trace_id sidecars created",
            ns(&f.trace_index_bytes),
        );
        counter(
            out,
            "fusescope_flush_superseded_total",
            "Flushes given a fresh id because the orphan sweep marked their path",
            ns(&f.superseded),
        );
        counter(
            out,
            "fusescope_flush_confirm_failures_total",
            "Registration confirmations that failed and wait for a retry",
            ns(&f.confirm_failures),
        );
        gauge(
            out,
            "fusescope_flush_unconfirmed",
            "Registered files whose confirmation has not landed",
            ns(&f.unconfirmed),
        );
        histogram(
            out,
            "fusescope_coalesce_seconds",
            "Time to build one chunk",
            &f.coalesce_time,
        );
    }

    fn render_compactor(&self, out: &mut String) {
        let Some(stats) = self.compactor.get() else {
            return;
        };
        let c = &stats.compaction;
        let o = &stats.orphans;
        let load = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed);
        for (name, help, value) in [
            (
                "fusescope_compaction_passes_total",
                "Compaction passes that swapped outputs in",
                load(&c.passes),
            ),
            (
                "fusescope_compaction_inputs_total",
                "L0 files merged by completed passes",
                load(&c.inputs),
            ),
            (
                "fusescope_compaction_outputs_total",
                "L1 files published by completed passes",
                load(&c.outputs),
            ),
            (
                "fusescope_compaction_rows_total",
                "Rows written by completed passes",
                load(&c.rows),
            ),
            (
                "fusescope_compaction_input_bytes_total",
                "Bytes of inputs merged by completed passes",
                load(&c.input_bytes),
            ),
            (
                "fusescope_compaction_output_bytes_total",
                "Bytes of outputs published by completed passes",
                load(&c.output_bytes),
            ),
            (
                "fusescope_compaction_conflicts_total",
                "Swaps refused because an input was no longer visible",
                load(&c.conflicts),
            ),
            (
                "fusescope_compaction_lease_lost_total",
                "Swaps refused because the lease had changed hands",
                load(&c.lease_lost),
            ),
            (
                "fusescope_compaction_failures_total",
                "Passes that failed for any other reason",
                load(&c.failures),
            ),
            (
                "fusescope_compaction_orphaned_total",
                "Outputs uploaded by passes that did not publish them",
                load(&c.orphaned),
            ),
            (
                "fusescope_compaction_skipped_total",
                "Hours skipped because their two oldest files exceed the input cap",
                load(&c.skipped),
            ),
            (
                "fusescope_orphan_sweeps_total",
                "Orphan sweeps completed",
                load(&o.sweeps),
            ),
            (
                "fusescope_orphan_listed_total",
                "Objects listed by completed sweeps",
                load(&o.listed),
            ),
            (
                "fusescope_orphan_seen_total",
                "Objects seen without a catalog row",
                load(&o.seen),
            ),
            (
                "fusescope_orphan_deleted_total",
                "Objects deleted after a TTL without a row",
                load(&o.deleted),
            ),
            (
                "fusescope_orphan_forgotten_total",
                "Remembered orphans found with a row, or gone, before their TTL",
                load(&o.forgotten),
            ),
            (
                "fusescope_orphan_delete_failures_total",
                "Deletes that failed and wait their backoff",
                load(&o.delete_failures),
            ),
        ] {
            counter(out, name, help, value);
        }
    }

    fn render_query(&self, out: &mut String) {
        if let Some(q) = self.query.get() {
            counter(
                out,
                "fusescope_query_statements_total",
                "Statements planned",
                q.queries.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_query_errors_total",
                "Statements refused or failed to plan",
                q.errors.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_query_rows_total",
                "Rows returned by collected queries",
                q.rows.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_query_hot_fanouts_total",
                "Hot-window fan-outs",
                q.hot_fanouts.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_query_hot_unavailable_total",
                "Ingesters that did not answer a fan-out",
                q.hot_unavailable.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_query_trace_index_fetches_total",
                "trace_id sidecars fetched from the bucket",
                q.trace_index_fetches.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_query_trace_index_misses_total",
                "Sidecars missing, unreadable or unreachable; their files were scanned",
                q.trace_index_misses.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_query_trace_index_pruned_total",
                "Files a trace lookup skipped because their sidecar lacked the trace",
                q.trace_index_pruned.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_query_plannings_total",
                "Times a statement went through the planner",
                q.plannings.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_query_prepared_hits_total",
                "Tickets executed from their prepared plan",
                q.prepared_hits.load(Ordering::Relaxed),
            );
            counter(
                out,
                "fusescope_query_prepared_misses_total",
                "Tickets whose prepared plan was gone and whose SQL was planned again",
                q.prepared_misses.load(Ordering::Relaxed),
            );
            gauge(
                out,
                "fusescope_query_prepared_plans",
                "Plans waiting for their DoGet",
                q.prepared.load(Ordering::Relaxed),
            );
            gauge(
                out,
                "fusescope_query_prepared_bytes",
                "Bytes the waiting plans are estimated to retain",
                q.prepared_bytes.load(Ordering::Relaxed),
            );
        }
    }
}

fn counter(out: &mut String, name: &str, help: &str, value: u64) {
    let _ = writeln!(
        out,
        "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}"
    );
}

fn histogram(out: &mut String, name: &str, help: &str, h: &fs_ingest::histogram::Histogram) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} histogram");
    let cumulative = h.cumulative();
    for (bound, count) in fs_ingest::histogram::BOUNDS.iter().zip(cumulative) {
        let _ = writeln!(out, "{name}_bucket{{le=\"{bound}\"}} {count}");
    }
    let _ = writeln!(
        out,
        "{name}_bucket{{le=\"+Inf\"}} {}\n{name}_sum {}\n{name}_count {}",
        cumulative[10],
        std::time::Duration::from_nanos(h.sum_nanos()).as_secs_f64(),
        h.count()
    );
}

fn gauge(out: &mut String, name: &str, help: &str, value: u64) {
    let _ = writeln!(
        out,
        "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}"
    );
}

/// Serves `GET /metrics` on `listen` until `shutdown` flips. Any other path
/// gets 404; anything that is not a GET gets 405.
///
/// # Errors
/// Bind failure.
pub async fn serve(
    listen: std::net::SocketAddr,
    metrics: Arc<Metrics>,
    mut shutdown: watch::Receiver<bool>,
    bound: Option<tokio::sync::oneshot::Sender<std::net::SocketAddr>>,
) -> Result<(), ServerError> {
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|source| ServerError::ReadConfig {
            path: std::path::PathBuf::from(listen.to_string()),
            source,
        })?;
    let addr = listener
        .local_addr()
        .map_err(|source| ServerError::ReadConfig {
            path: "local_addr".into(),
            source,
        })?;
    info!(%addr, "metrics listening");
    if let Some(tx) = bound {
        let _ = tx.send(addr);
    }
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let metrics = Arc::clone(&metrics);
                    tokio::spawn(async move {
                        if let Err(e) = respond(stream, &metrics).await {
                            debug!(error = %e, "metrics connection failed");
                        }
                    });
                }
                Err(e) => warn!(error = %e, "metrics accept failed"),
            },
            _ = shutdown.changed() => break,
        }
    }
    info!("metrics stopped");
    Ok(())
}

async fn respond(mut stream: tokio::net::TcpStream, metrics: &Metrics) -> std::io::Result<()> {
    let mut buf = vec![0u8; 4096];
    let n =
        tokio::time::timeout(std::time::Duration::from_secs(5), stream.read(&mut buf)).await??;
    let request = String::from_utf8_lossy(buf.get(..n).unwrap_or_default());
    let mut parts = request.split_whitespace();
    let (method, path) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    let (status, body) = match (method, path) {
        ("GET", "/metrics") => ("200 OK", metrics.render()),
        ("GET", _) => ("404 Not Found", "not found\n".to_owned()),
        _ => ("405 Method Not Allowed", "method not allowed\n".to_owned()),
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_registered_counters_in_exposition_format() {
        let metrics = Metrics::default();
        let page = metrics.render();
        assert!(page.contains("# TYPE fusescope_uptime_seconds gauge"));
        assert!(!page.contains("fusescope_ingest_requests_total"));

        let ingest = Arc::new(fs_ingest::IngestStats::default());
        ingest.requests.fetch_add(3, Ordering::Relaxed);
        metrics.register_ingest(ingest, Arc::new(|| 42));
        let page = metrics.render();
        assert!(
            page.contains("fusescope_ingest_requests_total 3\n"),
            "{page}"
        );
        assert!(page.contains("fusescope_ingest_wal_bytes 42\n"), "{page}");
        let writer = Arc::new(fs_ingest::walwriter::WriterStats::default());
        writer.backpressure.fetch_add(5, Ordering::Relaxed);
        metrics.register_writer(writer);
        let page = metrics.render();
        assert!(
            page.contains("# TYPE fusescope_wal_backpressure_total counter\n"),
            "{page}"
        );
        assert!(
            page.contains("fusescope_wal_backpressure_total 5\n"),
            "{page}"
        );
        let flush = Arc::new(fs_ingest::FlushStats::default());
        flush.flushes.fetch_add(2, Ordering::Relaxed);
        flush.live_bytes.store(77, Ordering::Relaxed);
        metrics.register_flush(flush);
        let page = metrics.render();
        assert!(page.contains("fusescope_flush_total 2\n"), "{page}");
        assert!(
            page.contains("# TYPE fusescope_buffer_live_bytes gauge\n"),
            "{page}"
        );
        assert!(page.contains("fusescope_buffer_live_bytes 77\n"), "{page}");
        let compactor = Arc::new(fs_compactor::Stats::default());
        compactor.compaction.passes.fetch_add(4, Ordering::Relaxed);
        compactor
            .compaction
            .orphaned
            .fetch_add(3, Ordering::Relaxed);
        compactor.compaction.skipped.fetch_add(1, Ordering::Relaxed);
        compactor.orphans.deleted.fetch_add(6, Ordering::Relaxed);
        metrics.register_compactor(compactor);
        let page = metrics.render();
        assert!(
            page.contains("fusescope_orphan_deleted_total 6\n"),
            "{page}"
        );
        assert!(
            page.contains("fusescope_compaction_passes_total 4\n"),
            "{page}"
        );
        assert!(
            page.contains("fusescope_compaction_orphaned_total 3\n"),
            "{page}"
        );
        assert!(
            page.contains("fusescope_compaction_skipped_total 1\n"),
            "{page}"
        );
        let query = Arc::new(fs_query::QueryStats::default());
        query.plannings.fetch_add(2, Ordering::Relaxed);
        query.prepared.store(5, Ordering::Relaxed);
        query.prepared_bytes.store(9, Ordering::Relaxed);
        metrics.register_query(query);
        let page = metrics.render();
        assert!(
            page.contains("fusescope_query_plannings_total 2\n"),
            "{page}"
        );
        assert!(
            page.contains(
                "# TYPE fusescope_query_prepared_plans gauge\nfusescope_query_prepared_plans 5\n"
            ),
            "{page}"
        );
        assert!(
            page.contains(
                "# TYPE fusescope_query_prepared_bytes gauge\nfusescope_query_prepared_bytes 9\n"
            ),
            "{page}"
        );
        assert!(page.lines().all(|l| {
            l.starts_with('#')
                || l.split_once(' ')
                    .is_some_and(|(_, v)| v.parse::<u64>().is_ok())
        }));
    }
}
