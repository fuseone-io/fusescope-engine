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
    wal_bytes: OnceLock<Arc<dyn Fn() -> u64 + Send + Sync>>,
    query: OnceLock<Arc<fs_query::QueryStats>>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            ingest: OnceLock::new(),
            writer: OnceLock::new(),
            wal_bytes: OnceLock::new(),
            query: OnceLock::new(),
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

    /// Registers the querier's counters.
    pub fn register_query(&self, stats: Arc<fs_query::QueryStats>) {
        let _ = self.query.set(stats);
    }

    /// Renders the exposition page.
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(4096);
        self.render_ingest(&mut out);
        self.render_query(&mut out);
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
        }
    }
}

fn counter(out: &mut String, name: &str, help: &str, value: u64) {
    let _ = writeln!(
        out,
        "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}"
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
        assert!(page.lines().all(|l| {
            l.starts_with('#')
                || l.split_once(' ')
                    .is_some_and(|(_, v)| v.parse::<u64>().is_ok())
        }));
    }
}
