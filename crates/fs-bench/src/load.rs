//! OTLP gRPC load against a running ingester.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use serde::Serialize;
use tokio::sync::Mutex;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

use crate::stats::{Samples, per_sec};
use crate::traffic::{Generator, Shape};

/// Parameters of a load run.
#[derive(Debug, Clone)]
pub struct LoadParams {
    /// `host:port` of the OTLP gRPC listener.
    pub endpoint: String,
    /// Ingest token.
    pub token: String,
    /// Concurrent senders.
    pub concurrency: usize,
    /// Total duration.
    pub duration: Duration,
    /// Traffic shape.
    pub shape: Shape,
    /// Seed.
    pub seed: u64,
    /// `host:port` of the engine's /metrics listener, to report flush stages.
    pub metrics: Option<String>,
}

/// Flush stage averages over the run, from /metrics deltas.
#[derive(Debug, Clone, Serialize, Default)]
pub struct FlushReport {
    /// Flushes completed during the run.
    pub flushes: u64,
    /// Rows per flush.
    pub rows_per_flush: f64,
    /// Parquet MiB per flush.
    pub parquet_mib_per_flush: f64,
    /// Milliseconds per flush concatenating request batches.
    pub concat_ms: f64,
    /// Milliseconds per flush in sort.
    pub sort_ms: f64,
    /// Milliseconds per flush encoding Parquet.
    pub encode_ms: f64,
    /// Milliseconds per flush uploading.
    pub upload_ms: f64,
    /// Milliseconds per flush registering.
    pub register_ms: f64,
    /// Milliseconds per flush end to end.
    pub total_ms: f64,
    /// Buffer bytes at the end of the run: live, flushing, persisted.
    pub buffer_bytes: (u64, u64, u64),
}

/// Outcome of a load run.
#[derive(Debug, Clone, Serialize)]
pub struct LoadReport {
    /// Requests acknowledged.
    pub requests_ok: u64,
    /// Requests refused or failed.
    pub requests_err: u64,
    /// Spans acknowledged.
    pub spans_ok: u64,
    /// Wall time.
    pub seconds: f64,
    /// Requests per second.
    pub rps: f64,
    /// Spans per second.
    pub spans_per_sec: f64,
    /// Latency percentiles in milliseconds.
    pub p50_ms: f64,
    /// 95th percentile.
    pub p95_ms: f64,
    /// 99th percentile.
    pub p99_ms: f64,
    /// Error messages seen, deduplicated.
    pub errors: Vec<String>,
    /// Flush stages, when `--metrics` was given.
    pub flush: Option<FlushReport>,
}

/// Runs the load and returns the report.
///
/// # Errors
/// Connection failure.
pub async fn run(
    params: LoadParams,
) -> Result<LoadReport, Box<dyn std::error::Error + Send + Sync>> {
    // One HTTP/2 connection per sender: a shared connection serialises frame
    // handling on both sides and caps the generator, not the engine.
    let endpoint = Channel::from_shared(format!("http://{}", params.endpoint))?;
    let before = match &params.metrics {
        Some(addr) => Some(scrape(addr).await?),
        None => None,
    };
    let ok = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let spans_ok = Arc::new(AtomicU64::new(0));
    let samples = Arc::new(Mutex::new(Samples::default()));
    let errors = Arc::new(Mutex::new(std::collections::BTreeSet::new()));
    // Every sender connects first, then all start together behind a barrier;
    // the clock starts when the barrier opens.
    let barrier = Arc::new(tokio::sync::Barrier::new(params.concurrency + 1));
    let duration = params.duration;
    let header: MetadataValue<_> = format!("Bearer {}", params.token).parse()?;

    let mut tasks = Vec::new();
    for worker in 0..params.concurrency {
        let mut client = TraceServiceClient::new(endpoint.connect().await?);
        let barrier = Arc::clone(&barrier);
        let now_ns =
            u64::try_from(chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)).unwrap_or(0);
        let mut generator = Generator::new(
            params.shape.clone(),
            params.seed.wrapping_add(worker as u64 * 7919),
            now_ns,
        );
        let (ok, err, spans_ok, samples, errors, header) = (
            ok.clone(),
            err.clone(),
            spans_ok.clone(),
            samples.clone(),
            errors.clone(),
            header.clone(),
        );
        tasks.push(tokio::spawn(async move {
            let mut local = Samples::default();
            barrier.wait().await;
            let deadline = Instant::now() + duration;
            while Instant::now() < deadline {
                let request = generator.request();
                let spans: usize = request
                    .resource_spans
                    .iter()
                    .flat_map(|r| r.scope_spans.iter())
                    .map(|s| s.spans.len())
                    .sum();
                let mut req = tonic::Request::new(request);
                req.metadata_mut().insert("authorization", header.clone());
                let start = Instant::now();
                match client.export(req).await {
                    Ok(_) => {
                        ok.fetch_add(1, Ordering::Relaxed);
                        spans_ok.fetch_add(spans as u64, Ordering::Relaxed);
                        local.push(start.elapsed());
                    }
                    Err(status) => {
                        err.fetch_add(1, Ordering::Relaxed);
                        errors.lock().await.insert(format!(
                            "{:?}: {}",
                            status.code(),
                            status.message()
                        ));
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            }
            samples
                .lock()
                .await
                .values_mut()
                .extend(local.into_values());
        }));
    }
    barrier.wait().await;
    let start = Instant::now();
    for t in tasks {
        let _ = t.await;
    }
    let seconds = start.elapsed();
    let samples = samples.lock().await.clone();
    let requests_ok = ok.load(Ordering::Relaxed);
    let spans_ok_n = spans_ok.load(Ordering::Relaxed);
    let flush = match (&params.metrics, before) {
        (Some(addr), Some(before)) => Some(flush_report(&before, &scrape(addr).await?)),
        _ => None,
    };
    Ok(LoadReport {
        flush,
        requests_ok,
        requests_err: err.load(Ordering::Relaxed),
        spans_ok: spans_ok_n,
        seconds: seconds.as_secs_f64(),
        rps: per_sec(usize::try_from(requests_ok).unwrap_or(0), seconds),
        spans_per_sec: per_sec(usize::try_from(spans_ok_n).unwrap_or(0), seconds),
        p50_ms: samples.percentile(50.0).as_secs_f64() * 1e3,
        p95_ms: samples.percentile(95.0).as_secs_f64() * 1e3,
        p99_ms: samples.percentile(99.0).as_secs_f64() * 1e3,
        errors: errors.lock().await.iter().cloned().collect(),
    })
}

/// Prints the report.
pub fn print(report: &LoadReport) {
    println!(
        "requests ok={} err={} in {:.1}s → {:.0} req/s, {:.0} spans/s",
        report.requests_ok, report.requests_err, report.seconds, report.rps, report.spans_per_sec
    );
    println!(
        "latency p50={:.2}ms p95={:.2}ms p99={:.2}ms",
        report.p50_ms, report.p95_ms, report.p99_ms
    );
    for e in &report.errors {
        println!("error: {e}");
    }
    if let Some(f) = &report.flush {
        println!(
            "flushes={} rows/flush={:.0} parquet/flush={:.1} MiB | per flush: concat={:.0}ms sort={:.0}ms encode={:.0}ms upload={:.0}ms register={:.0}ms total={:.0}ms | buffers at end: live={} MiB flushing={} MiB persisted={} MiB",
            f.flushes,
            f.rows_per_flush,
            f.parquet_mib_per_flush,
            f.concat_ms,
            f.sort_ms,
            f.encode_ms,
            f.upload_ms,
            f.register_ms,
            f.total_ms,
            f.buffer_bytes.0 >> 20,
            f.buffer_bytes.1 >> 20,
            f.buffer_bytes.2 >> 20
        );
    }
}

/// Fetches the Prometheus page as `name → value`.
async fn scrape(
    addr: &str,
) -> Result<std::collections::HashMap<String, f64>, Box<dyn std::error::Error + Send + Sync>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: engine\r\nConnection: close\r\n\r\n")
        .await?;
    let mut body = String::new();
    stream.read_to_string(&mut body).await?;
    let body = body
        .split_once("\r\n\r\n")
        .map_or(body.as_str(), |(_, b)| b);
    Ok(body
        .lines()
        .filter(|l| !l.starts_with('#'))
        .filter_map(|l| l.split_once(' '))
        .filter_map(|(k, v)| v.trim().parse::<f64>().ok().map(|v| (k.to_owned(), v)))
        .collect())
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn flush_report(
    before: &std::collections::HashMap<String, f64>,
    after: &std::collections::HashMap<String, f64>,
) -> FlushReport {
    let delta =
        |k: &str| after.get(k).copied().unwrap_or(0.0) - before.get(k).copied().unwrap_or(0.0);
    let flushes = delta("fusescope_flush_total").max(0.0);
    let per = |k: &str| {
        if flushes > 0.0 {
            delta(k) / flushes
        } else {
            0.0
        }
    };
    let ms = |k: &str| per(k) / 1e6;
    let gauge = |k: &str| after.get(k).copied().unwrap_or(0.0) as u64;
    FlushReport {
        flushes: flushes as u64,
        rows_per_flush: per("fusescope_flush_rows_total"),
        parquet_mib_per_flush: per("fusescope_flush_parquet_bytes_total") / (1024.0 * 1024.0),
        concat_ms: ms("fusescope_flush_concat_nanoseconds_total"),
        sort_ms: ms("fusescope_flush_sort_nanoseconds_total"),
        encode_ms: ms("fusescope_flush_encode_nanoseconds_total"),
        upload_ms: ms("fusescope_flush_upload_nanoseconds_total"),
        register_ms: ms("fusescope_flush_register_nanoseconds_total"),
        total_ms: ms("fusescope_flush_nanoseconds_total"),
        buffer_bytes: (
            gauge("fusescope_buffer_live_bytes"),
            gauge("fusescope_buffer_flushing_bytes"),
            gauge("fusescope_buffer_persisted_bytes"),
        ),
    }
}
