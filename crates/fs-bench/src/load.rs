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
}

/// Runs the load and returns the report.
///
/// # Errors
/// Connection failure.
pub async fn run(
    params: LoadParams,
) -> Result<LoadReport, Box<dyn std::error::Error + Send + Sync>> {
    let channel = Channel::from_shared(format!("http://{}", params.endpoint))?
        .connect()
        .await?;
    let ok = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let spans_ok = Arc::new(AtomicU64::new(0));
    let samples = Arc::new(Mutex::new(Samples::default()));
    let errors = Arc::new(Mutex::new(std::collections::BTreeSet::new()));
    let deadline = Instant::now() + params.duration;
    let header: MetadataValue<_> = format!("Bearer {}", params.token).parse()?;

    let mut tasks = Vec::new();
    for worker in 0..params.concurrency {
        let mut client = TraceServiceClient::new(channel.clone());
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
    let start = Instant::now();
    for t in tasks {
        let _ = t.await;
    }
    let seconds = start.elapsed();
    let samples = samples.lock().await.clone();
    let requests_ok = ok.load(Ordering::Relaxed);
    let spans_ok_n = spans_ok.load(Ordering::Relaxed);
    Ok(LoadReport {
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
}
