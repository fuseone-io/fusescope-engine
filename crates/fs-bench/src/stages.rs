//! Ingest decomposed into its stages, in process, no network.
//!
//! Each stage is timed over the same generated traffic so the costs can be
//! compared: protobuf encode, WAL append and sync, OTLP to Arrow conversion,
//! sort, Parquet encode, upload, hot-window filter. The stages run one after
//! another on the whole corpus, which measures pure CPU and I/O cost per
//! stage; the `load` mode measures the pipeline under concurrency instead.

use std::sync::Arc;
use std::time::Duration;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use fs_catalog::{FileId, TenantId};
use fs_ingest::hot::{HotScanRequest, apply};
use fs_ingest::record::{RecordSignal, WalRecord};
use fs_schema::limits::Limits;
use fs_schema::spans::{SpanBatchBuilder, span_schema_v1};
use fs_wal::{SyncPolicy, Wal, WalConfig};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use serde::Serialize;

use crate::stats::{fmt, mib_per_sec, per_sec, timed};
use crate::traffic::{Generator, Shape};

/// Parameters of a stage run.
#[derive(Debug, Clone)]
pub struct StageParams {
    /// Requests to generate (one trace each).
    pub requests: usize,
    /// Traffic shape.
    pub shape: Shape,
    /// WAL sync policy under test.
    pub sync: SyncPolicy,
    /// Where the WAL lives.
    pub wal_dir: std::path::PathBuf,
    /// Store to upload the Parquet file to.
    pub store: Arc<dyn ObjectStore>,
    /// Prefix inside the store.
    pub prefix: String,
    /// Seed.
    pub seed: u64,
}

/// One measured stage.
#[derive(Debug, Clone, Serialize)]
pub struct StageResult {
    /// Stage name.
    pub stage: &'static str,
    /// Wall time for the whole corpus.
    pub total_ms: f64,
    /// Nanoseconds per span.
    pub ns_per_span: f64,
    /// Bytes processed, for throughput.
    pub bytes: usize,
    /// MiB per second over `bytes`.
    pub mib_per_sec: f64,
}

/// Everything a run produced.
#[derive(Debug, Clone, Serialize)]
pub struct StagesReport {
    /// Requests generated.
    pub requests: usize,
    /// Spans generated.
    pub spans: usize,
    /// Protobuf bytes of the corpus.
    pub protobuf_bytes: usize,
    /// Arrow memory of the converted corpus.
    pub arrow_bytes: usize,
    /// Parquet bytes of the encoded corpus.
    pub parquet_bytes: usize,
    /// Per stage.
    pub stages: Vec<StageResult>,
}

#[allow(clippy::cast_precision_loss)]
fn stage(name: &'static str, spans: usize, bytes: usize, d: Duration) -> StageResult {
    StageResult {
        stage: name,
        total_ms: d.as_secs_f64() * 1e3,
        ns_per_span: if spans == 0 {
            0.0
        } else {
            d.as_nanos() as f64 / spans as f64
        },
        bytes,
        mib_per_sec: mib_per_sec(bytes, d),
    }
}

/// Runs every stage and returns the report.
///
/// # Errors
/// Any stage failure.
#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
pub async fn run(
    params: StageParams,
) -> Result<StagesReport, Box<dyn std::error::Error + Send + Sync>> {
    let tenant = TenantId::new();
    let now_ns = u64::try_from(chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)).unwrap_or(0);
    let mut generator = Generator::new(params.shape.clone(), params.seed, now_ns);
    let requests: Vec<_> = (0..params.requests).map(|_| generator.request()).collect();
    let spans: usize = requests
        .iter()
        .map(|r| {
            r.resource_spans
                .iter()
                .flat_map(|rs| rs.scope_spans.iter())
                .map(|ss| ss.spans.len())
                .sum::<usize>()
        })
        .sum();
    let mut stages = Vec::new();

    // 1. protobuf encode (what the gRPC layer hands us, re-encoded for the WAL record)
    let (encoded, d) = timed(|| {
        requests
            .iter()
            .map(prost::Message::encode_to_vec)
            .collect::<Vec<_>>()
    });
    let protobuf_bytes: usize = encoded.iter().map(Vec::len).sum();
    stages.push(stage("protobuf encode", spans, protobuf_bytes, d));

    // 2. WAL record framing + append + sync per request
    let wal_config = WalConfig {
        sync: params.sync,
        ..WalConfig::new(&params.wal_dir)
    };
    let mut wal = Wal::open(wal_config)?;
    let start = std::time::Instant::now();
    for payload in &encoded {
        let record = WalRecord {
            version: fs_ingest::record::VERSION,
            tenant,
            signal: RecordSignal::Traces,
            payload: bytes::Bytes::copy_from_slice(payload),
        };
        wal.append(&record.encode())?;
        if params.sync == SyncPolicy::Batch {
            wal.sync()?;
        }
    }
    let d = start.elapsed();
    let label = if params.sync == SyncPolicy::Always {
        "wal append+sync (every append)"
    } else {
        "wal append+sync (per request)"
    };
    stages.push(stage(label, spans, protobuf_bytes, d));

    // 2b. WAL through the group-commit writer, 64 concurrent appenders, same records
    let gc_dir = params.wal_dir.join("group-commit");
    let gc_config = WalConfig {
        sync: params.sync,
        ..WalConfig::new(&gc_dir)
    };
    let gc_wal = Arc::new(std::sync::Mutex::new(Wal::open(gc_config)?));
    let writer = fs_ingest::walwriter::WalWriter::spawn(
        Arc::clone(&gc_wal),
        fs_ingest::walwriter::GroupCommitConfig::default(),
    );
    let records: Vec<Vec<u8>> = encoded
        .iter()
        .map(|payload| {
            WalRecord {
                version: fs_ingest::record::VERSION,
                tenant,
                signal: RecordSignal::Traces,
                payload: bytes::Bytes::copy_from_slice(payload),
            }
            .encode()
        })
        .collect();
    let start = std::time::Instant::now();
    let chunk = records.len().div_ceil(64).max(1);
    let mut tasks = Vec::new();
    for slice in records.chunks(chunk) {
        let writer = writer.clone();
        let slice = slice.to_vec();
        tasks.push(tokio::spawn(async move {
            for record in slice {
                writer.append(tenant, record).await?;
            }
            Ok::<_, fs_ingest::IngestError>(())
        }));
    }
    for t in tasks {
        t.await??;
    }
    let d = start.elapsed();
    let writer_stats = writer.stats();
    let batches = writer_stats
        .batches
        .load(std::sync::atomic::Ordering::Relaxed)
        .max(1);
    stages.push(stage(
        "wal group commit (64 senders)",
        spans,
        protobuf_bytes,
        d,
    ));
    eprintln!(
        "group commit: {} records in {} batches, {:.1} records per fsync",
        writer_stats
            .records
            .load(std::sync::atomic::Ordering::Relaxed),
        batches,
        writer_stats
            .records
            .load(std::sync::atomic::Ordering::Relaxed) as f64
            / batches as f64
    );
    drop(writer);

    // 3. OTLP → Arrow
    let (batches, d) = timed(|| {
        requests
            .iter()
            .map(|r| {
                let mut b = SpanBatchBuilder::new(Limits::default());
                b.append_request(r)?;
                b.finish()
            })
            .collect::<Result<Vec<RecordBatch>, _>>()
    });
    let batches = batches?;
    let arrow_bytes: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
    stages.push(stage("otlp -> arrow", spans, arrow_bytes, d));

    // 4. concat + sort
    let (sorted, d) = timed(|| {
        let merged = concat_batches(&span_schema_v1(), &batches)?;
        fs_ingest::flush::sort_spans(&merged)
    });
    let sorted = sorted?;
    stages.push(stage("concat + sort", spans, arrow_bytes, d));

    // 5. parquet encode
    let (parquet, d) = timed(|| fs_ingest::flush::encode_parquet(&sorted));
    let parquet = parquet?;
    stages.push(stage("parquet encode", spans, parquet.len(), d));

    // 6. upload
    let path = Path::from(format!(
        "{}bench/{}.parquet",
        params.prefix,
        FileId::new().0
    ));
    let start = std::time::Instant::now();
    params
        .store
        .put(&path, object_store::PutPayload::from_bytes(parquet.clone()))
        .await?;
    let d = start.elapsed();
    stages.push(stage("upload", spans, parquet.len(), d));
    params.store.delete(&path).await?;

    // 7. hot-window filter: a recent-window scan with service filter and projection over the live batches
    let request = HotScanRequest {
        tenant,
        ts_min: Some(i64::try_from(now_ns.saturating_sub(300_000_000_000)).unwrap_or(0)),
        ts_max: None,
        services: Some(vec!["svc-00".into(), "svc-01".into()]),
        columns: Some(vec![
            "timestamp".into(),
            "service_name".into(),
            "duration_nano".into(),
            "has_error".into(),
        ]),
        trace_ids: None,
        span_ids: None,
        distinct: None,
    };
    let (filtered, d) = timed(|| {
        batches
            .iter()
            .map(|b| apply(b, &request))
            .collect::<Result<Vec<_>, _>>()
    });
    let filtered = filtered?;
    let kept: usize = filtered.iter().map(RecordBatch::num_rows).sum();
    stages.push(stage("hot-window filter+project", spans, arrow_bytes, d));
    stages.push(stage(
        "hot-window rows kept",
        kept.max(1),
        0,
        Duration::ZERO,
    ));

    Ok(StagesReport {
        requests: params.requests,
        spans,
        protobuf_bytes,
        arrow_bytes,
        parquet_bytes: parquet.len(),
        stages,
    })
}

/// Prints the report as a markdown table.
#[allow(clippy::cast_precision_loss)]
pub fn print(report: &StagesReport) {
    println!(
        "requests={} spans={} protobuf={} MiB arrow={} MiB parquet={} MiB (ratio arrow/parquet {:.1}x)",
        report.requests,
        report.spans,
        report.protobuf_bytes / (1024 * 1024),
        report.arrow_bytes / (1024 * 1024),
        report.parquet_bytes / (1024 * 1024),
        if report.parquet_bytes == 0 {
            0.0
        } else {
            report.arrow_bytes as f64 / report.parquet_bytes as f64
        }
    );
    println!();
    println!("| stage | total | ns/span | MiB/s | spans/s |");
    println!("|---|---:|---:|---:|---:|");
    for s in &report.stages {
        let d = Duration::from_secs_f64(s.total_ms / 1e3);
        println!(
            "| {} | {} | {:.0} | {:.1} | {:.0} |",
            s.stage,
            fmt(d),
            s.ns_per_span,
            s.mib_per_sec,
            per_sec(report.spans, d)
        );
    }
}
