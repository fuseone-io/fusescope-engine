//! Logs end to end: OTLP logs over gRPC → WAL → buffer → Parquet in a local
//! bucket → catalog, with a restart at every durable step.
//!
//! Needs `FS_TEST_DATABASE_URL` (`make dev-up`); skips otherwise. The bucket is
//! a temporary directory.
#![allow(
    clippy::too_many_lines,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use fs_catalog::{Catalog, FileRecord, NewTenant, Signal, TenantId, TokenScope};
use fs_ingest::{IngestConfig, Ingester};
use fs_wal::{Wal, WalConfig};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};
use sqlx::{Connection, Executor, PgConnection};
use tonic::Request;
use tonic::metadata::MetadataValue;

const T0: u64 = 1_792_678_623_000_000_000; // 2026-10-24T14:17:03Z

struct TestDb {
    admin_url: String,
    schema: String,
    catalog: Catalog,
}

impl TestDb {
    async fn new() -> Option<Self> {
        let Ok(admin_url) = std::env::var("FS_TEST_DATABASE_URL") else {
            eprintln!("skipped: FS_TEST_DATABASE_URL is not set (run `make dev-up`)");
            return None;
        };
        let schema = format!("logs_{}", uuid::Uuid::new_v4().simple());
        let mut admin = PgConnection::connect(&admin_url).await.unwrap();
        admin
            .execute(format!("create schema {schema}").as_str())
            .await
            .unwrap();
        admin.close().await.unwrap();
        let sep = if admin_url.contains('?') { '&' } else { '?' };
        let url = format!("{admin_url}{sep}options=-c%20search_path%3D{schema}");
        let catalog = Catalog::connect(&url, 4).await.unwrap();
        catalog.migrate().await.unwrap();
        Some(Self {
            admin_url,
            schema,
            catalog,
        })
    }

    async fn tenant(&self) -> (TenantId, String) {
        let tenant = self
            .catalog
            .create_tenant(&NewTenant::with_defaults("acme"))
            .await
            .unwrap()
            .id;
        let (token, _) = self
            .catalog
            .create_token(tenant, "collector", TokenScope::Ingest)
            .await
            .unwrap();
        (tenant, token)
    }

    async fn files(&self, tenant: TenantId, signal: Signal) -> Vec<FileRecord> {
        self.catalog
            .all_visible_files(tenant, signal)
            .await
            .unwrap()
    }

    async fn finish(self) {
        self.catalog.pool().close().await;
        let mut admin = PgConnection::connect(&self.admin_url).await.unwrap();
        admin
            .execute(format!("drop schema {} cascade", self.schema).as_str())
            .await
            .unwrap();
        admin.close().await.unwrap();
    }
}

struct Harness {
    dir: tempfile::TempDir,
    ingester: Arc<Ingester>,
    addr: std::net::SocketAddr,
    shutdown: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<(), fs_ingest::IngestError>>,
}

async fn start(catalog: &Catalog, dir: tempfile::TempDir, config: IngestConfig) -> Harness {
    std::fs::create_dir_all(dir.path().join("bucket")).unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path().join("bucket")).unwrap());
    let wal = Wal::open(WalConfig::new(dir.path().join("wal"))).unwrap();
    let ingester =
        Arc::new(Ingester::new(config, catalog.clone(), store, Path::from(""), wal).unwrap());
    let (shutdown, rx) = tokio::sync::watch::channel(false);
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn({
        let ingester = ingester.clone();
        async move { ingester.run(rx, Some(bound_tx), None).await }
    });
    let addr = tokio::time::timeout(Duration::from_secs(5), bound_rx)
        .await
        .unwrap()
        .unwrap();
    Harness {
        dir,
        ingester,
        addr,
        shutdown,
        task,
    }
}

impl Harness {
    async fn export_logs(&self, request: ExportLogsServiceRequest, token: &str) -> i64 {
        let mut client = LogsServiceClient::connect(format!("http://{}", self.addr))
            .await
            .unwrap();
        let response = client.export(authed(request, token)).await.unwrap();
        response
            .into_inner()
            .partial_success
            .map_or(0, |p| p.rejected_log_records)
    }

    async fn export_spans(&self, request: ExportTraceServiceRequest, token: &str) {
        let mut client = TraceServiceClient::connect(format!("http://{}", self.addr))
            .await
            .unwrap();
        client.export(authed(request, token)).await.unwrap();
    }

    /// Ends the run as a crash would: no flush, no confirmation, no sync.
    async fn crash(self) -> tempfile::TempDir {
        self.task.abort();
        let _ = self.task.await;
        self.dir
    }

    async fn stop(self) {
        self.shutdown.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    fn bucket(&self) -> std::path::PathBuf {
        self.dir.path().join("bucket")
    }
}

fn authed<T>(message: T, token: &str) -> Request<T> {
    let mut r = Request::new(message);
    r.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
    );
    r
}

fn test_config() -> IngestConfig {
    IngestConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        hot_listen: None,
        flush_interval: Duration::from_secs(3600),
        persisted_retain: Duration::from_secs(2),
        ..IngestConfig::default()
    }
}

fn service(name: &str) -> Resource {
    Resource {
        attributes: vec![KeyValue {
            key: "service.name".into(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(name.into())),
            }),
            ..KeyValue::default()
        }],
        ..Resource::default()
    }
}

/// `count` records of `name` a millisecond apart from `T0 + offset_ms`, every
/// `reject_every`th without a time.
fn logs(name: &str, count: u64, offset_ms: u64, reject_every: u64) -> ExportLogsServiceRequest {
    let log_records = (0..count)
        .map(|i| {
            let time = if reject_every > 0 && i % reject_every == reject_every - 1 {
                0
            } else {
                T0 + (offset_ms + i) * 1_000_000
            };
            LogRecord {
                time_unix_nano: time,
                severity_text: "INFO".into(),
                severity_number: 9,
                body: Some(AnyValue {
                    value: Some(Value::StringValue(format!("{name} line {i}"))),
                }),
                trace_id: vec![u8::try_from(i % 200).unwrap() + 1; 16],
                ..LogRecord::default()
            }
        })
        .collect();
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(service(name)),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn spans(name: &str, count: u8) -> ExportTraceServiceRequest {
    let spans = (0..count)
        .map(|i| Span {
            trace_id: vec![9; 16],
            span_id: vec![i + 1; 8],
            name: "op".into(),
            start_time_unix_nano: T0 + u64::from(i),
            end_time_unix_nano: T0 + u64::from(i) + 1_000,
            ..Span::default()
        })
        .collect();
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(service(name)),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn files_under(dir: &std::path::Path, part: &str, extension: &str) -> Vec<std::path::PathBuf> {
    walkdir(dir)
        .into_iter()
        .filter(|p| p.extension().is_some_and(|x| x == extension))
        .filter(|p| p.to_string_lossy().contains(part))
        .collect()
}

fn walkdir(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

fn sha256_of(path: &std::path::Path) -> [u8; 32] {
    Sha256::digest(std::fs::read(path).unwrap()).into()
}

fn rows_in(path: &std::path::Path) -> i64 {
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap()).unwrap();
    reader.metadata().file_metadata().num_rows()
}

/// Spans and logs of one tenant land in files of their own, each registered
/// under its signal; the log file has no trace sidecar and bloom filters on
/// its two key columns, and a record without a time is refused in the
/// response.
#[tokio::test]
async fn logs_are_flushed_to_log_files_beside_the_spans() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    h.export_spans(spans("api", 3), &token).await;
    assert_eq!(h.export_logs(logs("api", 6, 0, 3), &token).await, 2);
    h.ingester.flush_all().await;

    let log_files = files_under(&h.bucket(), &format!("t/{}/logs/l0/", tenant.0), "parquet");
    let span_files = files_under(
        &h.bucket(),
        &format!("t/{}/traces/l0/", tenant.0),
        "parquet",
    );
    assert_eq!((log_files.len(), span_files.len()), (1, 1));
    assert_eq!(rows_in(&log_files[0]), 4);
    assert_eq!(files_under(&h.bucket(), "", "tix").len(), 1, "spans only");

    let reader =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&log_files[0]).unwrap())
            .unwrap();
    assert_eq!(
        reader.schema().metadata()["fusescope.signal"],
        "logs",
        "the file holds log rows"
    );
    let blooms: Vec<String> = reader
        .metadata()
        .row_group(0)
        .columns()
        .iter()
        .filter(|c| c.bloom_filter_offset().is_some())
        .map(|c| c.column_path().string())
        .collect();
    assert_eq!(blooms, ["trace_id", "service_name"]);

    let registered = db.files(tenant, Signal::Logs).await;
    assert_eq!(registered.len(), 1);
    assert_eq!(registered[0].row_count, 4);
    assert_eq!(registered[0].services, ["api"]);
    assert_eq!(registered[0].content_sha256, Some(sha256_of(&log_files[0])));
    assert_eq!(db.files(tenant, Signal::Traces).await.len(), 1);
    let stats = h.ingester.stats();
    assert_eq!(stats.logs.load(Ordering::Relaxed), 4);
    assert_eq!(stats.logs_rejected.load(Ordering::Relaxed), 2);
    h.stop().await;
    db.finish().await;
}

/// A crash before any intent: the WAL brings the log rows back and they
/// are flushed once.
#[tokio::test]
async fn a_crash_before_the_intent_replays_the_log_rows() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    h.export_logs(logs("api", 20, 0, 0), &token).await;
    let dir = h.crash().await;
    assert_eq!(walkdir(&dir.path().join("bucket")).len(), 0);

    let h = start(&db.catalog, dir, test_config()).await;
    h.ingester.flush_all().await;
    let registered = db.files(tenant, Signal::Logs).await;
    assert_eq!(
        registered.iter().map(|f| f.row_count).collect::<Vec<_>>(),
        [20]
    );
    h.stop().await;
    db.finish().await;
}

/// The object is uploaded and its registration fails, then the process
/// crashes: the replay converts the records again, ids included, into the
/// same bytes, so the object in the bucket is registered as it is.
#[tokio::test]
async fn a_crash_between_upload_and_registration_reproduces_the_object() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let fails_once = IngestConfig {
        inject_register_failures: 1,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), fails_once).await;
    h.export_logs(logs("api", 5, 0, 0), &token).await;
    h.export_logs(logs("web", 7, 3, 0), &token).await;
    h.ingester.flush_all().await;
    let written = files_under(&h.bucket(), "/logs/l0/", "parquet");
    assert_eq!(written.len(), 1, "uploaded before the registration failed");
    let before = sha256_of(&written[0]);
    assert_eq!(db.files(tenant, Signal::Logs).await.len(), 0);
    let dir = h.crash().await;

    let h = start(&db.catalog, dir, test_config()).await;
    h.ingester.flush_all().await;
    let now = files_under(&h.bucket(), "/logs/l0/", "parquet");
    assert_eq!(now, written, "the same path");
    assert_eq!(sha256_of(&now[0]), before, "the same bytes");
    let flush = h.ingester.flush_stats();
    assert_eq!(flush.replay_mismatch.load(Ordering::Relaxed), 0);
    assert_eq!(flush.superseded.load(Ordering::Relaxed), 0);
    let registered = db.files(tenant, Signal::Logs).await;
    assert_eq!(registered.len(), 1);
    assert_eq!(registered[0].row_count, 12);
    assert_eq!(registered[0].content_sha256, Some(before));
    h.stop().await;
    db.finish().await;
}

/// Logs, spans and logs again in one tenant's log: each signal's intent
/// covers the tenant's records of that signal in its range and no other.
/// The uploads fail and the process crashes after both intents; the
/// replay writes one file per signal with exactly its rows.
#[tokio::test]
async fn a_crash_after_the_intents_of_both_signals_resumes_each_with_its_rows() {
    use std::os::unix::fs::PermissionsExt;

    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    h.export_logs(logs("api", 4, 0, 0), &token).await;
    h.export_spans(spans("api", 3), &token).await;
    h.export_logs(logs("api", 5, 10, 0), &token).await;
    let bucket = h.bucket();
    std::fs::set_permissions(&bucket, std::fs::Permissions::from_mode(0o555)).unwrap();
    h.ingester.flush_all().await;
    assert!(h.ingester.stats().flush_failures.load(Ordering::Relaxed) >= 2);
    std::fs::set_permissions(&bucket, std::fs::Permissions::from_mode(0o755)).unwrap();
    let dir = h.crash().await;

    let h = start(&db.catalog, dir, test_config()).await;
    assert_eq!(
        h.ingester.stats().flushes_recovered.load(Ordering::Relaxed),
        2,
        "one intent per signal"
    );
    h.ingester.flush_all().await;
    let rows = |files: Vec<FileRecord>| files.iter().map(|f| f.row_count).collect::<Vec<_>>();
    assert_eq!(rows(db.files(tenant, Signal::Logs).await), [9]);
    assert_eq!(rows(db.files(tenant, Signal::Traces).await), [3]);
    assert_eq!(files_under(&h.bucket(), "", "parquet").len(), 2);
    h.stop().await;
    db.finish().await;
}

/// Registered, never confirmed, then a crash: the checkpoint stayed behind
/// the intent, so the replay redoes the flush and confirms it, with no
/// second file.
#[tokio::test]
async fn a_crash_before_the_confirmation_is_replayed_and_confirmed() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let never_confirms = IngestConfig {
        inject_confirm_failures: u32::MAX,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), never_confirms).await;
    h.export_logs(logs("api", 8, 0, 0), &token).await;
    h.ingester.flush_all().await;
    let confirmed = || async {
        sqlx::query_scalar::<_, bool>(
            "select confirmed_at is not null from engine_file where signal = $1",
        )
        .bind(Signal::Logs.code())
        .fetch_all(db.catalog.pool())
        .await
        .unwrap()
    };
    assert_eq!(confirmed().await, [false]);
    let dir = h.crash().await;

    let h = start(&db.catalog, dir, test_config()).await;
    assert!(h.ingester.stats().flushes_recovered.load(Ordering::Relaxed) >= 1);
    h.ingester.flush_all().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while confirmed().await != [true] {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the replay never confirmed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(files_under(&h.bucket(), "/logs/l0/", "parquet").len(), 1);
    let registered = db.files(tenant, Signal::Logs).await;
    assert_eq!(
        registered.iter().map(|f| f.row_count).collect::<Vec<_>>(),
        [8]
    );
    h.stop().await;
    db.finish().await;
}

/// Exports with no row to keep, spans or logs, empty or wholly refused,
/// are answered without reaching the WAL: no record, no object, no row.
#[tokio::test]
async fn an_export_without_rows_to_keep_is_answered_without_the_wal() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    h.export_spans(ExportTraceServiceRequest::default(), &token)
        .await;
    assert_eq!(
        h.export_logs(ExportLogsServiceRequest::default(), &token)
            .await,
        0
    );
    assert_eq!(h.export_logs(logs("api", 3, 0, 1), &token).await, 3);
    h.ingester.flush_all().await;

    assert_eq!(h.ingester.writer_stats().records.load(Ordering::Relaxed), 0);
    assert_eq!(walkdir(&h.bucket()).len(), 0);
    assert_eq!(db.files(tenant, Signal::Logs).await.len(), 0);
    assert_eq!(db.files(tenant, Signal::Traces).await.len(), 0);
    let stats = h.ingester.stats();
    assert_eq!(stats.requests.load(Ordering::Relaxed), 3);
    assert_eq!(stats.logs.load(Ordering::Relaxed), 0);
    assert_eq!(stats.logs_rejected.load(Ordering::Relaxed), 3);
    assert_eq!(stats.spans.load(Ordering::Relaxed), 0);
    h.stop().await;
    db.finish().await;
}
