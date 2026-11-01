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

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use fs_catalog::{Catalog, FileRecord, NewTenant, Signal, TenantId, TokenScope};
use fs_index::TextIndex;
use fs_index::text_index::Probe;
use fs_ingest::{IngestConfig, Ingester};
use fs_wal::{Wal, WalConfig};
use object_store::ObjectStore;
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
    hot_addr: std::net::SocketAddr,
    shutdown: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<(), fs_ingest::IngestError>>,
}

async fn start(catalog: &Catalog, dir: tempfile::TempDir, config: IngestConfig) -> Harness {
    std::fs::create_dir_all(dir.path().join("bucket")).unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path().join("bucket")).unwrap());
    start_on(catalog, dir, config, store).await
}

async fn start_on(
    catalog: &Catalog,
    dir: tempfile::TempDir,
    config: IngestConfig,
    store: Arc<dyn ObjectStore>,
) -> Harness {
    let wal = Wal::open(WalConfig::new(dir.path().join("wal"))).unwrap();
    let ingester =
        Arc::new(Ingester::new(config, catalog.clone(), store, Path::from(""), wal).unwrap());
    let (shutdown, rx) = tokio::sync::watch::channel(false);
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let (hot_tx, hot_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn({
        let ingester = ingester.clone();
        async move { ingester.run(rx, Some(bound_tx), Some(hot_tx)).await }
    });
    let addr = tokio::time::timeout(Duration::from_secs(5), bound_rx)
        .await
        .unwrap()
        .unwrap();
    let hot_addr = tokio::time::timeout(Duration::from_secs(5), hot_rx)
        .await
        .unwrap()
        .unwrap();
    Harness {
        dir,
        ingester,
        addr,
        hot_addr,
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
        hot_listen: Some("127.0.0.1:0".parse().unwrap()),
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

/// The hot window answers a scan of logs with log rows in the log schema,
/// its projection, filters and distinct values checked against that
/// schema; a scan that names no signal still gets span rows only.
#[tokio::test]
async fn the_hot_window_serves_log_rows_by_their_schema() {
    use arrow::array::AsArray;
    use arrow::record_batch::RecordBatch;
    use fs_ingest::hot::{HotScanRequest, scan};
    use fs_ingest::record::RowSignal;

    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    h.export_spans(spans("api", 3), &token).await;
    h.export_logs(logs("api", 6, 0, 0), &token).await;
    h.export_logs(logs("web", 2, 10, 0), &token).await;
    let addr = h.hot_addr.to_string();
    let rows = |batches: &[RecordBatch]| batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    let wait = Duration::from_secs(5);

    let spans_only = HotScanRequest {
        tenant,
        ts_min: None,
        ts_max: None,
        services: None,
        columns: None,
        trace_ids: None,
        span_ids: None,
        distinct: None,
        signal: RowSignal::Traces,
    };
    let got = scan(&addr, &spans_only, wait).await.unwrap();
    assert_eq!(rows(&got.batches), 3);
    assert_eq!(
        got.batches[0].schema().metadata()["fusescope.signal"],
        "spans"
    );

    let api_bodies = HotScanRequest {
        signal: RowSignal::Logs,
        services: Some(vec!["api".into()]),
        columns: Some(vec![
            "timestamp".into(),
            "body".into(),
            "severity_text".into(),
        ]),
        trace_ids: Some(vec![vec![2; 16], vec![5; 16]]),
        ..spans_only.clone()
    };
    let got = scan(&addr, &api_bodies, wait).await.unwrap();
    assert_eq!(rows(&got.batches), 2, "lines 1 and 4 of api");
    assert_eq!(got.batches[0].num_columns(), 3);
    assert_eq!(
        got.batches[0].schema().metadata()["fusescope.signal"],
        "logs"
    );
    let bodies: Vec<&str> = got.batches[0]
        .column(1)
        .as_string::<i32>()
        .iter()
        .flatten()
        .collect();
    assert_eq!(bodies, ["api line 1", "api line 4"]);
    assert_eq!(
        got.persisted_files.len(),
        1,
        "the id the live log rows will get"
    );

    let levels = HotScanRequest {
        signal: RowSignal::Logs,
        columns: Some(vec!["severity_text".into()]),
        distinct: Some(10),
        ..spans_only.clone()
    };
    let got = scan(&addr, &levels, wait).await.unwrap();
    assert_eq!(got.batches.len(), 1);
    // Flight hydrates dictionaries in transit; the querier casts them back.
    let column =
        arrow::compute::cast(got.batches[0].column(0), &arrow::datatypes::DataType::Utf8).unwrap();
    let values: Vec<&str> = column.as_string::<i32>().iter().flatten().collect();
    assert_eq!(values, ["INFO"], "once, whatever the rows");
    let span_distinct = HotScanRequest {
        columns: Some(vec!["kind_string".into()]),
        ..levels.clone()
    };
    assert!(
        scan(&addr, &span_distinct, wait).await.is_err(),
        "a span column"
    );

    let span_column = HotScanRequest {
        signal: RowSignal::Logs,
        columns: Some(vec!["name".into()]),
        ..spans_only.clone()
    };
    assert!(
        scan(&addr, &span_column, wait).await.is_err(),
        "logs have no name"
    );
    h.stop().await;
    db.finish().await;
}

fn relative(bucket: &std::path::Path, path: &std::path::Path) -> String {
    path.strip_prefix(bucket)
        .unwrap()
        .to_string_lossy()
        .into_owned()
}

/// The text sidecar of the log file at `parquet`, decoded.
fn text_index_of(bucket: &std::path::Path, parquet: &std::path::Path) -> TextIndex {
    let sidecar = fs_index::text_index::sidecar_path(&relative(bucket, parquet)).unwrap();
    TextIndex::decode(std::fs::read(bucket.join(sidecar)).unwrap()).unwrap()
}

fn row_group_rows(path: &std::path::Path) -> Vec<u64> {
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap()).unwrap();
    reader
        .metadata()
        .row_groups()
        .iter()
        .map(|g| u64::try_from(g.num_rows()).unwrap())
        .collect()
}

fn small_groups() -> IngestConfig {
    IngestConfig {
        row_group_rows: 4,
        ..test_config()
    }
}

/// A log file gets a text sidecar bound to its path, its bytes and its row
/// groups, whose blooms find a body's token in its row group only.
#[tokio::test]
async fn a_log_file_gets_a_text_sidecar_bound_to_its_bytes() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), small_groups()).await;
    h.export_logs(logs("api", 10, 0, 0), &token).await;
    h.ingester.flush_all().await;

    let bucket = h.bucket();
    let log_files = files_under(&bucket, "/logs/l0/", "parquet");
    assert_eq!(log_files.len(), 1);
    let sidecars = files_under(&bucket, "/logs/index/ngram/", "ngb");
    assert_eq!(sidecars.len(), 1);
    let index = text_index_of(&bucket, &log_files[0]);
    assert_eq!(index.path(), relative(&bucket, &log_files[0]));
    let registered = db.files(tenant, Signal::Logs).await;
    assert_eq!(Some(*index.content_sha256()), registered[0].content_sha256);
    assert_eq!(
        index.rows().collect::<Vec<_>>(),
        row_group_rows(&log_files[0])
    );
    assert_eq!(index.rows().collect::<Vec<_>>(), [4, 4, 2]);
    // Rows are sorted by time: "api line 6" is the third of row group 1.
    assert_eq!(index.select(&Probe::token(b"6", false)), [1]);
    assert_eq!(index.select(&Probe::fragment(b"API LINE", true)), [0, 1, 2]);
    assert_eq!(files_under(&bucket, "", "tix").len(), 0, "no trace sidecar");
    let flush = h.ingester.flush_stats();
    assert_eq!(
        flush.text_index_bytes.load(Ordering::Relaxed),
        std::fs::metadata(&sidecars[0]).unwrap().len()
    );
    assert_eq!(flush.trace_index_bytes.load(Ordering::Relaxed), 0);
    h.stop().await;
    db.finish().await;
}

/// The sidecar upload fails after the Parquet object, then the process
/// crashes: the replay finds the object, writes the sidecar and registers
/// the file once.
#[tokio::test]
async fn a_crash_before_the_text_sidecar_writes_it_on_replay() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let fails = IngestConfig {
        inject_index_failures: 1,
        ..small_groups()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), fails).await;
    h.export_logs(logs("api", 6, 0, 0), &token).await;
    h.ingester.flush_all().await;
    let bucket = h.bucket();
    assert_eq!(files_under(&bucket, "/logs/l0/", "parquet").len(), 1);
    assert_eq!(files_under(&bucket, "", "ngb").len(), 0);
    assert_eq!(db.files(tenant, Signal::Logs).await.len(), 0);
    let dir = h.crash().await;

    let h = start(&db.catalog, dir, small_groups()).await;
    h.ingester.flush_all().await;
    let log_files = files_under(&h.bucket(), "/logs/l0/", "parquet");
    assert_eq!(log_files.len(), 1);
    let registered = db.files(tenant, Signal::Logs).await;
    assert_eq!(registered.len(), 1);
    let index = text_index_of(&h.bucket(), &log_files[0]);
    assert_eq!(Some(*index.content_sha256()), registered[0].content_sha256);
    h.stop().await;
    db.finish().await;
}

/// Both objects are written and the registration fails, then the process
/// crashes: the replay writes the same sidecar bytes, finds them in place
/// and registers the file under its id.
#[tokio::test]
async fn a_crash_between_the_text_sidecar_and_the_registration_reproduces_it() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let fails = IngestConfig {
        inject_register_failures: 1,
        ..small_groups()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), fails).await;
    h.export_logs(logs("api", 6, 0, 0), &token).await;
    h.ingester.flush_all().await;
    let sidecars = files_under(&h.bucket(), "", "ngb");
    assert_eq!(sidecars.len(), 1);
    let before = std::fs::read(&sidecars[0]).unwrap();
    let dir = h.crash().await;

    let h = start(&db.catalog, dir, small_groups()).await;
    h.ingester.flush_all().await;
    assert_eq!(files_under(&h.bucket(), "", "ngb"), sidecars);
    assert_eq!(
        std::fs::read(&sidecars[0]).unwrap(),
        before,
        "the same bytes"
    );
    let flush = h.ingester.flush_stats();
    assert_eq!(flush.replay_mismatch.load(Ordering::Relaxed), 0);
    assert_eq!(flush.superseded.load(Ordering::Relaxed), 0);
    assert_eq!(db.files(tenant, Signal::Logs).await.len(), 1);
    h.stop().await;
    db.finish().await;
}

/// Registered, never confirmed, then a crash: the replay meets both
/// objects with the bytes it produces and confirms the file, with no
/// second sidecar.
#[tokio::test]
async fn a_crash_after_the_registration_keeps_the_text_sidecar() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let never_confirms = IngestConfig {
        inject_confirm_failures: u32::MAX,
        ..small_groups()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), never_confirms).await;
    h.export_logs(logs("api", 6, 0, 0), &token).await;
    h.ingester.flush_all().await;
    assert_eq!(db.files(tenant, Signal::Logs).await.len(), 1);
    let sidecars = files_under(&h.bucket(), "", "ngb");
    let before = std::fs::read(&sidecars[0]).unwrap();
    let dir = h.crash().await;

    let h = start(&db.catalog, dir, small_groups()).await;
    h.ingester.flush_all().await;
    assert_eq!(files_under(&h.bucket(), "", "ngb"), sidecars);
    assert_eq!(std::fs::read(&sidecars[0]).unwrap(), before);
    assert_eq!(
        h.ingester.flush_stats().superseded.load(Ordering::Relaxed),
        0
    );
    assert_eq!(db.files(tenant, Signal::Logs).await.len(), 1);
    h.stop().await;
    db.finish().await;
}

/// A retried flush meets a sidecar with other bytes at its path: the rows
/// take a fresh id and are published under it with a sidecar of their own,
/// instead of retrying forever against the one in place.
#[tokio::test]
async fn a_text_sidecar_with_other_bytes_gives_the_rows_a_fresh_id() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let fails = IngestConfig {
        inject_register_failures: 1,
        checkpoint_after_flush: false,
        ..small_groups()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), fails).await;
    h.export_logs(logs("api", 6, 0, 0), &token).await;
    h.ingester.flush_all().await;
    let bucket = h.bucket();
    let old = files_under(&bucket, "/logs/l0/", "parquet");
    let sidecars = files_under(&bucket, "", "ngb");
    assert_eq!((old.len(), sidecars.len()), (1, 1));
    std::fs::write(&sidecars[0], b"other bytes").unwrap();

    h.ingester.flush_all().await;
    let flush = h.ingester.flush_stats();
    assert_eq!(flush.replay_mismatch.load(Ordering::Relaxed), 1);
    assert_eq!(flush.superseded.load(Ordering::Relaxed), 1);
    h.ingester.flush_all().await;

    let registered = db.files(tenant, Signal::Logs).await;
    assert_eq!(registered.len(), 1);
    let old_id = fs_catalog::FileId(
        uuid::Uuid::parse_str(old[0].file_stem().unwrap().to_str().unwrap()).unwrap(),
    );
    assert_ne!(registered[0].id, old_id);
    assert_eq!(registered[0].origin, old_id);
    let new = bucket.join(&registered[0].path);
    let index = text_index_of(&bucket, &new);
    assert_eq!(Some(*index.content_sha256()), registered[0].content_sha256);
    assert_eq!(
        files_under(&bucket, "", "ngb").len(),
        2,
        "the damaged one is left to the sweep"
    );
    h.stop().await;
    db.finish().await;
}

/// A local bucket that refuses to read a text sidecar whole and notes the
/// largest range read of one, as a store that would hand a whole object
/// back in one piece would expose a second copy.
#[derive(Debug)]
struct RangesOnly {
    inner: LocalFileSystem,
    largest_range: std::sync::atomic::AtomicU64,
    whole_reads: std::sync::atomic::AtomicU64,
}

impl std::fmt::Display for RangesOnly {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RangesOnly({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for RangesOnly {
    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let sidecar = std::path::Path::new(location.as_ref())
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("ngb"));
        if sidecar && !options.head {
            let Some(object_store::GetRange::Bounded(range)) = &options.range else {
                self.whole_reads.fetch_add(1, Ordering::Relaxed);
                return Err(object_store::Error::Generic {
                    store: "RangesOnly",
                    source: "a text sidecar is never read whole".into(),
                });
            };
            self.largest_range
                .fetch_max(range.end - range.start, Ordering::Relaxed);
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// `count` records of `name` with bodies of `body_bytes` of distinct words.
fn big_logs(name: &str, count: u64, body_bytes: usize) -> ExportLogsServiceRequest {
    let mut request = logs(name, count, 0, 0);
    for (i, record) in request.resource_logs[0].scope_logs[0]
        .log_records
        .iter_mut()
        .enumerate()
    {
        let mut body = String::with_capacity(body_bytes);
        let mut n = i * 1_000_000;
        while body.len() + 12 < body_bytes {
            let _ = write!(body, "w{n:x} ");
            n += 1;
        }
        record.body = Some(AnyValue {
            value: Some(Value::StringValue(body)),
        });
    }
    request
}

/// Under a tight memory cap, bodies of 64 KiB and a sidecar already in
/// place at the retry: the retry reads the sidecar back in ranges only,
/// the memory counted never passes the cap at any moment, and the flush
/// completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_meets_its_sidecar_within_the_memory_cap() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    // Just above what admitting four rows of 64 KiB bodies needs: the
    // rows, four copies for their flush and the file's sidecar scratch.
    let cap = 1536 * 1024;
    let config = IngestConfig {
        inject_register_failures: 1,
        checkpoint_after_flush: false,
        max_memory_bytes: cap,
        max_tenant_bytes: cap,
        max_buffer_bytes: cap,
        ..test_config()
    };
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("bucket")).unwrap();
    let store = Arc::new(RangesOnly {
        inner: LocalFileSystem::new_with_prefix(dir.path().join("bucket")).unwrap(),
        largest_range: std::sync::atomic::AtomicU64::new(0),
        whole_reads: std::sync::atomic::AtomicU64::new(0),
    });
    let h = start_on(&db.catalog, dir, config, store.clone()).await;
    assert_eq!(
        h.export_logs(big_logs("api", 4, 64 * 1024), &token).await,
        0
    );
    h.ingester.flush_all().await;
    let sidecars = files_under(&h.bucket(), "", "ngb");
    assert_eq!(sidecars.len(), 1, "written before the registration failed");
    let sidecar_len = std::fs::metadata(&sidecars[0]).unwrap().len();
    let chunk = fs_ingest::flush::COMPARE_CHUNK as u64;
    assert!(sidecar_len > 3 * chunk, "{sidecar_len}");
    assert_eq!(db.files(tenant, Signal::Logs).await.len(), 0);

    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let sampler = tokio::spawn({
        let ingester = Arc::clone(&h.ingester);
        let running = Arc::clone(&running);
        async move {
            let mut peak = 0usize;
            while running.load(Ordering::Relaxed) {
                peak = peak.max(ingester.memory_held().await);
                tokio::time::sleep(Duration::from_micros(200)).await;
            }
            peak
        }
    });
    h.ingester.flush_all().await;
    running.store(false, Ordering::Relaxed);
    let peak = sampler.await.unwrap();

    assert_eq!(
        db.files(tenant, Signal::Logs).await.len(),
        1,
        "the retry completed"
    );
    assert!(peak <= cap, "{peak} counted over the cap of {cap}");
    assert!(peak > 0, "the sampler saw the attempt");
    assert_eq!(store.whole_reads.load(Ordering::Relaxed), 0);
    assert_eq!(store.largest_range.load(Ordering::Relaxed), chunk);
    let flush = h.ingester.flush_stats();
    assert_eq!(flush.replay_mismatch.load(Ordering::Relaxed), 0);
    h.stop().await;
    db.finish().await;
}
