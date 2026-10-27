//! End to end: OTLP over gRPC → WAL → buffer → Parquet in a local bucket → catalog.
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

use arrow::record_batch::RecordBatch;
use fs_catalog::{Catalog, NewTenant, Signal, TenantId, TimeRange, TokenScope};
use fs_ingest::{IngestConfig, Ingester};
use fs_wal::{Wal, WalConfig};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prost::Message;
use sha2::{Digest, Sha256};
use sqlx::{Connection, Executor, PgConnection};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Code, Request};

const T0: u64 = 1_792_678_623_000_000_000;

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
        let schema = format!("ing_{}", uuid::Uuid::new_v4().simple());
        let mut admin = PgConnection::connect(&admin_url)
            .await
            .expect("connect to test database");
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

fn request(service: &str, trace: u8, spans: u8) -> ExportTraceServiceRequest {
    let spans = (0..spans)
        .map(|i| Span {
            trace_id: vec![trace; 16],
            span_id: vec![i + 1; 8],
            name: format!("op-{i}"),
            kind: 2,
            start_time_unix_nano: T0 + u64::from(i) * 1_000_000,
            end_time_unix_nano: T0 + u64::from(i) * 1_000_000 + 500_000,
            ..Span::default()
        })
        .collect();
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(service.into())),
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
    let wal_config = WalConfig::new(dir.path().join("wal"));
    start_with_wal(catalog, dir, config, wal_config).await
}

async fn start_with_wal(
    catalog: &Catalog,
    dir: tempfile::TempDir,
    config: IngestConfig,
    wal_config: WalConfig,
) -> Harness {
    std::fs::create_dir_all(dir.path().join("bucket")).unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path().join("bucket")).unwrap());
    let wal = Wal::open(wal_config).unwrap();
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
    async fn client(&self) -> TraceServiceClient<Channel> {
        TraceServiceClient::connect(format!("http://{}", self.addr))
            .await
            .unwrap()
    }

    async fn stop(self) -> tempfile::TempDir {
        self.shutdown.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        self.dir
    }
}

fn authed(req: ExportTraceServiceRequest, token: &str) -> Request<ExportTraceServiceRequest> {
    let mut r = Request::new(req);
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

fn sha256_of(path: &std::path::Path) -> [u8; 32] {
    Sha256::digest(std::fs::read(path).unwrap()).into()
}

fn parquet_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(parquet_files(&path));
            } else if path.extension().is_some_and(|e| e == "parquet") {
                out.push(path);
            }
        }
    }
    out
}

#[tokio::test]
async fn authenticates_buffers_flushes_and_registers() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    let mut client = h.client().await;

    let denied = client
        .export(Request::new(request("api", 1, 1)))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::Unauthenticated);
    let denied = client
        .export(authed(
            request("api", 1, 1),
            "fst_not_a_real_token_at_all_but_long_enough_xx",
        ))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::Unauthenticated);

    client
        .export(authed(request("api", 1, 10), &token))
        .await
        .unwrap();
    let mut with_bad = request("worker", 2, 5);
    with_bad.resource_spans[0].scope_spans[0].spans[0].trace_id = vec![1, 2, 3];
    let response = client
        .export(authed(with_bad, &token))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.partial_success.as_ref().map(|p| p.rejected_spans),
        Some(1)
    );

    assert_eq!(h.ingester.stats().spans.load(Ordering::Relaxed), 14);
    assert_eq!(
        parquet_files(&h.dir.path().join("bucket")).len(),
        0,
        "nothing flushed yet"
    );
    assert!(h.dir.path().join("wal").read_dir().unwrap().count() >= 1);

    h.ingester.flush_all().await;
    let fs = h.ingester.flush_stats();
    assert_eq!(fs.flushes.load(Ordering::Relaxed), 1);
    assert_eq!(fs.rows.load(Ordering::Relaxed), 14);
    assert!(fs.parquet_bytes.load(Ordering::Relaxed) > 0);
    assert!(
        fs.concat_nanos.load(Ordering::Relaxed) > 0 && fs.sort_nanos.load(Ordering::Relaxed) > 0
    );
    assert!(fs.encode_nanos.load(Ordering::Relaxed) > 0);
    assert_eq!(fs.attempts.load(Ordering::Relaxed), 1);
    assert!(
        fs.upload_nanos.load(Ordering::Relaxed) > 0
            && fs.register_nanos.load(Ordering::Relaxed) > 0
    );
    assert!(fs.total_nanos.load(Ordering::Relaxed) >= fs.encode_nanos.load(Ordering::Relaxed));
    let files = parquet_files(&h.dir.path().join("bucket"));
    assert_eq!(files.len(), 1, "one tenant, one L0 file");
    assert!(
        files[0]
            .to_string_lossy()
            .contains(&format!("t/{}/traces/l0/dt=2026-10-22/hr=14/", tenant.0))
    );

    let reader =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&files[0]).unwrap()).unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 14);
    let bloom_columns: Vec<_> = reader
        .metadata()
        .row_group(0)
        .columns()
        .iter()
        .filter(|c| c.bloom_filter_offset().is_some())
        .map(|c| c.column_path().string())
        .collect();
    assert!(
        bloom_columns.contains(&"trace_id".to_owned()),
        "{bloom_columns:?}"
    );
    assert!(
        bloom_columns.contains(&"service_name".to_owned()),
        "{bloom_columns:?}"
    );

    let registered = db
        .catalog
        .visible_files(
            tenant,
            Signal::Traces,
            TimeRange {
                start: chrono::DateTime::UNIX_EPOCH,
                end: chrono::Utc::now(),
            },
        )
        .await
        .unwrap();
    assert_eq!(registered.len(), 1);
    assert_eq!(registered[0].row_count, 14);
    assert_eq!(
        registered[0].services,
        vec!["api".to_owned(), "worker".to_owned()]
    );
    assert_eq!(
        registered[0].size_bytes,
        i64::try_from(std::fs::metadata(&files[0]).unwrap().len()).unwrap()
    );

    h.stop().await;
    db.finish().await;
}

#[tokio::test]
async fn replays_wal_after_crash_without_flush() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();

    // First run: ingest, then drop the harness without a clean shutdown.
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    h.client()
        .await
        .export(authed(request("api", 7, 20), &token))
        .await
        .unwrap();
    h.task.abort();
    let _ = h.task.await;
    let dir = h.dir;
    assert_eq!(parquet_files(&dir.path().join("bucket")).len(), 0);

    // Second run on the same directories: the WAL brings the rows back.
    let h = start(&db.catalog, dir, test_config()).await;
    h.ingester.flush_all().await;
    let files = parquet_files(&h.dir.path().join("bucket"));
    assert_eq!(files.len(), 1);
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&files[0]).unwrap()).unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 20);

    // Flushing released the WAL segments that only held flushed rows.
    h.client()
        .await
        .export(authed(request("api", 8, 1), &token))
        .await
        .unwrap();
    h.ingester.flush_all().await;
    let segments: Vec<_> = h
        .dir
        .path()
        .join("wal")
        .read_dir()
        .unwrap()
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "wal"))
        .collect();
    assert_eq!(
        segments.len(),
        1,
        "only the active segment remains: {segments:?}"
    );
    h.stop().await;
    db.finish().await;
}

#[tokio::test]
async fn anonymous_tenant_and_rate_limit() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("dev"))
        .await
        .unwrap()
        .id;
    let config = IngestConfig {
        anonymous_tenant: Some(tenant),
        tenant_bytes_per_sec: Some(600),
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;

    client
        .export(Request::new(request("api", 1, 2)))
        .await
        .unwrap();
    let mut limited = false;
    for i in 0..20 {
        if let Err(status) = client.export(Request::new(request("api", 1, 2 + i))).await {
            assert_eq!(status.code(), Code::ResourceExhausted);
            limited = true;
            break;
        }
    }
    assert!(limited, "600 bytes per second must throttle a burst");
    assert!(h.ingester.stats().rate_limited.load(Ordering::Relaxed) >= 1);
    h.stop().await;
    db.finish().await;
}

#[tokio::test]
async fn shutdown_flushes_pending_rows() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    h.client()
        .await
        .export(authed(request("api", 3, 4), &token))
        .await
        .unwrap();
    let dir = h.stop().await;
    assert_eq!(parquet_files(&dir.path().join("bucket")).len(), 1);
    let _ = TenantId::new();
    db.finish().await;
}

#[tokio::test]
async fn hot_window_serves_live_and_recently_persisted_rows() {
    use fs_ingest::hot::{HotScanRequest, scan};

    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let other = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("other"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    let mut client = h.client().await;
    client
        .export(authed(request("api", 1, 10), &token))
        .await
        .unwrap();
    client
        .export(authed(request("worker", 2, 4), &token))
        .await
        .unwrap();
    let addr = h.hot_addr.to_string();

    // Live rows, with filter and projection applied by the ingester.
    let all = HotScanRequest {
        tenant,
        ts_min: None,
        ts_max: None,
        services: None,
        columns: None,
        trace_ids: None,
        span_ids: None,
    };
    let got = scan(&addr, &all, Duration::from_secs(5)).await.unwrap();
    assert_eq!(
        got.batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        14
    );
    assert_eq!(
        got.persisted_files.len(),
        1,
        "live rows announce the id their file will get"
    );

    let api_names = HotScanRequest {
        tenant,
        ts_min: Some(i64::try_from(T0 + 5_000_000).unwrap()),
        ts_max: None,
        services: Some(vec!["api".into()]),
        columns: Some(vec!["timestamp".into(), "name".into()]),
        trace_ids: None,
        span_ids: None,
    };
    let got = scan(&addr, &api_names, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        got.batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        5,
        "api spans 5..9"
    );
    assert_eq!(got.batches[0].num_columns(), 2);

    // Another tenant sees nothing.
    let empty = scan(
        &addr,
        &HotScanRequest {
            tenant: other,
            ..all.clone()
        },
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(
        empty
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );

    // After a flush the rows stay served and the file is reported, so a
    // querier can skip it; after the retention they disappear from memory.
    h.ingester.flush_all().await;
    let got = scan(&addr, &all, Duration::from_secs(5)).await.unwrap();
    assert_eq!(
        got.batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        14
    );
    assert_eq!(got.persisted_files.len(), 1);
    let registered = db
        .catalog
        .visible_files(
            tenant,
            Signal::Traces,
            TimeRange {
                start: chrono::DateTime::UNIX_EPOCH,
                end: chrono::Utc::now(),
            },
        )
        .await
        .unwrap();
    assert_eq!(registered[0].id, got.persisted_files[0]);

    tokio::time::sleep(Duration::from_millis(2500)).await;
    h.ingester.flush_all().await; // sweeps persisted rows past the retention
    let got = scan(&addr, &all, Duration::from_secs(5)).await.unwrap();
    assert_eq!(
        got.batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        0
    );
    assert_eq!(got.persisted_files.len(), 0);

    h.stop().await;
    db.finish().await;
}

#[tokio::test]
async fn restart_after_flush_does_not_duplicate_rows() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();

    // Ingest, flush, then die without a clean shutdown.
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    h.client()
        .await
        .export(authed(request("api", 7, 20), &token))
        .await
        .unwrap();
    h.ingester.flush_all().await;
    h.client()
        .await
        .export(authed(request("api", 8, 3), &token))
        .await
        .unwrap(); // unflushed
    h.task.abort();
    let _ = h.task.await;
    let dir = h.dir;
    assert_eq!(parquet_files(&dir.path().join("bucket")).len(), 1);

    // Restart: only the 3 unflushed spans come back from the WAL.
    let h = start(&db.catalog, dir, test_config()).await;
    assert_eq!(
        h.ingester.stats().spans.load(Ordering::Relaxed),
        0,
        "stats count live traffic only"
    );
    h.ingester.flush_all().await;
    let files = parquet_files(&h.dir.path().join("bucket"));
    assert_eq!(
        files.len(),
        2,
        "one file per flush, no replayed duplicate of the first"
    );
    let total: i64 = files
        .iter()
        .map(|f| {
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(f).unwrap())
                .unwrap()
                .metadata()
                .file_metadata()
                .num_rows()
        })
        .sum();
    assert_eq!(total, 23);
    let registered = db
        .catalog
        .visible_files(
            tenant,
            Signal::Traces,
            TimeRange {
                start: chrono::DateTime::UNIX_EPOCH,
                end: chrono::Utc::now(),
            },
        )
        .await
        .unwrap();
    assert_eq!(registered.len(), 2);
    assert_eq!(registered.iter().map(|f| f.row_count).sum::<i64>(), 23);
    h.stop().await;
    db.finish().await;
}

/// Crash after the file is registered but before the checkpoint, with more
/// rows arriving while the flush ran. Replay must keep the original grouping:
/// the first file is reproduced byte for byte and the later rows become a
/// second file, instead of one file silently growing under the same id.
#[tokio::test]
async fn crash_after_register_keeps_flush_boundaries() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();

    let no_checkpoint = IngestConfig {
        checkpoint_after_flush: false,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), no_checkpoint).await;
    h.client()
        .await
        .export(authed(request("api", 7, 10), &token))
        .await
        .unwrap();
    h.ingester.flush_all().await; // intent + upload + register, no checkpoint
    h.client()
        .await
        .export(authed(request("api", 8, 3), &token))
        .await
        .unwrap(); // arrives after the flush
    h.task.abort();
    let _ = h.task.await;
    let dir = h.dir;
    let first_files = parquet_files(&dir.path().join("bucket"));
    assert_eq!(first_files.len(), 1);
    let first_len = std::fs::metadata(&first_files[0]).unwrap().len();

    let h = start(&db.catalog, dir, test_config()).await;
    assert!(
        h.ingester.stats().flushes_recovered.load(Ordering::Relaxed) >= 1,
        "the intent was resumed"
    );
    h.ingester.flush_all().await;
    let files = parquet_files(&h.dir.path().join("bucket"));
    assert_eq!(files.len(), 2, "the 3 late rows become their own file");
    assert_eq!(
        std::fs::metadata(&first_files[0]).unwrap().len(),
        first_len,
        "the first object is untouched"
    );
    let registered = db
        .catalog
        .visible_files(
            tenant,
            Signal::Traces,
            TimeRange {
                start: chrono::DateTime::UNIX_EPOCH,
                end: chrono::Utc::now(),
            },
        )
        .await
        .unwrap();
    let mut rows: Vec<i64> = registered.iter().map(|f| f.row_count).collect();
    rows.sort_unstable();
    assert_eq!(rows, vec![3, 10]);
    h.stop().await;
    db.finish().await;
}

/// Crash after the intent but before the upload succeeded: the retry after
/// restart writes exactly the intended file, once.
#[tokio::test]
async fn crash_after_intent_before_upload_is_resumed() {
    use std::os::unix::fs::PermissionsExt;

    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();

    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    h.client()
        .await
        .export(authed(request("api", 7, 10), &token))
        .await
        .unwrap();
    // Make the bucket unwritable so the upload fails after the intent is logged.
    let bucket = h.dir.path().join("bucket");
    std::fs::set_permissions(&bucket, std::fs::Permissions::from_mode(0o555)).unwrap();
    h.ingester.flush_all().await;
    assert!(h.ingester.stats().flush_failures.load(Ordering::Relaxed) >= 1);
    std::fs::set_permissions(&bucket, std::fs::Permissions::from_mode(0o755)).unwrap();
    h.task.abort();
    let _ = h.task.await;
    let dir = h.dir;
    assert_eq!(parquet_files(&dir.path().join("bucket")).len(), 0);

    let h = start(&db.catalog, dir, test_config()).await;
    h.ingester.flush_all().await;
    assert_eq!(parquet_files(&h.dir.path().join("bucket")).len(), 1);
    let registered = db
        .catalog
        .visible_files(
            tenant,
            Signal::Traces,
            TimeRange {
                start: chrono::DateTime::UNIX_EPOCH,
                end: chrono::Utc::now(),
            },
        )
        .await
        .unwrap();
    assert_eq!(registered.len(), 1);
    assert_eq!(registered[0].row_count, 10);
    h.stop().await;
    db.finish().await;
}

/// Live rows already announce the id their file will get, so a querier that
/// sees the file appear while planning still excludes it.
#[tokio::test]
async fn hot_window_announces_prospective_file_id() {
    use fs_ingest::hot::{HotScanRequest, scan};

    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    h.client()
        .await
        .export(authed(request("api", 1, 4), &token))
        .await
        .unwrap();
    let all = HotScanRequest {
        tenant,
        ts_min: None,
        ts_max: None,
        services: None,
        columns: None,
        trace_ids: None,
        span_ids: None,
    };
    let before = scan(&h.hot_addr.to_string(), &all, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        before.persisted_files.len(),
        1,
        "live rows carry their future file id"
    );

    h.ingester.flush_all().await;
    let registered = db
        .catalog
        .visible_files(
            tenant,
            Signal::Traces,
            TimeRange {
                start: chrono::DateTime::UNIX_EPOCH,
                end: chrono::Utc::now(),
            },
        )
        .await
        .unwrap();
    assert_eq!(registered.len(), 1);
    assert_eq!(
        registered[0].id, before.persisted_files[0],
        "the announced id is the registered one"
    );
    h.stop().await;
    db.finish().await;
}

/// Registration fails after the object exists, more rows arrive, and the
/// retry runs without a restart. The rows bound by the intent must stay
/// bound: the retry registers exactly the uploaded content and the late rows
/// become a second file.
#[tokio::test]
async fn register_failure_keeps_rows_bound_to_their_file() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    let config = IngestConfig {
        inject_register_failures: 1,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;

    h.client()
        .await
        .export(authed(request("api", 7, 10), &token))
        .await
        .unwrap();
    h.ingester.flush_all().await; // upload succeeds, registration is injected to fail
    assert_eq!(h.ingester.stats().flush_failures.load(Ordering::Relaxed), 1);
    let fs = h.ingester.flush_stats();
    assert_eq!(fs.attempts.load(Ordering::Relaxed), 1);
    assert_eq!(fs.flushes.load(Ordering::Relaxed), 0);
    assert_eq!(
        fs.encode_nanos.load(Ordering::Relaxed),
        0,
        "stage times wait for a completed flush"
    );
    let files = parquet_files(&h.dir.path().join("bucket"));
    assert_eq!(files.len(), 1, "the object exists although unregistered");
    let first_len = std::fs::metadata(&files[0]).unwrap().len();
    assert_eq!(
        db.catalog
            .visible_files(
                tenant,
                Signal::Traces,
                TimeRange {
                    start: chrono::DateTime::UNIX_EPOCH,
                    end: chrono::Utc::now()
                }
            )
            .await
            .unwrap()
            .len(),
        0
    );

    h.client()
        .await
        .export(authed(request("api", 8, 3), &token))
        .await
        .unwrap(); // arrives before the retry
    h.ingester.flush_all().await; // retry registers F(P); R becomes its own file
    let files = parquet_files(&h.dir.path().join("bucket"));
    assert_eq!(files.len(), 2);
    assert!(
        files
            .iter()
            .any(|f| std::fs::metadata(f).unwrap().len() == first_len),
        "the first object is untouched"
    );
    let registered = db
        .catalog
        .visible_files(
            tenant,
            Signal::Traces,
            TimeRange {
                start: chrono::DateTime::UNIX_EPOCH,
                end: chrono::Utc::now(),
            },
        )
        .await
        .unwrap();
    let mut rows: Vec<i64> = registered.iter().map(|f| f.row_count).collect();
    rows.sort_unstable();
    assert_eq!(rows, vec![3, 10], "catalog describes each object exactly");
    assert!(h.ingester.stats().flush_retries.load(Ordering::Relaxed) >= 1);
    h.stop().await;
    db.finish().await;
}

/// A WAL that hits its size cap refuses writes, and a flush that checkpoints
/// away old segments must reopen it without a restart.
#[tokio::test]
async fn wal_full_recovers_after_checkpoint() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    let config = IngestConfig {
        wal_max_bytes: 256 * 1024,
        ..test_config()
    };
    let dir = tempfile::tempdir().unwrap();
    let wal_config = WalConfig {
        segment_max_bytes: 64 * 1024,
        ..WalConfig::new(dir.path().join("wal"))
    };
    let h = start_with_wal(&db.catalog, dir, config, wal_config).await;
    let mut client = h.client().await;

    let mut refused = false;
    for i in 0..400u16 {
        let trace = u8::try_from(i % 250 + 1).unwrap();
        if let Err(status) = client
            .export(authed(request("api", trace, 40), &token))
            .await
        {
            assert_eq!(status.code(), Code::Unavailable, "{status:?}");
            assert!(
                status.message().contains("write-ahead log full"),
                "{status:?}"
            );
            refused = true;
            break;
        }
    }
    assert!(refused, "the cap must be reached");

    h.ingester.flush_all().await; // registers files, checkpoints, removes segments
    client
        .export(authed(request("api", 1, 1), &token))
        .await
        .expect("writes resume after the checkpoint");
    assert!(h.ingester.wal_bytes() < 256 * 1024);
    h.stop().await;
    db.finish().await;
}

/// Rows retained for the hot window count toward the memory cap and are not
/// evicted to make room; admission waits for the retention to pass.
#[tokio::test]
async fn memory_cap_counts_persisted_rows_without_evicting_them() {
    use fs_ingest::hot::{HotScanRequest, scan};

    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    // Four raw 30-span batches, each with the three copies its flush keeps
    // room for, fit under 512 KiB. After the flush the rows stay as one
    // compact batch, so more requests fit before the retention plus the
    // live rows and their margin cross the cap.
    let config = IngestConfig {
        max_memory_bytes: 512 * 1024,
        persisted_retain: Duration::from_secs(2),
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;

    for i in 1..=4u8 {
        client
            .export(authed(request("api", i, 30), &token))
            .await
            .unwrap();
    }
    h.ingester.flush_all().await; // rows move to persisted, still in memory
    let mut admitted = 0u8;
    let refused = loop {
        match client
            .export(authed(request("api", 10 + admitted, 30), &token))
            .await
        {
            Ok(_) => admitted += 1,
            Err(status) => break status,
        }
        assert!(admitted < 32, "the retention never counted against the cap");
    };
    assert_eq!(refused.code(), Code::ResourceExhausted, "{refused:?}");
    assert!(
        refused.message().contains("ingester memory full"),
        "{refused:?}"
    );

    // The persisted rows are still served meanwhile, with the live ones.
    let all = HotScanRequest {
        tenant,
        ts_min: None,
        ts_max: None,
        services: None,
        columns: None,
        trace_ids: None,
        span_ids: None,
    };
    let got = scan(&h.hot_addr.to_string(), &all, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        got.batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        120 + usize::from(admitted) * 30
    );

    tokio::time::sleep(Duration::from_millis(2500)).await;
    h.ingester.flush_all().await; // sweeps the retention
    client
        .export(authed(request("api", 9, 30), &token))
        .await
        .expect("admitted once the retention passed");
    h.stop().await;
    db.finish().await;
}

/// Two flushers racing, as the flusher tick and a shutdown flush do, must
/// not materialise the same bound rows twice.
#[tokio::test]
async fn concurrent_flushes_do_not_duplicate_work() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    h.client()
        .await
        .export(authed(request("api", 7, 50), &token))
        .await
        .unwrap();
    let a = {
        let ingester = h.ingester.clone();
        tokio::spawn(async move { ingester.flush_all().await })
    };
    let b = {
        let ingester = h.ingester.clone();
        tokio::spawn(async move { ingester.flush_all().await })
    };
    a.await.unwrap();
    b.await.unwrap();
    h.ingester.flush_all().await;
    let fs = h.ingester.flush_stats();
    assert_eq!(
        fs.attempts.load(Ordering::Relaxed),
        1,
        "one attempt for one bound buffer"
    );
    assert_eq!(fs.flushes.load(Ordering::Relaxed), 1);
    assert_eq!(parquet_files(&h.dir.path().join("bucket")).len(), 1);
    h.stop().await;
    db.finish().await;
}

/// Request A takes the lower WAL position but reaches the buffer after
/// request B. The intent must still start at A's position, and the id the
/// hot window announced while only B was live must be the id registered,
/// across the upload, a crash before the checkpoint and the replay.
#[tokio::test]
async fn out_of_order_pushes_keep_intent_and_file_id() {
    use fs_ingest::hot::{HotScanRequest, scan};

    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    let config = IngestConfig {
        inject_push_delay: Some(Duration::from_secs(2)),
        checkpoint_after_flush: false,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;

    let mut client_a = h.client().await;
    let token_a = token.clone();
    let a = tokio::spawn(async move {
        client_a
            .export(authed(request("api", 1, 10), &token_a))
            .await
    });
    // Once A's record is synced, A sleeps before its push and B overtakes it.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while h.ingester.writer_stats().records.load(Ordering::Relaxed) < 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "first request never reached the wal"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    h.client()
        .await
        .export(authed(request("api", 2, 20), &token))
        .await
        .unwrap();
    assert!(
        !a.is_finished(),
        "the delayed request must still be pending"
    );
    let all = HotScanRequest {
        tenant,
        ts_min: None,
        ts_max: None,
        services: None,
        columns: None,
        trace_ids: None,
        span_ids: None,
    };
    let announced = scan(&h.hot_addr.to_string(), &all, Duration::from_secs(5))
        .await
        .unwrap()
        .persisted_files;
    assert_eq!(announced.len(), 1);
    a.await.unwrap().unwrap();

    h.ingester.flush_all().await;
    assert_eq!(parquet_files(&h.dir.path().join("bucket")).len(), 1);
    h.task.abort();
    let _ = h.task.await;
    let dir = h.dir;

    let h = start(&db.catalog, dir, test_config()).await;
    h.ingester.flush_all().await;
    assert_eq!(
        h.ingester.stats().flush_failures.load(Ordering::Relaxed),
        0,
        "the replayed intent covers the same rows as the uploaded object"
    );
    assert_eq!(parquet_files(&h.dir.path().join("bucket")).len(), 1);
    let registered = db
        .catalog
        .visible_files(
            tenant,
            Signal::Traces,
            TimeRange {
                start: chrono::DateTime::UNIX_EPOCH,
                end: chrono::Utc::now(),
            },
        )
        .await
        .unwrap();
    assert_eq!(registered.len(), 1);
    assert_eq!(registered[0].row_count, 30);
    assert_eq!(announced, vec![registered[0].id]);
    h.stop().await;
    db.finish().await;
}

/// One tenant holding its hot-window rows is refused at its own cap while
/// another tenant is still admitted.
#[tokio::test]
async fn tenant_memory_cap_isolates_tenants() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let mut tokens = Vec::new();
    for name in ["acme", "globex"] {
        let tenant = db
            .catalog
            .create_tenant(&NewTenant::with_defaults(name))
            .await
            .unwrap()
            .id;
        let (token, _) = db
            .catalog
            .create_token(tenant, "collector", TokenScope::Ingest)
            .await
            .unwrap();
        tokens.push(token);
    }
    // Four raw 30-span batches fit under 80 KiB; after the flush the rows
    // stay compact, so a few more fit before the tenant's retention plus
    // its live rows cross its cap.
    let config = IngestConfig {
        max_tenant_bytes: 80 * 1024,
        persisted_retain: Duration::from_secs(2),
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    for i in 1..=4u8 {
        client
            .export(authed(request("api", i, 30), &tokens[0]))
            .await
            .unwrap();
    }
    h.ingester.flush_all().await;
    let mut admitted = 0u8;
    let refused = loop {
        match client
            .export(authed(request("api", 10 + admitted, 30), &tokens[0]))
            .await
        {
            Ok(_) => admitted += 1,
            Err(status) => break status,
        }
        assert!(admitted < 8, "the retention never counted against the cap");
    };
    assert_eq!(refused.code(), Code::ResourceExhausted, "{refused:?}");
    assert!(
        refused.message().contains("tenant memory full"),
        "{refused:?}"
    );
    client
        .export(authed(request("web", 1, 30), &tokens[1]))
        .await
        .expect("another tenant is not held by the first one's retention");

    tokio::time::sleep(Duration::from_millis(2500)).await;
    h.ingester.flush_all().await; // sweeps the retention
    client
        .export(authed(request("api", 9, 30), &tokens[0]))
        .await
        .expect("admitted once the retention passed");
    h.stop().await;
    db.finish().await;
}

/// Requests admitted together cannot overshoot the cap: admission reserves
/// the batch under the buffer lock, so exactly as many fit as the cap allows.
#[tokio::test]
async fn memory_cap_is_a_ceiling_under_concurrent_requests() {
    use fs_ingest::hot::{HotScanRequest, scan};

    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    // Three 30-span batches with their flush margin fit under 256 KiB, a
    // fourth would cross it.
    let config = IngestConfig {
        max_memory_bytes: 256 * 1024,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut tasks = Vec::new();
    for i in 1..=12u8 {
        let mut client = h.client().await;
        let token = token.clone();
        tasks.push(tokio::spawn(async move {
            client
                .export(authed(request("api", i, 30), &token))
                .await
                .map(|_| ())
        }));
    }
    let mut admitted = 0;
    let mut refused = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(()) => admitted += 1,
            Err(status) => {
                assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
                assert!(
                    status.message().contains("ingester memory full"),
                    "{status:?}"
                );
                refused += 1;
            }
        }
    }
    assert_eq!((admitted, refused), (3, 9));
    let all = HotScanRequest {
        tenant,
        ts_min: None,
        ts_max: None,
        services: None,
        columns: None,
        trace_ids: None,
        span_ids: None,
    };
    let got = scan(&h.hot_addr.to_string(), &all, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        got.batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        90
    );
    h.stop().await;
    db.finish().await;
}

/// Chunks built in the background hold the rows in log order, so the file
/// written from chunks is, byte for byte, the file replay rebuilds from the
/// records. The object is removed after the crash so the retry has to write
/// it again, and the two digests are compared.
#[tokio::test]
async fn coalesced_flush_matches_replay_byte_for_byte() {
    use fs_ingest::hot::{HotScanRequest, scan};

    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    let config = IngestConfig {
        coalesce_batches: 3,
        checkpoint_after_flush: false,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    for i in 1..=8u8 {
        let service = ["api", "web", "db"][usize::from(i % 3)];
        client
            .export(authed(request(service, i, 5), &token))
            .await
            .unwrap();
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while h.ingester.flush_stats().coalesces.load(Ordering::Relaxed) < 2 {
        assert!(std::time::Instant::now() < deadline, "coalescer never ran");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let all = HotScanRequest {
        tenant,
        ts_min: None,
        ts_max: None,
        services: None,
        columns: None,
        trace_ids: None,
        span_ids: None,
    };
    let served = scan(&h.hot_addr.to_string(), &all, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        served
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        40,
        "rows stay served while chunks replace batches"
    );

    h.ingester.flush_all().await;
    let written = parquet_files(&h.dir.path().join("bucket"));
    assert_eq!(written.len(), 1);
    let from_chunks = sha256_of(&written[0]);
    h.task.abort();
    let _ = h.task.await;
    let dir = h.dir;
    std::fs::remove_file(&written[0]).unwrap();

    let h = start(&db.catalog, dir, test_config()).await;
    h.ingester.flush_all().await;
    assert_eq!(
        h.ingester.stats().flush_failures.load(Ordering::Relaxed),
        0,
        "the file rebuilt from the log registers as the same content"
    );
    let rewritten = parquet_files(&h.dir.path().join("bucket"));
    assert_eq!(rewritten, written);
    assert_eq!(
        sha256_of(&rewritten[0]),
        from_chunks,
        "replayed records produce the same parquet bytes as the chunks did"
    );
    let registered = db
        .catalog
        .visible_files(
            tenant,
            Signal::Traces,
            TimeRange {
                start: chrono::DateTime::UNIX_EPOCH,
                end: chrono::Utc::now(),
            },
        )
        .await
        .unwrap();
    assert_eq!(registered.len(), 1);
    assert_eq!(registered[0].row_count, 40);
    h.stop().await;
    db.finish().await;
}

/// A record accepted while the checkpoint is being chosen must stay above
/// the cut. The request is held between its append and its push while the
/// checkpoint runs; after a crash the row must be replayed exactly once.
#[tokio::test]
async fn checkpoint_never_skips_an_accepted_record() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    let config = IngestConfig {
        inject_push_delay: Some(Duration::from_secs(2)),
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    let delayed =
        tokio::spawn(async move { client.export(authed(request("api", 1, 20), &token)).await });
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while h.ingester.writer_stats().records.load(Ordering::Relaxed) < 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "request never reached the wal"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(!delayed.is_finished());
    h.ingester.checkpoint().await;
    delayed.await.unwrap().unwrap();
    h.task.abort();
    let _ = h.task.await;
    let dir = h.dir;

    let h = start(&db.catalog, dir, test_config()).await;
    h.ingester.flush_all().await;
    let files = parquet_files(&h.dir.path().join("bucket"));
    assert_eq!(files.len(), 1, "the replayed rows are flushed once");
    let rows: usize =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&files[0]).unwrap())
            .unwrap()
            .build()
            .unwrap()
            .map(|b| b.unwrap().num_rows())
            .sum();
    assert_eq!(rows, 20);
    h.stop().await;
    db.finish().await;
}

/// The id announced for live rows must be the id their file gets even when
/// the ingester restarts in between with no intent logged: a querier that
/// scanned the hot window before the restart excludes exactly that file.
#[tokio::test]
async fn announced_file_id_survives_a_restart_without_intent() {
    use fs_ingest::hot::{HotScanRequest, scan};

    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    let mut client = h.client().await;
    for i in 1..=2u8 {
        client
            .export(authed(request("api", i, 6), &token))
            .await
            .unwrap();
    }
    let all = HotScanRequest {
        tenant,
        ts_min: None,
        ts_max: None,
        services: None,
        columns: None,
        trace_ids: None,
        span_ids: None,
    };
    let announced = scan(&h.hot_addr.to_string(), &all, Duration::from_secs(5))
        .await
        .unwrap()
        .persisted_files;
    assert_eq!(announced.len(), 1);
    h.task.abort();
    let _ = h.task.await;
    let dir = h.dir;

    let h = start(&db.catalog, dir, test_config()).await;
    h.ingester.flush_all().await;
    let registered = db
        .catalog
        .visible_files(
            tenant,
            Signal::Traces,
            TimeRange {
                start: chrono::DateTime::UNIX_EPOCH,
                end: chrono::Utc::now(),
            },
        )
        .await
        .unwrap();
    assert_eq!(registered.len(), 1);
    assert_eq!(registered[0].row_count, 12);
    assert_eq!(announced, vec![registered[0].id]);
    h.stop().await;
    db.finish().await;
}

/// Retries beyond the flush permits must not be lost: with one permit,
/// three failed flushes and the first retry holding the permit for a while,
/// only the flusher tick retries the others, and every one of them must
/// eventually be written.
#[tokio::test]
async fn retries_beyond_the_permits_are_not_lost() {
    use std::os::unix::fs::PermissionsExt;

    let Some(db) = TestDb::new().await else {
        return;
    };
    let mut tokens = Vec::new();
    for name in ["acme", "globex", "initech"] {
        let tenant = db
            .catalog
            .create_tenant(&NewTenant::with_defaults(name))
            .await
            .unwrap()
            .id;
        let (token, _) = db
            .catalog
            .create_token(tenant, "collector", TokenScope::Ingest)
            .await
            .unwrap();
        tokens.push(token);
    }
    let config = IngestConfig {
        max_flushes: 1,
        inject_retry_delay: Some(Duration::from_secs(3)),
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    for (i, token) in tokens.iter().enumerate() {
        client
            .export(authed(
                request("api", u8::try_from(i).unwrap() + 1, 5),
                token,
            ))
            .await
            .unwrap();
    }
    let bucket = h.dir.path().join("bucket");
    std::fs::set_permissions(&bucket, std::fs::Permissions::from_mode(0o555)).unwrap();
    h.ingester.flush_all().await;
    assert!(h.ingester.stats().flush_failures.load(Ordering::Relaxed) >= 3);
    std::fs::set_permissions(&bucket, std::fs::Permissions::from_mode(0o755)).unwrap();

    // No flush_all from here: the tick alone retries, one permit at a time.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while parquet_files(&bucket).len() < 3 {
        assert!(
            std::time::Instant::now() < deadline,
            "{} of 3 retries written",
            parquet_files(&bucket).len()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    h.stop().await;
    db.finish().await;
}

/// A forced pass, as `flush_all` runs at shutdown, tries each pending flush
/// once and returns: a flush that keeps failing waits for its backoff
/// instead of being retried for as long as the failure lasts.
#[tokio::test]
async fn forced_retry_tries_each_pending_flush_once() {
    use std::os::unix::fs::PermissionsExt;

    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    // One permit: a pass that re-took a failed entry would never run out.
    let config = IngestConfig {
        max_flushes: 1,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    h.client()
        .await
        .export(authed(request("api", 1, 5), &token))
        .await
        .unwrap();
    let bucket = h.dir.path().join("bucket");
    std::fs::set_permissions(&bucket, std::fs::Permissions::from_mode(0o555)).unwrap();
    h.ingester.flush_all().await;
    let fs = h.ingester.flush_stats();
    assert_eq!(fs.attempts.load(Ordering::Relaxed), 1);

    tokio::time::timeout(Duration::from_secs(10), h.ingester.flush_all())
        .await
        .expect("a forced pass ends while the failure lasts");
    assert_eq!(
        fs.attempts.load(Ordering::Relaxed),
        2,
        "exactly one forced attempt, then the backoff"
    );
    assert_eq!(h.ingester.stats().flush_failures.load(Ordering::Relaxed), 2);
    assert_eq!(parquet_files(&bucket).len(), 0);

    std::fs::set_permissions(&bucket, std::fs::Permissions::from_mode(0o755)).unwrap();
    h.ingester.flush_all().await;
    assert_eq!(parquet_files(&bucket).len(), 1);
    h.stop().await;
    db.finish().await;
}

/// The hot scan keeps only the rows of the traces and spans the request
/// names, so a trace lookup ships a handful of rows, not the whole window.
#[tokio::test]
async fn hot_scan_keeps_only_the_requested_traces_and_spans() {
    use arrow::array::AsArray;
    use fs_ingest::hot::{HotScanRequest, scan};

    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    let mut client = h.client().await;
    client
        .export(authed(request("api", 1, 6), &token))
        .await
        .unwrap();
    client
        .export(authed(request("web", 2, 6), &token))
        .await
        .unwrap();
    let base = HotScanRequest {
        tenant,
        ts_min: None,
        ts_max: None,
        services: None,
        columns: None,
        trace_ids: None,
        span_ids: None,
    };
    let hot = h.hot_addr.to_string();

    let by_trace = HotScanRequest {
        trace_ids: Some(vec![vec![2u8; 16]]),
        ..base.clone()
    };
    let got = scan(&hot, &by_trace, Duration::from_secs(5)).await.unwrap();
    assert_eq!(
        got.batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        6
    );
    for batch in &got.batches {
        let ids = batch
            .column(batch.schema().index_of("trace_id").unwrap())
            .as_fixed_size_binary()
            .iter()
            .map(|v| v.unwrap().to_vec())
            .collect::<Vec<_>>();
        assert!(ids.iter().all(|id| *id == vec![2u8; 16]));
    }

    let by_span = HotScanRequest {
        trace_ids: Some(vec![vec![1u8; 16]]),
        span_ids: Some(vec![vec![3u8; 8]]),
        ..base.clone()
    };
    let got = scan(&hot, &by_span, Duration::from_secs(5)).await.unwrap();
    assert_eq!(
        got.batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        1
    );

    let unknown = HotScanRequest {
        trace_ids: Some(vec![vec![9u8; 16]]),
        ..base
    };
    let got = scan(&hot, &unknown, Duration::from_secs(5)).await.unwrap();
    assert_eq!(
        got.batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        0
    );
    h.stop().await;
    db.finish().await;
}

/// Rows that fill the cap must be the rows that free it: admission keeps
/// room for the scratch their flush needs, so a flush under resident-only
/// pressure, with nothing to expire, still runs.
#[tokio::test]
async fn admitted_rows_can_always_be_flushed() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    // A raw 30-span batch is under 20 KiB; with its flush margin it needs
    // four times that, so one fits under 128 KiB and a second does not.
    let config = IngestConfig {
        max_memory_bytes: 128 * 1024,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    let mut admitted = 0u8;
    let refused = loop {
        match client
            .export(authed(request("api", 1 + admitted, 30), &token))
            .await
        {
            Ok(_) => admitted += 1,
            Err(status) => break status,
        }
        assert!(admitted < 8, "the margin never counted against the cap");
    };
    assert_eq!(refused.code(), Code::ResourceExhausted, "{refused:?}");
    assert!(
        refused.message().contains("ingester memory full"),
        "{refused:?}"
    );
    assert!(
        admitted >= 1,
        "nothing fits: the cap is too small for the test"
    );

    h.ingester.flush_all().await;
    assert_eq!(
        h.ingester.stats().flush_failures.load(Ordering::Relaxed),
        0,
        "the flush of the admitted rows had its room"
    );
    assert_eq!(parquet_files(&h.dir.path().join("bucket")).len(), 1);
    client
        .export(authed(request("api", 20, 30), &token))
        .await
        .expect("the flush freed the cap");
    h.stop().await;
    db.finish().await;
}

/// A flush keeps its full scratch lease until its outcome is known. While
/// the registration is pending the room is not free, so an admission in that
/// window is refused; when the registration then fails, the lease and the
/// margin trade places under the lock and nothing crosses the cap.
#[tokio::test]
async fn lease_holds_until_the_attempt_is_decided() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    // One raw 30-span batch with its margin fills most of 128 KiB: a second
    // one fits only if the pending flush let go of its lease.
    let config = IngestConfig {
        max_memory_bytes: 128 * 1024,
        inject_register_failures: 1,
        inject_register_delay: Some(Duration::from_secs(3)),
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    client
        .export(authed(request("api", 1, 30), &token))
        .await
        .unwrap();
    let pending = {
        let ingester = h.ingester.clone();
        tokio::spawn(async move { ingester.flush_all().await })
    };
    // Inside the held registration: past the encode and the upload.
    let fs = h.ingester.flush_stats();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while fs.registrations_held.load(Ordering::SeqCst) < 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "the registration was never held"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(!pending.is_finished());
    let refused = client
        .export(authed(request("api", 2, 30), &token))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::ResourceExhausted, "{refused:?}");
    assert!(
        refused.message().contains("ingester memory full"),
        "the lease is still held while the attempt is pending: {refused:?}"
    );

    pending.await.unwrap();
    assert_eq!(h.ingester.stats().flush_failures.load(Ordering::Relaxed), 1);
    // The failed rows wait with their margin; the same request is refused
    // for the same reason, and admitted once the retry writes them out.
    let refused = client
        .export(authed(request("api", 2, 30), &token))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::ResourceExhausted, "{refused:?}");
    h.ingester.flush_all().await;
    assert_eq!(parquet_files(&h.dir.path().join("bucket")).len(), 1);
    client
        .export(authed(request("api", 2, 30), &token))
        .await
        .expect("admitted once the rows are persisted");
    h.stop().await;
    db.finish().await;
}

/// Chunks and retained file batches carry their trace ids: a scan for one
/// trace skips the batches that hold none of it and reads the rest row by
/// row, with the same rows as a plain filter.
#[tokio::test]
async fn hot_scan_skips_batches_without_the_trace() {
    use fs_ingest::hot::{HotScanRequest, scan};

    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    let config = IngestConfig {
        coalesce_batches: 2,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    let fs = h.ingester.flush_stats();
    // Two chunks of two requests each: traces {1, 2} and {3, 4}.
    for (chunks, traces) in [(1, [1u8, 2]), (2, [3, 4])] {
        for trace in traces {
            client
                .export(authed(request("api", trace, 5), &token))
                .await
                .unwrap();
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while fs.coalesces.load(Ordering::Relaxed) < chunks {
            assert!(
                std::time::Instant::now() < deadline,
                "chunk {chunks} never built"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    let skipped = || {
        h.ingester
            .stats()
            .hot_batches_skipped
            .load(Ordering::Relaxed)
    };
    let lookup = |trace: u8| HotScanRequest {
        tenant,
        ts_min: None,
        ts_max: None,
        services: None,
        columns: None,
        trace_ids: Some(vec![vec![trace; 16]]),
        span_ids: None,
    };
    let hot = h.hot_addr.to_string();

    let got = scan(&hot, &lookup(3), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        got.batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        5
    );
    assert_eq!(skipped(), 1, "the chunk without trace 3 is not read");

    let got = scan(&hot, &lookup(1), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        got.batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        5
    );
    assert_eq!(skipped(), 2);

    let everything = HotScanRequest {
        trace_ids: None,
        ..lookup(0)
    };
    let got = scan(&hot, &everything, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        got.batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        20
    );
    assert_eq!(skipped(), 2, "without traces nothing is skipped");

    // After the flush the retained file batch carries all four traces.
    h.ingester.flush_all().await;
    let got = scan(&hot, &lookup(9), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        got.batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        0
    );
    assert_eq!(skipped(), 3, "the retained batch holds no trace 9");
    let got = scan(&hot, &lookup(4), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        got.batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        5
    );
    assert_eq!(skipped(), 3);
    h.stop().await;
    db.finish().await;
}

/// A request larger than a chunk is never merged, but it gets its trace set
/// on its own, so a lookup skips it like any chunk.
#[tokio::test]
async fn oversized_requests_are_indexed_alone() {
    use fs_ingest::hot::{HotScanRequest, scan};

    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    // Every request is larger than a chunk, and none waits for company.
    let config = IngestConfig {
        coalesce_bytes: 1024,
        coalesce_batches: 64,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    for trace in [1u8, 2] {
        client
            .export(authed(request("api", trace, 5), &token))
            .await
            .unwrap();
    }
    let fs = h.ingester.flush_stats();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while fs.coalesces.load(Ordering::Relaxed) < 2 {
        assert!(
            std::time::Instant::now() < deadline,
            "oversized requests never indexed"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        fs.coalesced_batches.load(Ordering::Relaxed),
        2,
        "each one alone"
    );
    let lookup = HotScanRequest {
        tenant,
        ts_min: None,
        ts_max: None,
        services: None,
        columns: None,
        trace_ids: Some(vec![vec![2u8; 16]]),
        span_ids: None,
    };
    let got = scan(&h.hot_addr.to_string(), &lookup, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        got.batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        5
    );
    assert_eq!(
        h.ingester
            .stats()
            .hot_batches_skipped
            .load(Ordering::Relaxed),
        1,
        "the other request has its trace set and is skipped"
    );
    h.stop().await;
    db.finish().await;
}

/// A flush writes the file one row group at a time and keeps each row group
/// for the hot window, so a lookup after registration skips the groups
/// that hold none of the trace.
#[tokio::test]
async fn flush_writes_row_groups_and_retains_each() {
    use fs_ingest::hot::{HotScanRequest, scan};
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    let config = IngestConfig {
        row_group_rows: 7,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    // One service per trace: sorted by service, the rows of trace 3 are the
    // last five, so the first row group holds none of them.
    for (service, trace) in [("api", 1u8), ("db", 2), ("web", 3)] {
        client
            .export(authed(request(service, trace, 5), &token))
            .await
            .unwrap();
    }
    h.ingester.flush_all().await;
    let files = parquet_files(&h.dir.path().join("bucket"));
    assert_eq!(files.len(), 1);
    let reader = SerializedFileReader::new(std::fs::File::open(&files[0]).unwrap()).unwrap();
    assert_eq!(
        reader.metadata().num_row_groups(),
        3,
        "15 rows in groups of 7"
    );
    assert_eq!(reader.metadata().file_metadata().num_rows(), 15);
    assert_eq!(
        h.ingester.flush_stats().row_groups.load(Ordering::Relaxed),
        3
    );
    assert!(
        h.ingester
            .flush_stats()
            .writer_peak_bytes
            .load(Ordering::Relaxed)
            > 0
    );

    // Every row group is served and carries its own trace set.
    let lookup = HotScanRequest {
        tenant,
        ts_min: None,
        ts_max: None,
        services: None,
        columns: None,
        trace_ids: Some(vec![vec![3u8; 16]]),
        span_ids: None,
    };
    let got = scan(&h.hot_addr.to_string(), &lookup, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        got.batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        5
    );
    assert!(
        h.ingester
            .stats()
            .hot_batches_skipped
            .load(Ordering::Relaxed)
            >= 1,
        "at least one retained row group holds no trace 3"
    );
    h.stop().await;
    db.finish().await;
}

/// An intent recorded by a newer engine names a layout this one does not
/// write. The rows stay bound to their id and nothing reaches the bucket:
/// writing other bytes under that id would break every later retry.
#[tokio::test]
async fn future_layout_version_fails_closed() {
    use fs_ingest::record::{FileLayout, FlushIntent, RecordSignal, WalRecord};

    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let dir = tempfile::tempdir().unwrap();
    // A log left by that newer engine: one request and the intent covering it.
    {
        let mut wal = Wal::open(WalConfig::new(dir.path().join("wal"))).unwrap();
        let record = WalRecord {
            tenant,
            signal: RecordSignal::Traces,
            payload: bytes::Bytes::from(request("api", 1, 5).encode_to_vec()),
        };
        let first = wal.append(&record.encode()).unwrap();
        let end = wal.position();
        let intent = FlushIntent {
            tenant,
            file: fs_catalog::FileId::new(),
            first,
            end,
            layout: Some(FileLayout {
                version: FileLayout::CURRENT_VERSION + 1,
                row_group_rows: 7,
            }),
        };
        wal.append(&intent.to_record().encode()).unwrap();
        wal.sync().unwrap();
    }
    let h = start(&db.catalog, dir, test_config()).await;
    h.ingester.flush_all().await;
    assert!(
        h.ingester.stats().flush_failures.load(Ordering::Relaxed) >= 1,
        "the attempt is refused"
    );
    assert_eq!(
        parquet_files(&h.dir.path().join("bucket")).len(),
        0,
        "nothing is written under that id"
    );
    assert_eq!(
        h.ingester.stats().flushes_recovered.load(Ordering::Relaxed),
        1,
        "the rows are still bound to the intent"
    );
    h.stop().await;
    db.finish().await;
}

/// The object is uploaded, the registration fails, and the engine restarts
/// with another row group size: the retry reproduces the intent's layout,
/// finds the same bytes in the bucket and registers them.
#[tokio::test]
async fn retry_after_restart_with_another_row_group_size_reproduces_the_object() {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    let config = IngestConfig {
        row_group_rows: 7,
        inject_register_failures: 1,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    for (service, trace) in [("api", 1u8), ("db", 2), ("web", 3)] {
        client
            .export(authed(request(service, trace, 5), &token))
            .await
            .unwrap();
    }
    h.ingester.flush_all().await;
    assert_eq!(h.ingester.stats().flush_failures.load(Ordering::Relaxed), 1);
    let bucket = h.dir.path().join("bucket");
    let written = parquet_files(&bucket);
    assert_eq!(written.len(), 1, "uploaded before the registration failed");
    let before = sha256_of(&written[0]);
    h.task.abort();
    let _ = h.task.await;
    let dir = h.dir;

    let config = IngestConfig {
        row_group_rows: 4096,
        ..test_config()
    };
    let h = start(&db.catalog, dir, config).await;
    h.ingester.flush_all().await;
    assert_eq!(
        h.ingester.stats().flush_failures.load(Ordering::Relaxed),
        0,
        "the retry reproduced the bytes the intent recorded"
    );
    let after = parquet_files(&bucket);
    assert_eq!(after, written);
    assert_eq!(sha256_of(&after[0]), before, "the object is untouched");
    let reader = SerializedFileReader::new(std::fs::File::open(&after[0]).unwrap()).unwrap();
    assert_eq!(
        reader.metadata().num_row_groups(),
        3,
        "the intent's seven-row groups, not the new configuration"
    );
    let registered = db
        .catalog
        .visible_files(
            tenant,
            Signal::Traces,
            TimeRange {
                start: chrono::DateTime::UNIX_EPOCH,
                end: chrono::Utc::now(),
            },
        )
        .await
        .unwrap();
    assert_eq!(registered.len(), 1);
    assert_eq!(registered[0].row_count, 15);
    h.stop().await;
    db.finish().await;
}

/// An intent whose layout this engine cannot even read, left by a newer
/// one, stops the replay: the log must not be served by an engine that
/// would write those rows as other bytes under the same id.
#[tokio::test]
async fn unreadable_intent_refuses_to_start() {
    use fs_ingest::record::{RecordSignal, WalRecord};

    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let dir = tempfile::tempdir().unwrap();
    let file = fs_catalog::FileId::new();
    {
        let mut wal = Wal::open(WalConfig::new(dir.path().join("wal"))).unwrap();
        let record = WalRecord {
            tenant,
            signal: RecordSignal::Traces,
            payload: bytes::Bytes::from(request("api", 1, 5).encode_to_vec()),
        };
        let first = wal.append(&record.encode()).unwrap();
        let end = wal.position();
        // A layout with a shape this version does not know.
        let payload = format!(
            r#"{{"tenant":"{}","file":"{}","first":{{"segment":{},"offset":{}}},"end":{{"segment":{},"offset":{}}},"layout":{{"version":9,"codec":"zstd","groups":[7]}}}}"#,
            tenant.0, file.0, first.segment, first.offset, end.segment, end.offset
        );
        let intent = WalRecord {
            tenant,
            signal: RecordSignal::FlushIntent,
            payload: bytes::Bytes::from(payload),
        };
        wal.append(&intent.encode()).unwrap();
        wal.sync().unwrap();
    }
    std::fs::create_dir_all(dir.path().join("bucket")).unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path().join("bucket")).unwrap());
    let wal = Wal::open(WalConfig::new(dir.path().join("wal"))).unwrap();
    let outcome = Ingester::new(
        test_config(),
        db.catalog.clone(),
        store,
        Path::from(""),
        wal,
    );
    let err = match outcome {
        Ok(_) => panic!("an unreadable intent must stop the start"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("cannot be read"), "{err}");
    assert_eq!(parquet_files(&dir.path().join("bucket")).len(), 0);
    db.finish().await;
}
