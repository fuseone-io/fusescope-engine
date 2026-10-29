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
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
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
    request_at(service, trace, spans, T0)
}

/// `spans` spans of one trace, a millisecond apart from `start`.
fn request_at(service: &str, trace: u8, spans: u8, start: u64) -> ExportTraceServiceRequest {
    let spans = (0..spans)
        .map(|i| Span {
            trace_id: vec![trace; 16],
            span_id: vec![i + 1; 8],
            name: format!("op-{i}"),
            kind: 2,
            start_time_unix_nano: start + u64::from(i) * 1_000_000,
            end_time_unix_nano: start + u64::from(i) * 1_000_000 + 500_000,
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
    start_with_store(catalog, dir, config, wal_config, store).await
}

async fn start_with_store(
    catalog: &Catalog,
    dir: tempfile::TempDir,
    config: IngestConfig,
    wal_config: WalConfig,
    store: Arc<dyn ObjectStore>,
) -> Harness {
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

/// A store that holds every put under `held` until the gate opens, so one
/// tenant's flush is as slow as the test wants and nothing else is;
/// `retained` counts the puts that met the gate closed and `arrived` wakes
/// whoever waits for one, so a test knows the slow flush has its permit.
#[derive(Debug)]
struct HeldStore {
    inner: Arc<dyn ObjectStore>,
    held: String,
    gate: tokio::sync::watch::Receiver<bool>,
    retained: Arc<std::sync::atomic::AtomicUsize>,
    arrived: Arc<tokio::sync::Notify>,
}

impl std::fmt::Display for HeldStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HeldStore({})", self.held)
    }
}

#[async_trait::async_trait]
impl ObjectStore for HeldStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if location.as_ref().starts_with(&self.held) && !*self.gate.borrow() {
            self.retained.fetch_add(1, Ordering::SeqCst);
            self.arrived.notify_waiters();
            let mut gate = self.gate.clone();
            while !*gate.borrow() {
                if gate.changed().await.is_err() {
                    break;
                }
            }
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
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
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
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
    files_with_extension(dir, "parquet")
}

fn sidecar_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    files_with_extension(dir, "tix")
}

fn files_with_extension(dir: &std::path::Path, extension: &str) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(files_with_extension(&path, extension));
            } else if path.extension().is_some_and(|e| e == extension) {
                out.push(path);
            }
        }
    }
    out.sort();
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
        distinct: None,
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
        distinct: None,
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

    // A distinct scan answers each value of its one column once, in the
    // column's declared type, however many rows carry it.
    let names = HotScanRequest {
        columns: Some(vec!["service_name".into()]),
        distinct: Some(10),
        ..all.clone()
    };
    let got = scan(&addr, &names, Duration::from_secs(5)).await.unwrap();
    assert_eq!(got.batches.len(), 1, "one batch");
    let column =
        arrow::compute::cast(got.batches[0].column(0), &arrow::datatypes::DataType::Utf8).unwrap();
    let values: Vec<&str> = arrow::array::AsArray::as_string::<i32>(&column)
        .iter()
        .map(|v| v.unwrap())
        .collect();
    assert_eq!(values, ["api", "worker"]);
    let capped = scan(
        &addr,
        &HotScanRequest {
            columns: Some(vec!["service_name".into()]),
            distinct: Some(1),
            ..all.clone()
        },
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(
        capped
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1,
        "the ingester keeps no more than the cap"
    );
    let err = scan(
        &addr,
        &HotScanRequest {
            columns: Some(vec!["timestamp".into(), "name".into()]),
            distinct: Some(10),
            ..all.clone()
        },
        Duration::from_secs(5),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("exactly one column"), "{err}");

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

/// Whether each registered file is confirmed, oldest registration first.
async fn confirmations(db: &TestDb) -> Vec<bool> {
    sqlx::query_scalar("select confirmed_at is not null from engine_file order by created_at")
        .fetch_all(db.catalog.pool())
        .await
        .unwrap()
}

/// A confirmation that fails waits for its backoff and is retried off the
/// flusher: a flush meanwhile does not retry it, the pending count is
/// published until it lands, and it lands without another flush.
#[tokio::test]
async fn a_failed_confirmation_is_retried_after_its_backoff() {
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
        inject_confirm_failures: 1,
        confirm_interval: Duration::from_millis(50),
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    h.client()
        .await
        .export(authed(request("api", 7, 10), &token))
        .await
        .unwrap();
    h.ingester.flush_all().await;
    let stats = h.ingester.flush_stats();
    assert_eq!(stats.confirm_failures.load(Ordering::Relaxed), 1);
    assert_eq!(stats.unconfirmed.load(Ordering::Relaxed), 1);
    h.ingester.flush_all().await;
    assert_eq!(
        confirmations(&db).await,
        [false],
        "a flush does not retry it and the backoff has not passed"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while confirmations(&db).await != [true] {
        assert!(tokio::time::Instant::now() < deadline, "never confirmed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(stats.unconfirmed.load(Ordering::Relaxed), 0);
    h.stop().await;
    db.finish().await;
}

/// One tenant's flush stuck in its upload holds one permit and nothing
/// else: the other tenants keep flushing through the permits left, again
/// and again, and the stuck tenant's file lands once its upload returns.
#[tokio::test]
async fn a_slow_tenants_flush_does_not_hold_the_others() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let mut tenants = Vec::new();
    for name in ["slow", "b", "c"] {
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
        tenants.push((tenant, token));
    }
    let (slow, slow_token) = tenants[0].clone();
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("bucket")).unwrap();
    let (open, gate) = tokio::sync::watch::channel(false);
    let retained = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let arrived = Arc::new(tokio::sync::Notify::new());
    let store: Arc<dyn ObjectStore> = Arc::new(HeldStore {
        inner: Arc::new(LocalFileSystem::new_with_prefix(dir.path().join("bucket")).unwrap()),
        held: format!("t/{}/", slow.0),
        gate,
        retained: Arc::clone(&retained),
        arrived: Arc::clone(&arrived),
    });
    let config = IngestConfig {
        max_flushes: 2,
        flush_interval: Duration::from_millis(200),
        ..test_config()
    };
    let wal_config = WalConfig::new(dir.path().join("wal"));
    let h = start_with_store(&db.catalog, dir, config, wal_config, store).await;
    let mut client = h.client().await;
    let registered = |tenant: TenantId| {
        let catalog = db.catalog.clone();
        async move {
            catalog
                .all_visible_files(tenant, Signal::Traces)
                .await
                .unwrap()
                .len()
        }
    };
    let wait_for = |tenant: TenantId, files: usize, what: &'static str| async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while registered(tenant).await < files {
            assert!(tokio::time::Instant::now() < deadline, "{what}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };

    // The slow tenant exports first, and only once its upload is waiting
    // at the gate, holding its permit, do the others export.
    client
        .export(authed(request("api", 1, 5), &slow_token))
        .await
        .unwrap();
    let arrival = tokio::time::timeout(Duration::from_secs(10), async {
        while retained.load(Ordering::SeqCst) == 0 {
            arrived.notified().await;
        }
    });
    arrival
        .await
        .expect("the slow tenant's upload reached the gate");
    assert_eq!(retained.load(Ordering::SeqCst), 1);
    for (i, (_, token)) in tenants.iter().enumerate().skip(1) {
        client
            .export(authed(
                request("api", u8::try_from(i).unwrap() + 1, 5),
                token,
            ))
            .await
            .unwrap();
    }
    wait_for(tenants[1].0, 1, "b flushed beside the held upload").await;
    wait_for(tenants[2].0, 1, "c flushed beside the held upload").await;
    assert_eq!(
        registered(slow).await,
        0,
        "the slow tenant is still uploading"
    );

    // And again: the permit the slow tenant holds is the only one it holds.
    client
        .export(authed(request("api", 9, 5), &tenants[1].1))
        .await
        .unwrap();
    client
        .export(authed(request("api", 8, 5), &tenants[2].1))
        .await
        .unwrap();
    wait_for(tenants[1].0, 2, "b flushed a second time").await;
    wait_for(tenants[2].0, 2, "c flushed a second time").await;
    assert_eq!(registered(slow).await, 0);
    assert!(
        h.ingester
            .flush_stats()
            .flushing_bytes
            .load(Ordering::Relaxed)
            > 0,
        "the held flush is still accounted as flushing"
    );

    // The upload returns: the slow tenant's file lands like any other.
    open.send(true).unwrap();
    wait_for(slow, 1, "the slow tenant flushed once released").await;
    assert_eq!(
        retained.load(Ordering::SeqCst),
        1,
        "the file was held once and never retried; the sidecar met the gate open"
    );
    h.stop().await;
    db.finish().await;
}

/// A confirmation the catalog holds takes no flush permit and does not
/// hold the stop: the flush returns once its rows are retained, the next
/// flushes get the only flush permit, and the stop cancels the held
/// attempt, waits for it to give its entry back, and confirms everything
/// due in its last pass, refilling the single confirmation permit.
#[tokio::test]
async fn a_held_confirmation_holds_neither_a_flush_permit_nor_the_stop() {
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
        max_flushes: 1,
        max_confirms: 1,
        inject_confirm_failures: 1,
        inject_confirm_delay: Some(Duration::from_secs(30)),
        confirm_timeout: Duration::from_secs(60),
        confirm_interval: Duration::from_millis(50),
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    for trace in 7..10 {
        client
            .export(authed(request("api", trace, 10), &token))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), h.ingester.flush_all())
            .await
            .expect("a flush neither waits for its confirmation nor for the held one");
    }
    assert_eq!(parquet_files(&h.dir.path().join("bucket")).len(), 3);
    assert_eq!(
        h.ingester
            .flush_stats()
            .confirm_failures
            .load(Ordering::Relaxed),
        0,
        "still held, not failed"
    );
    assert_eq!(confirmations(&db).await, [false, false, false]);
    tokio::time::timeout(Duration::from_secs(5), h.stop())
        .await
        .expect("a held confirmation does not hold the stop");
    assert_eq!(
        confirmations(&db).await,
        [true, true, true],
        "the stop gave the attempt back and its last pass confirmed everything due"
    );
    db.finish().await;
}

/// A backlog of confirmations due drains at `max_confirms` calls at a
/// time, a finished attempt starting the next, not `max_confirms` per
/// interval.
#[tokio::test]
async fn a_backlog_of_confirmations_drains_without_waiting_for_the_interval() {
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
        max_confirms: 1,
        inject_confirm_failures: 3,
        confirm_interval: Duration::from_secs(5),
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    for trace in 7..10 {
        client
            .export(authed(request("api", trace, 10), &token))
            .await
            .unwrap();
        h.ingester.flush_all().await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !confirmations(&db).await.contains(&true) {
        assert!(tokio::time::Instant::now() < deadline, "no retry ever ran");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        confirmations(&db).await,
        [true, true, true],
        "the rest followed within the same interval"
    );
    h.stop().await;
    db.finish().await;
}

/// Crash after the registration, before its confirmation: the checkpoint
/// stayed behind the intent, so the restart replays the flush, meets the
/// same row and confirms it.
#[tokio::test]
async fn crash_before_the_confirmation_is_replayed_and_confirmed() {
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
    let never_confirms = IngestConfig {
        inject_confirm_failures: u32::MAX,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), never_confirms).await;
    h.client()
        .await
        .export(authed(request("api", 7, 10), &token))
        .await
        .unwrap();
    h.ingester.flush_all().await; // registered, checkpointed, never confirmed
    assert_eq!(confirmations(&db).await, [false]);
    h.task.abort();
    let _ = h.task.await;
    let dir = h.dir;

    let h = start(&db.catalog, dir, test_config()).await;
    assert!(
        h.ingester.stats().flushes_recovered.load(Ordering::Relaxed) >= 1,
        "the checkpoint stayed behind the unconfirmed intent"
    );
    h.ingester.flush_all().await;
    // The replayed flush confirms off the flusher: wait for it to land.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while confirmations(&db).await != [true] {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the replay never confirmed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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
    assert_eq!(
        registered.iter().map(|f| f.row_count).collect::<Vec<_>>(),
        [10]
    );
    h.stop().await;
    db.finish().await;
}

/// Registered files of `tenant`, oldest range first: `(hour directory, rows, range)`.
async fn registered_by_hour(db: &TestDb, tenant: TenantId) -> Vec<(String, i64, TimeRange)> {
    let mut files = db
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
    files.sort_by_key(|f| f.range.start);
    files
        .into_iter()
        .map(|f| {
            let hour = f
                .path
                .split('/')
                .find(|part| part.starts_with("hr="))
                .unwrap()
                .to_owned();
            (hour, f.row_count, f.range)
        })
        .collect()
}

/// Spans of two hours in one buffer are cut into one file per hour, each
/// within its hour and under its own id, both announced by the hot window
/// before the flush; a crash before the checkpoint replays both intents
/// to the same two files, byte for byte, without a duplicate.
#[tokio::test]
async fn a_flush_with_spans_across_an_hour_boundary_writes_one_file_per_hour() {
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
    let no_checkpoint = IngestConfig {
        checkpoint_after_flush: false,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), no_checkpoint).await;
    let mut client = h.client().await;
    // T0 is 14:17:03Z: the second request starts at 14:59:59.999 and
    // straddles 15:00 with its three spans a millisecond apart.
    client
        .export(authed(request_at("api", 1, 4, T0), &token))
        .await
        .unwrap();
    client
        .export(authed(
            request_at("api", 2, 3, T0 + (42 * 60 + 57) * 1_000_000_000 - 1_000_000),
            &token,
        ))
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
        distinct: None,
    };
    let announced = scan(&h.hot_addr.to_string(), &all, Duration::from_secs(5))
        .await
        .unwrap()
        .persisted_files;

    h.ingester.flush_all().await;
    let bucket = h.dir.path().join("bucket");
    let files = parquet_files(&bucket);
    assert_eq!(files.len(), 2, "one file per hour");
    let digests: Vec<_> = files.iter().map(|f| (f.clone(), sha256_of(f))).collect();
    let registered = registered_by_hour(&db, tenant).await;
    assert_eq!(
        registered
            .iter()
            .map(|(hour, rows, _)| (hour.as_str(), *rows))
            .collect::<Vec<_>>(),
        [("hr=14", 5), ("hr=15", 2)]
    );
    for (_, _, range) in &registered {
        assert_eq!(
            range.start.format("%Y-%m-%dT%H").to_string(),
            range.end.format("%Y-%m-%dT%H").to_string(),
            "a file stays within its hour"
        );
    }
    assert_eq!(sidecar_files(&bucket).len(), 2, "a sidecar per file");
    let ids: Vec<_> = db
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
        .unwrap()
        .into_iter()
        .map(|f| f.id)
        .collect();
    assert!(
        ids.iter().all(|id| announced.contains(id)),
        "both ids were announced before the flush: {announced:?} vs {ids:?}"
    );

    h.task.abort();
    let _ = h.task.await;
    let h = start(&db.catalog, h.dir, test_config()).await;
    assert!(
        h.ingester.stats().flushes_recovered.load(Ordering::Relaxed) >= 2,
        "both intents were resumed"
    );
    h.ingester.flush_all().await;
    assert_eq!(parquet_files(&bucket).len(), 2, "no third file");
    for (file, digest) in digests {
        assert_eq!(sha256_of(&file), digest, "the replay reproduced {file:?}");
    }
    assert_eq!(registered_by_hour(&db, tenant).await.len(), 2);
    h.stop().await;
    db.finish().await;
}

/// The registration of the first hour's file fails after its upload and
/// the ingester dies: the restart registers it from the object it finds
/// and leaves the second hour's file as it was.
#[tokio::test]
async fn crash_between_the_files_of_a_split_flush_is_resumed() {
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
        checkpoint_after_flush: false,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    client
        .export(authed(request_at("api", 1, 2, T0), &token))
        .await
        .unwrap();
    client
        .export(authed(
            request_at("api", 2, 2, T0 + 60 * 60 * 1_000_000_000),
            &token,
        ))
        .await
        .unwrap();
    h.ingester.flush_all().await;
    assert_eq!(h.ingester.stats().flush_failures.load(Ordering::Relaxed), 1);
    let bucket = h.dir.path().join("bucket");
    assert_eq!(
        parquet_files(&bucket).len(),
        2,
        "both objects were uploaded"
    );
    assert_eq!(registered_by_hour(&db, tenant).await.len(), 1);
    h.task.abort();
    let _ = h.task.await;

    let h = start(&db.catalog, h.dir, test_config()).await;
    h.ingester.flush_all().await;
    assert_eq!(parquet_files(&bucket).len(), 2);
    assert_eq!(
        registered_by_hour(&db, tenant)
            .await
            .iter()
            .map(|(hour, rows, _)| (hour.as_str(), *rows))
            .collect::<Vec<_>>(),
        [("hr=14", 2), ("hr=15", 2)]
    );
    h.stop().await;
    db.finish().await;
}

/// The second hour's intent cannot be logged, so its rows go back live
/// and are cut later under an intent of their own hour over a range that
/// also holds the first hour's rows. That intent claims only its hour, so
/// a crash and replay reproduce both files, byte for byte, and the first
/// hour is never written twice.
#[tokio::test]
async fn a_part_left_live_by_a_failed_intent_keeps_its_hour_through_a_replay() {
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
        inject_intent_failures: 1,
        checkpoint_after_flush: false,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    client
        .export(authed(request_at("api", 1, 2, T0), &token))
        .await
        .unwrap();
    // 14:59:59.999: one span of 14h and two of 15h in one record.
    client
        .export(authed(
            request_at("api", 2, 3, T0 + (42 * 60 + 57) * 1_000_000_000 - 1_000_000),
            &token,
        ))
        .await
        .unwrap();
    // The first pass logs one intent and fails the other; the rows left
    // live are cut by the next pass of the same call.
    h.ingester.flush_all().await;
    assert_eq!(h.ingester.stats().flush_failures.load(Ordering::Relaxed), 1);
    let bucket = h.dir.path().join("bucket");
    let files = parquet_files(&bucket);
    assert_eq!(files.len(), 2, "the rows left live made their own file");
    let digests: Vec<_> = files.iter().map(|f| (f.clone(), sha256_of(f))).collect();
    let counts = |registered: Vec<(String, i64, TimeRange)>| {
        registered
            .into_iter()
            .map(|(hour, rows, _)| (hour, rows))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        counts(registered_by_hour(&db, tenant).await),
        [("hr=14".to_owned(), 3), ("hr=15".to_owned(), 2)]
    );

    h.task.abort();
    let _ = h.task.await;
    let h = start(&db.catalog, h.dir, test_config()).await;
    assert!(h.ingester.stats().flushes_recovered.load(Ordering::Relaxed) >= 2);
    h.ingester.flush_all().await;
    assert_eq!(
        h.ingester.stats().flush_failures.load(Ordering::Relaxed),
        0,
        "the replay met the objects it expected"
    );
    assert_eq!(parquet_files(&bucket).len(), 2);
    for (file, digest) in digests {
        assert_eq!(sha256_of(&file), digest, "the replay reproduced {file:?}");
    }
    assert_eq!(
        counts(registered_by_hour(&db, tenant).await),
        [("hr=14".to_owned(), 3), ("hr=15".to_owned(), 2)]
    );
    h.stop().await;
    db.finish().await;
}

/// A cut flushes at most `max_hours_per_cut` hours, oldest first, and the
/// rows of later hours stay live; one stop still flushes every hour, cut
/// after cut, and a replay reproduces every file.
#[tokio::test]
async fn a_stop_flushes_every_hour_however_many_a_cut_takes() {
    const HOUR: u64 = 3_600_000_000_000;

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
        max_hours_per_cut: 2,
        checkpoint_after_flush: false,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    for (trace, hours_later) in [(1, 2), (2, 0), (3, 1)] {
        client
            .export(authed(
                request_at("api", trace, 1, T0 + hours_later * HOUR),
                &token,
            ))
            .await
            .unwrap();
    }
    let bucket = h.dir.path().join("bucket");
    let dir = h.stop().await;
    let hours = |registered: Vec<(String, i64, TimeRange)>| {
        registered
            .into_iter()
            .map(|(hour, _, _)| hour)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        hours(registered_by_hour(&db, tenant).await),
        ["hr=14", "hr=15", "hr=16"],
        "the stop cut twice"
    );
    let files = parquet_files(&bucket);
    assert_eq!(files.len(), 3);
    let digests: Vec<_> = files.iter().map(|f| (f.clone(), sha256_of(f))).collect();

    let h = start(&db.catalog, dir, test_config()).await;
    h.ingester.flush_all().await;
    assert_eq!(parquet_files(&bucket).len(), 3);
    for (file, digest) in digests {
        assert_eq!(sha256_of(&file), digest, "the replay reproduced {file:?}");
    }
    h.stop().await;
    db.finish().await;
}

/// A request whose spans fall in more hours than allowed is refused before
/// the log; a tenant whose live rows would span more hours than allowed is
/// refused until a flush drains them.
#[tokio::test]
async fn requests_are_bounded_by_the_hours_they_span_and_the_hours_live() {
    const HOUR: u64 = 3_600_000_000_000;

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
        max_hours_per_batch: 2,
        max_live_hours: 3,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    let mut three_hours = request_at("api", 9, 3, T0);
    for (i, span) in three_hours.resource_spans[0].scope_spans[0]
        .spans
        .iter_mut()
        .enumerate()
    {
        span.start_time_unix_nano = T0 + u64::try_from(i).unwrap() * HOUR;
        span.end_time_unix_nano = span.start_time_unix_nano + 1;
    }
    let status = client
        .export(authed(three_hours, &token))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
    assert!(status.message().contains("3 calendar hours"), "{status:?}");

    for hours_later in 0..3 {
        client
            .export(authed(
                request_at("api", 1, 1, T0 + hours_later * HOUR),
                &token,
            ))
            .await
            .unwrap();
    }
    let status = client
        .export(authed(request_at("api", 2, 1, T0 + 3 * HOUR), &token))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
    assert!(status.message().contains("too many hours"), "{status:?}");
    assert!(h.ingester.stats().over_capacity.load(Ordering::Relaxed) >= 1);
    client
        .export(authed(request_at("api", 3, 1, T0 + HOUR), &token))
        .await
        .expect("an hour already live costs nothing");

    h.ingester.flush_all().await;
    client
        .export(authed(request_at("api", 2, 1, T0 + 3 * HOUR), &token))
        .await
        .expect("flushed hours no longer count");
    h.stop().await;
    db.finish().await;
}

/// The hours a tenant holds count a request from its admission, not its
/// push: two requests of new hours admitted together cannot both fit under
/// the cap.
#[tokio::test]
async fn the_live_hours_cap_counts_requests_admitted_but_not_yet_pushed() {
    const HOUR: u64 = 3_600_000_000_000;

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
        max_hours_per_batch: 1,
        max_live_hours: 1,
        inject_push_delay: Some(Duration::from_secs(2)),
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client_a = h.client().await;
    let token_a = token.clone();
    let a = tokio::spawn(async move {
        client_a
            .export(authed(request_at("api", 1, 1, T0), &token_a))
            .await
    });
    // Once A's record is synced, A sleeps before its push: its hour is
    // admitted and not yet in the buffer.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while h.ingester.writer_stats().records.load(Ordering::Relaxed) < 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "first request never reached the wal"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let status = h
        .client()
        .await
        .export(authed(request_at("api", 2, 1, T0 + HOUR), &token))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
    assert!(status.message().contains("too many hours"), "{status:?}");
    a.await.unwrap().unwrap();
    h.client()
        .await
        .export(authed(request_at("api", 3, 1, T0), &token))
        .await
        .expect("the hour A holds costs nothing more");
    h.stop().await;
    db.finish().await;
}

/// A tenant refused a request for the hours it holds is cut by the next
/// flusher pass, whatever its rows, bytes and age, so the request is
/// admitted again without waiting for the flush interval.
#[tokio::test]
async fn a_tenant_refused_for_its_hours_is_cut_without_waiting_for_the_interval() {
    const HOUR: u64 = 3_600_000_000_000;

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
        max_hours_per_batch: 1,
        max_live_hours: 2,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    for hours_later in 0..2 {
        client
            .export(authed(
                request_at("api", 1, 1, T0 + hours_later * HOUR),
                &token,
            ))
            .await
            .unwrap();
    }
    let third = || authed(request_at("api", 2, 1, T0 + 2 * HOUR), &token);
    let status = client.export(third()).await.unwrap_err();
    assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while client.export(third()).await.is_err() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the refusal did not make the tenant due"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // The cut freed the hours before its file was written; the write lands.
    while h.ingester.stats().files_flushed.load(Ordering::Relaxed) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the flusher cut the tenant on its own"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    h.stop().await;
    db.finish().await;
}

/// The hours of a cut count until its intent is durable and while its
/// rows are flushed: a new hour cannot slip in between the cut and the
/// log; an intent that fails brings the rows back under the cap, and the
/// refusal that made the tenant due outlives it, so the flusher cuts the
/// rows again on its own.
#[tokio::test]
async fn hours_being_cut_still_count_until_their_intent_holds() {
    const HOUR: u64 = 3_600_000_000_000;

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
        max_hours_per_batch: 1,
        max_live_hours: 2,
        inject_intent_failures: 1,
        inject_intent_delay: Some(Duration::from_secs(2)),
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    for hours_later in 0..2 {
        client
            .export(authed(
                request_at("api", 1, 1, T0 + hours_later * HOUR),
                &token,
            ))
            .await
            .unwrap();
    }
    // The refusal makes the tenant due; the flusher cuts both hours and
    // the first intent is held, then fails.
    let third = || authed(request_at("api", 2, 1, T0 + 2 * HOUR), &token);
    let status = client.export(third()).await.unwrap_err();
    assert_eq!(status.code(), Code::ResourceExhausted, "{status:?}");
    let held = h.ingester.flush_stats();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while held.intents_held.load(Ordering::SeqCst) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the flusher never cut the pressed tenant"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let status = client.export(third()).await.unwrap_err();
    assert_eq!(
        status.code(),
        Code::ResourceExhausted,
        "the hours being cut still count: {status:?}"
    );

    // The failed intent's rows come back live and the flusher, not a
    // flush_all, cuts them again: the pressure survived the failure.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let hours: Vec<_> = registered_by_hour(&db, tenant)
            .await
            .into_iter()
            .map(|(hour, _, _)| hour)
            .collect();
        if hours == ["hr=14", "hr=15"] {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the rows of the failed intent were not cut again: {hours:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(h.ingester.stats().flush_failures.load(Ordering::Relaxed), 1);
    client
        .export(third())
        .await
        .expect("flushed hours no longer count");
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
        distinct: None,
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

    h.ingester.flush_all().await; // registers files; the confirmations land off the flusher
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match client.export(authed(request("api", 1, 1), &token)).await {
            Ok(_) => break,
            Err(status) => {
                assert_eq!(status.code(), Code::Unavailable, "{status:?}");
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "writes resume once the confirmations checkpointed the old segments away"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
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
        distinct: None,
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
        distinct: None,
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
        distinct: None,
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
        distinct: None,
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
        distinct: None,
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
        distinct: None,
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
        distinct: None,
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
        distinct: None,
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
        distinct: None,
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

/// Every flush leaves a `trace_id` sidecar next to its file, under the
/// path the querier derives from the catalog row, listing exactly the
/// traces the file holds.
#[tokio::test]
async fn flush_writes_a_trace_index_sidecar_listing_its_traces() {
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
    for (service, trace) in [("api", 1u8), ("db", 2), ("web", 3)] {
        client
            .export(authed(request(service, trace, 5), &token))
            .await
            .unwrap();
    }
    h.ingester.flush_all().await;
    let bucket = h.dir.path().join("bucket");
    let sidecars = sidecar_files(&bucket);
    assert_eq!(sidecars.len(), 1);
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
        sidecars[0],
        bucket.join(fs_index::sidecar_path(&registered[0].path).unwrap())
    );
    let index = fs_index::TraceIndex::decode(&std::fs::read(&sidecars[0]).unwrap()).unwrap();
    assert_eq!(index.files(), std::slice::from_ref(&registered[0].path));
    assert_eq!(index.len(), 3);
    for trace in 1u8..=3 {
        assert!(index.contains_any(&[[trace; 16]]), "trace {trace}");
    }
    assert!(!index.contains_any(&[[4u8; 16]]));
    assert_eq!(
        h.ingester
            .flush_stats()
            .trace_index_bytes
            .load(Ordering::Relaxed),
        std::fs::metadata(&sidecars[0]).unwrap().len()
    );
    h.stop().await;
    db.finish().await;
}

/// A crash after the Parquet upload and before the sidecar leaves an
/// unregistered object. The retry after restart finds it, writes the
/// sidecar from the same rows and registers the file once.
#[tokio::test]
async fn crash_between_parquet_and_sidecar_is_resumed() {
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
        inject_index_failures: 1,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    for (service, trace) in [("api", 1u8), ("db", 2)] {
        client
            .export(authed(request(service, trace, 4), &token))
            .await
            .unwrap();
    }
    h.ingester.flush_all().await;
    assert_eq!(h.ingester.stats().flush_failures.load(Ordering::Relaxed), 1);
    let bucket = h.dir.path().join("bucket");
    let written = parquet_files(&bucket);
    assert_eq!(
        written.len(),
        1,
        "the object was uploaded before the failure"
    );
    assert_eq!(sidecar_files(&bucket).len(), 0);
    let before = sha256_of(&written[0]);
    h.task.abort();
    let _ = h.task.await;
    let dir = h.dir;

    let h = start(&db.catalog, dir, test_config()).await;
    h.ingester.flush_all().await;
    assert_eq!(h.ingester.stats().flush_failures.load(Ordering::Relaxed), 0);
    assert_eq!(parquet_files(&bucket), written);
    assert_eq!(sha256_of(&written[0]), before, "the object is untouched");
    let sidecars = sidecar_files(&bucket);
    assert_eq!(sidecars.len(), 1);
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
    assert_eq!(registered[0].row_count, 8);
    let index = fs_index::TraceIndex::decode(&std::fs::read(&sidecars[0]).unwrap()).unwrap();
    assert_eq!(index.files(), std::slice::from_ref(&registered[0].path));
    assert!(index.contains_any(&[[1u8; 16]]) && index.contains_any(&[[2u8; 16]]));
    h.stop().await;
    db.finish().await;
}

/// A sidecar already under the file's path that is valid but lists other
/// traces would make the querier skip the file: the retry refuses it and
/// the file stays unregistered, with the object and the sidecar untouched.
#[tokio::test]
async fn a_foreign_sidecar_under_the_files_path_blocks_registration() {
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
        inject_index_failures: 1,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    for (service, trace) in [("api", 1u8), ("db", 2)] {
        client
            .export(authed(request(service, trace, 4), &token))
            .await
            .unwrap();
    }
    h.ingester.flush_all().await;
    let bucket = h.dir.path().join("bucket");
    let written = parquet_files(&bucket);
    assert_eq!(written.len(), 1);
    let relative = written[0].strip_prefix(&bucket).unwrap().to_str().unwrap();
    let sidecar = bucket.join(fs_index::sidecar_path(relative).unwrap());
    std::fs::create_dir_all(sidecar.parent().unwrap()).unwrap();
    let foreign = fs_index::TraceIndex::new(vec![relative.to_owned()], [([1u8; 16], 0)])
        .unwrap()
        .encode();
    std::fs::write(&sidecar, &foreign).unwrap();
    h.task.abort();
    let _ = h.task.await;
    let dir = h.dir;

    let h = start(&db.catalog, dir, test_config()).await;
    h.ingester.flush_all().await;
    assert!(h.ingester.stats().flush_failures.load(Ordering::Relaxed) >= 1);
    assert_eq!(
        std::fs::read(&sidecar).unwrap(),
        foreign,
        "the sidecar is not overwritten"
    );
    assert_eq!(parquet_files(&bucket), written);
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
    assert_eq!(
        registered.len(),
        0,
        "never registered behind an index that hides it"
    );
    h.stop().await;
    db.finish().await;
}

/// A registration refused because the orphan sweep marked the object: the
/// rows take a fresh id through a second intent, the retry writes the same
/// bytes under the new path and registers them, the marked path is never
/// written again, and a restart before the checkpoint keeps the new id.
#[tokio::test]
async fn a_flush_refused_by_the_orphan_sweep_takes_a_fresh_id() {
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
        checkpoint_after_flush: false,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = h.client().await;
    client
        .export(authed(request("api", 1, 5), &token))
        .await
        .unwrap();
    h.ingester.flush_all().await;
    assert_eq!(h.ingester.stats().flush_failures.load(Ordering::Relaxed), 1);
    let bucket = h.dir.path().join("bucket");
    let written = parquet_files(&bucket);
    assert_eq!(written.len(), 1);
    let old = written[0].clone();
    let bytes = sha256_of(&old);
    let old_relative = old
        .strip_prefix(&bucket)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    // The sweep marks the object: the retry is refused and the rows get a
    // fresh id, logged as a second intent.
    db.catalog
        .orphans_seen(std::slice::from_ref(&old_relative))
        .await
        .unwrap();
    assert_eq!(
        db.catalog
            .orphan_doom(&old_relative, std::slice::from_ref(&old_relative))
            .await
            .unwrap(),
        fs_catalog::Doom::Marked
    );
    h.ingester.flush_all().await;
    assert_eq!(h.ingester.stats().flush_failures.load(Ordering::Relaxed), 2);
    assert_eq!(
        h.ingester.flush_stats().superseded.load(Ordering::Relaxed),
        1
    );
    let visible = |catalog: Catalog| async move {
        catalog
            .visible_files(
                tenant,
                Signal::Traces,
                TimeRange {
                    start: chrono::DateTime::UNIX_EPOCH,
                    end: chrono::Utc::now(),
                },
            )
            .await
            .unwrap()
    };
    assert_eq!(visible(db.catalog.clone()).await.len(), 0);

    // The hot window keeps announcing the id the rows were first given,
    // which the file registers as its origin, so a querier that scanned
    // the hot window before the change still leaves the file out.
    let old_id = fs_catalog::FileId(
        uuid::Uuid::parse_str(old.file_stem().unwrap().to_str().unwrap()).unwrap(),
    );
    let everything = fs_ingest::hot::HotScanRequest {
        tenant,
        ts_min: None,
        ts_max: None,
        services: None,
        columns: None,
        trace_ids: None,
        span_ids: None,
        distinct: None,
    };
    let scanned =
        fs_ingest::hot::scan(&h.hot_addr.to_string(), &everything, Duration::from_secs(5))
            .await
            .unwrap();
    assert!(
        scanned.persisted_files.contains(&old_id),
        "announced by its first id after the change"
    );

    // The next attempt writes the same bytes under the new path and
    // registers them; the marked object is left as it is.
    h.ingester.flush_all().await;
    assert_eq!(h.ingester.stats().flush_failures.load(Ordering::Relaxed), 2);
    let now = parquet_files(&bucket);
    assert_eq!(now.len(), 2, "the marked object and the new one");
    let new = now.iter().find(|p| **p != old).unwrap().clone();
    assert_eq!(sha256_of(&new), bytes, "same bytes under the new id");
    assert_eq!(sha256_of(&old), bytes, "the marked object untouched");
    let registered = visible(db.catalog.clone()).await;
    assert_eq!(registered.len(), 1);
    assert_eq!(bucket.join(&registered[0].path), new);
    assert_ne!(registered[0].path, old_relative);
    assert_eq!(
        registered[0].origin, old_id,
        "registered with its first id as origin"
    );
    assert_ne!(registered[0].id, old_id);

    // Crash before the checkpoint: replay sees both intents for these
    // records and keeps the last id, so the retry finds its row and
    // nothing is written or registered twice.
    h.task.abort();
    let _ = h.task.await;
    let dir = h.dir;
    let h = start(&db.catalog, dir, test_config()).await;
    let scanned =
        fs_ingest::hot::scan(&h.hot_addr.to_string(), &everything, Duration::from_secs(5))
            .await
            .unwrap();
    assert!(
        scanned.persisted_files.contains(&old_id),
        "after the restart the hot window still announces the first id, taken from the log"
    );
    h.ingester.flush_all().await;
    assert_eq!(h.ingester.stats().flush_failures.load(Ordering::Relaxed), 0);
    assert_eq!(parquet_files(&bucket).len(), 2);
    let registered = visible(db.catalog.clone()).await;
    assert_eq!(registered.len(), 1);
    assert_eq!(bucket.join(&registered[0].path), new);
    assert_eq!(registered[0].origin, old_id);
    h.stop().await;
    db.finish().await;
}

/// The ingester never sees the refusal: it is down while the sweep marks,
/// deletes and tombstones its object. After the restart the old intent
/// is retried, the tombstone refuses it, the rows take a fresh id, and the
/// deleted path is never written again.
#[tokio::test]
async fn a_path_deleted_while_the_ingester_was_down_is_never_written_again() {
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
    let mut client = h.client().await;
    client
        .export(authed(request("api", 1, 5), &token))
        .await
        .unwrap();
    h.ingester.flush_all().await;
    let bucket = h.dir.path().join("bucket");
    let written = parquet_files(&bucket);
    assert_eq!(written.len(), 1);
    let old = written[0].clone();
    let bytes = sha256_of(&old);
    let old_relative = old
        .strip_prefix(&bucket)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    h.task.abort();
    let _ = h.task.await;
    let dir = h.dir;

    // The sweep, meanwhile: mark, delete, tombstone.
    db.catalog
        .orphans_seen(std::slice::from_ref(&old_relative))
        .await
        .unwrap();
    assert_eq!(
        db.catalog
            .orphan_doom(&old_relative, std::slice::from_ref(&old_relative))
            .await
            .unwrap(),
        fs_catalog::Doom::Marked
    );
    let claim = db
        .catalog
        .orphan_claim(&old_relative)
        .await
        .unwrap()
        .unwrap();
    std::fs::remove_file(&old).unwrap();
    claim.deleted().await.unwrap();

    let h = start(&db.catalog, dir, test_config()).await;
    h.ingester.flush_all().await;
    assert_eq!(
        h.ingester.stats().flush_failures.load(Ordering::Relaxed),
        1,
        "refused by the tombstone"
    );
    assert_eq!(
        h.ingester.flush_stats().superseded.load(Ordering::Relaxed),
        1
    );
    h.ingester.flush_all().await;
    let now = parquet_files(&bucket);
    assert_eq!(now.len(), 1);
    assert_ne!(now[0], old, "the deleted path is never written again");
    assert_eq!(sha256_of(&now[0]), bytes);
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
    assert_eq!(bucket.join(&registered[0].path), now[0]);
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
            version: fs_ingest::record::VERSION,
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
            hour: None,
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
            version: fs_ingest::record::VERSION,
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
            version: fs_ingest::record::VERSION,
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

/// A record framed by a newer engine, with a valid checksum but a record
/// version this one does not know, may be a flush intent: the start fails
/// rather than replaying around it.
#[tokio::test]
async fn future_record_version_refuses_to_start() {
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
    {
        use fs_ingest::record::{RecordSignal, WalRecord};
        let mut wal = Wal::open(WalConfig::new(dir.path().join("wal"))).unwrap();
        let record = WalRecord {
            version: fs_ingest::record::VERSION,
            tenant,
            signal: RecordSignal::Traces,
            payload: bytes::Bytes::from(request("api", 1, 5).encode_to_vec()),
        };
        wal.append(&record.encode()).unwrap();
        // The same header layout with a record version from the future.
        let mut newer = record.encode();
        newer[0] = 9;
        wal.append(&newer).unwrap();
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
        Ok(_) => panic!("an unreadable record must stop the start"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("cannot be read"), "{err}");
    db.finish().await;
}

/// Damage inside a sealed segment, where every record was acknowledged,
/// may hide a flush intent: the start fails. Damage at the tail of the
/// active segment is a torn write of records never acknowledged and is
/// replayed around.
#[tokio::test]
async fn damage_in_a_sealed_segment_refuses_to_start() {
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
    let wal_dir = dir.path().join("wal");
    let small = WalConfig {
        segment_max_bytes: 4096,
        ..WalConfig::new(wal_dir.clone())
    };
    {
        use fs_ingest::record::{RecordSignal, WalRecord};
        let mut wal = Wal::open(small.clone()).unwrap();
        for i in 0..40u8 {
            let record = WalRecord {
                version: fs_ingest::record::VERSION,
                tenant,
                signal: RecordSignal::Traces,
                payload: bytes::Bytes::from(request("api", i, 5).encode_to_vec()),
            };
            wal.append(&record.encode()).unwrap();
        }
        wal.sync().unwrap();
    }
    let mut segments: Vec<_> = std::fs::read_dir(&wal_dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "wal"))
        .collect();
    segments.sort();
    assert!(segments.len() >= 2, "the log rolled over at least once");
    // Flip a byte in the middle of the first, sealed segment.
    let first = &segments[0];
    let mut bytes = std::fs::read(first).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(first, bytes).unwrap();

    std::fs::create_dir_all(dir.path().join("bucket")).unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path().join("bucket")).unwrap());
    let wal = Wal::open(small).unwrap();
    let outcome = Ingester::new(
        test_config(),
        db.catalog.clone(),
        store,
        Path::from(""),
        wal,
    );
    let err = match outcome {
        Ok(_) => panic!("damage in a sealed segment must stop the start"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("cannot be trusted"), "{err}");
    db.finish().await;
}

/// Corruption in the middle of the only, active segment, with acknowledged
/// records after it, is not a torn tail: the log does not open, so no
/// replay can send an intent's rows back to live.
#[tokio::test]
async fn corruption_in_the_active_segment_refuses_to_open() {
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
    let wal_dir = dir.path().join("wal");
    {
        let mut wal = Wal::open(WalConfig::new(wal_dir.clone())).unwrap();
        for i in 0..10u8 {
            let record = WalRecord {
                version: fs_ingest::record::VERSION,
                tenant,
                signal: RecordSignal::Traces,
                payload: bytes::Bytes::from(request("api", i, 5).encode_to_vec()),
            };
            wal.append(&record.encode()).unwrap();
        }
        wal.sync().unwrap();
    }
    let segment = std::fs::read_dir(&wal_dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|x| x == "wal"))
        .unwrap();
    let mut bytes = std::fs::read(&segment).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(&segment, &bytes).unwrap();

    let Err(err) = Wal::open(WalConfig::new(wal_dir)) else {
        panic!("the log must not open over corrupted, acknowledged records")
    };
    assert!(matches!(err, fs_wal::WalError::Corrupt { .. }), "{err}");
    db.finish().await;
}
