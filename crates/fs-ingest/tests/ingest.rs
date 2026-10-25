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
        async move { ingester.run(rx, Some(bound_tx)).await }
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
        flush_interval: Duration::from_secs(3600),
        ..IngestConfig::default()
    }
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
