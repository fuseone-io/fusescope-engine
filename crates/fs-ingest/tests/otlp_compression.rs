//! OTLP ingest of compressed requests, as collectors send them: gzip and zstd on
//! the trace, log and metric services, still under mTLS and a token, and still
//! bounded by `max_message_bytes` once decompressed.
//!
//! Needs `FS_TEST_DATABASE_URL` (`make dev-up`); skips otherwise.
#![allow(
    clippy::too_many_lines,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use fs_catalog::{Catalog, NewTenant, TokenScope};
use fs_ingest::{IngestConfig, Ingester};
use fs_wal::{Wal, WalConfig};
use object_store::local::LocalFileSystem;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use sqlx::{Connection, Executor, PgConnection};
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic::{Code, Request};

/// 2026-10-22T14:17:03Z.
const T0: u64 = 1_792_678_623_000_000_000;
/// The ingester's bound on a decompressed message in these tests.
const MAX_MESSAGE_BYTES: usize = 64 * 1024;

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
        let schema = format!("ocmp_{}", uuid::Uuid::new_v4().simple());
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

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../fs-auth/tests/fixtures")
        .join(name)
}

/// The server TLS of the fixtures: collectors from ca-c.
fn server_tls(dir: &Path) -> Arc<fs_auth::Tls> {
    let files = fs_auth::TlsFiles {
        cert: dir.join("tls.crt"),
        key: dir.join("tls.key"),
        public_client_ca: dir.join("public-ca.crt"),
        hot_client_ca: dir.join("hot-ca.crt"),
        ingest_client_ca: Some(dir.join("ingest-ca.crt")),
    };
    std::fs::copy(fixture("server-a.pem"), &files.cert).unwrap();
    std::fs::copy(fixture("server-a.key"), &files.key).unwrap();
    std::fs::copy(fixture("ca-b.pem"), &files.public_client_ca).unwrap();
    std::fs::copy(fixture("ca-a.pem"), &files.hot_client_ca).unwrap();
    std::fs::copy(
        fixture("ca-c.pem"),
        files.ingest_client_ca.as_ref().unwrap(),
    )
    .unwrap();
    fs_auth::Tls::load(
        files,
        fs_auth::TlsLimits {
            reload_interval: Duration::from_secs(3600),
            handshake_timeout: Duration::from_millis(500),
            max_handshakes: 16,
        },
    )
    .unwrap()
}

/// A collector presenting the fixture identity `cert`, trusting ca-a for the server.
fn client_tls(cert: &str) -> fs_auth::ClientTls {
    let dir = tempfile::tempdir().unwrap();
    let ca = std::fs::read(fixture("ca-a.pem")).unwrap();
    let files = fs_auth::TlsFiles {
        cert: dir.path().join("tls.crt"),
        key: dir.path().join("tls.key"),
        public_client_ca: dir.path().join("public-ca.crt"),
        hot_client_ca: dir.path().join("hot-ca.crt"),
        ingest_client_ca: Some(dir.path().join("ingest-ca.crt")),
    };
    std::fs::copy(fixture(&format!("{cert}.pem")), &files.cert).unwrap();
    std::fs::copy(fixture(&format!("{cert}.key")), &files.key).unwrap();
    std::fs::copy(fixture("ca-b.pem"), &files.public_client_ca).unwrap();
    std::fs::copy(fixture("ca-a.pem"), &files.hot_client_ca).unwrap();
    std::fs::copy(
        fixture("ca-c.pem"),
        files.ingest_client_ca.as_ref().unwrap(),
    )
    .unwrap();
    fs_auth::Tls::load(
        files,
        fs_auth::TlsLimits {
            reload_interval: Duration::from_secs(3600),
            handshake_timeout: Duration::from_millis(500),
            max_handshakes: 16,
        },
    )
    .unwrap()
    .client()
    .trusting_only(&ca)
}

/// `spans` spans named `name_bytes` long, so a request compresses far below its size.
fn traces(spans: u8, name_bytes: usize) -> ExportTraceServiceRequest {
    let spans = (0..spans)
        .map(|i| Span {
            trace_id: vec![7; 16],
            span_id: vec![i + 1; 8],
            name: "x".repeat(name_bytes),
            start_time_unix_nano: T0 + u64::from(i) * 1_000_000,
            end_time_unix_nano: T0 + u64::from(i) * 1_000_000 + 500_000,
            ..Span::default()
        })
        .collect();
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans,
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    }
}

fn authed<T>(req: T, token: &str) -> Request<T> {
    let mut r = Request::new(req);
    r.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    r
}

async fn channel(addr: &str, tls: Option<&fs_auth::ClientTls>) -> Option<Channel> {
    fs_auth::endpoint(addr, tls)
        .unwrap()
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .ok()
}

async fn export_traces(
    addr: &str,
    tls: Option<&fs_auth::ClientTls>,
    token: &str,
    encoding: CompressionEncoding,
    request: ExportTraceServiceRequest,
) -> Result<(), Code> {
    let Some(channel) = channel(addr, tls).await else {
        return Err(Code::Unavailable);
    };
    TraceServiceClient::new(channel)
        .send_compressed(encoding)
        .max_encoding_message_size(usize::MAX)
        .export(authed(request, token))
        .await
        .map(|_| ())
        .map_err(|status| status.code())
}

struct Running {
    addr: String,
    ingester: Arc<Ingester>,
    shutdown: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<(), fs_ingest::IngestError>>,
    _dir: tempfile::TempDir,
}

async fn start(catalog: &Catalog, tls: &fs_auth::Tls) -> Running {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("bucket")).unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path().join("bucket")).unwrap());
    let wal = Wal::open(WalConfig::new(dir.path().join("wal"))).unwrap();
    let config = IngestConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        hot_listen: Some("127.0.0.1:0".parse().unwrap()),
        flush_interval: Duration::from_secs(3600),
        max_message_bytes: MAX_MESSAGE_BYTES,
        ..IngestConfig::default()
    };
    let ingester = Ingester::new(
        config,
        catalog.clone(),
        store,
        object_store::path::Path::from(""),
        wal,
    )
    .unwrap()
    .with_hot_tls(tls.hot_server())
    .with_otlp_tls(tls.ingest_server().unwrap());
    let ingester = Arc::new(ingester);
    let (shutdown, rx) = tokio::sync::watch::channel(false);
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn({
        let ingester = ingester.clone();
        async move { ingester.run(rx, Some(bound_tx), None).await }
    });
    let bound = tokio::time::timeout(Duration::from_secs(5), bound_rx)
        .await
        .unwrap()
        .unwrap();
    Running {
        addr: format!("localhost:{}", bound.port()),
        ingester,
        shutdown,
        task,
        _dir: dir,
    }
}

impl Running {
    fn accepted(&self) -> u64 {
        self.ingester.shared_stats().spans.load(Ordering::Relaxed)
    }

    async fn stop(self) {
        self.shutdown.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

async fn ingest_token(catalog: &Catalog) -> String {
    let tenant = catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap()
        .0
}

/// gzip and zstd reach every OTLP service, as the collector's gateway sends them.
#[tokio::test]
async fn gzip_and_zstd_requests_are_accepted_on_every_service() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let token = ingest_token(&db.catalog).await;
    let server_dir = tempfile::tempdir().unwrap();
    let running = start(&db.catalog, &server_tls(server_dir.path())).await;
    let collector = client_tls("client-c");

    let mut sent = 0;
    for encoding in [CompressionEncoding::Zstd, CompressionEncoding::Gzip] {
        assert_eq!(
            export_traces(
                &running.addr,
                Some(&collector),
                &token,
                encoding,
                traces(3, 32)
            )
            .await,
            Ok(()),
            "{encoding:?} traces"
        );
        sent += 3;
        assert_eq!(running.accepted(), sent, "{encoding:?}");

        let channel = channel(&running.addr, Some(&collector)).await.unwrap();
        LogsServiceClient::new(channel.clone())
            .send_compressed(encoding)
            .export(authed(ExportLogsServiceRequest::default(), &token))
            .await
            .unwrap_or_else(|status| panic!("{encoding:?} logs: {status}"));
        MetricsServiceClient::new(channel)
            .send_compressed(encoding)
            .export(authed(ExportMetricsServiceRequest::default(), &token))
            .await
            .unwrap_or_else(|status| panic!("{encoding:?} metrics: {status}"));
    }
    running.stop().await;
    db.finish().await;
}

/// A request that compresses below `max_message_bytes` but decompresses past it is refused.
#[tokio::test]
async fn a_compressed_request_past_the_bound_once_decompressed_is_refused() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let token = ingest_token(&db.catalog).await;
    let server_dir = tempfile::tempdir().unwrap();
    let running = start(&db.catalog, &server_tls(server_dir.path())).await;
    let collector = client_tls("client-c");

    // About 256 KiB of repeated names: a few KiB compressed, four times the bound decompressed.
    for encoding in [CompressionEncoding::Zstd, CompressionEncoding::Gzip] {
        let refused = export_traces(
            &running.addr,
            Some(&collector),
            &token,
            encoding,
            traces(64, 4096),
        )
        .await;
        assert_eq!(refused, Err(Code::ResourceExhausted), "{encoding:?}");
    }
    assert_eq!(
        running.accepted(),
        0,
        "nothing past the bound reached the receivers"
    );
    running.stop().await;
    db.finish().await;
}

/// Compression opens nothing: a compressed request still needs the collector's certificate and a token.
#[tokio::test]
async fn a_compressed_request_still_needs_mtls_and_a_token() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let token = ingest_token(&db.catalog).await;
    let server_dir = tempfile::tempdir().unwrap();
    let running = start(&db.catalog, &server_tls(server_dir.path())).await;

    for encoding in [CompressionEncoding::Zstd, CompressionEncoding::Gzip] {
        assert_eq!(
            export_traces(
                &running.addr,
                Some(&client_tls("client-c")),
                "fst_unknown",
                encoding,
                traces(2, 32)
            )
            .await,
            Err(Code::Unauthenticated),
            "{encoding:?} without a token"
        );
        assert!(
            export_traces(&running.addr, None, &token, encoding, traces(2, 32))
                .await
                .is_err(),
            "{encoding:?} in plain text"
        );
        assert!(
            export_traces(
                &running.addr,
                Some(&client_tls("client-b")),
                &token,
                encoding,
                traces(2, 32)
            )
            .await
            .is_err(),
            "{encoding:?} with the backend's certificate"
        );
    }
    assert_eq!(running.accepted(), 0);
    running.stop().await;
    db.finish().await;
}
