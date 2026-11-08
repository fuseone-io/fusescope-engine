//! OTLP ingest over mTLS: only collectors of the ingest CA bundle reach the
//! receivers, and they still need a token.
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
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use sqlx::{Connection, Executor, PgConnection};
use tokio::io::AsyncReadExt;
use tonic::transport::Channel;
use tonic::{Code, Request};

/// 2026-10-22T14:17:03Z.
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
        let schema = format!("otls_{}", uuid::Uuid::new_v4().simple());
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

/// A process TLS from the fixtures, copied under `dir` so it can rotate:
/// the backend's clients from ca-b, the queriers' from ca-a and the
/// collectors' from `ingest_ca`.
fn tls_in(dir: &Path, cert: &str, ingest_ca: &str, max_handshakes: usize) -> Arc<fs_auth::Tls> {
    let files = fs_auth::TlsFiles {
        cert: dir.join("tls.crt"),
        key: dir.join("tls.key"),
        public_client_ca: dir.join("public-ca.crt"),
        hot_client_ca: dir.join("hot-ca.crt"),
        ingest_client_ca: Some(dir.join("ingest-ca.crt")),
    };
    std::fs::copy(fixture(&format!("{cert}.pem")), &files.cert).unwrap();
    std::fs::copy(fixture(&format!("{cert}.key")), &files.key).unwrap();
    std::fs::copy(fixture("ca-b.pem"), &files.public_client_ca).unwrap();
    std::fs::copy(fixture("ca-a.pem"), &files.hot_client_ca).unwrap();
    std::fs::copy(fixture(ingest_ca), files.ingest_client_ca.as_ref().unwrap()).unwrap();
    fs_auth::Tls::load(
        files,
        fs_auth::TlsLimits {
            reload_interval: Duration::from_secs(3600),
            handshake_timeout: Duration::from_millis(500),
            max_handshakes,
        },
    )
    .unwrap()
}

/// A client presenting the fixture identity `cert`, trusting ca-a for the
/// server.
fn client_tls(cert: &str) -> fs_auth::ClientTls {
    let dir = tempfile::tempdir().unwrap();
    let ca = std::fs::read(fixture("ca-a.pem")).unwrap();
    tls_in(dir.path(), cert, "ca-c.pem", 16)
        .client()
        .trusting_only(&ca)
}

fn request(trace: u8, spans: u8) -> ExportTraceServiceRequest {
    let spans = (0..spans)
        .map(|i| Span {
            trace_id: vec![trace; 16],
            span_id: vec![i + 1; 8],
            name: format!("op-{i}"),
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

fn authed(req: ExportTraceServiceRequest, token: &str) -> Request<ExportTraceServiceRequest> {
    let mut r = Request::new(req);
    r.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    r
}

async fn channel(addr: &str, tls: Option<&fs_auth::ClientTls>) -> Option<Channel> {
    let endpoint = fs_auth::endpoint(addr, tls)
        .unwrap()
        .connect_timeout(Duration::from_secs(5));
    endpoint.connect().await.ok()
}

/// Whether a client with `tls` exports `spans` spans with `token`; a
/// refused connection or call is `Err` with what refused it.
async fn export(
    addr: &str,
    tls: Option<&fs_auth::ClientTls>,
    token: &str,
    spans: u8,
) -> Result<(), Code> {
    let Some(channel) = channel(addr, tls).await else {
        return Err(Code::Unavailable);
    };
    TraceServiceClient::new(channel)
        .export(authed(request(spans, spans), token))
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

/// A collector certificate with an ingest token exports; plain text, no
/// certificate, the backend's or a querier's certificate never reach the
/// receivers, and a collector without a token is refused by them.
#[tokio::test]
async fn otlp_over_mtls_admits_only_collectors_and_still_needs_a_token() {
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
    let server_dir = tempfile::tempdir().unwrap();
    let tls = tls_in(server_dir.path(), "server-a", "ca-c.pem", 16);
    let running = start(&db.catalog, &tls).await;
    let collector = client_tls("client-c");

    assert_eq!(
        export(&running.addr, Some(&collector), &token, 3).await,
        Ok(())
    );
    assert_eq!(running.accepted(), 3);
    assert_eq!(
        export(&running.addr, Some(&collector), "fst_unknown", 2).await,
        Err(Code::Unauthenticated),
        "a certificate does not stand for a token"
    );
    assert!(
        export(&running.addr, None, &token, 2).await.is_err(),
        "plain text"
    );
    for client in ["client-b", "client-a"] {
        assert!(
            export(&running.addr, Some(&client_tls(client)), &token, 2)
                .await
                .is_err(),
            "{client}"
        );
    }
    assert_eq!(
        running.accepted(),
        3,
        "nothing refused reached the receivers"
    );
    let ingest = tls.stats();
    assert!(
        ingest
            .listener(fs_auth::Listener::Ingest)
            .handshake_failures
            .load(Ordering::Relaxed)
            >= 2
    );
    running.stop().await;
    db.finish().await;
}

/// The collector CA rotates on the same listener: a new connection follows
/// the reloaded bundle while an open one keeps exporting.
#[tokio::test]
async fn a_reload_of_the_ingest_bundle_reaches_new_connections() {
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
    let server_dir = tempfile::tempdir().unwrap();
    let tls = tls_in(server_dir.path(), "server-a", "ca-c.pem", 16);
    let running = start(&db.catalog, &tls).await;
    let collector = client_tls("client-c");
    let renewed = client_tls("client-b2");
    let mut open = TraceServiceClient::new(channel(&running.addr, Some(&collector)).await.unwrap());
    open.export(authed(request(1, 1), &token)).await.unwrap();
    assert!(
        export(&running.addr, Some(&renewed), &token, 1)
            .await
            .is_err()
    );

    std::fs::copy(
        fixture("ca-b2.pem"),
        server_dir.path().join("ingest-ca.crt"),
    )
    .unwrap();
    assert_eq!(tls.reload().unwrap(), fs_auth::Reload::Reloaded);
    assert_eq!(
        export(&running.addr, Some(&renewed), &token, 2).await,
        Ok(())
    );
    assert!(
        export(&running.addr, Some(&collector), &token, 1)
            .await
            .is_err(),
        "the retired collector CA on a new connection"
    );
    open.export(authed(request(3, 3), &token))
        .await
        .expect("an open connection keeps the config it began with");
    assert_eq!(running.accepted(), 6);
    running.stop().await;
    db.finish().await;
}

/// `tls.max_handshakes` bounds the OTLP listener like the Flight ones.
#[tokio::test]
async fn the_otlp_listener_bounds_its_handshakes() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let server_dir = tempfile::tempdir().unwrap();
    let tls = tls_in(server_dir.path(), "server-a", "ca-c.pem", 1);
    let running = start(&db.catalog, &tls).await;
    let addr = running.addr.replace("localhost", "127.0.0.1");
    let _silent = tokio::net::TcpStream::connect(&addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut excess = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let mut buf = [0u8; 1];
    let closed = tokio::time::timeout(Duration::from_secs(2), excess.read(&mut buf))
        .await
        .unwrap();
    assert!(matches!(closed, Ok(0) | Err(_)), "closed at once");
    let ingest = tls.stats();
    let ingest = ingest.listener(fs_auth::Listener::Ingest);
    assert_eq!(ingest.handshakes_refused.load(Ordering::Relaxed), 1);
    running.stop().await;
    db.finish().await;
}
