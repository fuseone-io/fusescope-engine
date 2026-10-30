//! In-process smoke test of the server against the development services.
//!
//! Runs when `FS_TEST_DATABASE_URL` is set (`make dev-up`); the bucket is a
//! temporary local directory so only Postgres is required. Skips otherwise.
#![allow(
    clippy::too_many_lines,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::time::Duration;

use fs_catalog::Catalog;
use fs_server::config::{Config, Secrets};
use fs_server::role::Role;
use fs_server::server;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use sqlx::{Connection, Executor, PgConnection};
use tokio::sync::watch;

struct TestDb {
    admin_url: String,
    schema: String,
    url: String,
}

impl TestDb {
    async fn new() -> Option<Self> {
        let Ok(admin_url) = std::env::var("FS_TEST_DATABASE_URL") else {
            eprintln!("skipped: FS_TEST_DATABASE_URL is not set (run `make dev-up`)");
            return None;
        };
        let schema = format!("srv_{}", uuid::Uuid::new_v4().simple());
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
        Some(Self {
            admin_url,
            schema,
            url,
        })
    }

    async fn finish(self) {
        let mut admin = PgConnection::connect(&self.admin_url).await.unwrap();
        admin
            .execute(format!("drop schema {} cascade", self.schema).as_str())
            .await
            .unwrap();
        admin.close().await.unwrap();
    }
}

fn config(dir: &std::path::Path) -> Config {
    Config::parse(&format!(
        r#"
        [server]
        node_id = "test-node"
        flight_addr = "127.0.0.1:0"
        metrics_addr = "127.0.0.1:0"
        [ingest]
        listen = "127.0.0.1:0"
        hot_listen = "127.0.0.1:0"
        [bucket]
        kind = "local"
        path = "{bucket}"
        [wal]
        dir = "{wal}"
        [query]
        timeout_secs = 1
        [compactor]
        purge_interval_secs = 1
        quarantine_ttl_secs = 6
        "#,
        bucket = dir.join("bucket").display(),
        wal = dir.join("wal").display(),
    ))
    .unwrap()
}

#[tokio::test]
async fn check_reports_connectivity() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let secrets = Secrets {
        catalog_url: Some(db.url.clone()),
        ..Secrets::default()
    };
    let report = server::check(config(dir.path()), secrets, "checker".into())
        .await
        .unwrap();
    assert!(report.bucket_ok && report.catalog_ok);
    assert_eq!(report.node_id, "checker");
    // The probe marker was deleted again.
    assert!(
        !dir.path().join("bucket").join("_probe").exists()
            || std::fs::read_dir(dir.path().join("bucket/_probe"))
                .unwrap()
                .count()
                == 0
    );
    db.finish().await;
}

#[tokio::test]
async fn all_roles_start_heartbeat_and_stop_cleanly() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let secrets = Secrets {
        catalog_url: Some(db.url.clone()),
        ..Secrets::default()
    };
    let (tx, rx) = watch::channel(false);
    let cfg = config(dir.path());
    let server = tokio::spawn(server::run(cfg, secrets, Role::All, "test-node".into(), rx));

    // The ingester heartbeats on start; wait for it to show up in the catalog.
    let catalog = Catalog::connect(&db.url, 2).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        // Migrations run inside the server; until they land the table is missing.
        let live = catalog
            .live_ingesters(chrono::Duration::seconds(10))
            .await
            .unwrap_or_default();
        if live.iter().any(|i| i.id == "test-node") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "ingester never heartbeated"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(dir.path().join("wal").exists(), "ingester opened its WAL");

    tx.send(true).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("shutdown in time")
        .unwrap();
    result.unwrap();
    catalog.pool().close().await;
    db.finish().await;
}

/// The operator client reaches a running querier with the backend's own
/// ticket and action bodies: a statement answers, an action answers, a bad
/// token is refused.
#[tokio::test]
async fn the_operator_client_queries_a_running_server() {
    use fs_server::client;
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let secrets = Secrets {
        catalog_url: Some(db.url.clone()),
        ..Secrets::default()
    };
    // The port is chosen here, not by the server, so the client knows it.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let addr = format!("127.0.0.1:{port}");
    let mut cfg = config(dir.path());
    cfg.server.flight_addr = addr.clone();
    let (tx, rx) = watch::channel(false);
    let server = tokio::spawn(server::run(cfg, secrets, Role::All, "test-node".into(), rx));

    let catalog = Catalog::connect(&db.url, 2).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let tenant = loop {
        match catalog
            .create_tenant(&fs_catalog::NewTenant::with_defaults("acme"))
            .await
        {
            Ok(tenant) => break tenant.id,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("migrations never landed: {e}"),
        }
    };
    let (token, _) = catalog
        .create_token(tenant, "operator", fs_catalog::TokenScope::Query)
        .await
        .unwrap();
    let counted = loop {
        match client::query(&addr, &token, "select count(*) as n from spans", None).await {
            Ok(batches) => break batches,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("the querier never answered: {e}"),
        }
    };
    let rendered = client::render(&counted).unwrap();
    assert!(rendered.contains("| 0 |"), "{rendered}");
    assert!(rendered.ends_with("1 rows"), "{rendered}");

    let services = client::action(&addr, &token, "metadata.services", "{}", None)
        .await
        .unwrap();
    assert_eq!(
        client::render(&services).unwrap().lines().last(),
        Some("0 rows")
    );
    let err = client::query(&addr, "fst_nope", "select 1", None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Unauthenticated"), "{err}");

    tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("shutdown in time")
        .unwrap()
        .unwrap();
    catalog.pool().close().await;
    db.finish().await;
}

/// A bucket prefix belongs to the catalog that first wrote it: the same
/// catalog starts again, another catalog is refused, a prefix with objects
/// and no marker is refused until the operator adopts it.
#[tokio::test]
async fn a_bucket_prefix_belongs_to_one_catalog() {
    use fs_server::bucket::Ownership;
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let secrets = Secrets {
        catalog_url: Some(db.url.clone()),
        ..Secrets::default()
    };
    let report = server::check(config(dir.path()), secrets.clone(), "a".into())
        .await
        .unwrap();
    assert_eq!(report.ownership, Ownership::Claimed);
    let marker = dir.path().join("bucket").join("_catalog");
    let written = std::fs::read_to_string(&marker).unwrap();
    assert!(written.starts_with("fusescope-catalog v1\n"), "{written}");
    let report = server::check(config(dir.path()), secrets.clone(), "a".into())
        .await
        .unwrap();
    assert_eq!(report.ownership, Ownership::Held);
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), written);

    // Another catalog, same prefix: refused before anything runs, and
    // nothing at all is written there, not even the write probe.
    let Some(other) = TestDb::new().await else {
        return;
    };
    let other_secrets = Secrets {
        catalog_url: Some(other.url.clone()),
        ..Secrets::default()
    };
    let before = snapshot(&dir.path().join("bucket"));
    let err = server::check(config(dir.path()), other_secrets.clone(), "b".into())
        .await
        .unwrap_err();
    assert!(
        matches!(err, fs_server::ServerError::PrefixOwnedByAnother { .. }),
        "{err}"
    );
    let (tx, rx) = watch::channel(false);
    let err = server::run(config(dir.path()), other_secrets, Role::All, "b".into(), rx)
        .await
        .unwrap_err();
    assert!(
        matches!(err, fs_server::ServerError::PrefixOwnedByAnother { .. }),
        "{err}"
    );
    drop(tx);
    assert_eq!(
        snapshot(&dir.path().join("bucket")),
        before,
        "another catalog's prefix is never written"
    );

    // Objects without a marker: refused, then adopted on the operator's say.
    let stray = tempfile::tempdir().unwrap();
    let objects = stray.path().join("bucket").join("t");
    std::fs::create_dir_all(&objects).unwrap();
    std::fs::write(objects.join("leftover.parquet"), b"x").unwrap();
    let err = server::check(config(stray.path()), secrets.clone(), "a".into())
        .await
        .unwrap_err();
    assert!(
        matches!(err, fs_server::ServerError::PrefixUnclaimed(_)),
        "{err}"
    );
    let mut adopting = config(stray.path());
    adopting.bucket.adopt_prefix = true;
    let report = server::check(adopting, secrets.clone(), "a".into())
        .await
        .unwrap();
    assert_eq!(report.ownership, Ownership::Adopted);
    let report = server::check(config(stray.path()), secrets, "a".into())
        .await
        .unwrap();
    assert_eq!(
        report.ownership,
        Ownership::Held,
        "marked, no adoption needed again"
    );
    other.finish().await;
    db.finish().await;
}

/// A port nobody listens on right now, for a server whose address a test
/// must know before it starts.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// `GET path` on the metrics listener: status code and body.
async fn http_get(addr: &str, path: &str) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap();
    let body = text
        .split_once("\r\n\r\n")
        .map_or(String::new(), |(_, b)| b.to_owned());
    (status, body)
}

/// The held request has appended to the WAL: it is in flight.
async fn wait_for_wal_bytes(addr: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (_, body) = http_get(addr, "/metrics").await;
        let bytes = body
            .lines()
            .find_map(|l| l.strip_prefix("fusescope_ingest_wal_bytes "))
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0);
        if bytes > 0 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the request never reached the WAL"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_ready(addr: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(stream) = tokio::net::TcpStream::connect(addr).await {
            drop(stream);
            let (status, body) = http_get(addr, "/readyz").await;
            if status == 200 {
                assert!(body.starts_with("ready\n"), "{body}");
                return;
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "never ready");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn one_span(service: &str) -> ExportTraceServiceRequest {
    let now = u64::try_from(chrono::Utc::now().timestamp_nanos_opt().unwrap()).unwrap();
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
                spans: vec![Span {
                    trace_id: vec![1; 16],
                    span_id: vec![2; 8],
                    name: "op".into(),
                    kind: 2,
                    start_time_unix_nano: now,
                    end_time_unix_nano: now + 1,
                    ..Span::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Starts every role with the metrics listener and the OTLP listener on
/// known ports and `ingest` in place of the file's ingester configuration.
fn start_all(
    db: &TestDb,
    dir: &std::path::Path,
    ingest: fs_ingest::IngestConfig,
    drain_timeout_secs: u64,
) -> (
    String,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<Result<(), fs_server::ServerError>>,
) {
    let metrics_addr = format!("127.0.0.1:{}", free_port());
    let secrets = Secrets {
        catalog_url: Some(db.url.clone()),
        ..Secrets::default()
    };
    let mut cfg = config(dir);
    cfg.server.metrics_addr.clone_from(&metrics_addr);
    cfg.server.health_interval_secs = 1;
    cfg.server.drain_timeout_secs = drain_timeout_secs;
    let (tx, rx) = watch::channel(false);
    let server = tokio::spawn(server::run_with(
        cfg,
        secrets,
        Role::All,
        "test-node".into(),
        rx,
        Some(ingest),
    ));
    (metrics_addr, tx, server)
}

/// `/healthz` answers as long as the process does; `/readyz` turns 200
/// once every role is up and the probes answered, and 503 the moment a
/// stop is asked for, for the whole drain, while the roles finish what
/// they hold.
#[tokio::test]
async fn readiness_follows_the_roles_and_the_drain() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let otlp_port = free_port();
    // The first request is held three seconds between its WAL append and
    // its buffer push: a request in flight the stop has to wait for.
    let ingest = fs_ingest::IngestConfig {
        listen: format!("127.0.0.1:{otlp_port}").parse().unwrap(),
        hot_listen: Some("127.0.0.1:0".parse().unwrap()),
        flush_interval: Duration::from_secs(3600),
        inject_push_delay: Some(Duration::from_secs(3)),
        ..fs_ingest::IngestConfig::default()
    };
    let (metrics_addr, tx, server) = start_all(&db, dir.path(), ingest, 30);
    wait_for_ready(&metrics_addr).await;
    let (status, body) = http_get(&metrics_addr, "/healthz").await;
    assert_eq!((status, body.as_str()), (200, "ok\n"));
    let (_, body) = http_get(&metrics_addr, "/readyz").await;
    assert!(
        body.contains("roles=3/3\n") && body.contains("wal=open\n"),
        "{body}"
    );

    // A request the stop will have to let finish.
    let catalog = Catalog::connect(&db.url, 2).await.unwrap();
    let tenant = catalog
        .create_tenant(&fs_catalog::NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = catalog
        .create_token(tenant, "collector", fs_catalog::TokenScope::Ingest)
        .await
        .unwrap();
    let mut client = TraceServiceClient::connect(format!("http://127.0.0.1:{otlp_port}"))
        .await
        .unwrap();
    let mut request = tonic::Request::new(one_span("api"));
    request.metadata_mut().insert(
        "authorization",
        tonic::metadata::MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
    );
    let export = tokio::spawn(async move { client.export(request).await });
    wait_for_wal_bytes(&metrics_addr).await;

    tx.send(true).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let (status, body) = http_get(&metrics_addr, "/readyz").await;
        if status == 503 && body.contains("draining=true\n") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "never draining: {status} {body}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let (status, _) = http_get(&metrics_addr, "/healthz").await;
    assert_eq!(status, 200, "alive while draining");
    assert!(!server.is_finished(), "the drain is still going");
    tokio::time::timeout(Duration::from_secs(15), server)
        .await
        .expect("the drain ends once the held request returns")
        .unwrap()
        .unwrap();
    export.await.unwrap().unwrap();
    assert!(
        tokio::net::TcpStream::connect(&metrics_addr).await.is_err(),
        "the health listener stops with the process"
    );
    catalog.pool().close().await;
    db.finish().await;
}

/// A health listener that cannot bind ends the start before any role
/// runs: a process without `/readyz` never runs on unwatched.
#[tokio::test]
async fn a_health_listener_that_cannot_bind_stops_the_process() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let secrets = Secrets {
        catalog_url: Some(db.url.clone()),
        ..Secrets::default()
    };
    let mut cfg = config(dir.path());
    cfg.server.metrics_addr = taken.local_addr().unwrap().to_string();
    let (_tx, rx) = watch::channel(false);
    let err = tokio::time::timeout(
        Duration::from_secs(10),
        server::run(cfg, secrets, Role::All, "test-node".into(), rx),
    )
    .await
    .expect("the start ends promptly")
    .unwrap_err();
    assert!(
        err.to_string()
            .contains(&taken.local_addr().unwrap().to_string()),
        "{err}"
    );
    assert!(!dir.path().join("wal").exists(), "no role ran");
    drop(taken);
    db.finish().await;
}

/// A role whose server cannot bind its port returns at once with the
/// error, taking its heartbeat or follower with it, so the process ends
/// instead of beating or following for a server that is gone.
#[tokio::test]
async fn a_role_that_cannot_bind_ends_the_process_at_once() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = taken.local_addr().unwrap().to_string();
    let secrets = Secrets {
        catalog_url: Some(db.url.clone()),
        ..Secrets::default()
    };

    // The ingester's OTLP port.
    let dir = tempfile::tempdir().unwrap();
    let ingest = fs_ingest::IngestConfig {
        listen: addr.parse().unwrap(),
        hot_listen: Some("127.0.0.1:0".parse().unwrap()),
        ..fs_ingest::IngestConfig::default()
    };
    let (_tx, rx) = watch::channel(false);
    let started = tokio::time::Instant::now();
    let err = tokio::time::timeout(
        Duration::from_secs(10),
        server::run_with(
            config(dir.path()),
            secrets.clone(),
            Role::All,
            "n".into(),
            rx,
            Some(ingest),
        ),
    )
    .await
    .expect("the ingester's bind failure ends the process")
    .unwrap_err();
    assert!(err.to_string().contains(&addr), "{err}");
    assert!(started.elapsed() < Duration::from_secs(8));

    // The querier's Flight port.
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.server.flight_addr.clone_from(&addr);
    let (_tx, rx) = watch::channel(false);
    let started = tokio::time::Instant::now();
    let err = tokio::time::timeout(
        Duration::from_secs(10),
        server::run(cfg, secrets, Role::All, "n".into(), rx),
    )
    .await
    .expect("the querier's bind failure ends the process")
    .unwrap_err();
    assert!(err.to_string().contains(&addr), "{err}");
    assert!(started.elapsed() < Duration::from_secs(8));
    drop(taken);
    db.finish().await;
}

/// A drain that does not end by `server.drain_timeout_secs` is abandoned:
/// the roles are aborted and the process reports it.
#[tokio::test]
async fn the_drain_has_a_deadline() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let otlp_port = free_port();
    let ingest = fs_ingest::IngestConfig {
        listen: format!("127.0.0.1:{otlp_port}").parse().unwrap(),
        hot_listen: Some("127.0.0.1:0".parse().unwrap()),
        flush_interval: Duration::from_secs(3600),
        inject_push_delay: Some(Duration::from_secs(60)),
        ..fs_ingest::IngestConfig::default()
    };
    let (metrics_addr, tx, server) = start_all(&db, dir.path(), ingest, 1);
    wait_for_ready(&metrics_addr).await;
    let catalog = Catalog::connect(&db.url, 2).await.unwrap();
    let tenant = catalog
        .create_tenant(&fs_catalog::NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = catalog
        .create_token(tenant, "collector", fs_catalog::TokenScope::Ingest)
        .await
        .unwrap();
    let mut client = TraceServiceClient::connect(format!("http://127.0.0.1:{otlp_port}"))
        .await
        .unwrap();
    let mut request = tonic::Request::new(one_span("api"));
    request.metadata_mut().insert(
        "authorization",
        tonic::metadata::MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
    );
    let export = tokio::spawn(async move { client.export(request).await });
    wait_for_wal_bytes(&metrics_addr).await;

    let stopped_at = tokio::time::Instant::now();
    tx.send(true).unwrap();
    let err = tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("the deadline, not the held request, ends the drain")
        .unwrap()
        .unwrap_err();
    export.abort();
    assert!(
        matches!(err, fs_server::ServerError::DrainTimeout(d) if d == Duration::from_secs(1)),
        "{err}"
    );
    assert!(stopped_at.elapsed() < Duration::from_secs(8));
    catalog.pool().close().await;
    db.finish().await;
}

/// Every entry under `dir`, with its size and modification time.
fn snapshot(dir: &std::path::Path) -> Vec<(String, u64, std::time::SystemTime)> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        for entry in std::fs::read_dir(&next).unwrap() {
            let entry = entry.unwrap();
            let meta = entry.metadata().unwrap();
            out.push((
                entry.path().display().to_string(),
                meta.len(),
                meta.modified().unwrap(),
            ));
            if meta.is_dir() {
                stack.push(entry.path());
            }
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn bootstrap_fails_fast_on_bad_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let secrets = Secrets {
        catalog_url: Some("postgres://nobody:x@127.0.0.1:1/none".into()),
        ..Secrets::default()
    };
    let err = server::check(config(dir.path()), secrets, "n".into())
        .await
        .unwrap_err();
    assert!(matches!(err, fs_server::ServerError::Catalog(_)), "{err}");
}

fn tls_fixture(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../fs-auth/tests/fixtures")
        .join(name)
}

fn tls_on(cfg: &mut Config, cert: &str, key: &str) {
    cfg.tls = fs_server::tls::TlsSection {
        enabled: true,
        cert: tls_fixture(cert).display().to_string(),
        key: tls_fixture(key).display().to_string(),
        public_client_ca: tls_fixture("ca-b.pem").display().to_string(),
        hot_client_ca: tls_fixture("ca-a.pem").display().to_string(),
        ..fs_server::tls::TlsSection::default()
    };
}

/// A client of the fixtures' `cert`, trusting the CA of the servers.
fn certified(cert: &str, key: &str) -> fs_auth::ClientTls {
    let identity = fs_auth::Tls::load(
        fs_auth::TlsFiles {
            cert: tls_fixture(cert),
            key: tls_fixture(key),
            public_client_ca: tls_fixture("ca-b.pem"),
            hot_client_ca: tls_fixture("ca-a.pem"),
        },
        fs_server::tls::TlsSection::default().limits(),
    )
    .unwrap();
    let ca = std::fs::read(tls_fixture("ca-a.pem")).unwrap();
    identity.client().trusting_only(&ca)
}

/// With `[tls]` on, the server refuses to start on files that do not load.
/// On good files, in one process running every role, a backend
/// certificate opens Flight SQL and not the hot window, a querier
/// certificate the reverse, plain text opens neither, and readiness and
/// the metrics report the TLS.
#[tokio::test]
async fn with_tls_on_each_flight_admits_only_its_own_clients() {
    use fs_server::client;
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let secrets = Secrets {
        catalog_url: Some(db.url.clone()),
        ..Secrets::default()
    };
    let mut broken = config(dir.path());
    tls_on(&mut broken, "server-a.pem", "client-a.key");
    let err = server::check(broken, secrets.clone(), "checker".into())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("server-a.pem"), "{err}");

    let port = free_port();
    let hot_port = free_port();
    let metrics_port = free_port();
    let mut cfg = config(dir.path());
    cfg.server.flight_addr = format!("127.0.0.1:{port}");
    cfg.server.metrics_addr = format!("127.0.0.1:{metrics_port}");
    cfg.ingest.hot_listen = format!("127.0.0.1:{hot_port}");
    cfg.ingest.hot_advertise = format!("localhost:{hot_port}");
    tls_on(&mut cfg, "server-a.pem", "server-a.key");
    let (tx, rx) = watch::channel(false);
    let server = tokio::spawn(server::run(cfg, secrets, Role::All, "test-node".into(), rx));
    let metrics_addr = format!("127.0.0.1:{metrics_port}");
    wait_for_ready(&metrics_addr).await;
    let (_, body) = http_get(&metrics_addr, "/readyz").await;
    assert!(body.contains("tls=current\n"), "{body}");
    let (_, body) = http_get(&metrics_addr, "/metrics").await;
    assert!(body.contains("fusescope_tls_stale 0\n"), "{body}");

    let catalog = Catalog::connect(&db.url, 2).await.unwrap();
    let tenant = catalog
        .create_tenant(&fs_catalog::NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (token, _) = catalog
        .create_token(tenant, "operator", fs_catalog::TokenScope::Query)
        .await
        .unwrap();
    let backend = certified("client-b.pem", "client-b.key");
    let querier = certified("client-a.pem", "client-a.key");
    let flight = format!("localhost:{port}");
    let hot = format!("localhost:{hot_port}");
    let scan = fs_ingest::hot::HotScanRequest {
        tenant,
        ts_min: None,
        ts_max: None,
        services: None,
        columns: None,
        trace_ids: None,
        span_ids: None,
        distinct: None,
    };
    let timeout = Duration::from_secs(5);

    let counted = client::query(
        &flight,
        &token,
        "select count(*) as n from spans",
        Some(&backend),
    )
    .await
    .unwrap();
    assert!(client::render(&counted).unwrap().ends_with("1 rows"));
    assert!(
        fs_ingest::hot::scan_with(&hot, &scan, timeout, Some(&backend))
            .await
            .is_err(),
        "a backend certificate on the hot window"
    );
    fs_ingest::hot::scan_with(&hot, &scan, timeout, Some(&querier))
        .await
        .unwrap();
    assert!(
        client::query(&flight, &token, "select 1", Some(&querier))
            .await
            .is_err(),
        "a querier certificate on Flight SQL"
    );
    assert!(
        client::query(&flight, &token, "select 1", None)
            .await
            .is_err()
    );
    assert!(fs_ingest::hot::scan(&hot, &scan, timeout).await.is_err());
    let (_, body) = http_get(&metrics_addr, "/metrics").await;
    assert!(
        !body.contains("fusescope_tls_handshake_failures_total 0\n"),
        "the refused attempts failed their handshakes: {body}"
    );

    tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("shutdown in time")
        .unwrap()
        .unwrap();
    catalog.pool().close().await;
    db.finish().await;
}

/// A backend token for `tenant`, signed with the fixtures' key `a`.
fn backend_jwt(tenant: fs_catalog::TenantId) -> String {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use tokio_rustls::rustls::pki_types::pem::PemObject;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let segment = |value: serde_json::Value| URL_SAFE_NO_PAD.encode(value.to_string());
    let signed = format!(
        "{}.{}",
        segment(serde_json::json!({"alg": "EdDSA", "kid": "a"})),
        segment(serde_json::json!({
            "iss": "fusescope-backend",
            "aud": "fusescope-engine",
            "sub": "user:operator",
            "fusescope_tenant": tenant.0.to_string(),
            "iat": now,
            "exp": now + 120,
        }))
    );
    let key = tokio_rustls::rustls::crypto::ring::default_provider()
        .key_provider
        .load_private_key(
            tokio_rustls::rustls::pki_types::PrivateKeyDer::from_pem_file(tls_fixture("jwt-a.key"))
                .unwrap(),
        )
        .unwrap();
    let signature = key
        .choose_scheme(&[tokio_rustls::rustls::SignatureScheme::ED25519])
        .unwrap()
        .sign(signed.as_bytes())
        .unwrap();
    format!("{signed}.{}", URL_SAFE_NO_PAD.encode(signature))
}

/// With `auth.mode = "jwt"` the server refuses to start without TLS or on
/// a key set that does not load; running over mTLS, Flight SQL answers the
/// backend's JWT, refuses the catalog's static token, and readiness and
/// the metrics report the key set.
#[tokio::test]
async fn in_jwt_mode_the_querier_takes_only_backend_tokens() {
    use fs_server::client;
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let secrets = Secrets {
        catalog_url: Some(db.url.clone()),
        ..Secrets::default()
    };
    let jwt_on = |cfg: &mut Config, keys: &str| {
        cfg.auth = fs_server::auth::AuthSection {
            mode: fs_server::auth::AuthMode::Jwt,
            jwt_keys: tls_fixture(keys).display().to_string(),
            jwt_issuer: "fusescope-backend".into(),
            ..fs_server::auth::AuthSection::default()
        };
    };
    let mut plain = config(dir.path());
    jwt_on(&mut plain, "jwks-a.json");
    let err = server::check(plain, secrets.clone(), "checker".into())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("requires tls.enabled"), "{err}");
    let mut broken = config(dir.path());
    tls_on(&mut broken, "server-a.pem", "server-a.key");
    jwt_on(&mut broken, "server-a.pem");
    let err = server::check(broken, secrets.clone(), "checker".into())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("server-a.pem"), "{err}");

    let port = free_port();
    let metrics_port = free_port();
    let mut cfg = config(dir.path());
    cfg.server.flight_addr = format!("127.0.0.1:{port}");
    cfg.server.metrics_addr = format!("127.0.0.1:{metrics_port}");
    tls_on(&mut cfg, "server-a.pem", "server-a.key");
    jwt_on(&mut cfg, "jwks-a.json");
    let (tx, rx) = watch::channel(false);
    let server = tokio::spawn(server::run(cfg, secrets, Role::All, "test-node".into(), rx));
    let metrics_addr = format!("127.0.0.1:{metrics_port}");
    wait_for_ready(&metrics_addr).await;
    let (_, body) = http_get(&metrics_addr, "/readyz").await;
    assert!(body.contains("jwt_keys=current\n"), "{body}");

    let catalog = Catalog::connect(&db.url, 2).await.unwrap();
    let tenant = catalog
        .create_tenant(&fs_catalog::NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (static_token, _) = catalog
        .create_token(tenant, "operator", fs_catalog::TokenScope::Query)
        .await
        .unwrap();
    let addr = format!("localhost:{port}");
    let backend = certified("client-b.pem", "client-b.key");
    let counted = client::query(
        &addr,
        &backend_jwt(tenant),
        "select count(*) as n from spans",
        Some(&backend),
    )
    .await
    .unwrap();
    assert!(client::render(&counted).unwrap().ends_with("1 rows"));
    let err = client::query(&addr, &static_token, "select 1", Some(&backend))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Unauthenticated"), "{err}");
    let (_, body) = http_get(&metrics_addr, "/metrics").await;
    assert!(
        body.contains("fusescope_query_auth_refused_total 1\n"),
        "{body}"
    );
    assert!(body.contains("fusescope_jwt_keys_stale 0\n"), "{body}");

    tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("shutdown in time")
        .unwrap()
        .unwrap();
    catalog.pool().close().await;
    db.finish().await;
}

/// `admin token` issues, lists, rotates and revokes by id: the rotated
/// token keeps working through its overlap, the revoked one stops at once,
/// and no secret is ever listed.
#[tokio::test]
async fn admin_rotates_and_revokes_tokens_by_id() {
    use fs_server::server::AdminAction;
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let secrets = Secrets {
        catalog_url: Some(db.url.clone()),
        ..Secrets::default()
    };
    let admin = |action| server::admin(config(dir.path()), secrets.clone(), action);
    let tenant: uuid::Uuid = admin(AdminAction::CreateTenant {
        name: "acme".into(),
    })
    .await
    .unwrap()[0]
        .strip_prefix("tenant_id=")
        .unwrap()
        .parse()
        .unwrap();
    let field = |line: &str, key: &str| {
        line.split(' ')
            .find_map(|kv| kv.strip_prefix(&format!("{key}=")))
            .map(str::to_owned)
    };
    let issued = admin(AdminAction::CreateToken {
        tenant,
        name: "collector".into(),
        scope: fs_catalog::TokenScope::Ingest,
    })
    .await
    .unwrap();
    let id = field(&issued[0], "id").unwrap();
    let secret = issued[1].strip_prefix("token=").unwrap().to_owned();

    let rotated = admin(AdminAction::RotateToken {
        id: id.clone(),
        overlap_secs: 600,
    })
    .await
    .unwrap();
    assert!(field(&rotated[0], "stops_at").is_some(), "{rotated:?}");
    let new_id = field(&rotated[1], "id").unwrap();
    let successor = rotated[2].strip_prefix("token=").unwrap().to_owned();
    let catalog = Catalog::connect(&db.url, 2).await.unwrap();
    for live in [&secret, &successor] {
        assert!(
            catalog
                .resolve_token(live, fs_catalog::TokenScope::Ingest)
                .await
                .unwrap()
                .is_some()
        );
    }

    let listed = admin(AdminAction::ListTokens { tenant }).await.unwrap();
    assert_eq!(listed.len(), 2);
    assert!(
        listed.iter().all(|line| !line.contains("fst_")),
        "{listed:?}"
    );

    let revoked = admin(AdminAction::RevokeToken { id: id.clone() })
        .await
        .unwrap();
    assert!(field(&revoked[0], "revoked_at").is_some(), "{revoked:?}");
    assert!(
        catalog
            .resolve_token(&secret, fs_catalog::TokenScope::Ingest)
            .await
            .unwrap()
            .is_none()
    );
    let err = admin(AdminAction::RevokeToken { id }).await.unwrap_err();
    assert!(err.to_string().contains("no live token"), "{err}");
    assert!(
        catalog
            .resolve_token(&successor, fs_catalog::TokenScope::Ingest)
            .await
            .unwrap()
            .is_some(),
        "the successor {new_id} lives on"
    );
    catalog.pool().close().await;
    db.finish().await;
}
