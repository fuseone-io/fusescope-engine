//! In-process smoke test of the server against the development services.
//!
//! Runs when `FS_TEST_DATABASE_URL` is set (`make dev-up`); the bucket is a
//! temporary local directory so only Postgres is required. Skips otherwise.
#![allow(
    clippy::too_many_lines,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

use std::time::Duration;

use fs_catalog::Catalog;
use fs_server::config::{Config, Secrets};
use fs_server::role::Role;
use fs_server::server;
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
        match client::query(&addr, &token, "select count(*) as n from spans").await {
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

    let services = client::action(&addr, &token, "metadata.services", "{}")
        .await
        .unwrap();
    assert_eq!(
        client::render(&services).unwrap().lines().last(),
        Some("0 rows")
    );
    let err = client::query(&addr, "fst_nope", "select 1")
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

    // Another catalog, same prefix: refused before anything runs.
    let Some(other) = TestDb::new().await else {
        return;
    };
    let other_secrets = Secrets {
        catalog_url: Some(other.url.clone()),
        ..Secrets::default()
    };
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
