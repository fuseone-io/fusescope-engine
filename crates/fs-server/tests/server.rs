//! In-process smoke test of the server against the development services.
//!
//! Runs when `FS_TEST_DATABASE_URL` is set (`make dev-up`); the bucket is a
//! temporary local directory so only Postgres is required. Skips otherwise.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

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
        [bucket]
        kind = "local"
        path = "{bucket}"
        [wal]
        dir = "{wal}"
        [compactor]
        purge_interval_secs = 1
        quarantine_ttl_secs = 0
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
