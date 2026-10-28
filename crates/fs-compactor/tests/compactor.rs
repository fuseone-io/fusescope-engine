//! The compactor against a real Postgres and a local bucket.
#![allow(
    clippy::too_many_lines,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use fs_catalog::{
    COMPACTOR_LEASE, Catalog, FileId, Level, NewFile, NewTenant, Signal, TenantId, TimeRange,
};
use fs_compactor::{CompactorConfig, Leadership};
use object_store::ObjectStoreExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection};

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
        let schema = format!("c_{}", uuid::Uuid::new_v4().simple());
        let mut admin = PgConnection::connect(&admin_url).await.unwrap();
        admin
            .execute(format!("create schema {schema}").as_str())
            .await
            .unwrap();
        admin.close().await.unwrap();
        let options: PgConnectOptions = admin_url.parse().unwrap();
        let options = options.options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .unwrap();
        let catalog = Catalog::from_pool(pool);
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

fn file(tenant: TenantId) -> NewFile {
    let id = FileId::new();
    NewFile {
        id,
        tenant_id: tenant,
        signal: Signal::Traces,
        level: Level::L0,
        path: format!(
            "t/{}/traces/l0/dt=2026-10-24/hr=00/{}.parquet",
            tenant.0, id.0
        ),
        size_bytes: 3,
        row_count: 1,
        range: TimeRange {
            start: Utc.with_ymd_and_hms(2026, 10, 24, 0, 0, 0).unwrap(),
            end: Utc.with_ymd_and_hms(2026, 10, 24, 0, 30, 0).unwrap(),
        },
        services: vec![],
        summary: None,
    }
}

/// A quarantined file past its TTL whose object is in the bucket; returns
/// its path.
async fn quarantined(catalog: &Catalog, store: &LocalFileSystem) -> String {
    let tenant = catalog
        .create_tenant(&NewTenant::with_defaults(format!(
            "t{}",
            uuid::Uuid::new_v4().simple()
        )))
        .await
        .unwrap()
        .id;
    let old = catalog.register_file(&file(tenant)).await.unwrap();
    store
        .put(&Path::from(old.path.as_str()), b"old".to_vec().into())
        .await
        .unwrap();
    catalog
        .swap_files(tenant, Signal::Traces, &[old.id], &[file(tenant)])
        .await
        .unwrap();
    old.path
}

#[tokio::test]
async fn two_instances_hand_the_lease_over_by_release_or_expiry() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let ttl = Duration::from_millis(600);
    let mut a = Leadership::new(db.catalog.clone(), "a".into(), ttl);
    let mut b = Leadership::new(db.catalog.clone(), "b".into(), ttl);
    assert!(a.refresh().await);
    assert!(!b.refresh().await);
    assert!(a.is_leader() && !b.is_leader());
    assert_eq!(a.renew_every(), Duration::from_millis(200));

    a.release().await;
    assert!(!a.is_leader());
    assert!(b.refresh().await, "handed over at once");
    assert!(!a.refresh().await);

    // b stops renewing; after the TTL, a takes over.
    tokio::time::sleep(ttl + Duration::from_millis(200)).await;
    assert!(a.refresh().await, "lapsed");
    assert!(!b.refresh().await, "b lost it");
    assert!(!b.is_leader());
    db.finish().await;
}

/// The compactor loop purges only while it holds the lease: with the
/// lease held elsewhere, an expired quarantined file stays; once the
/// holder lets go, the loop takes the lease and purges it.
#[tokio::test]
async fn the_loop_purges_only_while_it_holds_the_lease() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let path = quarantined(&db.catalog, &store).await;
    assert!(
        db.catalog
            .acquire_lease(COMPACTOR_LEASE, "elsewhere", Duration::from_secs(60))
            .await
            .unwrap()
    );

    let (shutdown, rx) = tokio::sync::watch::channel(false);
    let config = CompactorConfig {
        purge_interval: Duration::from_secs(1),
        quarantine_ttl: Duration::ZERO,
        purge_batch: 10,
        lease_ttl: Duration::from_millis(600),
    };
    let task = tokio::spawn(fs_compactor::run(
        db.catalog.clone(),
        store.clone(),
        config,
        "loop".into(),
        rx,
    ));
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(
        store.head(&Path::from(path.as_str())).await.is_ok(),
        "not the leader: nothing purged"
    );
    assert_eq!(
        db.catalog
            .lease(COMPACTOR_LEASE)
            .await
            .unwrap()
            .unwrap()
            .holder,
        "elsewhere"
    );

    assert!(
        db.catalog
            .release_lease(COMPACTOR_LEASE, "elsewhere")
            .await
            .unwrap()
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while store.head(&Path::from(path.as_str())).await.is_ok()
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        store.head(&Path::from(path.as_str())).await.is_err(),
        "leader now: purged"
    );
    assert_eq!(
        db.catalog
            .lease(COMPACTOR_LEASE)
            .await
            .unwrap()
            .unwrap()
            .holder,
        "loop"
    );

    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert!(
        db.catalog.lease(COMPACTOR_LEASE).await.unwrap().is_none(),
        "released on shutdown"
    );
    db.finish().await;
}
