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
    COMPACTOR_LEASE, Catalog, CatalogError, FileId, Level, NewFile, NewTenant, Signal, TenantId,
    TimeRange,
};
use fs_compactor::{CompactorConfig, Leadership};
use futures_util::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::throttle::{ThrottleConfig, ThrottledStore};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
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
        origin: id,
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

/// A quarantined file past its TTL whose object and sidecar are in the
/// bucket; returns its path.
async fn quarantined(catalog: &Catalog, store: &dyn ObjectStore) -> String {
    quarantined_file(catalog, store).await.path
}

/// As [`quarantined`], returning the registration so it can be replayed.
async fn quarantined_file(catalog: &Catalog, store: &dyn ObjectStore) -> NewFile {
    let tenant = catalog
        .create_tenant(&NewTenant::with_defaults(format!(
            "t{}",
            uuid::Uuid::new_v4().simple()
        )))
        .await
        .unwrap()
        .id;
    let registration = file(tenant);
    let old = catalog.register_file(&registration).await.unwrap();
    store
        .put(&Path::from(old.path.as_str()), b"old".to_vec().into())
        .await
        .unwrap();
    store
        .put(
            &Path::from(fs_index::sidecar_path(&old.path).unwrap().as_str()),
            b"tix".to_vec().into(),
        )
        .await
        .unwrap();
    catalog
        .swap_files(tenant, Signal::Traces, &[old.id], &[file(tenant)])
        .await
        .unwrap();
    registration
}

/// A store that holds every delete until the gate opens and wakes whoever
/// waits for the first one, so a test runs between the purge's catalog
/// step and its bucket step.
#[derive(Debug)]
struct HeldDeletes {
    inner: Arc<dyn ObjectStore>,
    gate: tokio::sync::watch::Receiver<bool>,
    arrived: Arc<tokio::sync::Notify>,
}

impl std::fmt::Display for HeldDeletes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HeldDeletes")
    }
}

#[async_trait::async_trait]
impl ObjectStore for HeldDeletes {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
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
        let gate = self.gate.clone();
        let arrived = Arc::clone(&self.arrived);
        let held = locations.then(move |location| {
            let mut gate = gate.clone();
            let arrived = Arc::clone(&arrived);
            async move {
                if !*gate.borrow() {
                    arrived.notify_one();
                    while !*gate.borrow() {
                        if gate.changed().await.is_err() {
                            break;
                        }
                    }
                }
                location
            }
        });
        self.inner.delete_stream(held.boxed())
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

#[tokio::test]
async fn two_instances_hand_the_lease_over_by_release_or_expiry() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let ttl = Duration::from_millis(600);
    let a = Leadership::new(db.catalog.clone(), "node-a", ttl);
    let b = Leadership::new(db.catalog.clone(), "node-b", ttl);
    assert!(a.refresh().await);
    assert!(!b.refresh().await);
    assert!(a.is_leader() && !b.is_leader());
    assert_eq!(a.renew_every(), Duration::from_millis(200));
    let fence = a.fence().unwrap();
    assert_eq!(fence.holder, a.holder());
    assert_eq!(fence.generation, 1);
    assert!(b.fence().is_none());

    a.release().await;
    assert!(!a.is_leader());
    assert!(b.refresh().await, "handed over at once");
    assert!(!a.refresh().await);

    // b stops renewing; after the TTL, a takes over at a new generation.
    tokio::time::sleep(ttl + Duration::from_millis(200)).await;
    assert!(a.refresh().await, "lapsed");
    assert!(!b.refresh().await, "b lost it");
    assert!(!b.is_leader());
    assert_eq!(a.fence().unwrap().generation, 3);
    db.finish().await;
}

/// Two processes configured with the same node id are two holders: the
/// second does not pass for a renewal of the first.
#[tokio::test]
async fn the_same_node_id_twice_is_two_holders() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let ttl = Duration::from_secs(30);
    let first = Leadership::new(db.catalog.clone(), "node-0", ttl);
    let second = Leadership::new(db.catalog.clone(), "node-0", ttl);
    assert_ne!(first.holder(), second.holder());
    assert!(first.holder().starts_with("node-0/"));
    assert!(first.refresh().await);
    assert!(!second.refresh().await, "not a renewal");
    second.release().await;
    assert!(
        first.refresh().await,
        "the second cannot release the first's lease"
    );
    db.finish().await;
}

/// A purge removes the file's object and its sidecar; a file without a
/// sidecar purges the same.
#[tokio::test]
async fn a_purge_removes_the_object_and_its_sidecar() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let with = quarantined(&db.catalog, &store).await;
    let without = quarantined(&db.catalog, &store).await;
    let without_sidecar = Path::from(fs_index::sidecar_path(&without).unwrap().as_str());
    store.delete(&without_sidecar).await.unwrap();

    let purged = fs_compactor::purge_once(
        &db.catalog,
        &store,
        Duration::ZERO,
        10,
        Duration::from_secs(600),
    )
    .await;
    assert_eq!(purged, 2);
    for path in [
        with.clone(),
        fs_index::sidecar_path(&with).unwrap(),
        without,
    ] {
        assert!(
            store.head(&Path::from(path.as_str())).await.is_err(),
            "{path} is gone"
        );
    }
    assert_eq!(
        db.catalog.orphans_doomed(10).await.unwrap().len(),
        0,
        "every path deleted under its claim, tombstones left"
    );
    assert_eq!(
        db.catalog.object_refused(&with).await.unwrap().as_deref(),
        Some(with.as_str())
    );
    db.finish().await;
}

/// Between the purge's catalog step and its bucket step a writer replaying
/// the file finds no row; its registration waits behind the claim that
/// holds the path while the object is deleted and is refused once the
/// claim ends, and an upload is refused before it starts, so no row ever
/// points at an object the purge deletes.
#[tokio::test]
async fn a_replay_during_the_purge_is_refused_and_never_leaves_a_row_without_an_object() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let inner: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let replayed = quarantined_file(&db.catalog, inner.as_ref()).await;
    let path = replayed.path.clone();
    let (open, gate) = tokio::sync::watch::channel(false);
    let arrived = Arc::new(tokio::sync::Notify::new());
    let held = Arc::new(HeldDeletes {
        inner: Arc::clone(&inner),
        gate,
        arrived: Arc::clone(&arrived),
    });
    let catalog = db.catalog.clone();
    let purge = tokio::spawn(async move {
        fs_compactor::purge_once(
            &catalog,
            held.as_ref(),
            Duration::ZERO,
            10,
            Duration::from_secs(600),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), arrived.notified())
        .await
        .expect("the purge never reached the delete");

    assert_eq!(
        db.catalog
            .registered_paths(std::slice::from_ref(&path))
            .await
            .unwrap()
            .len(),
        0,
        "the row is gone while the delete is held"
    );
    assert!(
        inner.head(&Path::from(path.as_str())).await.is_ok(),
        "the object is still there"
    );
    assert!(
        db.catalog.object_refused(&path).await.unwrap().is_some(),
        "an upload is refused before it starts"
    );
    let catalog = db.catalog.clone();
    let again = replayed.clone();
    let registration = tokio::spawn(async move { catalog.register_file(&again).await });
    wait_for_lock_waiter(&db.catalog, &path).await;
    assert!(
        !registration.is_finished(),
        "the registration waits behind the claim"
    );

    open.send(true).unwrap();
    assert_eq!(purge.await.unwrap(), 1);
    assert!(inner.head(&Path::from(path.as_str())).await.is_err());
    let sidecar = fs_index::sidecar_path(&path).unwrap();
    let err = registration.await.unwrap().unwrap_err();
    assert!(
        matches!(err, CatalogError::ObjectDoomed { ref path } if *path == replayed.path || *path == sidecar),
        "refused for the file or its sidecar, both doomed: {err}"
    );
    let err = db.catalog.register_file(&replayed).await.unwrap_err();
    assert!(matches!(err, CatalogError::ObjectDoomed { .. }), "{err}");
    assert_eq!(
        db.catalog
            .registered_paths(std::slice::from_ref(&path))
            .await
            .unwrap()
            .len(),
        0
    );
    db.finish().await;
}

/// Waits until a backend of the catalog waits on the advisory lock of
/// `path`, the one every registration and claim of it takes, or fails
/// after a while.
async fn wait_for_lock_waiter(catalog: &Catalog, path: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let (waiting,): (i64,) = sqlx::query_as(
            "select count(*) from pg_stat_activity a join pg_locks l on l.pid = a.pid
             where a.wait_event_type = 'Lock'
               and l.locktype = 'advisory' and not l.granted
               and l.classid = ((hashtextextended($1, 0) >> 32) & 4294967295)::oid
               and l.objid = (hashtextextended($1, 0) & 4294967295)::oid",
        )
        .bind(path)
        .fetch_one(catalog.pool())
        .await
        .unwrap();
        if waiting > 0 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no backend ever waited on the path's lock"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A pass slower than the lease TTL keeps the lease: renewal runs on its
/// own loop. Losing the lease mid-pass stops the pass.
#[tokio::test]
async fn slow_work_neither_lapses_nor_outlives_the_lease() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    // In memory: the throttle wrapper serves `head` through a streamed
    // `get`, which a local file store does not offer.
    let memory = object_store::memory::InMemory::new();
    let mut paths = Vec::new();
    for _ in 0..6 {
        paths.push(quarantined(&db.catalog, &memory).await);
    }
    // Each delete takes longer than the renewal period; a pass takes
    // longer than the TTL.
    let slow: Arc<dyn ObjectStore> = Arc::new(ThrottledStore::new(
        memory,
        ThrottleConfig {
            wait_delete_per_call: Duration::from_millis(500),
            ..ThrottleConfig::default()
        },
    ));
    let (shutdown, rx) = tokio::sync::watch::channel(false);
    let config = CompactorConfig {
        purge_interval: Duration::from_secs(1),
        quarantine_ttl: Duration::ZERO,
        purge_batch: 10,
        lease_ttl: Duration::from_millis(600),
        compaction_interval: Duration::from_secs(3600),
        compaction: no_compaction(),
        orphan_interval: Duration::from_secs(3600),
        sweep: fs_compactor::SweepConfig {
            ttl: Duration::from_secs(3600),
            objects_per_pass: 100_000,
            deletes_per_pass: 100,
            delete_backoff: Duration::from_secs(600),
            inject_delete_failure: None,
        },
        retention_interval: Duration::from_secs(3600),
        retention: fs_compactor::RetentionConfig {
            files_per_pass: 100,
            rebuild_max_input_bytes: 1 << 30,
            rebuild_max_bytes: 1 << 30,
            batch_rows: 16,
            limits: fs_schema::limits::Limits::default(),
            rebuilds_per_tick: 64,
            rebuild_backoff: Duration::from_secs(3600),
            inject_swap_failures: 0,
            inject_list_failures: 0,
        },
    };
    let task = tokio::spawn(fs_compactor::run(
        db.catalog.clone(),
        slow.clone(),
        Path::from(""),
        config,
        "loop",
        Arc::new(fs_compactor::Stats::default()),
        rx,
    ));

    // The pass is under way: the lease keeps being renewed meanwhile.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let during = db.catalog.lease(COMPACTOR_LEASE).await.unwrap().unwrap();
    assert!(during.holder.starts_with("loop/"));
    tokio::time::sleep(Duration::from_millis(700)).await;
    let later = db.catalog.lease(COMPACTOR_LEASE).await.unwrap().unwrap();
    assert_eq!(
        later.holder, during.holder,
        "still ours after a TTL of work"
    );
    assert!(
        later.expires_at > during.expires_at,
        "renewed during the pass"
    );
    // A slow runner may still be listing: wait for the deletes to show.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while count_present(slow.as_ref(), &paths).await >= 12 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "deletes are happening"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Take the lease away in one statement, so the renewer cannot slip in
    // between: the pass stops within a renewal period.
    take_over(&db.catalog, "elsewhere").await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    let at_loss = count_present(slow.as_ref(), &paths).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let after = count_present(slow.as_ref(), &paths).await;
    assert!(
        after > 0,
        "the pass did not run to completion without the lease"
    );
    assert!(
        after + 1 >= at_loss,
        "at most one delete in flight finished after the loss"
    );

    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        db.catalog
            .lease(COMPACTOR_LEASE)
            .await
            .unwrap()
            .unwrap()
            .holder,
        "elsewhere",
        "a lease that is not ours is left alone on shutdown"
    );
    db.finish().await;
}

/// Moves the compactor lease to `holder` for a minute, atomically, as a
/// takeover after expiry would.
async fn take_over(catalog: &Catalog, holder: &str) {
    sqlx::query(
        "update engine_lease set holder = $1, generation = generation + 1,
         expires_at = now() + interval '60 seconds' where name = $2",
    )
    .bind(holder)
    .bind(COMPACTOR_LEASE)
    .execute(catalog.pool())
    .await
    .unwrap();
}

/// A renewal that cannot complete within the renewal period reports the
/// lease lost, whatever the catalog would have answered.
#[tokio::test]
async fn a_stuck_renewal_counts_as_losing_the_lease() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let ttl = Duration::from_millis(900);
    let a = Leadership::new(db.catalog.clone(), "node-a", ttl);
    assert!(a.refresh().await);

    // The row is locked elsewhere: the renewal cannot complete.
    let mut holding = db.catalog.pool().begin().await.unwrap();
    sqlx::query("select generation from engine_lease where name = $1 for update")
        .bind(COMPACTOR_LEASE)
        .execute(&mut *holding)
        .await
        .unwrap();
    let started = tokio::time::Instant::now();
    assert!(!a.refresh().await, "timed out: not held");
    assert!(started.elapsed() < ttl, "reported before the TTL");
    assert!(!a.is_leader());
    assert!(a.fence().is_none());

    holding.commit().await.unwrap();
    assert!(a.refresh().await, "held again once the catalog answers");
    db.finish().await;
}

/// Compaction settings that never find a closed hour in these tests.
fn no_compaction() -> fs_compactor::CompactionConfig {
    fs_compactor::CompactionConfig {
        hour_close_delay: Duration::from_secs(3600),
        l1_target_bytes: 512 * 1024 * 1024,
        max_inputs_per_pass: 16,
        max_input_bytes: 2 << 30,
        row_group_rows: 1024,
        inject_swap_failures: 0,
        inject_put_failure_at: None,
        inject_put_hold: None,
    }
}

/// Objects and sidecars of `paths` still in the store.
async fn count_present(store: &dyn ObjectStore, paths: &[String]) -> usize {
    let mut present = 0;
    for path in paths {
        for p in [path.clone(), fs_index::sidecar_path(path).unwrap()] {
            if store.head(&Path::from(p.as_str())).await.is_ok() {
                present += 1;
            }
        }
    }
    present
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
            .is_some()
    );

    let (shutdown, rx) = tokio::sync::watch::channel(false);
    let config = CompactorConfig {
        purge_interval: Duration::from_secs(1),
        quarantine_ttl: Duration::ZERO,
        purge_batch: 10,
        lease_ttl: Duration::from_millis(600),
        compaction_interval: Duration::from_secs(3600),
        compaction: no_compaction(),
        orphan_interval: Duration::from_secs(3600),
        sweep: fs_compactor::SweepConfig {
            ttl: Duration::from_secs(3600),
            objects_per_pass: 100_000,
            deletes_per_pass: 100,
            delete_backoff: Duration::from_secs(600),
            inject_delete_failure: None,
        },
        retention_interval: Duration::from_secs(3600),
        retention: fs_compactor::RetentionConfig {
            files_per_pass: 100,
            rebuild_max_input_bytes: 1 << 30,
            rebuild_max_bytes: 1 << 30,
            batch_rows: 16,
            limits: fs_schema::limits::Limits::default(),
            rebuilds_per_tick: 64,
            rebuild_backoff: Duration::from_secs(3600),
            inject_swap_failures: 0,
            inject_list_failures: 0,
        },
    };
    let task = tokio::spawn(fs_compactor::run(
        db.catalog.clone(),
        store.clone(),
        Path::from(""),
        config,
        "loop",
        Arc::new(fs_compactor::Stats::default()),
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
    assert!(
        db.catalog
            .lease(COMPACTOR_LEASE)
            .await
            .unwrap()
            .unwrap()
            .holder
            .starts_with("loop/")
    );

    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert!(
        db.catalog
            .acquire_lease(COMPACTOR_LEASE, "probe", Duration::from_secs(1))
            .await
            .unwrap()
            .is_some(),
        "released on shutdown"
    );
    db.finish().await;
}
