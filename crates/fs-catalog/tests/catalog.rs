//! Integration tests against a real Postgres.
//!
//! They run when `FS_TEST_DATABASE_URL` points at a reachable server
//! (`make dev-up` starts one) and are skipped with a message otherwise. Every
//! test works in its own schema so tests run in parallel and leave nothing
//! behind.
#![allow(
    clippy::too_many_lines,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::time::Duration as StdDuration;

use chrono::{Duration, TimeZone, Utc};
use fs_catalog::{
    Catalog, CatalogError, Doom, FileId, FileState, LeaseFence, Level, NewFile, NewTenant, Signal,
    TenantId, TimeRange, TokenScope, hash_token,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection};

struct TestDb {
    admin_url: String,
    schema: String,
    catalog: Catalog,
}

impl TestDb {
    /// `None` when no database is configured; the caller returns early.
    async fn new() -> Option<Self> {
        let Ok(admin_url) = std::env::var("FS_TEST_DATABASE_URL") else {
            eprintln!("skipped: FS_TEST_DATABASE_URL is not set (run `make dev-up`)");
            return None;
        };
        let schema = format!("t_{}", uuid::Uuid::new_v4().simple());
        let mut admin = PgConnection::connect(&admin_url)
            .await
            .expect("connect to test database");
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

fn hour(h: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 24, h, 0, 0).unwrap()
}

fn file(
    tenant: TenantId,
    signal: Signal,
    start_hour: u32,
    end_hour: u32,
    services: &[&str],
) -> NewFile {
    let id = FileId::new();
    NewFile {
        id,
        tenant_id: tenant,
        signal,
        level: Level::L0,
        path: format!("t/{}/{}/l0/{}.parquet", tenant.0, signal.path_name(), id.0),
        size_bytes: 128 << 20,
        row_count: 1_000_000,
        range: TimeRange {
            start: hour(start_hour),
            end: hour(end_hour),
        },
        services: services.iter().map(|s| (*s).to_owned()).collect(),
        summary: Some(vec![1, 2, 3]),
    }
}

#[tokio::test]
async fn migrations_are_idempotent() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    db.catalog.migrate().await.unwrap();
    db.catalog.migrate().await.unwrap();
    db.finish().await;
}

#[tokio::test]
async fn tenants_round_trip_and_names_are_unique() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let created = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap();
    assert_eq!(created.retention_days, 15);
    let fetched = db.catalog.tenant(created.id).await.unwrap().unwrap();
    assert_eq!(fetched, created);
    assert!(db.catalog.tenant(TenantId::new()).await.unwrap().is_none());
    let dup = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await;
    assert!(matches!(dup, Err(CatalogError::Database(_))));
    db.finish().await;
}

#[tokio::test]
async fn visible_files_filter_by_tenant_signal_and_overlap() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let a = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("a"))
        .await
        .unwrap()
        .id;
    let b = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("b"))
        .await
        .unwrap()
        .id;

    let f1 = db
        .catalog
        .register_file(&file(a, Signal::Traces, 0, 1, &["api"]))
        .await
        .unwrap();
    let f2 = db
        .catalog
        .register_file(&file(a, Signal::Traces, 1, 2, &["api", "worker"]))
        .await
        .unwrap();
    let _f3 = db
        .catalog
        .register_file(&file(a, Signal::Traces, 5, 6, &["api"]))
        .await
        .unwrap();
    let _logs = db
        .catalog
        .register_file(&file(a, Signal::Logs, 0, 1, &["api"]))
        .await
        .unwrap();
    let _other = db
        .catalog
        .register_file(&file(b, Signal::Traces, 0, 1, &["api"]))
        .await
        .unwrap();

    assert_eq!(f1.state, FileState::Visible);
    assert_eq!(f1.summary, Some(vec![1, 2, 3]));

    let hits = db
        .catalog
        .visible_files(
            a,
            Signal::Traces,
            TimeRange {
                start: hour(1),
                end: hour(1),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        hits.iter().map(|f| f.id).collect::<Vec<_>>(),
        vec![f1.id, f2.id],
        "inclusive overlap, oldest first"
    );

    let none = db
        .catalog
        .visible_files(
            a,
            Signal::Traces,
            TimeRange {
                start: hour(3),
                end: hour(4),
            },
        )
        .await
        .unwrap();
    assert_eq!(none.len(), 0);

    let covered = db.catalog.covered_until(a, Signal::Traces).await.unwrap();
    assert_eq!(covered, Some(hour(6)));
    assert_eq!(
        db.catalog.covered_until(b, Signal::Logs).await.unwrap(),
        None
    );

    // Unknown tenant violates the foreign key; duplicate path violates the unique index.
    let orphan = file(TenantId::new(), Signal::Traces, 0, 1, &[]);
    assert!(db.catalog.register_file(&orphan).await.is_err());
    let mut dup = file(a, Signal::Traces, 0, 1, &[]);
    dup.path.clone_from(&f1.path);
    assert!(db.catalog.register_file(&dup).await.is_err());
    db.finish().await;
}

#[tokio::test]
async fn swap_is_atomic_and_detects_conflicts() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("t"))
        .await
        .unwrap()
        .id;
    let in1 = db
        .catalog
        .register_file(&file(t, Signal::Traces, 0, 1, &["api"]))
        .await
        .unwrap();
    let in2 = db
        .catalog
        .register_file(&file(t, Signal::Traces, 1, 2, &["api"]))
        .await
        .unwrap();
    let mut out = file(t, Signal::Traces, 0, 2, &["api"]);
    out.level = Level::L1;

    let compaction = db
        .catalog
        .swap_files(
            t,
            Signal::Traces,
            &[in1.id, in2.id],
            std::slice::from_ref(&out),
        )
        .await
        .unwrap();
    assert_eq!(compaction.inputs, vec![in1.id, in2.id]);
    assert_eq!(compaction.outputs, vec![out.id]);

    let visible = db
        .catalog
        .visible_files(
            t,
            Signal::Traces,
            TimeRange {
                start: hour(0),
                end: hour(2),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        visible.iter().map(|f| f.id).collect::<Vec<_>>(),
        vec![out.id]
    );
    assert_eq!(visible[0].level, Level::L1);

    // A second compactor racing on the same inputs loses, and its outputs never appear.
    let loser = file(t, Signal::Traces, 0, 2, &["api"]);
    let err = db
        .catalog
        .swap_files(
            t,
            Signal::Traces,
            &[in1.id, in2.id],
            std::slice::from_ref(&loser),
        )
        .await;
    assert!(matches!(
        err,
        Err(CatalogError::CompactionConflict {
            expected: 2,
            found: 0
        })
    ));
    let after = db
        .catalog
        .visible_files(
            t,
            Signal::Traces,
            TimeRange {
                start: hour(0),
                end: hour(2),
            },
        )
        .await
        .unwrap();
    assert_eq!(after.len(), 1);
    assert!(after.iter().all(|f| f.id != loser.id));
    db.finish().await;
}

#[tokio::test]
async fn purge_returns_paths_of_old_quarantined_files_only() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("t"))
        .await
        .unwrap()
        .id;
    let old = db
        .catalog
        .register_file(&file(t, Signal::Logs, 0, 1, &[]))
        .await
        .unwrap();
    let keep = db
        .catalog
        .register_file(&file(t, Signal::Logs, 1, 2, &[]))
        .await
        .unwrap();
    let replacement = file(t, Signal::Logs, 0, 1, &[]);
    db.catalog
        .swap_files(t, Signal::Logs, &[old.id], &[replacement])
        .await
        .unwrap();

    let not_yet = db
        .catalog
        .purge_quarantined(StdDuration::from_secs(3600), 100)
        .await
        .unwrap();
    assert_eq!(not_yet.len(), 0, "quarantined just now, inside the TTL");

    let purged = db
        .catalog
        .purge_quarantined(StdDuration::ZERO, 100)
        .await
        .unwrap();
    assert_eq!(purged.len(), 1);
    assert_eq!(purged[0].id, old.id);
    assert_eq!(purged[0].path, old.path);

    let again = db
        .catalog
        .purge_quarantined(StdDuration::ZERO, 100)
        .await
        .unwrap();
    assert_eq!(again.len(), 0);
    let visible = db
        .catalog
        .visible_files(
            t,
            Signal::Logs,
            TimeRange {
                start: hour(0),
                end: hour(2),
            },
        )
        .await
        .unwrap();
    assert!(visible.iter().any(|f| f.id == keep.id));
    db.finish().await;
}

#[tokio::test]
async fn ingester_heartbeats_expire() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    db.catalog
        .heartbeat("ingester-0", "10.0.0.1:8815")
        .await
        .unwrap();
    db.catalog
        .heartbeat("ingester-0", "10.0.0.2:8815")
        .await
        .unwrap();
    db.catalog
        .heartbeat("ingester-1", "10.0.0.3:8815")
        .await
        .unwrap();
    let live = db
        .catalog
        .live_ingesters(Duration::seconds(10))
        .await
        .unwrap();
    assert_eq!(live.len(), 2);
    assert_eq!(live[0].address, "10.0.0.2:8815", "latest address wins");
    let none = db
        .catalog
        .live_ingesters(Duration::seconds(-1))
        .await
        .unwrap();
    assert_eq!(none.len(), 0);
    db.finish().await;
}

/// One holder at a time: a second taker fails until the lease expires or
/// is released; the holder renews without losing its acquisition time or
/// generation, and every change of hands bumps the generation.
#[tokio::test]
async fn leases_are_exclusive_until_they_expire_or_are_released() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let ttl = StdDuration::from_secs(30);
    assert_eq!(
        db.catalog
            .acquire_lease("compactor", "a", ttl)
            .await
            .unwrap(),
        Some(1)
    );
    assert_eq!(
        db.catalog
            .acquire_lease("compactor", "b", ttl)
            .await
            .unwrap(),
        None,
        "held by a"
    );
    let first = db.catalog.lease("compactor").await.unwrap().unwrap();
    assert_eq!(first.holder, "a");
    assert_eq!(first.generation, 1);

    assert_eq!(
        db.catalog
            .acquire_lease("compactor", "a", ttl)
            .await
            .unwrap(),
        Some(1),
        "renewal"
    );
    let renewed = db.catalog.lease("compactor").await.unwrap().unwrap();
    assert_eq!(
        renewed.acquired_at, first.acquired_at,
        "a renewal keeps the acquisition time"
    );
    assert!(renewed.expires_at >= first.expires_at);

    assert!(
        !db.catalog.release_lease("compactor", "b").await.unwrap(),
        "not b's to release"
    );
    assert!(db.catalog.release_lease("compactor", "a").await.unwrap());
    assert_eq!(
        db.catalog
            .acquire_lease("compactor", "b", ttl)
            .await
            .unwrap(),
        Some(2),
        "a released lease changes hands at the next generation"
    );

    // b stops renewing: once its short lease lapses, a takes over at the next generation.
    assert_eq!(
        db.catalog
            .acquire_lease("compactor", "b", StdDuration::from_secs(2))
            .await
            .unwrap(),
        Some(2),
        "b renews its own lease with a shorter TTL"
    );
    assert_eq!(
        db.catalog
            .acquire_lease("compactor", "a", ttl)
            .await
            .unwrap(),
        None
    );
    tokio::time::sleep(StdDuration::from_millis(2500)).await;
    assert_eq!(
        db.catalog
            .acquire_lease("compactor", "a", ttl)
            .await
            .unwrap(),
        Some(3),
        "expired"
    );
    let taken = db.catalog.lease("compactor").await.unwrap().unwrap();
    assert_eq!(taken.holder, "a");
    assert!(taken.acquired_at > first.acquired_at, "a new acquisition");

    // Other names are independent.
    assert_eq!(
        db.catalog.acquire_lease("other", "b", ttl).await.unwrap(),
        Some(1)
    );
    db.finish().await;
}

/// A swap fenced by a lease the holder lost changes nothing.
#[tokio::test]
async fn a_fenced_swap_is_refused_once_the_lease_changed_hands() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("t"))
        .await
        .unwrap()
        .id;
    let input = db
        .catalog
        .register_file(&file(t, Signal::Logs, 0, 1, &[]))
        .await
        .unwrap();
    let generation = db
        .catalog
        .acquire_lease("compactor", "a", StdDuration::from_millis(200))
        .await
        .unwrap()
        .unwrap();
    let fence = LeaseFence {
        name: "compactor".into(),
        holder: "a".into(),
        generation,
    };
    tokio::time::sleep(StdDuration::from_millis(400)).await;
    assert_eq!(
        db.catalog
            .acquire_lease("compactor", "b", StdDuration::from_secs(30))
            .await
            .unwrap(),
        Some(generation + 1)
    );

    let err = db
        .catalog
        .swap_files_fenced(
            t,
            Signal::Logs,
            &[input.id],
            &[file(t, Signal::Logs, 0, 1, &[])],
            Some(&fence),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, CatalogError::LeaseLost { ref holder, .. } if holder == "a"),
        "{err}"
    );
    let visible = db
        .catalog
        .visible_files(
            t,
            Signal::Logs,
            TimeRange {
                start: hour(0),
                end: hour(1),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        visible.len(),
        1,
        "the input is still visible, no output registered"
    );
    assert_eq!(visible[0].id, input.id);

    // The holder of the current generation may swap.
    let fence = LeaseFence {
        holder: "b".into(),
        generation: generation + 1,
        ..fence
    };
    db.catalog
        .swap_files_fenced(
            t,
            Signal::Logs,
            &[input.id],
            &[file(t, Signal::Logs, 0, 1, &[])],
            Some(&fence),
        )
        .await
        .unwrap();
    db.finish().await;
}

/// The fence check locks the lease row until the swap commits: a takeover
/// started in the window between the check and the commit waits, and a
/// swap whose fence went stale while it waited for the lock is refused.
#[tokio::test]
async fn a_fenced_swap_is_serialised_with_takeovers() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("t"))
        .await
        .unwrap()
        .id;
    let input = db
        .catalog
        .register_file(&file(t, Signal::Logs, 0, 1, &[]))
        .await
        .unwrap();
    let generation = db
        .catalog
        .acquire_lease("compactor", "a", StdDuration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let fence = LeaseFence {
        name: "compactor".into(),
        holder: "a".into(),
        generation,
    };

    // Someone holds the row as a swap would while it writes its outputs.
    let mut holding = db.catalog.pool().begin().await.unwrap();
    sqlx::query("select generation from engine_lease where name = 'compactor' for update")
        .execute(&mut *holding)
        .await
        .unwrap();
    let takeover = tokio::spawn({
        let catalog = db.catalog.clone();
        async move {
            catalog
                .acquire_lease("compactor", "b", StdDuration::from_secs(30))
                .await
                .unwrap()
        }
    });
    wait_for_lock_waiter(&db.catalog).await;
    assert!(!takeover.is_finished(), "the takeover waits for the swap");
    holding.commit().await.unwrap();
    assert_eq!(
        takeover.await.unwrap(),
        None,
        "and then finds the lease still a's"
    );

    // The other way round: a swap that waits behind a takeover is refused.
    let mut taking = db.catalog.pool().begin().await.unwrap();
    sqlx::query(
        "update engine_lease set holder = 'b', generation = generation + 1,
         expires_at = now() + interval '30 seconds' where name = 'compactor'",
    )
    .execute(&mut *taking)
    .await
    .unwrap();
    let swap = tokio::spawn({
        let catalog = db.catalog.clone();
        let fence = fence.clone();
        let output = file(t, Signal::Logs, 0, 1, &[]);
        async move {
            catalog
                .swap_files_fenced(t, Signal::Logs, &[input.id], &[output], Some(&fence))
                .await
        }
    });
    wait_for_lock_waiter(&db.catalog).await;
    assert!(!swap.is_finished(), "the swap waits for the takeover");
    taking.commit().await.unwrap();
    let err = swap.await.unwrap().unwrap_err();
    assert!(matches!(err, CatalogError::LeaseLost { .. }), "{err}");
    let visible = db
        .catalog
        .visible_files(
            t,
            Signal::Logs,
            TimeRange {
                start: hour(0),
                end: hour(1),
            },
        )
        .await
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id, input.id, "nothing published");
    db.finish().await;
}

/// Waits until some backend is blocked on a lock while holding a lock on
/// this schema's `engine_lease` or `engine_orphan`, so the test knows the task reached the
/// row rather than assuming it from a sleep. Panics after five seconds:
/// nothing waited, so nothing was serialised.
async fn wait_for_lock_waiter(catalog: &Catalog) {
    let deadline = tokio::time::Instant::now() + StdDuration::from_secs(5);
    loop {
        let (waiting,): (i64,) = sqlx::query_as(
            "select count(*) from pg_stat_activity a
             where a.wait_event_type = 'Lock'
               and exists (
                   select 1 from pg_locks l
                   where l.pid = a.pid
                     and (l.relation in ('engine_lease'::regclass, 'engine_orphan'::regclass)
                          or l.locktype = 'advisory')
               )",
        )
        .fetch_one(catalog.pool())
        .await
        .unwrap();
        if waiting > 0 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no backend ever waited on the lease row"
        );
        tokio::time::sleep(StdDuration::from_millis(20)).await;
    }
}

/// Backdates the registration of `ids` by an hour, as files flushed long
/// ago would show.
async fn registered_an_hour_ago(db: &TestDb, ids: &[FileId]) {
    sqlx::query("update engine_file set created_at = now() - interval '1 hour' where id = any($1)")
        .bind(ids)
        .execute(db.catalog.pool())
        .await
        .unwrap();
}

/// The oldest cold hour with enough L0 files comes first, with every L0
/// file of that hour; an hour with a file that ended or was registered
/// recently is not cold; other levels do not count.
#[tokio::test]
async fn compaction_inputs_are_the_l0_files_of_the_oldest_cold_hour() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("t"))
        .await
        .unwrap()
        .id;
    let closed = StdDuration::from_secs(60);
    let inputs = |min: i64| db.catalog.compaction_inputs(Signal::Traces, closed, min, 0);
    assert!(inputs(2).await.unwrap().is_none());

    // Hour 1 has two files, hour 0 one, hour 2 three; all ended long ago.
    let h0 = db
        .catalog
        .register_file(&file(t, Signal::Traces, 0, 0, &[]))
        .await
        .unwrap();
    let h1a = db
        .catalog
        .register_file(&file(t, Signal::Traces, 1, 1, &[]))
        .await
        .unwrap();
    let h1b = db
        .catalog
        .register_file(&file(t, Signal::Traces, 1, 1, &[]))
        .await
        .unwrap();
    let mut h2 = Vec::new();
    for _ in 0..3 {
        h2.push(
            db.catalog
                .register_file(&file(t, Signal::Traces, 2, 2, &[]))
                .await
                .unwrap()
                .id,
        );
    }
    assert!(
        inputs(2).await.unwrap().is_none(),
        "registered just now: late data may still be coming and the hot window may serve them"
    );
    registered_an_hour_ago(&db, &[h0.id, h1a.id, h1b.id]).await;
    registered_an_hour_ago(&db, &h2).await;

    let found = inputs(2).await.unwrap().unwrap();
    assert_eq!(found.tenant_id, t);
    assert_eq!(
        found.hour,
        hour(1),
        "hour 0 has one file; hour 1 is the oldest with two"
    );
    let second = db
        .catalog
        .compaction_inputs(Signal::Traces, closed, 2, 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.hour, hour(2), "skipping one lands on the next hour");
    assert_eq!(second.files.len(), 3);
    assert!(
        db.catalog
            .compaction_inputs(Signal::Traces, closed, 2, 2)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        found.files.iter().map(|f| f.id).collect::<Vec<_>>(),
        [h1a.id, h1b.id],
        "registration order"
    );

    // A file of hour 1 that ended recently keeps the whole hour out, even
    // though two cold files are there.
    let mut recent = file(t, Signal::Traces, 1, 1, &[]);
    recent.range.end = Utc::now();
    let recent = db.catalog.register_file(&recent).await.unwrap();
    registered_an_hour_ago(&db, &[recent.id]).await;
    assert_eq!(
        inputs(2).await.unwrap().unwrap().hour,
        hour(2),
        "hour 1 is not cold"
    );

    // Once hour 2 is swapped for an L1 file, nothing qualifies: the L1
    // does not count and hour 1 is still not cold.
    let mut l1 = file(t, Signal::Traces, 2, 2, &[]);
    l1.level = Level::L1;
    db.catalog
        .swap_files(t, Signal::Traces, &h2, &[l1])
        .await
        .unwrap();
    assert!(inputs(2).await.unwrap().is_none());
    assert!(
        inputs(1).await.unwrap().is_some(),
        "hour 0 alone qualifies at one file"
    );
    db.finish().await;
}

/// Orphans keep their first sighting across sweeps and come due by it.
#[tokio::test]
async fn orphans_keep_their_first_sighting() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("t"))
        .await
        .unwrap()
        .id;
    let registered = db
        .catalog
        .register_file(&file(t, Signal::Traces, 0, 0, &[]))
        .await
        .unwrap();
    let found = db
        .catalog
        .registered_paths(&[registered.path.clone(), "t/nobody.parquet".into()])
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    assert!(found.contains(&registered.path));

    db.catalog
        .orphans_seen(&["a".into(), "b".into()])
        .await
        .unwrap();
    assert_eq!(
        db.catalog
            .orphans_due(StdDuration::from_secs(60), 100)
            .await
            .unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(
        db.catalog
            .orphans_due(StdDuration::ZERO, 100)
            .await
            .unwrap(),
        ["a", "b"]
    );
    sqlx::query(
        "update engine_orphan set first_seen = now() - interval '2 hours' where path = 'a'",
    )
    .execute(db.catalog.pool())
    .await
    .unwrap();
    db.catalog
        .orphans_seen(&["a".into(), "c".into()])
        .await
        .unwrap();
    assert_eq!(
        db.catalog
            .orphans_due(StdDuration::from_secs(3600), 100)
            .await
            .unwrap(),
        ["a"],
        "seen again, a keeps its first sighting; c is new"
    );
    db.catalog
        .orphans_forget(&["a".into(), "zzz".into()])
        .await
        .unwrap();
    assert_eq!(
        db.catalog
            .orphans_due(StdDuration::ZERO, 100)
            .await
            .unwrap(),
        ["b", "c"]
    );
    db.finish().await;
}

/// The mark and a registration serialise: whichever commits first wins,
/// a sidecar's mark is refused by a row for the file it belongs to, and a
/// mark already made is reported as such, never undone.
#[tokio::test]
async fn a_marked_orphan_refuses_registration_and_a_registered_path_refuses_the_mark() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("t"))
        .await
        .unwrap()
        .id;
    let pending = file(t, Signal::Traces, 0, 0, &[]);
    let sidecar = fs_index::sidecar_path(&pending.path).unwrap();
    let doom = |path: String, owners: Vec<String>| {
        let catalog = db.catalog.clone();
        async move { catalog.orphan_doom(&path, &owners).await.unwrap() }
    };

    assert_eq!(
        doom("nobody".into(), vec!["nobody".into()]).await,
        Doom::Missing
    );

    // Marked first: the registration is refused, for the file and for its
    // sidecar; a second mark finds the first and leaves it.
    db.catalog
        .orphans_seen(std::slice::from_ref(&pending.path))
        .await
        .unwrap();
    assert_eq!(
        doom(pending.path.clone(), vec![pending.path.clone()]).await,
        Doom::Marked
    );
    assert_eq!(
        doom(pending.path.clone(), vec![pending.path.clone()]).await,
        Doom::AlreadyMarked
    );
    let err = db.catalog.register_file(&pending).await.unwrap_err();
    assert!(
        matches!(err, CatalogError::ObjectDoomed { ref path } if *path == pending.path),
        "{err}"
    );
    assert_eq!(
        db.catalog.orphans_doomed(10).await.unwrap(),
        std::slice::from_ref(&pending.path)
    );
    db.catalog
        .orphans_forget(std::slice::from_ref(&pending.path))
        .await
        .unwrap();
    db.catalog
        .orphans_seen(std::slice::from_ref(&sidecar))
        .await
        .unwrap();
    assert_eq!(
        doom(sidecar.clone(), fs_index::owners_of_sidecar(&sidecar)).await,
        Doom::Marked
    );
    let err = db.catalog.register_file(&pending).await.unwrap_err();
    assert!(
        matches!(err, CatalogError::ObjectDoomed { ref path } if *path == sidecar),
        "{err}"
    );
    db.catalog
        .orphans_forget(std::slice::from_ref(&sidecar))
        .await
        .unwrap();

    // Registered first: the mark is refused, for the file and for its sidecar.
    db.catalog.register_file(&pending).await.unwrap();
    db.catalog
        .orphans_seen(&[pending.path.clone(), sidecar.clone()])
        .await
        .unwrap();
    assert_eq!(
        doom(pending.path.clone(), vec![pending.path.clone()]).await,
        Doom::Owned
    );
    assert_eq!(
        doom(sidecar.clone(), fs_index::owners_of_sidecar(&sidecar)).await,
        Doom::Owned
    );
    assert_eq!(
        db.catalog.orphans_doomed(10).await.unwrap(),
        Vec::<String>::new()
    );

    // A registration holds the path's lock until it commits, before any
    // orphan row exists for it, so a mark started meanwhile waits and is
    // then refused. The registration is played by hand in one
    // transaction: the lock, then the sighting arriving, then the row.
    let late = file(t, Signal::Traces, 1, 1, &[]);
    let mut registering = db.catalog.pool().begin().await.unwrap();
    sqlx::query("select pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(&late.path)
        .execute(&mut *registering)
        .await
        .unwrap();
    db.catalog
        .orphans_seen(std::slice::from_ref(&late.path))
        .await
        .unwrap();
    let mark = tokio::spawn(doom(late.path.clone(), vec![late.path.clone()]));
    wait_for_lock_waiter(&db.catalog).await;
    assert!(!mark.is_finished());
    sqlx::query(
        "insert into engine_file
             (id, tenant_id, signal, level, path, size_bytes, row_count, ts_min, ts_max, services, state)
         values ($1, $2, 1, 0, $3, 1, 1, now(), now(), '{}', 1)",
    )
    .bind(late.id)
    .bind(t)
    .bind(&late.path)
    .execute(&mut *registering)
    .await
    .unwrap();
    registering.commit().await.unwrap();
    assert_eq!(
        mark.await.unwrap(),
        Doom::Owned,
        "the row the registration committed stops the mark"
    );
    db.finish().await;
}

/// A claim holds the path while its object is deleted: a second sweeper
/// claiming the same path waits and gets nothing once the first finished,
/// and a registration re-uploading the path waits for the claim to end.
#[tokio::test]
async fn a_claim_holds_the_path_against_another_sweeper_and_a_registration() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("t"))
        .await
        .unwrap()
        .id;
    let pending = file(t, Signal::Traces, 0, 0, &[]);
    db.catalog
        .orphans_seen(std::slice::from_ref(&pending.path))
        .await
        .unwrap();
    assert_eq!(
        db.catalog
            .orphan_doom(&pending.path, std::slice::from_ref(&pending.path))
            .await
            .unwrap(),
        Doom::Marked
    );

    // Sweeper A holds the claim; sweeper B's claim waits.
    let claim = db
        .catalog
        .orphan_claim(&pending.path)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.path(), pending.path);
    let second = tokio::spawn({
        let catalog = db.catalog.clone();
        let path = pending.path.clone();
        async move { catalog.orphan_claim(&path).await.unwrap().is_some() }
    });
    wait_for_lock_waiter(&db.catalog).await;
    assert!(!second.is_finished(), "B waits for A");
    claim.finish().await.unwrap();
    assert!(
        !second.await.unwrap(),
        "B finds nothing to delete once A finished"
    );

    // The writer re-uploads and registers: a claim in flight makes the
    // registration wait, and it goes through once the claim is finished.
    db.catalog
        .orphans_seen(std::slice::from_ref(&pending.path))
        .await
        .unwrap();
    assert_eq!(
        db.catalog
            .orphan_doom(&pending.path, std::slice::from_ref(&pending.path))
            .await
            .unwrap(),
        Doom::Marked
    );
    let claim = db
        .catalog
        .orphan_claim(&pending.path)
        .await
        .unwrap()
        .unwrap();
    let registration = tokio::spawn({
        let catalog = db.catalog.clone();
        let pending = pending.clone();
        async move { catalog.register_file(&pending).await }
    });
    wait_for_lock_waiter(&db.catalog).await;
    assert!(
        !registration.is_finished(),
        "the registration waits for the claim"
    );
    claim.finish().await.unwrap();
    registration.await.unwrap().unwrap();
    assert_eq!(
        db.catalog.orphans_doomed(10).await.unwrap(),
        Vec::<String>::new()
    );

    // A delete that failed waits its backoff behind the others, and longer
    // each time.
    let slow = file(t, Signal::Traces, 3, 3, &[]);
    let fast = file(t, Signal::Traces, 4, 4, &[]);
    for f in [&slow, &fast] {
        db.catalog
            .orphans_seen(std::slice::from_ref(&f.path))
            .await
            .unwrap();
        db.catalog
            .orphan_doom(&f.path, std::slice::from_ref(&f.path))
            .await
            .unwrap();
    }
    let claim = db.catalog.orphan_claim(&slow.path).await.unwrap().unwrap();
    claim
        .retry_later(StdDuration::from_secs(600))
        .await
        .unwrap();
    assert_eq!(
        db.catalog.orphans_doomed(10).await.unwrap(),
        std::slice::from_ref(&fast.path),
        "the failed one waits; the other is tried"
    );
    sqlx::query(
        "update engine_orphan set retry_after = now() - interval '1 second' where path = $1",
    )
    .bind(&slow.path)
    .execute(db.catalog.pool())
    .await
    .unwrap();
    assert_eq!(db.catalog.orphans_doomed(10).await.unwrap().len(), 2);
    let claim = db.catalog.orphan_claim(&slow.path).await.unwrap().unwrap();
    claim
        .retry_later(StdDuration::from_secs(600))
        .await
        .unwrap();
    let (attempts, wait): (i32, f64) = sqlx::query_as(
        "select attempts, extract(epoch from retry_after - now())::float8 from engine_orphan where path = $1",
    )
    .bind(&slow.path)
    .fetch_one(db.catalog.pool())
    .await
    .unwrap();
    assert_eq!(attempts, 2);
    assert!(
        wait > 1100.0 && wait <= 1200.0,
        "twice the backoff on the second failure: {wait}"
    );
    db.catalog
        .orphans_forget(&[slow.path.clone(), fast.path.clone()])
        .await
        .unwrap();

    // A dropped claim keeps the mark for a later pass.
    let other = file(t, Signal::Traces, 2, 2, &[]);
    db.catalog
        .orphans_seen(std::slice::from_ref(&other.path))
        .await
        .unwrap();
    db.catalog
        .orphan_doom(&other.path, std::slice::from_ref(&other.path))
        .await
        .unwrap();
    drop(db.catalog.orphan_claim(&other.path).await.unwrap());
    assert_eq!(
        db.catalog.orphans_doomed(10).await.unwrap(),
        std::slice::from_ref(&other.path)
    );
    assert!(
        db.catalog
            .orphan_claim("t/never.parquet")
            .await
            .unwrap()
            .is_none(),
        "not marked"
    );
    db.finish().await;
}

#[tokio::test]
async fn watcher_sees_file_changes() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("t"))
        .await
        .unwrap()
        .id;
    let mut watcher = db.catalog.watch().await.unwrap();
    db.catalog
        .register_file(&file(t, Signal::MetricSamples, 0, 1, &[]))
        .await
        .unwrap();

    // The channel is shared by every schema on the server, so wait for our tenant.
    let deadline = tokio::time::Instant::now() + StdDuration::from_secs(5);
    loop {
        let change = tokio::time::timeout_at(deadline, watcher.next())
            .await
            .expect("notification")
            .unwrap();
        if change.tenant_id == t {
            assert_eq!(change.signal, Signal::MetricSamples);
            break;
        }
    }
    // The watcher holds a pooled connection; closing the pool would wait for it.
    drop(watcher);
    db.finish().await;
}

#[tokio::test]
async fn tokens_resolve_until_revoked() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("t"))
        .await
        .unwrap()
        .id;
    let (secret, token) = db
        .catalog
        .create_token(t, "collector-a", TokenScope::Ingest)
        .await
        .unwrap();
    assert_eq!(token.scope, TokenScope::Ingest);
    assert_eq!(token.tenant_id, t);
    assert!(token.revoked_at.is_none());
    assert!(secret.starts_with("fst_"));

    assert_eq!(
        db.catalog
            .resolve_token(&secret, TokenScope::Ingest)
            .await
            .unwrap(),
        Some(t)
    );
    assert_eq!(
        db.catalog
            .resolve_token(&secret, TokenScope::Query)
            .await
            .unwrap(),
        None,
        "scope is enforced"
    );
    assert_eq!(
        db.catalog
            .resolve_token("fst_nope", TokenScope::Ingest)
            .await
            .unwrap(),
        None
    );
    let other = fs_catalog::generate_token().unwrap();
    assert_eq!(
        db.catalog
            .resolve_token(&other, TokenScope::Ingest)
            .await
            .unwrap(),
        None,
        "unknown but well formed"
    );

    assert!(db.catalog.revoke_token(&hash_token(&secret)).await.unwrap());
    assert!(
        !db.catalog.revoke_token(&hash_token(&secret)).await.unwrap(),
        "second revoke is a no-op"
    );
    assert_eq!(
        db.catalog
            .resolve_token(&secret, TokenScope::Ingest)
            .await
            .unwrap(),
        None
    );
    let listed = db.catalog.tokens(t).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert!(listed[0].revoked_at.is_some());
    assert!(
        db.catalog
            .create_token(TenantId::new(), "orphan", TokenScope::Query)
            .await
            .is_err(),
        "unknown tenant"
    );
    db.finish().await;
}

#[tokio::test]
async fn register_is_idempotent_on_id() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("t"))
        .await
        .unwrap()
        .id;
    let mut f = file(t, Signal::Traces, 0, 1, &["api"]);
    f.id = FileId::derived(uuid::Uuid::new_v4(), t, 7, 4096);
    f.path = format!("t/{}/traces/l0/{}.parquet", t.0, f.id.0);
    let first = db.catalog.register_file(&f).await.unwrap();
    let again = db.catalog.register_file(&f).await.unwrap();
    assert_eq!(first, again, "same id returns the existing row");
    let mut changed = f.clone();
    changed.row_count += 1;
    let err = db.catalog.register_file(&changed).await.unwrap_err();
    assert!(
        matches!(
            err,
            CatalogError::FileMismatch {
                field: "row_count",
                ..
            }
        ),
        "{err}"
    );
    let visible = db
        .catalog
        .visible_files(
            t,
            Signal::Traces,
            TimeRange {
                start: hour(0),
                end: hour(1),
            },
        )
        .await
        .unwrap();
    assert_eq!(visible.len(), 1);
    db.finish().await;
}
