//! What the compactor asks of metric file sets: inputs, swaps and
//! accounting, against a real Postgres.
#![allow(
    clippy::too_many_lines,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, TimeZone, Utc};
use fs_catalog::{
    COMPACTOR_LEASE, Catalog, CatalogError, FileId, FileSetId, FileState, LeaseFence, Level,
    MetricPass, MetricRole, NewFile, NewTenant, Signal, TenantId, TimeRange,
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
        let schema = format!("ms_{}", uuid::Uuid::new_v4().simple());
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

/// 2026-10-24T14:00:00Z, long closed.
fn hour() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 24, 14, 0, 0).unwrap()
}

fn file(
    tenant: TenantId,
    role: MetricRole,
    level: Level,
    start: DateTime<Utc>,
    minutes: i64,
) -> NewFile {
    let id = FileId::new();
    NewFile {
        id,
        origin: id,
        tenant_id: tenant,
        signal: role.signal(),
        level,
        path: format!(
            "t/{}/metrics/{}/{}.parquet",
            tenant.0,
            role.signal().code(),
            id.0
        ),
        size_bytes: 1024,
        row_count: 10,
        range: TimeRange {
            start,
            end: start + Duration::minutes(minutes),
        },
        services: Vec::new(),
        summary: None,
        content_sha256: Some([3; 32]),
        usage_accounted: level != Level::L0,
    }
}

fn pair(tenant: TenantId, level: Level, start: DateTime<Utc>, minutes: i64) -> [NewFile; 2] {
    [
        file(tenant, MetricRole::Samples, level, start, minutes),
        file(tenant, MetricRole::Series, level, start, minutes),
    ]
}

async fn tenant(db: &TestDb) -> TenantId {
    db.catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id
}

/// Registers and confirms `files` as a new set.
async fn set(db: &TestDb, files: &[NewFile; 2]) -> FileSetId {
    let set = FileSetId::new();
    db.catalog.register_file_set(set, files).await.unwrap();
    db.catalog.confirm_file_set(set).await.unwrap();
    set
}

async fn fence(db: &TestDb) -> LeaseFence {
    let generation = db
        .catalog
        .acquire_lease(COMPACTOR_LEASE, "me", StdDuration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    LeaseFence {
        name: COMPACTOR_LEASE.into(),
        holder: "me".into(),
        generation,
    }
}

async fn states(db: &TestDb, set: FileSetId) -> Vec<(i16, bool, bool)> {
    sqlx::query_as(
        "select state, usage_accounted, confirmed_at is not null from engine_file
         where file_set = $1 order by signal",
    )
    .bind(set)
    .fetch_all(db.catalog.pool())
    .await
    .unwrap()
}

const NOW: StdDuration = StdDuration::ZERO;

/// The oldest cold hour's L0 sets come whole, oldest first; an hour with
/// a file not confirmed stays out; the accounting pass takes the sets not
/// accounted only.
#[tokio::test]
async fn a_cold_hour_gives_its_sets_whole_and_accounting_its_open_ones() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = tenant(&db).await;
    let first = set(&db, &pair(t, Level::L0, hour() + Duration::minutes(1), 5)).await;
    let second = set(&db, &pair(t, Level::L0, hour() + Duration::minutes(10), 5)).await;
    let unconfirmed = pair(t, Level::L0, hour() + Duration::hours(1), 5);
    db.catalog
        .register_file_set(FileSetId::new(), &unconfirmed)
        .await
        .unwrap();

    let inputs = db
        .catalog
        .metric_inputs(MetricPass::Compact, NOW, 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((inputs.tenant_id, inputs.start), (t, hour()));
    let sets: Vec<FileSetId> = inputs.sets.iter().map(|s| s.set).collect();
    assert_eq!(sets, [first, second]);
    assert!(
        inputs
            .sets
            .iter()
            .all(|s| s.samples.signal == Signal::MetricSamples
                && s.series.signal == Signal::MetricSeries)
    );
    assert!(
        db.catalog
            .metric_inputs(MetricPass::Compact, NOW, 1)
            .await
            .unwrap()
            .is_none(),
        "the next hour holds a file not confirmed"
    );

    let fence = fence(&db).await;
    db.catalog
        .account_file_sets_fenced(t, &[first], &[], &fence)
        .await
        .unwrap();
    assert!(
        states(&db, first).await.iter().all(|s| s.1),
        "both accounted"
    );
    let open = db
        .catalog
        .metric_inputs(MetricPass::Account, NOW, 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        open.sets.iter().map(|s| s.set).collect::<Vec<_>>(),
        [second]
    );
    assert!(
        matches!(
            db.catalog
                .account_file_sets_fenced(t, &[first], &[], &fence)
                .await,
            Err(CatalogError::CompactionConflict { .. })
        ),
        "accounted once"
    );
    db.finish().await;
}

/// A set in part, or whose files disagree on their accounting, fails
/// every pass closed.
#[tokio::test]
async fn a_set_in_part_or_disagreeing_fails_the_inputs_closed() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = tenant(&db).await;
    let broken = set(&db, &pair(t, Level::L0, hour(), 5)).await;
    sqlx::query("update engine_file set usage_accounted = true where file_set = $1 and signal = 3")
        .bind(broken)
        .execute(db.catalog.pool())
        .await
        .unwrap();
    for pass in [MetricPass::Compact, MetricPass::Account] {
        assert!(
            matches!(
                db.catalog.metric_inputs(pass, NOW, 0).await,
                Err(CatalogError::InvalidFileSet { .. })
            ),
            "{pass:?}"
        );
    }
    sqlx::query("update engine_file set state = 2 where file_set = $1 and signal = 4")
        .bind(broken)
        .execute(db.catalog.pool())
        .await
        .unwrap();
    assert!(matches!(
        db.catalog.metric_inputs(MetricPass::Compact, NOW, 0).await,
        Err(CatalogError::PartialFileSet { set }) if set == broken.0
    ));
    db.finish().await;
}

/// A swap quarantines both files of every input set and publishes each
/// output as a confirmed set, or, an input gone, changes nothing.
#[tokio::test]
async fn a_swap_replaces_sets_by_sets_or_nothing() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = tenant(&db).await;
    let inputs = [
        set(&db, &pair(t, Level::L0, hour(), 5)).await,
        set(&db, &pair(t, Level::L0, hour() + Duration::minutes(10), 5)).await,
    ];
    let fence = fence(&db).await;
    let out = (FileSetId::new(), pair(t, Level::L1, hour(), 50));
    let mut derived = file(t, MetricRole::Samples, Level::L1, hour(), 1);
    derived.signal = Signal::MetricMetadata;
    derived.path = format!(
        "t/{}/metrics/derived/metric_metadata/{}.parquet",
        t.0, derived.id.0
    );

    let gone = inputs[1];
    sqlx::query("update engine_file set state = 2 where file_set = $1")
        .bind(gone)
        .execute(db.catalog.pool())
        .await
        .unwrap();
    assert!(matches!(
        db.catalog
            .swap_file_sets_fenced(
                t,
                &inputs,
                std::slice::from_ref(&out),
                &[derived.clone()],
                &fence
            )
            .await,
        Err(CatalogError::CompactionConflict {
            expected: 4,
            found: 2
        })
    ));
    assert!(
        states(&db, inputs[0])
            .await
            .iter()
            .all(|s| s.0 == FileState::Visible.code())
    );
    sqlx::query("update engine_file set state = 1 where file_set = $1")
        .bind(gone)
        .execute(db.catalog.pool())
        .await
        .unwrap();

    let swap = db
        .catalog
        .swap_file_sets_fenced(t, &inputs, std::slice::from_ref(&out), &[derived], &fence)
        .await
        .unwrap();
    assert_eq!(swap.inputs.len(), 4);
    assert_eq!(swap.outputs.len(), 3);
    for set in inputs {
        assert!(
            states(&db, set)
                .await
                .iter()
                .all(|s| s.0 == FileState::Quarantined.code())
        );
    }
    assert_eq!(
        states(&db, out.0).await,
        [(FileState::Visible.code(), true, true); 2],
        "visible, accounted and confirmed"
    );
    assert_eq!(
        db.catalog
            .all_visible_files(t, Signal::MetricMetadata)
            .await
            .unwrap()
            .len(),
        1
    );
    db.finish().await;
}

/// A closed day without L0 gives its L1 sets, beside L2 ones; a day with
/// an L0 set does not. An output of L2 spans its day, one of L1 its hour.
#[tokio::test]
async fn a_closed_day_without_l0_gives_its_l1_sets_and_l2_spans_the_day() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = tenant(&db).await;
    let l1 = set(&db, &pair(t, Level::L1, hour(), 50)).await;
    let l2 = set(
        &db,
        &pair(t, Level::L2, hour() - Duration::hours(14), 23 * 60),
    )
    .await;
    let merge = db
        .catalog
        .metric_inputs(MetricPass::Merge, NOW, 0)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(merge.start, hour() - Duration::hours(14));
    assert_eq!(
        merge.sets.iter().map(|s| s.set).collect::<Vec<_>>(),
        [l1],
        "L1 only, L2 {l2:?} left"
    );

    let late = set(&db, &pair(t, Level::L0, hour() + Duration::hours(3), 5)).await;
    assert!(
        db.catalog
            .metric_inputs(MetricPass::Merge, NOW, 0)
            .await
            .unwrap()
            .is_none(),
        "{late:?}"
    );

    assert!(matches!(
        db.catalog
            .register_file_set(FileSetId::new(), &pair(t, Level::L1, hour(), 61))
            .await,
        Err(CatalogError::InvalidFileSet { .. })
    ));
    db.finish().await;
}

/// Retention waits for a metric set's accounting, and then takes both.
#[tokio::test]
async fn retention_keeps_a_metric_set_until_it_is_accounted() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = tenant(&db).await;
    sqlx::query("update engine_tenant set retention_days = 1 where id = $1")
        .bind(t)
        .execute(db.catalog.pool())
        .await
        .unwrap();
    let old = set(&db, &pair(t, Level::L0, hour(), 5)).await;
    let fence = fence(&db).await;
    assert!(
        db.catalog
            .expire_files_fenced(Signal::MetricSamples, 10, &fence)
            .await
            .unwrap()
            .is_none()
    );
    db.catalog
        .account_file_sets_fenced(t, &[old], &[], &fence)
        .await
        .unwrap();
    let gone = db
        .catalog
        .expire_files_fenced(Signal::MetricSamples, 10, &fence)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(gone.files.len(), 2);
    db.finish().await;
}
