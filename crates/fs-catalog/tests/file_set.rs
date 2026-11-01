//! The two files of a metric flush as one set, against a real Postgres.
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
    MetricRole, NewFile, NewTenant, Signal, TenantId, TimeRange,
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
        let schema = format!("fs_{}", uuid::Uuid::new_v4().simple());
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

/// 2026-10-30T10:00:00Z.
fn ten() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 30, 10, 0, 0).unwrap()
}

fn metric_file(tenant: TenantId, role: MetricRole, hour: DateTime<Utc>) -> NewFile {
    let id = FileId::new();
    NewFile {
        id,
        origin: id,
        tenant_id: tenant,
        signal: role.signal(),
        level: Level::L0,
        path: format!(
            "t/{}/metrics/{}/{}.parquet",
            tenant.0,
            role.signal().code(),
            id.0
        ),
        size_bytes: 1024,
        row_count: 10,
        range: TimeRange {
            start: hour + Duration::minutes(5),
            end: hour + Duration::minutes(50),
        },
        services: Vec::new(),
        summary: None,
        content_sha256: Some([3; 32]),
        usage_accounted: true,
    }
}

fn pair(tenant: TenantId, hour: DateTime<Utc>) -> [NewFile; 2] {
    [
        metric_file(tenant, MetricRole::Samples, hour),
        metric_file(tenant, MetricRole::Series, hour),
    ]
}

async fn tenant(db: &TestDb, name: &str) -> TenantId {
    db.catalog
        .create_tenant(&NewTenant::with_defaults(name))
        .await
        .unwrap()
        .id
}

async fn visible(db: &TestDb, tenant: TenantId, signal: Signal) -> Vec<FileId> {
    db.catalog
        .all_visible_files(tenant, signal)
        .await
        .unwrap()
        .into_iter()
        .map(|f| f.id)
        .collect()
}

#[tokio::test]
async fn a_set_registers_and_confirms_both_files_and_again_changes_nothing() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = tenant(&db, "a").await;
    let set = FileSetId::new();
    let files = pair(t, ten());
    let [samples, series] = db.catalog.register_file_set(set, &files).await.unwrap();
    assert_eq!((samples.id, series.id), (files[0].id, files[1].id));
    let again = db.catalog.register_file_set(set, &files).await.unwrap();
    assert_eq!(again[0].created_at, samples.created_at);
    assert_eq!(
        visible(&db, t, Signal::MetricSamples).await,
        vec![files[0].id]
    );
    assert_eq!(
        visible(&db, t, Signal::MetricSeries).await,
        vec![files[1].id]
    );
    let unconfirmed: i64 = sqlx::query_scalar(
        "select count(*) from engine_file where file_set = $1 and confirmed_at is null",
    )
    .bind(set)
    .fetch_one(db.catalog.pool())
    .await
    .unwrap();
    assert_eq!(unconfirmed, 2);
    db.catalog.confirm_file_set(set).await.unwrap();
    let confirmed: i64 = sqlx::query_scalar(
        "select count(*) from engine_file where file_set = $1 and confirmed_at is not null",
    )
    .bind(set)
    .fetch_one(db.catalog.pool())
    .await
    .unwrap();
    assert_eq!(confirmed, 2);
    db.finish().await;
}

#[tokio::test]
async fn a_set_registered_in_part_or_elsewhere_is_refused_not_healed() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = tenant(&db, "a").await;
    let set = FileSetId::new();
    let files = pair(t, ten());
    // One row of the set alone: what no crash of this version leaves.
    let mut one = db.catalog.pool().begin().await.unwrap();
    sqlx::query(
        "insert into engine_file (id, origin, tenant_id, signal, level, path, size_bytes, row_count,
                                  ts_min, ts_max, services, state, file_set, usage_accounted)
         values ($1, $1, $2, 3, 0, $3, 1, 1, $4, $4, '{}', 1, $5, true)",
    )
    .bind(files[0].id)
    .bind(t)
    .bind(&files[0].path)
    .bind(files[0].range.start)
    .bind(set)
    .execute(&mut *one)
    .await
    .unwrap();
    one.commit().await.unwrap();
    assert!(matches!(
        db.catalog.register_file_set(set, &files).await,
        Err(CatalogError::PartialFileSet { .. })
    ));
    assert_eq!(
        visible(&db, t, Signal::MetricSamples).await,
        vec![],
        "served only in pairs"
    );

    let other = pair(t, ten() + Duration::hours(1));
    db.catalog
        .register_file_set(FileSetId::new(), &other)
        .await
        .unwrap();
    assert!(matches!(
        db.catalog.register_file_set(FileSetId::new(), &other).await,
        Err(CatalogError::FileMismatch {
            field: "file_set",
            ..
        })
    ));
    let mut changed = other.clone();
    changed[1].row_count += 1;
    let set_of_other: FileSetId =
        sqlx::query_scalar("select file_set from engine_file where id = $1")
            .bind(other[0].id)
            .fetch_one(db.catalog.pool())
            .await
            .unwrap();
    assert!(matches!(
        db.catalog.register_file_set(set_of_other, &changed).await,
        Err(CatalogError::FileMismatch {
            field: "row_count",
            ..
        })
    ));
    db.finish().await;
}

#[tokio::test]
async fn only_a_samples_and_a_series_file_of_one_tenant_and_hour_make_a_set() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (a, b) = (tenant(&db, "a").await, tenant(&db, "b").await);
    let swapped = {
        let [samples, series] = pair(a, ten());
        [series, samples]
    };
    let mut tenants = pair(a, ten());
    tenants[1].tenant_id = b;
    let mut hours = pair(a, ten());
    hours[1].range.start = ten() + Duration::hours(1);
    hours[1].range.end = ten() + Duration::hours(1);
    let mut spanning = pair(a, ten());
    spanning[0].range.end = ten() + Duration::minutes(61);
    let mut same = pair(a, ten());
    same[1].id = same[0].id;
    for files in [swapped, tenants, hours, spanning, same] {
        assert!(matches!(
            db.catalog.register_file_set(FileSetId::new(), &files).await,
            Err(CatalogError::InvalidFileSet { .. })
        ));
    }
    let rows: i64 = sqlx::query_scalar("select count(*) from engine_file")
        .fetch_one(db.catalog.pool())
        .await
        .unwrap();
    assert_eq!(rows, 0);
    db.finish().await;
}

#[tokio::test]
async fn the_catalog_refuses_metric_files_without_a_set_others_with_one_and_two_of_a_role() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = tenant(&db, "a").await;
    let insert = |signal: i16, set: Option<FileSetId>| {
        let pool = db.catalog.pool().clone();
        async move {
            let id = FileId::new();
            sqlx::query(
                "insert into engine_file (id, origin, tenant_id, signal, level, path, size_bytes,
                                          row_count, ts_min, ts_max, services, state, file_set)
                 values ($1, $1, $2, $3, 0, $4, 1, 1, now(), now(), '{}', 1, $5)",
            )
            .bind(id)
            .bind(t)
            .bind(signal)
            .bind(format!("p/{}", id.0))
            .bind(set)
            .execute(&pool)
            .await
        }
    };
    let set = FileSetId::new();
    assert!(insert(3, None).await.is_err(), "samples without a set");
    assert!(insert(4, None).await.is_err(), "series without a set");
    assert!(insert(1, Some(set)).await.is_err(), "spans in a set");
    assert!(insert(3, Some(set)).await.is_ok());
    assert!(
        insert(3, Some(set)).await.is_err(),
        "two samples files in one set"
    );
    assert!(insert(4, Some(set)).await.is_ok());
    db.finish().await;
}

#[tokio::test]
async fn a_metric_file_is_served_only_beside_its_partner_and_expires_with_it() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let t = tenant(&db, "a").await;
    sqlx::query("update engine_tenant set retention_days = 1 where id = $1")
        .bind(t)
        .execute(db.catalog.pool())
        .await
        .unwrap();
    let old = Utc::now() - Duration::days(3);
    let old = old
        - Duration::minutes(i64::from(chrono::Timelike::minute(&old)))
        - Duration::seconds(i64::from(chrono::Timelike::second(&old)))
        - Duration::nanoseconds(i64::from(chrono::Timelike::nanosecond(&old)));
    let expired = pair(t, old);
    let fresh = pair(t, ten() + Duration::days(3650));
    let expired_set = FileSetId::new();
    db.catalog
        .register_file_set(expired_set, &expired)
        .await
        .unwrap();
    db.catalog
        .register_file_set(FileSetId::new(), &fresh)
        .await
        .unwrap();

    // Quarantining one file hides the other.
    sqlx::query("update engine_file set state = 2 where id = $1")
        .bind(fresh[1].id)
        .execute(db.catalog.pool())
        .await
        .unwrap();
    assert_eq!(
        visible(&db, t, Signal::MetricSamples).await,
        vec![expired[0].id]
    );
    sqlx::query("update engine_file set state = 1 where id = $1")
        .bind(fresh[1].id)
        .execute(db.catalog.pool())
        .await
        .unwrap();

    let generation = db
        .catalog
        .acquire_lease(COMPACTOR_LEASE, "me", StdDuration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    let fence = LeaseFence {
        name: COMPACTOR_LEASE.into(),
        holder: "me".into(),
        generation,
    };
    let gone = db
        .catalog
        .expire_files_fenced(Signal::MetricSamples, 10, &fence)
        .await
        .unwrap()
        .unwrap();
    let mut ids: Vec<FileId> = gone.files.iter().map(|f| f.id).collect();
    ids.sort();
    let mut want = vec![expired[0].id, expired[1].id];
    want.sort();
    assert_eq!(ids, want, "the series file goes with its samples");
    assert!(gone.files.iter().all(|f| f.state == FileState::Quarantined));
    assert_eq!(
        visible(&db, t, Signal::MetricSeries).await,
        vec![fresh[1].id]
    );
    db.finish().await;
}
