//! Metric files and their derived tables under the compactor: the orphan
//! sweep, compaction by file set and the accounting of sets, against a
//! real Postgres and a local bucket.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::too_many_lines
)]

use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use fs_catalog::{
    Catalog, FileId, FileSetId, Level, MetricRole, NewFile, NewTenant, TenantId, TimeRange,
};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
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
        let schema = format!("km_{}", uuid::Uuid::new_v4().simple());
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

struct Fixture {
    db: TestDb,
    tenant: TenantId,
    store: Arc<LocalFileSystem>,
    _dir: tempfile::TempDir,
}

async fn fixture() -> Option<Fixture> {
    let db = TestDb::new().await?;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    Some(Fixture {
        db,
        tenant,
        store,
        _dir: dir,
    })
}

fn sweeper(f: &Fixture) -> fs_compactor::Sweeper {
    fs_compactor::Sweeper::new(
        f.db.catalog.clone(),
        f.store.clone(),
        Path::from(""),
        fs_compactor::SweepConfig {
            ttl: Duration::from_secs(3600),
            objects_per_pass: 1000,
            deletes_per_pass: 100,
            delete_backoff: Duration::from_secs(600),
            inject_delete_failure: None,
        },
        Arc::new(fs_compactor::OrphanStats::default()),
    )
}

/// Every kind of metric object without a row, L0 to L2 files of both
/// roles, postings and metadata contributions, is swept like any other.
#[tokio::test]
async fn metric_objects_without_a_row_are_swept() {
    let Some(f) = fixture().await else {
        return;
    };
    let t = f.tenant.0;
    let id = uuid::Uuid::now_v7();
    let paths = [
        format!("t/{t}/metrics/samples/l0/dt=2026-10-24/hr=14/{id}.parquet"),
        format!("t/{t}/metrics/series/l0/dt=2026-10-24/hr=14/{id}.parquet"),
        format!("t/{t}/metrics/samples/l1/dt=2026-10-24/hr=14/{id}.parquet"),
        format!("t/{t}/metrics/series/l1/dt=2026-10-24/hr=14/{id}.parquet"),
        format!("t/{t}/metrics/samples/l2/dt=2026-10-24/{id}.parquet"),
        format!("t/{t}/metrics/series/l2/dt=2026-10-24/{id}.parquet"),
        format!("t/{t}/metrics/index/postings/dt=2026-10-24/hr=14/{id}.pst"),
        format!("t/{t}/metrics/index/postings/dt=2026-10-24/{id}.pst"),
        format!("t/{t}/metrics/derived/metric_metadata/dt=2026-10-24/hr=14/{id}.parquet"),
    ];
    for path in &paths {
        f.store
            .put(&Path::from(path.as_str()), PutPayload::from_static(b"x"))
            .await
            .unwrap();
    }
    let mut sweeper = sweeper(&f);
    sweeper.sweep_once().await.unwrap();
    sqlx::query("update engine_orphan set first_seen = now() - interval '2 hours'")
        .execute(f.db.catalog.pool())
        .await
        .unwrap();
    let swept = sweeper.sweep_once().await.unwrap();
    assert_eq!(swept.deleted, paths.len());
    for path in &paths {
        assert!(
            f.store.head(&Path::from(path.as_str())).await.is_err(),
            "{path}"
        );
    }
    f.db.finish().await;
}

/// A registered set of L1 keeps its files and the postings of its series
/// file, which no row lists: their owner is the series file.
#[tokio::test]
async fn the_postings_of_a_registered_series_file_are_not_swept() {
    let Some(f) = fixture().await else {
        return;
    };
    let t = f.tenant.0;
    let hour = Utc.with_ymd_and_hms(2026, 10, 24, 14, 0, 0).unwrap();
    let file = |role: MetricRole| {
        let id = FileId::new();
        let name = if role == MetricRole::Samples {
            "samples"
        } else {
            "series"
        };
        NewFile {
            id,
            origin: id,
            tenant_id: f.tenant,
            signal: role.signal(),
            level: Level::L1,
            path: format!(
                "t/{t}/metrics/{name}/l1/dt=2026-10-24/hr=14/{}.parquet",
                id.0
            ),
            size_bytes: 1,
            row_count: 1,
            range: TimeRange {
                start: hour,
                end: hour + chrono::Duration::minutes(1),
            },
            services: Vec::new(),
            summary: None,
            content_sha256: Some([1; 32]),
            usage_accounted: true,
        }
    };
    let set = [file(MetricRole::Samples), file(MetricRole::Series)];
    f.db.catalog
        .register_file_set(FileSetId::new(), &set)
        .await
        .unwrap();
    let postings = fs_index::postings::sidecar_path(&set[1].path).unwrap();
    for path in [&set[0].path, &set[1].path, &postings] {
        f.store
            .put(&Path::from(path.as_str()), PutPayload::from_static(b"x"))
            .await
            .unwrap();
    }
    let mut sweeper = sweeper(&f);
    sweeper.sweep_once().await.unwrap();
    sqlx::query("update engine_orphan set first_seen = now() - interval '2 hours'")
        .execute(f.db.catalog.pool())
        .await
        .unwrap();
    let swept = sweeper.sweep_once().await.unwrap();
    assert_eq!(swept.deleted, 0);
    assert!(f.store.head(&Path::from(postings.as_str())).await.is_ok());
    f.db.finish().await;
}
