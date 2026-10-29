//! Retention by tenant against a real Postgres and a local bucket.
#![allow(
    clippy::too_many_lines,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use fs_catalog::Compaction;
use fs_catalog::{
    COMPACTOR_LEASE, Catalog, CatalogError, FileRecord, LeaseFence, Level, NewTenant, Signal,
    TenantId,
};
use fs_compactor::{
    CompactError, CompactionConfig, Compactor, CompactorStats, Rebuild, Retention, RetentionConfig,
    RetentionStats,
};
use fs_ingest::flush;
use fs_query::{Querier, QueryLimits};
use fs_schema::attribute_keys::KeyStats;
use fs_schema::limits::Limits;
use fs_schema::spans::SpanBatchBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
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
        let schema = format!("r_{}", uuid::Uuid::new_v4().simple());
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
    /// Keeps one day.
    tenant: TenantId,
    store: Arc<LocalFileSystem>,
    dir: tempfile::TempDir,
    /// The start of yesterday on the database clock: the tenant's cut.
    cut: chrono::DateTime<Utc>,
}

async fn fixture() -> Option<Fixture> {
    let db = TestDb::new().await?;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let tenant = db
        .catalog
        .create_tenant(&NewTenant {
            retention_days: 1,
            ..NewTenant::with_defaults("acme")
        })
        .await
        .unwrap()
        .id;
    let (cut,): (chrono::DateTime<Utc>,) = sqlx::query_as(
        "select date_bin('1 day', now(), timestamptz '1970-01-01 00:00:00+00') - interval '1 day'",
    )
    .fetch_one(db.catalog.pool())
    .await
    .unwrap();
    Some(Fixture {
        db,
        tenant,
        store,
        dir,
        cut,
    })
}

impl Fixture {
    /// Nanoseconds of `hours` after the cut; negative hours are beyond
    /// retention.
    fn at(&self, hours: i64) -> u64 {
        let at = self.cut + chrono::Duration::hours(hours);
        u64::try_from(at.timestamp_nanos_opt().unwrap()).unwrap()
    }

    fn hour(&self, hours: i64) -> i64 {
        (self.cut + chrono::Duration::hours(hours))
            .timestamp_nanos_opt()
            .unwrap()
    }
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
        ..KeyValue::default()
    }
}

fn request(
    service: &str,
    trace: u8,
    start: u64,
    spans: u8,
    attribute: (&str, &str),
) -> ExportTraceServiceRequest {
    let spans = (0..spans)
        .map(|i| Span {
            trace_id: vec![trace; 16],
            span_id: vec![i + 1; 8],
            name: format!("op-{i}"),
            kind: 2,
            start_time_unix_nano: start + u64::from(i) * 1_000_000,
            end_time_unix_nano: start + u64::from(i) * 1_000_000 + 250_000,
            attributes: vec![kv(attribute.0, attribute.1)],
            ..Span::default()
        })
        .collect();
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv("service.name", service)],
                ..Resource::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Writes and registers one L0 file of `spans` spans starting at `start`,
/// registered and confirmed an hour ago so its hour is cold.
async fn write_l0(
    f: &Fixture,
    service: &str,
    trace: u8,
    start: u64,
    spans: u8,
    attribute: (&str, &str),
) -> FileRecord {
    write_l0_for(f, f.tenant, service, trace, start, spans, attribute).await
}

async fn write_l0_for(
    f: &Fixture,
    tenant: TenantId,
    service: &str,
    trace: u8,
    start: u64,
    spans: u8,
    attribute: (&str, &str),
) -> FileRecord {
    let mut builder = SpanBatchBuilder::new(Limits::default());
    builder
        .append_request(&request(service, trace, start, spans, attribute))
        .unwrap();
    let batch = builder.finish().unwrap();
    let prepared = flush::prepare(
        tenant,
        &Path::from(""),
        fs_catalog::FileId::new(),
        &[batch],
        4,
    )
    .unwrap();
    flush::upload(f.store.as_ref(), &prepared).await.unwrap();
    flush::upload_index(f.store.as_ref(), &prepared)
        .await
        .unwrap();
    let file = f.db.catalog.register_file(&prepared.file).await.unwrap();
    sqlx::query(
        "update engine_file set created_at = now() - interval '1 hour',
                                confirmed_at = now() - interval '1 hour'
         where id = $1",
    )
    .bind(file.id)
    .execute(f.db.catalog.pool())
    .await
    .unwrap();
    file
}

async fn fence(f: &Fixture) -> LeaseFence {
    let generation =
        f.db.catalog
            .acquire_lease(COMPACTOR_LEASE, "me", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
    LeaseFence {
        name: COMPACTOR_LEASE.into(),
        holder: "me".into(),
        generation,
    }
}

fn retention_config() -> RetentionConfig {
    RetentionConfig {
        files_per_pass: 2,
        rebuild_max_input_bytes: 1 << 30,
        rebuild_max_bytes: 1 << 30,
        batch_rows: 4,
        limits: Limits::default(),
        rebuilds_per_tick: 64,
        rebuild_backoff: Duration::from_secs(2),
        inject_swap_failures: 0,
    }
}

/// The swap of a rebuild that went through.
fn done(outcome: Rebuild) -> Compaction {
    match outcome {
        Rebuild::Done(swap) => swap,
        other => panic!("expected a rebuild, got {other:?}"),
    }
}

fn retention(f: &Fixture, config: RetentionConfig) -> Retention {
    Retention::new(
        f.db.catalog.clone(),
        f.store.clone(),
        Path::from(""),
        config,
        Arc::new(RetentionStats::default()),
    )
}

fn compactor(f: &Fixture) -> Compactor {
    Compactor::new(
        f.db.catalog.clone(),
        f.store.clone(),
        Path::from(""),
        CompactionConfig {
            hour_close_delay: Duration::from_secs(60),
            l1_target_bytes: 512 * 1024 * 1024,
            max_inputs_per_pass: 16,
            max_input_bytes: 2 << 30,
            row_group_rows: 4,
            inject_swap_failures: 0,
            inject_put_failure_at: None,
            inject_put_hold: None,
        },
        Arc::new(CompactorStats::default()),
    )
}

fn querier(f: &Fixture) -> Querier {
    Querier::new(
        f.db.catalog.clone(),
        f.store.clone(),
        Some(f.dir.path().join("spill")),
        QueryLimits {
            timeout: Duration::from_secs(30),
            ..QueryLimits::default()
        },
    )
    .unwrap()
}

fn count(batches: &[RecordBatch]) -> i64 {
    batches
        .iter()
        .map(|b| b.column(0).as_primitive::<Int64Type>().value(0))
        .sum()
}

async fn rows(f: &Fixture, sql: &str) -> i64 {
    count(&querier(f).collect(f.tenant, sql).await.unwrap())
}

async fn visible(f: &Fixture) -> Vec<FileRecord> {
    f.db.catalog
        .all_visible_files(f.tenant, Signal::Traces)
        .await
        .unwrap()
}

async fn tables(f: &Fixture) -> Vec<FileRecord> {
    tables_of(f, f.tenant).await
}

async fn tables_of(f: &Fixture, tenant: TenantId) -> Vec<FileRecord> {
    f.db.catalog
        .all_visible_files(tenant, Signal::AttributeKeys)
        .await
        .unwrap()
}

fn keys_of(f: &Fixture, file: &FileRecord) -> KeyStats {
    let bytes = std::fs::read(f.dir.path().join(&file.path)).unwrap();
    fs_schema::attribute_keys::decode(bytes.into()).unwrap()
}

async fn present(f: &Fixture, path: &str) -> bool {
    f.store.head(&Path::from(path)).await.is_ok()
}

async fn objects_under(f: &Fixture, part: &str) -> Vec<String> {
    use futures_util::TryStreamExt;
    let mut out: Vec<String> = f
        .store
        .list(None)
        .map_ok(|m| m.location.to_string())
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .filter(|p| p.contains(part))
        .collect();
    out.sort();
    out
}

/// Files whose rows all ended before the tenant's cut, a UTC day boundary
/// on the database clock, leave the catalog under the fence, so many per
/// pass, and the passes resume after a restart; their objects stay for
/// the purge, which deletes them with their sidecars once the quarantine
/// TTL has passed. Queries are right in between.
#[tokio::test]
async fn expired_days_leave_the_catalog_first_and_the_bucket_at_the_purge() {
    let Some(f) = fixture().await else { return };
    let a1 = write_l0(&f, "web", 1, f.at(-3), 2, ("k", "v")).await;
    let a2 = write_l0(&f, "web", 2, f.at(-2), 2, ("k", "v")).await;
    let edge = write_l0(&f, "web", 3, f.at(-1) + 59 * 60_000_000_000, 1, ("k", "v")).await;
    let kept = write_l0(&f, "web", 4, f.at(0), 2, ("k", "v")).await;
    let recent = write_l0(&f, "web", 5, f.at(1), 3, ("k", "v")).await;
    assert_eq!(rows(&f, "select count(*) from spans").await, 10);

    let r = retention(&f, retention_config());
    let fence = fence(&f).await;
    let old_fence = LeaseFence {
        generation: fence.generation - 1,
        ..fence.clone()
    };
    let err = r.expire_once(&old_fence).await.unwrap_err();
    assert!(
        matches!(err, CompactError::Catalog(CatalogError::LeaseLost { .. })),
        "{err}"
    );
    assert_eq!(visible(&f).await.len(), 5, "a stale fence expires nothing");
    assert_eq!(r.stats().lease_lost.load(Ordering::Relaxed), 1);

    let first = r.expire_once(&fence).await.unwrap().unwrap();
    assert_eq!(first.tenant_id, f.tenant);
    assert_eq!(first.cutoff, f.cut);
    assert_eq!(
        first.files.iter().map(|x| x.id).collect::<Vec<_>>(),
        vec![a1.id, a2.id],
        "oldest first, two per pass"
    );
    assert_eq!(rows(&f, "select count(*) from spans").await, 6);
    assert!(
        present(&f, &a1.path).await,
        "the object stays for the purge"
    );
    assert_eq!(r.stats().passes.load(Ordering::Relaxed), 1);
    assert_eq!(r.stats().expired.load(Ordering::Relaxed), 2);

    // A restart resumes with what is left.
    let r2 = retention(&f, retention_config());
    let second = r2.expire_once(&fence).await.unwrap().unwrap();
    assert_eq!(
        second.files.iter().map(|x| x.id).collect::<Vec<_>>(),
        vec![edge.id],
        "a file ending a minute before the cut is beyond it"
    );
    assert!(r2.expire_once(&fence).await.unwrap().is_none());
    assert_eq!(
        visible(&f).await.iter().map(|x| x.id).collect::<Vec<_>>(),
        vec![kept.id, recent.id],
        "the file starting at the cut stays"
    );
    assert_eq!(rows(&f, "select count(*) from spans").await, 5);

    let purged = fs_compactor::purge_once(
        &f.db.catalog,
        f.store.as_ref(),
        Duration::ZERO,
        100,
        Duration::from_secs(600),
    )
    .await;
    assert_eq!(purged, 3);
    for file in [&a1, &a2, &edge] {
        assert!(!present(&f, &file.path).await, "{}", file.path);
        assert!(
            !present(&f, &fs_index::sidecar_path(&file.path).unwrap()).await,
            "sidecar of {}",
            file.path
        );
    }
    assert!(present(&f, &kept.path).await);
    assert!(present(&f, &recent.path).await);
    assert_eq!(rows(&f, "select count(*) from spans").await, 5);
    f.db.finish().await;
}

/// Once a tenant's expired files are gone, its `attribute_keys` table
/// still lists what they carried; it is rebuilt from the L1 files that
/// remain, with the admissions of the table before, and swapped in under
/// the fence. Every way the rebuild can fail leaves the table before in
/// place: a swap that does not commit, a lost lease, a tenant over the
/// read cap, an unreadable L1.
#[tokio::test]
async fn the_attribute_keys_table_is_rebuilt_from_the_l1_files_that_remain() {
    use fs_schema::attribute_keys::KeySource;
    let Some(f) = fixture().await else { return };
    // The hour beyond retention carries db.system; the hour kept carries
    // http.route.
    write_l0(&f, "api", 1, f.at(-2), 3, ("db.system", "pg")).await;
    write_l0(
        &f,
        "api",
        2,
        f.at(-2) + 60_000_000_000,
        3,
        ("db.system", "pg"),
    )
    .await;
    write_l0(&f, "web", 3, f.at(1), 3, ("http.route", "/a")).await;
    write_l0(
        &f,
        "web",
        4,
        f.at(1) + 60_000_000_000,
        3,
        ("http.route", "/b"),
    )
    .await;
    let c = compactor(&f);
    let fence = fence(&f).await;
    c.compact_once(&fence).await.unwrap().unwrap();
    c.compact_once(&fence).await.unwrap().unwrap();
    let before = tables(&f).await;
    assert_eq!(before.len(), 1);
    let before_keys = keys_of(&f, &before[0]);
    assert_eq!(before_keys.len(), 3, "service.name, db.system, http.route");
    let l1: Vec<_> = visible(&f).await;
    assert!(l1.iter().all(|x| x.level == Level::L1));
    assert_eq!(l1.len(), 2);

    let r = retention(&f, retention_config());
    assert!(
        matches!(r.rebuild_once(&fence).await.unwrap(), Rebuild::End),
        "not stale while the tenant has expired files"
    );
    let expired = r.expire_once(&fence).await.unwrap().unwrap();
    assert_eq!(
        expired.files.len(),
        1,
        "the L1 of the hour beyond retention"
    );
    assert!(r.expire_once(&fence).await.unwrap().is_none());
    assert_eq!(
        tables(&f).await[0].id,
        before[0].id,
        "the table is not a traces file"
    );

    // A swap that does not commit leaves the table before and an orphan.
    let failing = retention(
        &f,
        RetentionConfig {
            inject_swap_failures: 1,
            ..retention_config()
        },
    );
    let derived_before = objects_under(&f, "/derived/attribute_keys/").await.len();
    let err = failing.rebuild_once(&fence).await.unwrap_err();
    assert!(matches!(err, CompactError::Injected("swap")), "{err}");
    assert_eq!(tables(&f).await[0].id, before[0].id);
    assert_eq!(
        objects_under(&f, "/derived/attribute_keys/").await.len(),
        derived_before + 1
    );
    assert_eq!(failing.stats().rebuild_orphaned.load(Ordering::Relaxed), 1);
    assert_eq!(failing.stats().rebuild_failures.load(Ordering::Relaxed), 1);

    // A lost lease publishes nothing.
    let old_fence = LeaseFence {
        generation: fence.generation - 1,
        ..fence.clone()
    };
    let err = r.rebuild_once(&old_fence).await.unwrap_err();
    assert!(
        matches!(err, CompactError::Catalog(CatalogError::LeaseLost { .. })),
        "{err}"
    );
    assert_eq!(tables(&f).await[0].id, before[0].id);
    assert_eq!(r.stats().rebuild_lease_lost.load(Ordering::Relaxed), 1);

    // A tenant whose remaining L1 files exceed the input cap, or whose
    // rebuild would hold more than the memory budget, is passed over.
    let remaining = visible(&f).await;
    assert_eq!(remaining.len(), 1);
    let largest = u64::try_from(remaining[0].size_bytes).unwrap();
    let footprint = fs_compactor::retention::rebuild_footprint(
        u64::try_from(before[0].size_bytes).unwrap(),
        u64::try_from(before[0].row_count).unwrap(),
        largest,
        (remaining[0].path.len() + std::mem::size_of::<fs_catalog::RebuildInput>()) as u64,
        &retention_config(),
    );
    for config in [
        RetentionConfig {
            rebuild_max_input_bytes: largest - 1,
            ..retention_config()
        },
        RetentionConfig {
            rebuild_max_bytes: footprint - 1,
            ..retention_config()
        },
    ] {
        let capped = retention(&f, config);
        assert!(matches!(
            capped.rebuild_once(&fence).await.unwrap(),
            Rebuild::Skipped
        ));
        assert!(matches!(
            capped.rebuild_once(&fence).await.unwrap(),
            Rebuild::End
        ));
        assert!(matches!(
            capped.rebuild_once(&fence).await.unwrap(),
            Rebuild::Skipped
        ));
        assert_eq!(
            capped.stats().rebuild_skipped.load(Ordering::Relaxed),
            1,
            "reported once"
        );
        assert_eq!(tables(&f).await[0].id, before[0].id);
    }

    // A second tenant whose only hour is beyond retention, compacted and
    // expired: its table is stale too, with nothing left to read.
    let other =
        f.db.catalog
            .create_tenant(&NewTenant {
                retention_days: 1,
                ..NewTenant::with_defaults("other")
            })
            .await
            .unwrap()
            .id;
    let half = 30 * 60_000_000_000;
    write_l0_for(&f, other, "api", 5, f.at(-2) + half, 3, ("db.system", "pg")).await;
    write_l0_for(
        &f,
        other,
        "api",
        6,
        f.at(-2) + half + 60_000_000_000,
        3,
        ("db.system", "pg"),
    )
    .await;
    c.compact_once(&fence).await.unwrap().unwrap();
    let others_table = tables_of(&f, other).await;
    assert_eq!(others_table.len(), 1);
    assert_eq!(
        r.expire_once(&fence).await.unwrap().unwrap().tenant_id,
        other
    );
    assert!(r.expire_once(&fence).await.unwrap().is_none());
    std::fs::write(f.dir.path().join(&others_table[0].path), b"not parquet").unwrap();

    // An L1 that cannot be read ends the rebuild before anything is
    // created and holds the tenant back for the tick, so the next tenant
    // is tried: the other one, whose table goes without being decoded.
    let good = std::fs::read(f.dir.path().join(&remaining[0].path)).unwrap();
    std::fs::write(f.dir.path().join(&remaining[0].path), b"not parquet").unwrap();
    let derived_before = objects_under(&f, "/derived/attribute_keys/").await.len();
    let err = r.rebuild_once(&fence).await.unwrap_err();
    assert!(matches!(err, CompactError::Parquet(_)), "{err}");
    assert_eq!(tables(&f).await[0].id, before[0].id);
    assert_eq!(
        objects_under(&f, "/derived/attribute_keys/").await.len(),
        derived_before,
        "nothing created"
    );
    assert_eq!(r.stats().rebuild_failures.load(Ordering::Relaxed), 1);
    let swap = done(r.rebuild_once(&fence).await.unwrap());
    assert_eq!(swap.inputs, vec![others_table[0].id]);
    assert_eq!(swap.outputs.len(), 0);
    assert_eq!(tables_of(&f, other).await.len(), 0);
    assert!(
        matches!(r.rebuild_once(&fence).await.unwrap(), Rebuild::End),
        "the round is over"
    );
    assert!(
        matches!(r.rebuild_once(&fence).await.unwrap(), Rebuild::Skipped),
        "the next round comes to the failed tenant, held back"
    );
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    assert!(matches!(
        r.rebuild_once(&fence).await.unwrap(),
        Rebuild::End
    ));
    let err = r.rebuild_once(&fence).await.unwrap_err();
    assert!(
        matches!(err, CompactError::Parquet(_)),
        "tried again after its backoff: {err}"
    );
    assert_eq!(r.stats().rebuild_failures.load(Ordering::Relaxed), 2);
    std::fs::write(f.dir.path().join(&remaining[0].path), good).unwrap();

    // Repaired, and within the budget to the byte, the rebuild goes
    // through, from the L1 that remains alone.
    let exact = retention(
        &f,
        RetentionConfig {
            rebuild_max_bytes: footprint,
            ..retention_config()
        },
    );
    let done = done(exact.rebuild_once(&fence).await.unwrap());
    assert_eq!(
        done.inputs,
        vec![before[0].id],
        "the table before is quarantined"
    );
    assert_eq!(done.outputs.len(), 1);
    let after = tables(&f).await;
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].id, done.outputs[0]);
    assert!(
        after[0].path.contains("/derived/attribute_keys/dt="),
        "{}",
        after[0].path
    );
    let keys = keys_of(&f, &after[0]);
    let rows_now: Vec<_> = keys
        .iter()
        .map(|(source, key, _, seen)| (source, key.to_owned(), seen.count, seen.admitted_hour))
        .collect();
    assert_eq!(
        rows_now,
        vec![
            (
                KeySource::Resource,
                "service.name".to_owned(),
                6,
                f.hour(-2)
            ),
            (KeySource::Span, "http.route".to_owned(), 6, f.hour(1)),
        ],
        "counts from the hour kept; service.name keeps the hour that admitted it; db.system is gone"
    );
    assert!(
        keys.range().unwrap().0 >= f.hour(0),
        "no sighting before the cut"
    );
    assert_eq!(after[0].range.start, f.cut + chrono::Duration::hours(1));
    assert_eq!(exact.stats().rebuilds.load(Ordering::Relaxed), 1);
    assert_eq!(exact.stats().rebuild_skipped.load(Ordering::Relaxed), 0);
    assert!(
        matches!(exact.rebuild_once(&fence).await.unwrap(), Rebuild::End),
        "rebuilt once"
    );
    assert_eq!(rows(&f, "select count(*) from attribute_keys").await, 2);
    assert_eq!(rows(&f, "select count(*) from spans").await, 6);
    f.db.finish().await;
}

/// A tenant whose every L1 file is beyond retention ends with no table
/// at all, whatever state the table is in: nothing remains to count.
#[tokio::test]
async fn a_tenant_whose_days_are_all_gone_loses_its_table() {
    let Some(f) = fixture().await else { return };
    write_l0(&f, "api", 1, f.at(-2), 3, ("db.system", "pg")).await;
    write_l0(
        &f,
        "api",
        2,
        f.at(-2) + 60_000_000_000,
        3,
        ("db.system", "pg"),
    )
    .await;
    let fence = fence(&f).await;
    compactor(&f).compact_once(&fence).await.unwrap().unwrap();
    let before = tables(&f).await;
    assert_eq!(before.len(), 1);

    let r = retention(
        &f,
        RetentionConfig {
            rebuild_max_input_bytes: 1,
            rebuild_max_bytes: 1,
            ..retention_config()
        },
    );
    r.expire_once(&fence).await.unwrap().unwrap();
    std::fs::write(f.dir.path().join(&before[0].path), b"not parquet").unwrap();
    sqlx::query("update engine_file set size_bytes = $2, row_count = $2 where id = $1")
        .bind(before[0].id)
        .bind(1i64 << 40)
        .execute(f.db.catalog.pool())
        .await
        .unwrap();
    let done = done(r.rebuild_once(&fence).await.unwrap());
    assert_eq!(done.inputs, vec![before[0].id]);
    assert_eq!(
        r.stats().rebuild_skipped.load(Ordering::Relaxed),
        0,
        "no cap applies to a table with nothing left to read"
    );
    assert_eq!(done.outputs.len(), 0, "nothing to list");
    assert_eq!(tables(&f).await.len(), 0);
    assert_eq!(rows(&f, "select count(*) from attribute_keys").await, 0);
    assert!(matches!(
        r.rebuild_once(&fence).await.unwrap(),
        Rebuild::End
    ));
    assert_eq!(r.stats().rebuilds.load(Ordering::Relaxed), 1);
    f.db.finish().await;
}

/// The leader's loop expires files and rebuilds the tables on its own
/// interval, one rebuild per tick here, and a tenant whose rebuild fails
/// does not keep the others from theirs even when its backoff has ended
/// by the next tick: the walk goes on past it to the next tenant.
#[tokio::test]
async fn the_loop_expires_and_rebuilds_while_it_holds_the_lease() {
    let Some(f) = fixture().await else { return };
    write_l0(&f, "api", 1, f.at(-2), 3, ("db.system", "pg")).await;
    write_l0(
        &f,
        "api",
        2,
        f.at(-2) + 60_000_000_000,
        3,
        ("db.system", "pg"),
    )
    .await;
    write_l0(&f, "web", 3, f.at(1), 3, ("http.route", "/a")).await;
    write_l0(
        &f,
        "web",
        4,
        f.at(1) + 60_000_000_000,
        3,
        ("http.route", "/b"),
    )
    .await;
    let other =
        f.db.catalog
            .create_tenant(&NewTenant {
                retention_days: 1,
                ..NewTenant::with_defaults("other")
            })
            .await
            .unwrap()
            .id;
    let half = 30 * 60_000_000_000;
    write_l0_for(&f, other, "api", 5, f.at(-2) + half, 3, ("db.system", "pg")).await;
    write_l0_for(
        &f,
        other,
        "api",
        6,
        f.at(-2) + half + 60_000_000_000,
        3,
        ("db.system", "pg"),
    )
    .await;
    let c = compactor(&f);
    let fence = fence(&f).await;
    for _ in 0..3 {
        c.compact_once(&fence).await.unwrap().unwrap();
    }
    f.db.catalog
        .release_lease(COMPACTOR_LEASE, "me")
        .await
        .unwrap();
    let table_before = tables(&f).await[0].id;
    // The first tenant's remaining L1 cannot be read until it is repaired.
    let kept = visible(&f)
        .await
        .into_iter()
        .find(|x| x.range.start >= f.cut)
        .unwrap();
    let good = std::fs::read(f.dir.path().join(&kept.path)).unwrap();
    std::fs::write(f.dir.path().join(&kept.path), b"not parquet").unwrap();

    let (shutdown, rx) = tokio::sync::watch::channel(false);
    let stats = Arc::new(fs_compactor::Stats::default());
    let task = tokio::spawn(fs_compactor::run(
        f.db.catalog.clone(),
        f.store.clone(),
        Path::from(""),
        fs_compactor::CompactorConfig {
            purge_interval: Duration::from_secs(3600),
            quarantine_ttl: Duration::from_secs(3600),
            purge_batch: 10,
            lease_ttl: Duration::from_millis(600),
            compaction_interval: Duration::from_secs(3600),
            compaction: CompactionConfig {
                hour_close_delay: Duration::from_secs(3600),
                l1_target_bytes: 512 * 1024 * 1024,
                max_inputs_per_pass: 16,
                max_input_bytes: 2 << 30,
                row_group_rows: 4,
                inject_swap_failures: 0,
                inject_put_failure_at: None,
                inject_put_hold: None,
            },
            orphan_interval: Duration::from_secs(3600),
            sweep: fs_compactor::SweepConfig {
                ttl: Duration::from_secs(3600),
                objects_per_pass: 100_000,
                deletes_per_pass: 100,
                delete_backoff: Duration::from_secs(600),
                inject_delete_failure: None,
            },
            retention_interval: Duration::from_secs(1),
            retention: RetentionConfig {
                rebuilds_per_tick: 1,
                rebuild_backoff: Duration::ZERO,
                ..retention_config()
            },
        },
        "loop",
        Arc::clone(&stats),
        rx,
    ));
    let wait_for = |done: u64| {
        let stats = Arc::clone(&stats);
        async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            while stats.retention.rebuilds.load(Ordering::Relaxed) < done {
                assert!(tokio::time::Instant::now() < deadline, "no rebuild in time");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };
    wait_for(1).await;
    shutdown.send(true).unwrap();
    task.await.unwrap();
    assert_eq!(
        tables_of(&f, other).await.len(),
        0,
        "the other tenant's table went"
    );
    assert_eq!(
        tables(&f).await[0].id,
        table_before,
        "the failed tenant's stays"
    );
    assert!(
        stats.retention.rebuild_failures.load(Ordering::Relaxed) >= 1,
        "the broken tenant failed, the walk went on"
    );
    assert_eq!(stats.retention.expired.load(Ordering::Relaxed), 2);
    assert_eq!(visible(&f).await.len(), 1, "the L1 of the hour kept");
    std::fs::write(f.dir.path().join(&kept.path), good).unwrap();
    f.db.finish().await;
}
