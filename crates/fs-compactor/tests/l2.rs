//! L1 to L2: closed days merged into L2 files with their sidecars, against
//! a real Postgres and a local bucket.
#![allow(
    clippy::too_many_lines,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use arrow::array::AsArray;
use arrow::datatypes::{DataType, Int64Type, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use chrono::{TimeZone, Utc};
use fs_catalog::{
    COMPACTOR_LEASE, Catalog, CatalogError, FileRecord, LeaseFence, Level, NewTenant, Signal,
    TenantId,
};
use fs_compactor::{
    CompactError, CompactionConfig, Compactor, CompactorStats, Rebuild, Retention, RetentionConfig,
    RetentionStats, TickPasses, compaction_tick,
};
use fs_ingest::flush;
use fs_ingest::record::FileLayout;
use fs_query::{Querier, QueryLimits};
use fs_schema::attribute_keys::{KeyTable, decode};
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
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
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
        let schema = format!("k2_{}", uuid::Uuid::new_v4().simple());
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
    dir: tempfile::TempDir,
}

async fn fixture(retention_days: i32) -> Option<Fixture> {
    let db = TestDb::new().await?;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let tenant = db
        .catalog
        .create_tenant(&NewTenant {
            retention_days,
            ..NewTenant::with_defaults("acme")
        })
        .await
        .unwrap()
        .id;
    Some(Fixture {
        db,
        tenant,
        store,
        dir,
    })
}

const MINUTE: u64 = 60_000_000_000;
const HOUR: u64 = 60 * MINUTE;

/// 2026-10-24T00:00Z, a long closed day.
fn closed_day() -> u64 {
    nanos(Utc.with_ymd_and_hms(2026, 10, 24, 0, 0, 0).unwrap())
}

fn nanos(at: chrono::DateTime<Utc>) -> u64 {
    u64::try_from(at.timestamp_nanos_opt().unwrap()).unwrap()
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
    key: &str,
) -> ExportTraceServiceRequest {
    let spans = (0..spans)
        .map(|i| Span {
            trace_id: vec![trace; 16],
            span_id: vec![i + 1; 8],
            name: format!("op-{i}"),
            kind: 2,
            start_time_unix_nano: start + u64::from(i) * 1_000_000,
            end_time_unix_nano: start + u64::from(i) * 1_000_000 + 1_000,
            attributes: vec![kv(key, "v")],
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

/// Writes, registers and backdates one cold span L0 file.
async fn write_l0(f: &Fixture, service: &str, trace: u8, start: u64, key: &str) -> FileRecord {
    let mut builder = SpanBatchBuilder::new(Limits::default());
    builder
        .append_request(&request(service, trace, start, 3, key))
        .unwrap();
    let prepared = flush::prepare(
        f.tenant,
        &Path::from(""),
        fs_catalog::FileId::new(),
        &[builder.finish().unwrap()],
        FileLayout::current(4),
    )
    .unwrap();
    flush::upload(f.store.as_ref(), &prepared).await.unwrap();
    flush::upload_index(f.store.as_ref(), &prepared)
        .await
        .unwrap();
    let file = f.db.catalog.register_file(&prepared.file).await.unwrap();
    backdate(f).await;
    file
}

/// Every registration of the tenant an hour old, as long-closed files are.
async fn backdate(f: &Fixture) {
    sqlx::query(
        "update engine_file set created_at = now() - interval '1 hour',
                                confirmed_at = now() - interval '1 hour'
         where tenant_id = $1",
    )
    .bind(f.tenant)
    .execute(f.db.catalog.pool())
    .await
    .unwrap();
}

fn config() -> CompactionConfig {
    CompactionConfig {
        hour_close_delay: Duration::from_secs(60),
        l1_target_bytes: 512 * 1024 * 1024,
        max_inputs_per_pass: 16,
        max_input_bytes: 2 << 30,
        trace_summary_max_traces: 4_000_000,
        trace_summary_max_bytes: 256 << 20,
        service_graph_max_spans: 4_000_000,
        service_graph_max_bytes: 256 << 20,
        row_group_rows: 4,
        usage_max_bytes: 64 << 20,
        compact_l2: true,
        l2_target_bytes: 1 << 30,
        l2_output_bytes: 512 << 20,
        max_l2_input_bytes: 16 << 30,
        l2_passes_per_tick: 1,
        l1_passes_per_tick: 64,
        max_trace_index_bytes: 256 << 20,
        max_services_per_file: 10_000,
        text_index: fs_index::TextIndexLimits::DEFAULT,
        compact_logs: false,
        inject_swap_failures: 0,
        inject_put_failure_at: None,
        inject_put_hold: None,
    }
}

fn compactor(f: &Fixture, config: CompactionConfig) -> Compactor {
    Compactor::new(
        f.db.catalog.clone(),
        f.store.clone(),
        Path::from(""),
        config,
        Arc::new(CompactorStats::default()),
    )
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

async fn spans(f: &Fixture) -> Vec<FileRecord> {
    f.db.catalog
        .all_visible_files(f.tenant, Signal::Traces)
        .await
        .unwrap()
}

async fn count(f: &Fixture, sql: &str) -> i64 {
    let querier = Querier::new(
        f.db.catalog.clone(),
        f.store.clone(),
        None,
        QueryLimits::default(),
    )
    .unwrap();
    let batches: Vec<RecordBatch> = querier.collect(f.tenant, sql).await.unwrap();
    batches
        .iter()
        .map(|b| b.column(0).as_primitive::<Int64Type>().value(0))
        .sum()
}

/// Three hours of a closed day, compacted to three L1 files.
async fn day_of_l1(f: &Fixture, day: u64) -> Vec<FileRecord> {
    for (hour, service, trace) in [(10, "web", 1u8), (11, "api", 2), (12, "web", 3)] {
        write_l0(f, service, trace, day + hour * HOUR + 5 * MINUTE, "k").await;
    }
    let c = compactor(f, config());
    let me = fence(f).await;
    while c.compact_once(&me).await.unwrap().is_some() {}
    backdate(f).await;
    let l1 = spans(f).await;
    assert!(l1.iter().all(|x| x.level == Level::L1));
    l1
}

/// `(service, timestamp)` of a local Parquet file, in file order.
fn rows_of(path: &std::path::Path) -> Vec<(String, i64)> {
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap()).unwrap();
    let compression = reader.metadata().row_group(0).column(0).compression();
    assert!(
        matches!(compression, parquet::basic::Compression::ZSTD(_)),
        "{compression:?}"
    );
    let mut out = Vec::new();
    for batch in reader.build().unwrap() {
        let batch = batch.unwrap();
        let service = arrow::compute::cast(
            batch.column(batch.schema().index_of("service_name").unwrap()),
            &DataType::Utf8,
        )
        .unwrap();
        let ts = batch
            .column(batch.schema().index_of("timestamp").unwrap())
            .as_primitive::<TimestampNanosecondType>()
            .clone();
        let service = service.as_string::<i32>();
        for i in 0..batch.num_rows() {
            out.push((service.value(i).to_owned(), ts.value(i)));
        }
    }
    out
}

/// The L1 files of a closed day become one L2 file of the day, sorted,
/// with its `trace_id` sidecar; queries read the same rows, a lookup by
/// trace finds it, no usage is counted again, and nothing is left to do.
#[tokio::test]
async fn a_closed_day_of_l1_files_becomes_one_sorted_l2_file() {
    let Some(f) = fixture(15).await else { return };
    let l1 = day_of_l1(&f, closed_day()).await;
    let before = count(&f, "select count(*) from spans").await;
    let usage_before =
        f.db.catalog
            .all_visible_files(f.tenant, Signal::Usage1h)
            .await
            .unwrap();
    let c = compactor(&f, config());
    let me = fence(&f).await;
    let done = c.compact_l2_once(&me).await.unwrap().unwrap();
    let mut taken = done.inputs.clone();
    taken.sort();
    let mut want: Vec<_> = l1.iter().map(|x| x.id).collect();
    want.sort();
    assert_eq!(taken, want);

    let l2 = spans(&f).await;
    assert_eq!(l2.len(), 1);
    assert_eq!(l2[0].level, Level::L2);
    assert!(l2[0].usage_accounted);
    assert!(
        l2[0].path.contains("/traces/l2/dt=2026-10-24/") && !l2[0].path.contains("hr="),
        "{}",
        l2[0].path
    );
    assert_eq!(l2[0].services, ["api", "web"]);
    let rows = rows_of(&f.dir.path().join(&l2[0].path));
    let mut sorted = rows.clone();
    sorted.sort();
    assert_eq!(rows, sorted, "sorted by service and time across the day");
    assert_eq!(rows.len(), 9);
    let tix = fs_index::sidecar_path(&l2[0].path).unwrap();
    assert!(tix.contains("/traces/index/trace_id/dt=2026-10-24/"));
    let index =
        fs_index::TraceIndex::decode(&std::fs::read(f.dir.path().join(&tix)).unwrap()).unwrap();
    assert!(index.covers(&l2[0].path));
    assert_eq!(index.len(), 3, "the three traces");

    assert_eq!(count(&f, "select count(*) from spans").await, before);
    let trace = format!(
        "select count(*) from spans where trace_id = X'{}'",
        "02".repeat(16)
    );
    assert_eq!(count(&f, &trace).await, 3);
    assert_eq!(
        f.db.catalog
            .all_visible_files(f.tenant, Signal::Usage1h)
            .await
            .unwrap(),
        usage_before,
        "counted when it left L0"
    );
    assert!(c.compact_l2_once(&me).await.unwrap().is_none());
    assert_eq!(c.stats().l2_passes.load(Ordering::Relaxed), 1);
    f.db.finish().await;
}

/// No pass while L2 is off, or while a day still has an L0 file.
#[tokio::test]
async fn a_day_with_an_l0_file_or_l2_off_stays_at_l1() {
    let Some(f) = fixture(15).await else { return };
    day_of_l1(&f, closed_day()).await;
    let me = fence(&f).await;
    let off = compactor(
        &f,
        CompactionConfig {
            compact_l2: false,
            ..config()
        },
    );
    assert!(off.compact_l2_once(&me).await.unwrap().is_none());
    // A late L0 file whose confirmation is recent: not cold, not compacted.
    write_l0(&f, "web", 9, closed_day() + 20 * HOUR, "k").await;
    sqlx::query("update engine_file set confirmed_at = now() where level = 0")
        .execute(f.db.catalog.pool())
        .await
        .unwrap();
    let c = compactor(&f, config());
    assert!(c.compact_l2_once(&me).await.unwrap().is_none());
    assert!(spans(&f).await.iter().all(|x| x.level != Level::L2));
    f.db.finish().await;
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

/// A pass cut short before its swap, between its L2 file and its sidecar,
/// or by a lost lease publishes nothing and leaves its inputs visible; the
/// sweep removes what it uploaded, L2 paths of a day included; the next
/// pass merges the day once, and after its swap a restart has nothing to do.
#[tokio::test]
async fn an_l2_pass_cut_short_publishes_nothing_and_the_next_merges_once() {
    let Some(f) = fixture(15).await else { return };
    let l1 = day_of_l1(&f, closed_day()).await;
    let ids = |files: &[FileRecord]| {
        let mut ids: Vec<_> = files.iter().map(|x| x.id).collect();
        ids.sort();
        ids
    };
    let me = fence(&f).await;
    for (config, expected) in [
        (
            CompactionConfig {
                inject_swap_failures: 1,
                ..config()
            },
            CompactError::Injected("swap"),
        ),
        (
            CompactionConfig {
                inject_put_failure_at: Some(1),
                ..config()
            },
            CompactError::Injected("object put"),
        ),
    ] {
        let err = compactor(&f, config)
            .compact_l2_once(&me)
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), expected.to_string());
        assert_eq!(ids(&spans(&f).await), ids(&l1));
    }
    let old = LeaseFence {
        generation: me.generation - 1,
        ..me.clone()
    };
    let err = compactor(&f, config())
        .compact_l2_once(&old)
        .await
        .unwrap_err();
    assert!(
        matches!(err, CompactError::Catalog(CatalogError::LeaseLost { .. })),
        "{err}"
    );
    assert_eq!(ids(&spans(&f).await), ids(&l1));
    assert_eq!(objects_under(&f, "/traces/l2/").await.len(), 3);
    assert_eq!(
        objects_under(&f, "/traces/index/trace_id/dt=2026-10-24/0")
            .await
            .len(),
        2
    );

    let mut sweeper = fs_compactor::Sweeper::new(
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
    );
    sweeper.sweep_once().await.unwrap();
    sqlx::query("update engine_orphan set first_seen = now() - interval '2 hours'")
        .execute(f.db.catalog.pool())
        .await
        .unwrap();
    let swept = sweeper.sweep_once().await.unwrap();
    assert_eq!(swept.deleted, 5, "three L2 files and two of their sidecars");
    assert_eq!(objects_under(&f, "/traces/l2/").await.len(), 0);

    let c = compactor(&f, config());
    c.compact_l2_once(&me).await.unwrap().unwrap();
    assert_eq!(spans(&f).await.len(), 1);
    assert!(
        compactor(&f, config())
            .compact_l2_once(&me)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(count(&f, "select count(*) from spans").await, 9);
    f.db.finish().await;
}

/// An output closes at its memory budget; one whose trace ids pass their
/// cap has no sidecar, one whose services pass theirs lists none, and
/// both are still read by queries.
#[tokio::test]
async fn l2_outputs_stay_within_their_memory_and_sidecar_caps() {
    let Some(f) = fixture(15).await else { return };
    day_of_l1(&f, closed_day()).await;
    let me = fence(&f).await;
    let c = compactor(
        &f,
        CompactionConfig {
            l2_output_bytes: 1,
            max_trace_index_bytes: 0,
            max_services_per_file: 0,
            ..config()
        },
    );
    c.compact_l2_once(&me).await.unwrap().unwrap();
    let l2 = spans(&f).await;
    assert!(
        l2.len() >= 2,
        "each row group closes an output: {}",
        l2.len()
    );
    assert!(
        l2.iter().all(|x| x.services.is_empty()),
        "services over the cap: unknown"
    );
    assert_eq!(l2.iter().map(|x| x.row_count).sum::<i64>(), 9);
    let with_sidecar = l2
        .iter()
        .filter(|x| {
            f.dir
                .path()
                .join(fs_index::sidecar_path(&x.path).unwrap())
                .exists()
        })
        .count();
    assert_eq!(with_sidecar, 0, "an output past its trace cap has none");
    assert!(c.stats().sidecars_over_cap.load(Ordering::Relaxed) >= 1);
    let trace = format!(
        "select count(*) from spans where trace_id = X'{}'",
        "03".repeat(16)
    );
    assert_eq!(count(&f, &trace).await, 3, "scanned without a sidecar");
    assert_eq!(
        count(&f, "select count(*) from spans where service_name = 'api'").await,
        3
    );
    f.db.finish().await;
}

/// An L1 file written before usage was counted is counted by the L2 pass
/// that takes it, once.
#[tokio::test]
async fn a_legacy_l1_file_is_counted_by_its_l2_pass() {
    let Some(f) = fixture(15).await else { return };
    day_of_l1(&f, closed_day()).await;
    sqlx::query("update engine_file set usage_accounted = false where signal = 1")
        .execute(f.db.catalog.pool())
        .await
        .unwrap();
    sqlx::query("delete from engine_file where signal = $1")
        .bind(Signal::Usage1h.code())
        .execute(f.db.catalog.pool())
        .await
        .unwrap();
    let me = fence(&f).await;
    let c = compactor(&f, config());
    c.compact_l2_once(&me).await.unwrap().unwrap();
    assert_eq!(
        count(&f, "select cast(sum(rows) as bigint) from usage_1h").await,
        9
    );
    assert_eq!(
        f.db.catalog
            .all_visible_files(f.tenant, Signal::Usage1h)
            .await
            .unwrap()
            .len(),
        3,
        "one contribution per hour of the day"
    );
    assert!(
        c.account_once(&me).await.unwrap().is_none(),
        "nothing left to count"
    );
    f.db.finish().await;
}

/// Once a day beyond retention is gone, the rebuilt `attribute_keys`
/// table keeps the keys the L2 files that remain still hold.
#[tokio::test]
async fn the_key_rebuild_after_retention_reads_l2_files() {
    let Some(f) = fixture(1).await else { return };
    let (cut,): (chrono::DateTime<Utc>,) = sqlx::query_as(
        "select date_bin('1 day', now(), timestamptz '1970-01-01 00:00:00+00') - interval '1 day'",
    )
    .fetch_one(f.db.catalog.pool())
    .await
    .unwrap();
    let kept_day = nanos(cut);
    let old_day = kept_day - 24 * HOUR;
    write_l0(&f, "web", 1, old_day + 3 * HOUR, "old.key").await;
    write_l0(&f, "web", 2, kept_day + 3 * HOUR, "kept.key").await;
    let me = fence(&f).await;
    let c = compactor(&f, config());
    while c.compact_once(&me).await.unwrap().is_some() {}
    backdate(&f).await;
    while c.compact_l2_once(&me).await.unwrap().is_some() {}
    let files = spans(&f).await;
    assert!(files.iter().all(|x| x.level == Level::L2), "{files:?}");

    let retention = Retention::new(
        f.db.catalog.clone(),
        f.store.clone(),
        Path::from(""),
        RetentionConfig {
            files_per_pass: 100,
            rebuild_max_input_bytes: 1 << 30,
            rebuild_max_bytes: 1 << 30,
            batch_rows: 4,
            limits: Limits::default(),
            rebuilds_per_tick: 64,
            rebuild_backoff: Duration::from_secs(2),
            inject_swap_failures: 0,
            inject_list_failures: 0,
        },
        Arc::new(RetentionStats::default()),
    );
    while retention.expire_once(&me).await.unwrap().is_some() {}
    assert!(matches!(
        retention.rebuild_once(&me).await.unwrap(),
        Rebuild::Done(_)
    ));
    let table =
        f.db.catalog
            .all_visible_files(f.tenant, Signal::AttributeKeys)
            .await
            .unwrap();
    assert_eq!(table.len(), 1);
    let bytes = f
        .store
        .get(&Path::from(table[0].path.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let keys = decode(bytes, KeyTable::Spans).unwrap();
    let names: Vec<String> = keys.iter().map(|(_, name, ..)| name.to_string()).collect();
    assert!(names.contains(&"kept.key".to_owned()), "{names:?}");
    assert!(!names.contains(&"old.key".to_owned()), "{names:?}");
    f.db.finish().await;
}

/// A tick runs its L2 share however long the L1 backlog.
#[tokio::test]
async fn a_tick_runs_an_l2_pass_behind_an_l1_backlog() {
    let Some(f) = fixture(15).await else { return };
    day_of_l1(&f, closed_day()).await;
    let later = closed_day() + 24 * HOUR;
    for hour in [1, 2, 3] {
        write_l0(&f, "web", 9, later + hour * HOUR, "k").await;
    }
    let me = fence(&f).await;
    let c = compactor(
        &f,
        CompactionConfig {
            l1_passes_per_tick: 1,
            ..config()
        },
    );
    let ran = compaction_tick(&c, || Some(me.clone())).await;
    assert_eq!(
        ran,
        TickPasses {
            l1: 1,
            l2: 1,
            accounted: 0
        }
    );
    assert!(spans(&f).await.iter().any(|x| x.level == Level::L2));
    assert!(
        spans(&f).await.iter().any(|x| x.level == Level::L0),
        "the backlog waits"
    );
    f.db.finish().await;
}

/// A day larger than one pass closes over several: the L1 file the first
/// pass left beside its L2 file is taken by the next.
#[tokio::test]
async fn a_day_larger_than_one_pass_closes_over_several() {
    let Some(f) = fixture(15).await else { return };
    let l1 = day_of_l1(&f, closed_day()).await;
    let me = fence(&f).await;
    let c = compactor(
        &f,
        CompactionConfig {
            max_inputs_per_pass: 2,
            ..config()
        },
    );
    assert_eq!(
        c.compact_l2_once(&me).await.unwrap().unwrap().inputs.len(),
        2
    );
    backdate(&f).await;
    let rest = c.compact_l2_once(&me).await.unwrap().unwrap();
    assert_eq!(rest.inputs.len(), 1);
    assert!(l1.iter().any(|x| x.id == rest.inputs[0]));
    assert!(c.compact_l2_once(&me).await.unwrap().is_none());
    let now = spans(&f).await;
    assert_eq!(now.len(), 2);
    assert!(now.iter().all(|x| x.level == Level::L2));
    assert_eq!(count(&f, "select count(*) from spans").await, 9);
    f.db.finish().await;
}

/// Writes one cold L0 file of `spans` spans of `service` from `start`,
/// each carrying an attribute of `value_bytes` bytes.
async fn write_wide_l0(f: &Fixture, start: u64, spans: u8, value_bytes: usize) {
    let mut request = request("web", 7, start, spans, "k");
    for (i, span) in request.resource_spans[0].scope_spans[0]
        .spans
        .iter_mut()
        .enumerate()
    {
        // Hex of a running mix: a payload compression cannot shrink.
        let mut state = (i as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut value = String::with_capacity(value_bytes);
        while value.len() < value_bytes {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            write!(value, "{state:016x}").unwrap();
        }
        span.attributes = vec![kv("payload", &value)];
    }
    let mut builder = SpanBatchBuilder::new(Limits::default());
    builder.append_request(&request).unwrap();
    let prepared = flush::prepare(
        f.tenant,
        &Path::from(""),
        fs_catalog::FileId::new(),
        &[builder.finish().unwrap()],
        FileLayout::current(1024),
    )
    .unwrap();
    flush::upload(f.store.as_ref(), &prepared).await.unwrap();
    flush::upload_index(f.store.as_ref(), &prepared)
        .await
        .unwrap();
    f.db.catalog.register_file(&prepared.file).await.unwrap();
    backdate(f).await;
}

/// With row groups of up to 1024 rows and a budget far below a whole day,
/// the merge splits its row groups and closes outputs early enough that
/// the inputs and the output never hold more than the budget.
#[tokio::test]
async fn an_l2_merge_holds_no_more_than_its_output_budget() {
    let Some(f) = fixture(15).await else { return };
    for hour in [3, 4, 5] {
        write_wide_l0(&f, closed_day() + hour * HOUR, 200, 2048).await;
    }
    let budget = 3 << 19;
    let wide = CompactionConfig {
        row_group_rows: 1024,
        l2_output_bytes: budget,
        ..config()
    };
    let me = fence(&f).await;
    let c = compactor(&f, wide.clone());
    while c.compact_once(&me).await.unwrap().is_some() {}
    backdate(&f).await;
    c.compact_l2_once(&me).await.unwrap().unwrap();

    let peak = usize::try_from(c.stats().l2_peak_bytes.load(Ordering::Relaxed)).unwrap();
    assert!(
        peak > 0 && peak <= budget,
        "peak {peak} over the budget {budget}"
    );
    let l2 = spans(&f).await;
    assert!(l2.iter().all(|x| x.level == Level::L2));
    assert!(l2.len() >= 2, "{} outputs", l2.len());
    assert_eq!(l2.iter().map(|x| x.row_count).sum::<i64>(), 600);
    let largest_group = l2
        .iter()
        .flat_map(|x| {
            let file = std::fs::File::open(f.dir.path().join(&x.path)).unwrap();
            ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap()
                .metadata()
                .row_groups()
                .iter()
                .map(parquet::file::metadata::RowGroupMetaData::num_rows)
                .collect::<Vec<_>>()
        })
        .max()
        .unwrap();
    assert!(
        largest_group < 600,
        "row groups split by bytes: {largest_group}"
    );
    assert_eq!(count(&f, "select count(*) from spans").await, 600);
    f.db.finish().await;
}
