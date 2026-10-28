//! L0 to L1 compaction against a real Postgres and a local bucket.
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
use arrow::datatypes::{DataType, Int64Type, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use chrono::{TimeZone, Timelike, Utc};
use fs_catalog::{
    COMPACTOR_LEASE, Catalog, CatalogError, FileRecord, LeaseFence, Level, NewTenant, Signal,
    TenantId,
};
use fs_compactor::{CompactError, CompactionConfig, Compactor, CompactorStats};
use fs_ingest::flush;
use fs_query::{Querier, QueryLimits};
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
        let schema = format!("k_{}", uuid::Uuid::new_v4().simple());
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

/// A closed hour: 2026-10-24T14:00Z.
fn closed_hour() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 24, 14, 0, 0).unwrap()
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

fn request(service: &str, trace: u8, start: u64, spans: u8) -> ExportTraceServiceRequest {
    let spans = (0..spans)
        .map(|i| Span {
            trace_id: vec![trace; 16],
            span_id: vec![i + 1; 8],
            name: format!("op-{i}"),
            kind: 2,
            start_time_unix_nano: start + u64::from(i) * 1_000_000,
            end_time_unix_nano: start + u64::from(i) * 1_000_000 + 250_000,
            attributes: vec![kv("http.request.method", "GET")],
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

/// Writes and registers one L0 file for `requests`, with `row_group_rows`.
async fn write_l0(
    catalog: &Catalog,
    store: &LocalFileSystem,
    tenant: TenantId,
    requests: &[ExportTraceServiceRequest],
    row_group_rows: usize,
) -> FileRecord {
    let batches: Vec<RecordBatch> = requests
        .iter()
        .map(|r| {
            let mut builder = SpanBatchBuilder::new(Limits::default());
            builder.append_request(r).unwrap();
            builder.finish().unwrap()
        })
        .collect();
    let prepared = flush::prepare(
        tenant,
        &Path::from(""),
        fs_catalog::FileId::new(),
        &batches,
        row_group_rows,
    )
    .unwrap();
    flush::upload(store, &prepared).await.unwrap();
    flush::upload_index(store, &prepared).await.unwrap();
    catalog.register_file(&prepared.file).await.unwrap()
}

struct Fixture {
    db: TestDb,
    tenant: TenantId,
    store: Arc<LocalFileSystem>,
    dir: tempfile::TempDir,
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
        dir,
    })
}

fn config(row_group_rows: usize) -> CompactionConfig {
    CompactionConfig {
        hour_close_delay: Duration::from_secs(60),
        l1_target_bytes: 512 * 1024 * 1024,
        max_inputs_per_pass: 16,
        max_input_bytes: 2 << 30,
        row_group_rows,
        inject_swap_failures: 0,
        inject_put_failure_at: None,
        inject_put_hold: None,
    }
}

/// The fence of a lease taken for this test's holder.
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

/// Backdates the registration of `files` by an hour: cold, as files
/// flushed long ago would be.
async fn registered_an_hour_ago(f: &Fixture, files: &[FileRecord]) {
    let ids: Vec<_> = files.iter().map(|x| x.id).collect();
    sqlx::query("update engine_file set created_at = now() - interval '1 hour' where id = any($1)")
        .bind(&ids)
        .execute(f.db.catalog.pool())
        .await
        .unwrap();
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

/// Three L0 files in the closed hour with interleaved services and times,
/// returned in registration order.
async fn three_closed_l0(f: &Fixture, row_group_rows: usize) -> Vec<FileRecord> {
    let h = nanos(closed_hour());
    let minute = 60_000_000_000u64;
    let mut files = Vec::new();
    for (service, trace, offset) in [("web", 1u8, 5), ("api", 2, 1), ("web", 3, 2)] {
        let req = request(service, trace, h + offset * minute, 6);
        files.push(write_l0(&f.db.catalog, &f.store, f.tenant, &[req], row_group_rows).await);
    }
    registered_an_hour_ago(f, &files).await;
    files
}

fn request_without_service(trace: u8, start: u64, spans: u8) -> ExportTraceServiceRequest {
    let mut req = request("x", trace, start, spans);
    req.resource_spans[0].resource = Some(Resource::default());
    req
}

async fn visible(f: &Fixture) -> Vec<FileRecord> {
    f.db.catalog
        .all_visible_files(f.tenant, Signal::Traces)
        .await
        .unwrap()
}

/// Rows of a local Parquet file as `(service, timestamp)` in file order;
/// a null service is `None`.
fn rows_of(path: &std::path::Path) -> Vec<(Option<String>, i64)> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let schema = batch.schema();
        let service = arrow::compute::cast(
            batch.column(schema.index_of("service_name").unwrap()),
            &DataType::Utf8,
        )
        .unwrap();
        let ts = batch
            .column(schema.index_of("timestamp").unwrap())
            .as_primitive::<TimestampNanosecondType>()
            .values()
            .to_vec();
        for (s, t) in service.as_string::<i32>().iter().zip(ts) {
            out.push((s.map(str::to_owned), t));
        }
    }
    out
}

fn count(batches: &[RecordBatch]) -> i64 {
    batches
        .iter()
        .map(|b| b.column(0).as_primitive::<Int64Type>().value(0))
        .sum()
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

#[tokio::test]
async fn a_closed_hour_with_several_l0_files_becomes_one_sorted_l1_file() {
    let Some(f) = fixture().await else { return };
    let inputs = three_closed_l0(&f, 4).await;
    // An open hour is left alone.
    let open = write_l0(
        &f.db.catalog,
        &f.store,
        f.tenant,
        &[request("web", 9, nanos(Utc::now()), 3)],
        4,
    )
    .await;
    let q = querier(&f);
    let before = q
        .collect(f.tenant, "select count(*) from spans")
        .await
        .unwrap();
    let hex = "02".repeat(16);
    let trace_sql = format!("select count(*) from spans where trace_id = decode('{hex}', 'hex')");
    let before_trace = q.collect(f.tenant, &trace_sql).await.unwrap();

    let c = compactor(&f, config(4));
    let done = c.compact_once(&fence(&f).await).await.unwrap().unwrap();
    assert_eq!(done.inputs.len(), 3);
    assert_eq!(done.outputs.len(), 1);

    let now = visible(&f).await;
    assert_eq!(now.len(), 2, "one L1 and the open-hour L0");
    let l1 = now.iter().find(|x| x.level == Level::L1).unwrap();
    assert!(
        now.iter().any(|x| x.id == open.id),
        "the open hour is untouched"
    );
    assert!(
        l1.path.contains("/traces/l1/dt=2026-10-24/hr=14/"),
        "{}",
        l1.path
    );
    assert_eq!(l1.row_count, 18);
    assert_eq!(l1.services, ["api", "web"]);
    assert_eq!(
        l1.range.start,
        inputs.iter().map(|i| i.range.start).min().unwrap()
    );
    assert_eq!(
        l1.range.end,
        inputs.iter().map(|i| i.range.end).max().unwrap()
    );

    let rows = rows_of(&f.dir.path().join(&l1.path));
    assert_eq!(rows.len(), 18);
    assert!(
        rows.windows(2).all(|w| w[0] <= w[1]),
        "sorted by (service, timestamp): {rows:?}"
    );
    let sidecar = f.dir.path().join(fs_index::sidecar_path(&l1.path).unwrap());
    let index = fs_index::TraceIndex::decode(&std::fs::read(sidecar).unwrap()).unwrap();
    assert_eq!(index.files(), std::slice::from_ref(&l1.path));
    assert_eq!(index.len(), 3);
    for trace in 1u8..=3 {
        assert!(index.file_holds(&l1.path, &[[trace; 16]]));
    }

    // Queries answer the same before and after.
    q.cache().invalidate(f.tenant, Signal::Traces).await;
    assert_eq!(
        count(
            &q.collect(f.tenant, "select count(*) from spans")
                .await
                .unwrap()
        ),
        count(&before)
    );
    assert_eq!(
        count(&q.collect(f.tenant, &trace_sql).await.unwrap()),
        count(&before_trace)
    );
    assert_eq!(count(&before_trace), 6);

    let stats = c.stats();
    assert_eq!(stats.passes.load(Ordering::Relaxed), 1);
    assert_eq!(stats.inputs.load(Ordering::Relaxed), 3);
    assert_eq!(stats.outputs.load(Ordering::Relaxed), 1);
    assert_eq!(stats.rows.load(Ordering::Relaxed), 18);
    assert!(
        c.compact_once(&fence(&f).await).await.unwrap().is_none(),
        "nothing left to compact"
    );
    f.db.finish().await;
}

#[tokio::test]
async fn outputs_close_at_the_target_size() {
    let Some(f) = fixture().await else { return };
    three_closed_l0(&f, 4).await;
    let c = compactor(
        &f,
        CompactionConfig {
            l1_target_bytes: 1,
            ..config(4)
        },
    );
    let done = c.compact_once(&fence(&f).await).await.unwrap().unwrap();
    assert!(
        done.outputs.len() >= 2,
        "every row group closes an output: {}",
        done.outputs.len()
    );
    let now = visible(&f).await;
    assert_eq!(now.len(), done.outputs.len());
    assert_eq!(now.iter().map(|x| x.row_count).sum::<i64>(), 18);
    let mut all_rows = Vec::new();
    for file in &now {
        let rows = rows_of(&f.dir.path().join(&file.path));
        assert!(rows.len() <= 4, "one row group per output");
        let sidecar = f
            .dir
            .path()
            .join(fs_index::sidecar_path(&file.path).unwrap());
        let index = fs_index::TraceIndex::decode(&std::fs::read(sidecar).unwrap()).unwrap();
        assert!(index.covers(&file.path));
        all_rows.extend(rows);
    }
    assert!(
        all_rows.windows(2).all(|w| w[0] <= w[1]),
        "outputs follow each other in order"
    );
    f.db.finish().await;
}

#[tokio::test]
async fn a_pass_takes_the_oldest_inputs_up_to_its_limit() {
    let Some(f) = fixture().await else { return };
    let inputs = three_closed_l0(&f, 4).await;
    let c = compactor(
        &f,
        CompactionConfig {
            max_inputs_per_pass: 2,
            ..config(4)
        },
    );
    let done = c.compact_once(&fence(&f).await).await.unwrap().unwrap();
    assert_eq!(done.inputs, [inputs[0].id, inputs[1].id], "the two oldest");
    let now = visible(&f).await;
    assert!(
        now.iter().any(|x| x.id == inputs[2].id),
        "the newest stays as L0"
    );
    assert!(
        c.compact_once(&fence(&f).await).await.unwrap().is_none(),
        "one L0 left: not enough to compact"
    );
    f.db.finish().await;
}

/// A crash after the outputs are uploaded and before the swap: the
/// inputs stay visible, the outputs are objects without rows, and the
/// next pass compacts again under fresh ids.
#[tokio::test]
async fn a_crash_before_the_swap_leaves_inputs_visible_and_outputs_orphaned() {
    let Some(f) = fixture().await else { return };
    let inputs = three_closed_l0(&f, 4).await;
    let c = compactor(
        &f,
        CompactionConfig {
            inject_swap_failures: 1,
            ..config(4)
        },
    );
    let err = c.compact_once(&fence(&f).await).await.unwrap_err();
    assert!(matches!(err, CompactError::Injected("swap")), "{err}");
    let now = visible(&f).await;
    let mut still: Vec<_> = now.iter().map(|x| x.id).collect();
    still.sort();
    let mut expected: Vec<_> = inputs.iter().map(|x| x.id).collect();
    expected.sort();
    assert_eq!(still, expected, "every input is still visible");
    let orphans = l1_objects(&f).await;
    assert_eq!(orphans.len(), 1, "one L1 object with no row");
    let orphan_sidecar = fs_index::sidecar_path(&orphans[0]).unwrap();
    assert!(
        f.store
            .head(&Path::from(orphan_sidecar.as_str()))
            .await
            .is_ok(),
        "and its sidecar"
    );
    assert_eq!(c.stats().orphaned.load(Ordering::Relaxed), 1);
    assert_eq!(c.stats().failures.load(Ordering::Relaxed), 1);

    let done = c.compact_once(&fence(&f).await).await.unwrap().unwrap();
    assert_eq!(done.inputs.len(), 3);
    let now = visible(&f).await;
    assert_eq!(now.len(), 1);
    assert!(orphans.iter().all(|o| o != &now[0].path), "fresh ids");
    f.db.finish().await;
}

/// A swap carrying a fence the lease has moved past publishes nothing.
#[tokio::test]
async fn a_stale_fence_publishes_nothing() {
    let Some(f) = fixture().await else { return };
    let inputs = three_closed_l0(&f, 4).await;
    let generation =
        f.db.catalog
            .acquire_lease(COMPACTOR_LEASE, "other", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
    let stale = LeaseFence {
        name: COMPACTOR_LEASE.into(),
        holder: "me".into(),
        generation,
    };
    let c = compactor(&f, config(4));
    let err = c.compact_once(&stale).await.unwrap_err();
    assert!(
        matches!(err, CompactError::Catalog(CatalogError::LeaseLost { .. })),
        "{err}"
    );
    let now = visible(&f).await;
    assert_eq!(now.len(), inputs.len());
    assert!(now.iter().all(|x| x.level == Level::L0));
    assert_eq!(c.stats().lease_lost.load(Ordering::Relaxed), 1);
    assert_eq!(c.stats().orphaned.load(Ordering::Relaxed), 1);

    // The real holder's fence works.
    let real = LeaseFence {
        holder: "other".into(),
        ..stale
    };
    assert!(c.compact_once(&real).await.unwrap().is_some());
    f.db.finish().await;
}

/// Two cold L0 files do not make an hour compactable while a third file
/// in it ended recently or was registered recently: the hot window may
/// still serve it and late data may still arrive.
#[tokio::test]
async fn an_hour_with_a_recent_or_late_file_is_not_cold() {
    let Some(f) = fixture().await else { return };
    let cold = three_closed_l0(&f, 4).await;
    let hour = nanos(closed_hour());
    // Late data: old timestamps, registered just now, not backdated.
    let late = write_l0(
        &f.db.catalog,
        &f.store,
        f.tenant,
        &[request("web", 7, hour + 1, 2)],
        4,
    )
    .await;
    let compactor = compactor(&f, config(4));
    let me = fence(&f).await;
    assert!(
        compactor.compact_once(&me).await.unwrap().is_none(),
        "late data keeps the hour open"
    );

    // Once it is cold too, the whole hour compacts, the late file included.
    registered_an_hour_ago(&f, std::slice::from_ref(&late)).await;
    let done = compactor.compact_once(&me).await.unwrap().unwrap();
    assert_eq!(done.inputs.len(), cold.len() + 1);

    // A file whose rows end now keeps its hour open however old its
    // registration: two cold files at the start of the current hour and
    // one ending now, all in the same hour.
    let now = Utc::now();
    let recent_hour = now
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    let first = write_l0(
        &f.db.catalog,
        &f.store,
        f.tenant,
        &[request("web", 11, nanos(recent_hour), 2)],
        4,
    )
    .await;
    let second = write_l0(
        &f.db.catalog,
        &f.store,
        f.tenant,
        &[request("api", 12, nanos(recent_hour) + 1, 2)],
        4,
    )
    .await;
    let ending_now = write_l0(
        &f.db.catalog,
        &f.store,
        f.tenant,
        &[request("api", 13, nanos(now), 2)],
        4,
    )
    .await;
    registered_an_hour_ago(&f, &[first, second, ending_now]).await;
    assert!(
        compactor.compact_once(&me).await.unwrap().is_none(),
        "one file still ends now"
    );
    f.db.finish().await;
}

/// A null service sorts before every name, the empty one included, across
/// inputs as it does inside each of them.
#[tokio::test]
async fn a_null_service_sorts_before_an_empty_one_across_inputs() {
    let Some(f) = fixture().await else { return };
    let h = nanos(closed_hour());
    // The empty name has the earlier timestamps; the null the later ones.
    let empty = write_l0(
        &f.db.catalog,
        &f.store,
        f.tenant,
        &[request("", 1, h, 3)],
        4,
    )
    .await;
    let null = write_l0(
        &f.db.catalog,
        &f.store,
        f.tenant,
        &[request_without_service(2, h + 60_000_000_000, 3)],
        4,
    )
    .await;
    registered_an_hour_ago(&f, &[empty, null]).await;
    let c = compactor(&f, config(4));
    c.compact_once(&fence(&f).await).await.unwrap().unwrap();
    let l1 = visible(&f)
        .await
        .into_iter()
        .find(|x| x.level == Level::L1)
        .unwrap();
    let rows = rows_of(&f.dir.path().join(&l1.path));
    assert_eq!(rows.len(), 6);
    assert!(
        rows[..3].iter().all(|(s, _)| s.is_none()),
        "nulls first: {rows:?}"
    );
    assert!(
        rows[3..].iter().all(|(s, _)| s.as_deref() == Some("")),
        "{rows:?}"
    );
    f.db.finish().await;
}

/// The input byte cap is a ceiling: an hour whose two oldest files do not
/// fit is skipped and counted, not compacted over the cap.
#[tokio::test]
async fn the_input_cap_is_a_ceiling() {
    let Some(f) = fixture().await else { return };
    let inputs = three_closed_l0(&f, 4).await;
    let smallest_pair: u64 = inputs
        .iter()
        .take(2)
        .map(|x| u64::try_from(x.size_bytes).unwrap())
        .sum();
    let c = compactor(
        &f,
        CompactionConfig {
            max_input_bytes: smallest_pair - 1,
            ..config(4)
        },
    );
    let me = fence(&f).await;
    assert!(c.compact_once(&me).await.unwrap().is_none());
    assert_eq!(c.stats().skipped.load(Ordering::Relaxed), 1);
    assert_eq!(visible(&f).await.len(), 3, "nothing compacted");

    // Exactly the pair fits: two go, the third stays.
    let c = compactor(
        &f,
        CompactionConfig {
            max_input_bytes: smallest_pair,
            ..config(4)
        },
    );
    let done = c.compact_once(&me).await.unwrap().unwrap();
    assert_eq!(done.inputs.len(), 2);
    f.db.finish().await;
}

/// An hour over the input cap is passed over and the next cold hour
/// compacts; the stuck hour is reported once.
#[tokio::test]
async fn an_hour_over_the_cap_does_not_block_later_hours() {
    let Some(f) = fixture().await else { return };
    let stuck = three_closed_l0(&f, 4).await;
    let later_hour = nanos(closed_hour() + chrono::Duration::hours(1));
    let mut later = Vec::new();
    for (service, trace) in [("api", 21u8), ("api", 22)] {
        later.push(
            write_l0(
                &f.db.catalog,
                &f.store,
                f.tenant,
                &[request(service, trace, later_hour, 2)],
                4,
            )
            .await,
        );
    }
    registered_an_hour_ago(&f, &later).await;
    let pair: u64 = stuck
        .iter()
        .take(2)
        .map(|x| u64::try_from(x.size_bytes).unwrap())
        .sum();
    let c = compactor(
        &f,
        CompactionConfig {
            max_input_bytes: pair - 1,
            ..config(4)
        },
    );
    let me = fence(&f).await;
    let done = c.compact_once(&me).await.unwrap().unwrap();
    assert_eq!(
        done.inputs,
        later.iter().map(|x| x.id).collect::<Vec<_>>(),
        "the later hour"
    );
    assert_eq!(c.stats().skipped.load(Ordering::Relaxed), 1);
    assert!(c.compact_once(&me).await.unwrap().is_none());
    assert_eq!(
        c.stats().skipped.load(Ordering::Relaxed),
        1,
        "reported once"
    );
    assert_eq!(
        visible(&f)
            .await
            .iter()
            .filter(|x| x.level == Level::L0)
            .count(),
        3,
        "the stuck hour is untouched"
    );
    f.db.finish().await;
}

/// A failure between outputs counts every object created so far as an
/// orphan, sidecar put included.
#[tokio::test]
async fn a_failure_between_outputs_counts_the_orphans_created_so_far() {
    let Some(f) = fixture().await else { return };
    three_closed_l0(&f, 4).await;
    let me = fence(&f).await;
    // Puts go parquet, sidecar, parquet, sidecar, ...; the second
    // sidecar fails: two Parquet objects exist.
    let c = compactor(
        &f,
        CompactionConfig {
            l1_target_bytes: 1,
            inject_put_failure_at: Some(3),
            ..config(4)
        },
    );
    let err = c.compact_once(&me).await.unwrap_err();
    assert!(matches!(err, CompactError::Injected("object put")), "{err}");
    assert_eq!(c.stats().orphaned.load(Ordering::Relaxed), 2);
    assert_eq!(c.stats().failures.load(Ordering::Relaxed), 1);
    assert_eq!(l1_objects(&f).await.len(), 2);
    assert_eq!(visible(&f).await.len(), 3, "nothing published");

    // The second Parquet put fails: one object exists.
    let c = compactor(
        &f,
        CompactionConfig {
            l1_target_bytes: 1,
            inject_put_failure_at: Some(2),
            ..config(4)
        },
    );
    assert!(c.compact_once(&me).await.is_err());
    assert_eq!(c.stats().orphaned.load(Ordering::Relaxed), 1);
    assert_eq!(l1_objects(&f).await.len(), 3);
    f.db.finish().await;
}

/// A pass cancelled between outputs, as losing the lease or shutting down
/// cancels it, counts the objects it created as orphans.
#[tokio::test]
async fn a_cancelled_pass_counts_the_orphans_it_created() {
    let Some(f) = fixture().await else { return };
    three_closed_l0(&f, 4).await;
    let me = fence(&f).await;
    // The first Parquet goes up; its sidecar put is held, as a slow store
    // would, long enough for the lease to go.
    let c = Arc::new(compactor(
        &f,
        CompactionConfig {
            l1_target_bytes: 1,
            inject_put_hold: Some((1, Duration::from_secs(30))),
            ..config(4)
        },
    ));
    let stats = c.stats();
    let pass = tokio::spawn({
        let c = Arc::clone(&c);
        async move { c.compact_once(&me).await.map(|_| ()) }
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while l1_objects(&f).await.is_empty() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        l1_objects(&f).await.len(),
        1,
        "the first output is in the bucket"
    );
    assert_eq!(
        stats.orphaned.load(Ordering::Relaxed),
        0,
        "not an orphan while the pass runs"
    );

    // The lease goes: the leader loop drops the work task.
    pass.abort();
    assert!(pass.await.unwrap_err().is_cancelled());
    assert_eq!(
        stats.orphaned.load(Ordering::Relaxed),
        1,
        "counted when the task was dropped"
    );
    assert_eq!(l1_objects(&f).await.len(), 1);
    assert_eq!(visible(&f).await.len(), 3, "nothing published");
    f.db.finish().await;
}

/// Backdates the first sighting of every remembered orphan by two hours.
async fn orphans_seen_two_hours_ago(f: &Fixture) {
    sqlx::query("update engine_orphan set first_seen = now() - interval '2 hours'")
        .execute(f.db.catalog.pool())
        .await
        .unwrap();
}

fn sweeper(f: &Fixture, prefixes_per_pass: usize) -> fs_compactor::Sweeper {
    fs_compactor::Sweeper::new(
        f.db.catalog.clone(),
        f.store.clone(),
        Path::from(""),
        fs_compactor::SweepConfig {
            ttl: Duration::from_secs(3600),
            prefixes_per_pass,
            deletes_per_pass: 100,
        },
        Arc::new(fs_compactor::OrphanStats::default()),
    )
}

async fn present(f: &Fixture, path: &str) -> bool {
    f.store.head(&Path::from(path)).await.is_ok()
}

/// Objects without a row are remembered on first sight and deleted only
/// when still without a row a TTL later; registered files, their
/// sidecars and anything the engine does not lay out are left alone, and
/// an orphan that gets a row meanwhile is forgotten, not deleted.
#[tokio::test]
async fn orphans_are_deleted_only_after_a_ttl_without_a_row() {
    let Some(f) = fixture().await else { return };
    let registered = three_closed_l0(&f, 4).await;
    // A compaction that uploads and does not publish: an L1 and its sidecar.
    let c = compactor(
        &f,
        CompactionConfig {
            inject_swap_failures: 1,
            ..config(4)
        },
    );
    let me = fence(&f).await;
    assert!(c.compact_once(&me).await.is_err());
    let orphan_l1 = l1_objects(&f).await.remove(0);
    // A stray span file nobody registered, and objects that are not ours.
    let stray = format!(
        "t/{}/traces/l0/dt=2026-10-24/hr=14/{}.parquet",
        f.tenant.0,
        uuid::Uuid::new_v4()
    );
    let foreign = [
        "t/notes.txt".to_owned(),
        "t/notes.parquet".to_owned(),
        format!(
            "t/{}/traces/l7/dt=2026-10-24/hr=14/{}.parquet",
            f.tenant.0,
            uuid::Uuid::new_v4()
        ),
        format!(
            "t/{}/logs/l0/dt=2026-10-24/hr=14/{}.parquet",
            f.tenant.0,
            uuid::Uuid::new_v4()
        ),
        format!(
            "t/{}/traces/index/other/dt=2026-10-24/hr=14/{}.tix",
            f.tenant.0,
            uuid::Uuid::new_v4()
        ),
    ];
    for path in std::iter::once(&stray).chain(&foreign) {
        f.store
            .put(&Path::from(path.as_str()), b"x".to_vec().into())
            .await
            .unwrap();
    }
    let mut sweeper = sweeper(&f, 32);
    let stats = sweeper.stats();

    let first = sweeper.sweep_once().await.unwrap();
    assert_eq!(first.prefixes, 3, "the tenant's l0, l1 and sidecar days");
    assert_eq!(
        first.listed, 9,
        "the objects of those partitions, foreign levels and signals not walked"
    );
    assert_eq!(
        first.seen, 3,
        "the L1, its sidecar and the stray; nothing foreign"
    );
    assert_eq!(first.deleted, 0, "first sight: nothing deleted");
    assert!(first.wrapped);
    for path in [&orphan_l1, &stray] {
        assert!(present(&f, path).await, "{path} still there");
    }
    let again = sweeper.sweep_once().await.unwrap();
    assert_eq!(
        (again.seen, again.deleted),
        (3, 0),
        "seen twice within the TTL: still nothing"
    );

    // The stray gets a row before its TTL; the rest wait it out.
    let row = fs_catalog::NewFile {
        id: fs_catalog::FileId::new(),
        tenant_id: f.tenant,
        signal: Signal::Traces,
        level: Level::L0,
        path: stray.clone(),
        size_bytes: 1,
        row_count: 0,
        range: registered[0].range,
        services: vec![],
        summary: None,
    };
    f.db.catalog.register_file(&row).await.unwrap();
    orphans_seen_two_hours_ago(&f).await;
    let second = sweeper.sweep_once().await.unwrap();
    assert_eq!(second.seen, 2, "the stray has a row now");
    assert_eq!(second.deleted, 2, "the L1 and its sidecar");
    assert_eq!(second.forgotten, 1, "the stray");
    assert!(!present(&f, &orphan_l1).await);
    assert!(!present(&f, &fs_index::sidecar_path(&orphan_l1).unwrap()).await);
    assert!(present(&f, &stray).await);
    for path in &foreign {
        assert!(present(&f, path).await, "{path} is not ours to delete");
    }
    for file in &registered {
        assert!(present(&f, &file.path).await);
        assert!(present(&f, &fs_index::sidecar_path(&file.path).unwrap()).await);
    }
    assert_eq!(visible(&f).await.len(), 4, "catalog untouched by the sweep");
    assert_eq!(
        f.db.catalog.orphans_due(Duration::ZERO, 10).await.unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(stats.sweeps.load(Ordering::Relaxed), 3);
    assert_eq!(stats.deleted.load(Ordering::Relaxed), 2);
    assert_eq!(stats.forgotten.load(Ordering::Relaxed), 1);
    f.db.finish().await;
}

/// A pass walks a bounded stretch and the next one continues from there;
/// a mark left by a pass that stopped is finished by the next.
#[tokio::test]
async fn a_sweep_walks_in_bounded_passes_and_finishes_what_it_marked() {
    let Some(f) = fixture().await else { return };
    three_closed_l0(&f, 4).await;
    let strays: Vec<String> = (0..4)
        .map(|_| {
            format!(
                "t/{}/traces/l0/dt=2026-10-24/hr=14/{}.parquet",
                f.tenant.0,
                uuid::Uuid::new_v4()
            )
        })
        .collect();
    for path in &strays {
        f.store
            .put(&Path::from(path.as_str()), b"x".to_vec().into())
            .await
            .unwrap();
    }
    // Two partitions, the L0 day and the sidecar day, one per pass: two
    // passes to wrap.
    let mut sweeper = sweeper(&f, 1);
    let mut passes = 0;
    let mut seen = 0;
    loop {
        let swept = sweeper.sweep_once().await.unwrap();
        passes += 1;
        seen += swept.seen;
        assert_eq!(swept.prefixes, 1);
        if swept.wrapped {
            break;
        }
    }
    assert_eq!(passes, 2);
    assert_eq!(seen, 4, "every stray seen exactly once across the walk");

    // A pass that marked but died before deleting: the next pass deletes.
    orphans_seen_two_hours_ago(&f).await;
    for path in &strays[..2] {
        assert!(
            f.db.catalog
                .orphan_doom(path, std::slice::from_ref(path))
                .await
                .unwrap()
        );
    }
    let swept = sweeper.sweep_once().await.unwrap();
    assert_eq!(swept.deleted, 4, "two finished, two reaped");
    for path in &strays {
        assert!(!present(&f, path).await);
    }
    f.db.finish().await;
}

/// L1 objects under the bucket, by path relative to it.
async fn l1_objects(f: &Fixture) -> Vec<String> {
    use futures_util::TryStreamExt;
    let mut out: Vec<String> = f
        .store
        .list(None)
        .map_ok(|m| m.location.to_string())
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .filter(|p| p.contains("/l1/"))
        .collect();
    out.sort();
    out
}
