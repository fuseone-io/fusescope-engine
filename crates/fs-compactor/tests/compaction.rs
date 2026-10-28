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
use chrono::{TimeZone, Utc};
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
    files
}

async fn visible(f: &Fixture) -> Vec<FileRecord> {
    f.db.catalog
        .all_visible_files(f.tenant, Signal::Traces)
        .await
        .unwrap()
}

/// Rows of a local Parquet file as `(service, timestamp)` in file order.
fn rows_of(path: &std::path::Path) -> Vec<(String, i64)> {
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
            out.push((s.unwrap().to_owned(), t));
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
    let done = c.compact_once(None).await.unwrap().unwrap();
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
        c.compact_once(None).await.unwrap().is_none(),
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
    let done = c.compact_once(None).await.unwrap().unwrap();
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
    let done = c.compact_once(None).await.unwrap().unwrap();
    assert_eq!(done.inputs, [inputs[0].id, inputs[1].id], "the two oldest");
    let now = visible(&f).await;
    assert!(
        now.iter().any(|x| x.id == inputs[2].id),
        "the newest stays as L0"
    );
    assert!(
        c.compact_once(None).await.unwrap().is_none(),
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
    let err = c.compact_once(None).await.unwrap_err();
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

    let done = c.compact_once(None).await.unwrap().unwrap();
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
    let err = c.compact_once(Some(&stale)).await.unwrap_err();
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
    let fence = LeaseFence {
        holder: "other".into(),
        ..stale
    };
    assert!(c.compact_once(Some(&fence)).await.unwrap().is_some());
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
