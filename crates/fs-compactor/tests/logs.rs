//! Compaction of log files and their `log_attribute_keys` table, beside the
//! spans and their `attribute_keys`, against a real Postgres and a local
//! bucket.
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
use chrono::Utc;
use fs_catalog::{
    COMPACTOR_LEASE, Catalog, CatalogError, FileRecord, LeaseFence, Level, NewTenant, Signal,
    TenantId,
};
use fs_compactor::{
    CompactError, CompactionConfig, Compactor, CompactorStats, Rebuild, Retention, RetentionConfig,
    RetentionStats,
};
use fs_ingest::buffer::Stream;
use fs_ingest::flush;
use fs_ingest::record::FileLayout;
use fs_query::{Querier, QueryLimits};
use fs_schema::attribute_keys::{KeySource, KeyStats, KeyTable};
use fs_schema::limits::Limits;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
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
        let schema = format!("kl_{}", uuid::Uuid::new_v4().simple());
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
    fixture_with(NewTenant {
        retention_days: 1,
        ..NewTenant::with_defaults("acme")
    })
    .await
}

async fn fixture_with(tenant: NewTenant) -> Option<Fixture> {
    let db = TestDb::new().await?;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let tenant = db.catalog.create_tenant(&tenant).await.unwrap().id;
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
    /// Nanoseconds of `hours` after the cut, plus `minutes`; negative
    /// hours are beyond retention.
    fn at(&self, hours: i64, minutes: i64) -> u64 {
        let at = self.cut + chrono::Duration::hours(hours) + chrono::Duration::minutes(minutes);
        u64::try_from(at.timestamp_nanos_opt().unwrap()).unwrap()
    }
}

fn kv(key: &str, value: Value) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue { value: Some(value) }),
        ..KeyValue::default()
    }
}

fn text(value: &str) -> Value {
    Value::StringValue(value.into())
}

fn resource(service: &str) -> Resource {
    Resource {
        attributes: vec![kv("service.name", text(service))],
        ..Resource::default()
    }
}

/// `records` log records of `service` from `start`, a millisecond apart,
/// each carrying `attributes`.
fn logs(
    service: &str,
    start: u64,
    records: u8,
    attributes: &[(&str, Value)],
) -> ExportLogsServiceRequest {
    let log_records = (0..records)
        .map(|i| LogRecord {
            time_unix_nano: start + u64::from(i) * 1_000_000,
            severity_number: 9,
            body: Some(AnyValue {
                value: Some(text(&format!("{service} {i}"))),
            }),
            trace_id: vec![i + 1; 16],
            attributes: attributes
                .iter()
                .map(|(key, value)| kv(key, value.clone()))
                .collect(),
            ..LogRecord::default()
        })
        .collect();
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource(service)),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn spans(
    service: &str,
    trace: u8,
    start: u64,
    count: u8,
    attribute: &str,
) -> ExportTraceServiceRequest {
    let spans = (0..count)
        .map(|i| Span {
            trace_id: vec![trace; 16],
            span_id: vec![i + 1; 8],
            name: format!("op-{i}"),
            kind: 2,
            start_time_unix_nano: start + u64::from(i) * 1_000_000,
            end_time_unix_nano: start + u64::from(i) * 1_000_000 + 250_000,
            attributes: vec![kv(attribute, text("x"))],
            ..Span::default()
        })
        .collect();
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource(service)),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Registers `file` an hour ago, confirmed: cold, as files flushed long
/// ago would be.
async fn backdate(f: &Fixture, file: &FileRecord) {
    sqlx::query(
        "update engine_file set created_at = now() - interval '1 hour',
                                confirmed_at = now() - interval '1 hour'
         where id = $1",
    )
    .bind(file.id)
    .execute(f.db.catalog.pool())
    .await
    .unwrap();
}

/// Writes and registers one cold log L0 file of `request`.
async fn write_log_l0(f: &Fixture, request: &ExportLogsServiceRequest) -> FileRecord {
    let mut builder = fs_schema::logs::LogBatchBuilder::new(Limits::default());
    builder
        .append_request(request, uuid::Uuid::new_v4().as_bytes())
        .unwrap();
    let batch = builder.finish().unwrap();
    let prepared = flush::prepare_stream(
        Stream::logs(f.tenant),
        &Path::from(""),
        fs_catalog::FileId::new(),
        &[batch],
        FileLayout::current(4),
    )
    .unwrap();
    flush::upload(f.store.as_ref(), &prepared).await.unwrap();
    let file = f.db.catalog.register_file(&prepared.file).await.unwrap();
    backdate(f, &file).await;
    file
}

/// Writes and registers one cold span L0 file of `request`.
async fn write_span_l0(f: &Fixture, request: &ExportTraceServiceRequest) -> FileRecord {
    let mut builder = fs_schema::spans::SpanBatchBuilder::new(Limits::default());
    builder.append_request(request).unwrap();
    let batch = builder.finish().unwrap();
    let prepared = flush::prepare(
        f.tenant,
        &Path::from(""),
        fs_catalog::FileId::new(),
        &[batch],
        FileLayout::current(4),
    )
    .unwrap();
    flush::upload(f.store.as_ref(), &prepared).await.unwrap();
    flush::upload_index(f.store.as_ref(), &prepared)
        .await
        .unwrap();
    let file = f.db.catalog.register_file(&prepared.file).await.unwrap();
    backdate(f, &file).await;
    file
}

/// Three log L0 files in the hour after the cut, services and times
/// interleaved, nine rows: `log.file`, a number and a bool on every row.
async fn three_log_l0(f: &Fixture) -> Vec<FileRecord> {
    let attributes = [
        ("log.file", text("app.log")),
        ("line", Value::IntValue(7)),
        ("handled", Value::BoolValue(true)),
    ];
    let mut files = Vec::new();
    for (service, minute, records) in [("web", 5, 4), ("api", 1, 3), ("web", 2, 2)] {
        let request = logs(service, f.at(1, minute), records, &attributes);
        files.push(write_log_l0(f, &request).await);
    }
    files
}

fn config(compact_logs: bool) -> CompactionConfig {
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
        compact_logs,
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

fn retention(f: &Fixture) -> Retention {
    Retention::new(
        f.db.catalog.clone(),
        f.store.clone(),
        Path::from(""),
        RetentionConfig {
            files_per_pass: 16,
            rebuild_max_input_bytes: 1 << 30,
            rebuild_max_bytes: 1 << 30,
            batch_rows: 4,
            limits: Limits::default(),
            rebuilds_per_tick: 64,
            rebuild_backoff: Duration::from_secs(600),
            inject_swap_failures: 0,
            inject_list_failures: 0,
        },
        Arc::new(RetentionStats::default()),
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

async fn visible(f: &Fixture, signal: Signal) -> Vec<FileRecord> {
    f.db.catalog
        .all_visible_files(f.tenant, signal)
        .await
        .unwrap()
}

fn keys_of(f: &Fixture, file: &FileRecord, table: KeyTable) -> KeyStats {
    let bytes = std::fs::read(f.dir.path().join(&file.path)).unwrap();
    fs_schema::attribute_keys::decode(bytes.into(), table).unwrap()
}

/// The single table of `table` of the tenant, decoded, with its row.
async fn table_of(f: &Fixture, table: KeyTable) -> (FileRecord, KeyStats) {
    let signal = match table {
        KeyTable::Spans => Signal::AttributeKeys,
        KeyTable::Logs => Signal::LogAttributeKeys,
    };
    let files = visible(f, signal).await;
    assert_eq!(files.len(), 1, "{table:?}: {files:?}");
    let keys = keys_of(f, &files[0], table);
    (files[0].clone(), keys)
}

fn key_names(keys: &KeyStats) -> Vec<(KeySource, String)> {
    keys.iter()
        .map(|(source, key, _, _)| (source, key.to_owned()))
        .collect()
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

fn bloom_columns(path: &std::path::Path) -> Vec<String> {
    let file = std::fs::File::open(path).unwrap();
    let reader = parquet::file::reader::SerializedFileReader::new(file).unwrap();
    parquet::file::reader::FileReader::metadata(&reader)
        .row_group(0)
        .columns()
        .iter()
        .filter(|c| c.bloom_filter_offset().is_some())
        .map(|c| c.column_path().string())
        .collect()
}

/// Rows of a local Parquet file as `(service, timestamp)` in file order.
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

/// `sql`'s single count, by a querier that has seen nothing before.
async fn counted(f: &Fixture, sql: &str) -> i64 {
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

fn rows(files: &[FileRecord]) -> i64 {
    files.iter().map(|x| x.row_count).sum()
}

/// Log L0 files of an hour become one log L1 file, sorted, with blooms on
/// `trace_id` and `service_name` only and no sidecar, and the tenant's
/// `log_attribute_keys` lists their keys as log keys; the spans of the
/// same hour are compacted first and their `attribute_keys` is left as
/// the span pass wrote it.
#[tokio::test]
async fn log_l0_files_become_a_log_l1_file_with_their_own_keys() {
    let Some(f) = fixture().await else { return };
    let inputs = three_log_l0(&f).await;
    for trace in 1..=2 {
        write_span_l0(&f, &spans("api", trace, f.at(1, 3), 3, "http.route")).await;
    }
    assert_eq!(counted(&f, "select count(*) from logs").await, 9);
    let c = compactor(&f, config(true));
    let me = fence(&f).await;

    c.compact_once(&me).await.unwrap().unwrap();
    assert!(
        visible(&f, Signal::Traces)
            .await
            .iter()
            .all(|x| x.level == Level::L1),
        "spans first within the hour"
    );
    assert_eq!(visible(&f, Signal::Logs).await.len(), 3);
    let (span_table, span_keys) = table_of(&f, KeyTable::Spans).await;
    assert!(
        span_keys
            .iter()
            .all(|(source, ..)| source != KeySource::Log)
    );

    c.compact_once(&me).await.unwrap().unwrap();
    assert!(c.compact_once(&me).await.unwrap().is_none());
    let l1 = visible(&f, Signal::Logs).await;
    assert_eq!(l1.len(), 1);
    assert_eq!(l1[0].level, Level::L1);
    assert!(l1[0].path.contains("/logs/l1/"), "{}", l1[0].path);
    assert_eq!(rows(&l1), rows(&inputs));
    assert_eq!(l1[0].services, ["api", "web"]);
    let path = f.dir.path().join(&l1[0].path);
    let sorted = rows_of(&path);
    let mut expected = sorted.clone();
    expected.sort();
    assert_eq!(sorted, expected);
    assert_eq!(sorted.len(), 9);
    assert_eq!(bloom_columns(&path), ["trace_id", "service_name"]);
    assert_eq!(objects_under(&f, "/logs/index/").await.len(), 0);
    assert!(
        objects_under(&f, ".tix")
            .await
            .iter()
            .all(|p| p.contains("/traces/index/")),
        "no sidecar for log files"
    );
    for input in &inputs {
        assert!(
            !visible(&f, Signal::Logs)
                .await
                .iter()
                .any(|x| x.id == input.id)
        );
    }

    let (_, log_keys) = table_of(&f, KeyTable::Logs).await;
    assert_eq!(
        key_names(&log_keys),
        [
            (KeySource::Resource, "service.name".to_owned()),
            (KeySource::Log, "log.file".to_owned()),
            (KeySource::Log, "line".to_owned()),
            (KeySource::Log, "handled".to_owned()),
        ]
    );
    assert!(log_keys.iter().all(|(.., seen)| seen.count == 9));
    let (span_after, keys_after) = table_of(&f, KeyTable::Spans).await;
    assert_eq!(span_after.id, span_table.id, "the log pass left it alone");
    assert_eq!(keys_after, span_keys);

    assert_eq!(counted(&f, "select count(*) from logs").await, 9);
    assert_eq!(counted(&f, "select count(*) from spans").await, 6);
    assert_eq!(
        counted(
            &f,
            "select count(*) from log_attribute_keys where source = 'log'"
        )
        .await,
        3
    );
    assert_eq!(
        counted(
            &f,
            "select count(*) from attribute_keys where source = 'log'"
        )
        .await,
        0
    );
    f.db.finish().await;
}

/// With log compaction off no pass takes an hour of log files; spans are
/// compacted as before.
#[tokio::test]
async fn with_log_compaction_off_log_files_stay_as_they_are() {
    let Some(f) = fixture().await else { return };
    let inputs = three_log_l0(&f).await;
    for trace in 1..=2 {
        write_span_l0(&f, &spans("api", trace, f.at(1, 3), 3, "http.route")).await;
    }
    let c = compactor(&f, config(false));
    let me = fence(&f).await;
    c.compact_once(&me).await.unwrap().unwrap();
    assert!(c.compact_once(&me).await.unwrap().is_none());
    let ids = |files: &[FileRecord]| {
        let mut ids = files.iter().map(|x| x.id).collect::<Vec<_>>();
        ids.sort();
        ids
    };
    assert_eq!(ids(&visible(&f, Signal::Logs).await), ids(&inputs));
    assert_eq!(visible(&f, Signal::LogAttributeKeys).await.len(), 0);
    assert_eq!(objects_under(&f, "/logs/l1/").await.len(), 0);
    assert!(
        visible(&f, Signal::Traces)
            .await
            .iter()
            .all(|x| x.level == Level::L1)
    );
    f.db.finish().await;
}

/// A log pass cut short before its swap, after its L1 file or between the
/// L1 file and the table, publishes nothing and leaves the inputs
/// visible; what it uploaded is swept as orphans. The next engine
/// compacts the hour once, and after its swap a restart finds nothing to
/// do.
#[tokio::test]
async fn a_log_pass_cut_short_publishes_nothing_and_the_next_one_compacts_once() {
    let Some(f) = fixture().await else { return };
    let inputs = three_log_l0(&f).await;
    let ids = |files: &[FileRecord]| {
        let mut ids = files.iter().map(|x| x.id).collect::<Vec<_>>();
        ids.sort();
        ids
    };
    let me = fence(&f).await;

    let failing = [
        (
            CompactionConfig {
                inject_swap_failures: 1,
                ..config(true)
            },
            CompactError::Injected("swap"),
            (1, 1),
        ),
        (
            CompactionConfig {
                inject_put_failure_at: Some(1),
                ..config(true)
            },
            CompactError::Injected("object put"),
            (2, 1),
        ),
    ];
    for (config, expected, (l1_objects, table_objects)) in failing {
        let c = compactor(&f, config);
        let err = c.compact_once(&me).await.unwrap_err();
        assert_eq!(err.to_string(), expected.to_string());
        assert_eq!(ids(&visible(&f, Signal::Logs).await), ids(&inputs));
        assert_eq!(visible(&f, Signal::LogAttributeKeys).await.len(), 0);
        assert_eq!(objects_under(&f, "/logs/l1/").await.len(), l1_objects);
        assert_eq!(
            objects_under(&f, "/derived/log_attribute_keys/")
                .await
                .len(),
            table_objects
        );
    }

    let old = LeaseFence {
        generation: me.generation - 1,
        ..me.clone()
    };
    let err = compactor(&f, config(true))
        .compact_once(&old)
        .await
        .unwrap_err();
    assert!(
        matches!(err, CompactError::Catalog(CatalogError::LeaseLost { .. })),
        "{err}"
    );
    assert_eq!(ids(&visible(&f, Signal::Logs).await), ids(&inputs));
    assert_eq!(visible(&f, Signal::LogAttributeKeys).await.len(), 0);

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
    assert_eq!(swept.deleted, 5, "three log L1 files and two tables");
    assert_eq!(objects_under(&f, "/logs/l1/").await.len(), 0);
    assert_eq!(
        objects_under(&f, "/derived/log_attribute_keys/")
            .await
            .len(),
        0
    );

    let next = compactor(&f, config(true));
    next.compact_once(&me).await.unwrap().unwrap();
    let l1 = visible(&f, Signal::Logs).await;
    assert_eq!(l1.len(), 1);
    assert_eq!(rows(&l1), rows(&inputs));
    let (table, keys) = table_of(&f, KeyTable::Logs).await;
    assert!(
        keys.iter().all(|(.., seen)| seen.count == 9),
        "counted once"
    );

    let restarted = compactor(&f, config(true));
    assert!(restarted.compact_once(&me).await.unwrap().is_none());
    assert_eq!(ids(&visible(&f, Signal::Logs).await), ids(&l1));
    assert_eq!(table_of(&f, KeyTable::Logs).await.0.id, table.id);
    assert_eq!(counted(&f, "select count(*) from logs").await, 9);
    f.db.finish().await;
}

/// `max_keys_hour` bounds each table on its own: a burst of span keys
/// does not spend the budget of log keys of the same hour.
#[tokio::test]
async fn each_signal_admits_new_keys_on_its_own_budget() {
    let Some(f) = fixture_with(NewTenant {
        retention_days: 1,
        max_keys_hour: 2,
        ..NewTenant::with_defaults("acme")
    })
    .await
    else {
        return;
    };
    for (trace, attribute) in [(1, "span.a"), (2, "span.b")] {
        write_span_l0(&f, &spans("api", trace, f.at(1, 3), 3, attribute)).await;
    }
    for attribute in ["log.a", "log.b"] {
        write_log_l0(&f, &logs("api", f.at(1, 4), 3, &[(attribute, text("x"))])).await;
    }
    let c = compactor(&f, config(true));
    let me = fence(&f).await;
    c.compact_once(&me).await.unwrap().unwrap();
    c.compact_once(&me).await.unwrap().unwrap();
    assert_eq!(table_of(&f, KeyTable::Spans).await.1.len(), 2);
    assert_eq!(table_of(&f, KeyTable::Logs).await.1.len(), 2);
    assert_eq!(
        c.stats().derived_keys_dropped.load(Ordering::Relaxed),
        6,
        "the three rows of the third key of each signal"
    );
    f.db.finish().await;
}

/// An hour whose oldest files exceed the input cap in one signal is
/// passed over; the hour of the other signal is compacted all the same.
#[tokio::test]
async fn an_hour_over_the_cap_in_one_signal_does_not_hold_back_the_other() {
    let Some(f) = fixture().await else { return };
    let mut big = Vec::new();
    for trace in 1..=2 {
        big.push(write_span_l0(&f, &spans("api", trace, f.at(1, 3), 200, "http.route")).await);
    }
    let mut small = Vec::new();
    for minute in [4, 5] {
        small.push(write_log_l0(&f, &logs("api", f.at(2, minute), 2, &[])).await);
    }
    let cap = u64::try_from(rows_bytes(&small)).unwrap();
    assert!(rows_bytes(&big) > rows_bytes(&small));
    let c = compactor(
        &f,
        CompactionConfig {
            max_input_bytes: cap,
            ..config(true)
        },
    );
    let me = fence(&f).await;
    c.compact_once(&me).await.unwrap().unwrap();
    assert!(c.compact_once(&me).await.unwrap().is_none());
    assert_eq!(visible(&f, Signal::Logs).await[0].level, Level::L1);
    assert!(
        visible(&f, Signal::Traces)
            .await
            .iter()
            .all(|x| x.level == Level::L0)
    );
    assert_eq!(c.stats().skipped.load(Ordering::Relaxed), 1);
    f.db.finish().await;
}

fn rows_bytes(files: &[FileRecord]) -> i64 {
    files.iter().map(|x| x.size_bytes).sum()
}

/// The hour beyond retention carries `old.key`, the hour kept `new.key`,
/// in spans and in logs, two files each.
async fn two_days_of_both(f: &Fixture) {
    for (hour, key) in [(-2, "old.key"), (1, "new.key")] {
        for minute in [1, 2] {
            write_log_l0(f, &logs("api", f.at(hour, minute), 2, &[(key, text("x"))])).await;
            let trace = u8::try_from(minute).unwrap();
            write_span_l0(f, &spans("api", trace, f.at(hour, minute), 2, key)).await;
        }
    }
    let c = compactor(f, config(true));
    let me = fence(f).await;
    while c.compact_once(&me).await.unwrap().is_some() {}
    let r = retention(f);
    while r.expire_once(&me).await.unwrap().is_some() {}
}

/// Once its expired files are gone the `log_attribute_keys` table is
/// rebuilt from the log L1 files alone, with log compaction off: the
/// knob stops new passes, not the upkeep of what they wrote. The table
/// replaced is purged like any other file.
#[tokio::test]
async fn a_stale_log_table_is_rebuilt_from_log_files_and_purged_with_compaction_off() {
    let Some(f) = fixture().await else { return };
    two_days_of_both(&f).await;
    for minute in [1, 2] {
        write_log_l0(
            &f,
            &logs("api", f.at(2, minute), 2, &[("later.key", text("x"))]),
        )
        .await;
    }
    let off = compactor(&f, config(false));
    let me = fence(&f).await;
    assert!(off.compact_once(&me).await.unwrap().is_none());

    let (span_before, _) = table_of(&f, KeyTable::Spans).await;
    let (log_before, log_keys) = table_of(&f, KeyTable::Logs).await;
    assert!(key_names(&log_keys).contains(&(KeySource::Log, "old.key".to_owned())));
    let r = retention(&f);
    let mut outcomes = Vec::new();
    loop {
        match r.rebuild_once(&me).await.unwrap() {
            Rebuild::End => break,
            Rebuild::Done(_) => outcomes.push("done"),
            Rebuild::Skipped => outcomes.push("skipped"),
        }
    }
    assert_eq!(outcomes, ["done", "done"], "one table of each kind");
    let (_, spans_rebuilt) = table_of(&f, KeyTable::Spans).await;
    let (log_after, logs_rebuilt) = table_of(&f, KeyTable::Logs).await;
    assert_ne!(log_after.id, log_before.id);
    assert_eq!(
        key_names(&logs_rebuilt),
        [
            (KeySource::Resource, "service.name".to_owned()),
            (KeySource::Log, "new.key".to_owned()),
        ],
        "from the log L1 files kept, not the spans nor the L0 files"
    );
    assert_eq!(
        key_names(&spans_rebuilt),
        [
            (KeySource::Resource, "service.name".to_owned()),
            (KeySource::Span, "new.key".to_owned()),
        ]
    );
    assert_ne!(table_of(&f, KeyTable::Spans).await.0.id, span_before.id);

    assert!(present(&f, &log_before.path).await);
    let purged = fs_compactor::purge_once(
        &f.db.catalog,
        f.store.as_ref(),
        Duration::ZERO,
        100,
        Duration::from_secs(600),
    )
    .await;
    assert!(purged >= 2, "{purged}");
    assert!(!present(&f, &log_before.path).await);
    assert!(!present(&f, &span_before.path).await);
    f.db.finish().await;
}

/// A damaged `attribute_keys` table holds back its own rebuild only: the
/// `log_attribute_keys` table of the same tenant is rebuilt on the next
/// call, each call examining one table whatever its kind.
#[tokio::test]
async fn a_broken_span_table_does_not_hold_back_the_log_rebuild() {
    let Some(f) = fixture().await else { return };
    two_days_of_both(&f).await;
    let (span_table, _) = table_of(&f, KeyTable::Spans).await;
    let (log_table, _) = table_of(&f, KeyTable::Logs).await;
    std::fs::write(f.dir.path().join(&span_table.path), b"not parquet").unwrap();
    let r = retention(&f);
    let me = fence(&f).await;

    let err = r.rebuild_once(&me).await.unwrap_err();
    assert!(matches!(err, CompactError::Schema(_)), "{err}");
    assert!(matches!(
        r.rebuild_once(&me).await.unwrap(),
        Rebuild::Done(_)
    ));
    assert!(matches!(r.rebuild_once(&me).await.unwrap(), Rebuild::End));
    assert_ne!(table_of(&f, KeyTable::Logs).await.0.id, log_table.id);
    assert_eq!(
        visible(&f, Signal::AttributeKeys).await[0].id,
        span_table.id
    );

    assert!(
        matches!(r.rebuild_once(&me).await.unwrap(), Rebuild::Skipped),
        "the span table is held back for its backoff"
    );
    assert!(matches!(r.rebuild_once(&me).await.unwrap(), Rebuild::End));
    assert_eq!(r.stats().rebuild_failures.load(Ordering::Relaxed), 1);
    assert_eq!(r.stats().rebuilds.load(Ordering::Relaxed), 1);
    f.db.finish().await;
}
