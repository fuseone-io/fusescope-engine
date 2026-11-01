//! `usage_1h`: rows and logical bytes counted exactly once, by the pass
//! that takes a file off L0 or by the accounting pass, against a real
//! Postgres and a local bucket.
#![allow(
    clippy::too_many_lines,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, TimeUnit, TimestampNanosecondType, UInt8Type, UInt64Type};
use arrow::record_batch::RecordBatch;
use chrono::{TimeZone, Utc};
use fs_catalog::{
    COMPACTOR_LEASE, Catalog, FileRecord, LeaseFence, Level, NewTenant, Signal, TenantId,
};
use fs_compactor::{
    CompactError, CompactionConfig, Compactor, CompactorStats, Retention, RetentionConfig,
    RetentionStats,
};
use fs_ingest::flush;
use fs_ingest::record::FileLayout;
use fs_query::{Querier, QueryLimits};
use fs_schema::limits::Limits;
use fs_schema::spans::SpanBatchBuilder;
use fs_schema::usage::{UsageSignal, row_bytes};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
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
        let schema = format!("ku_{}", uuid::Uuid::new_v4().simple());
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

/// 2026-10-24T14:00Z, long closed.
fn closed_hour() -> u64 {
    u64::try_from(
        Utc.with_ymd_and_hms(2026, 10, 24, 14, 0, 0)
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap(),
    )
    .unwrap()
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

fn request(service: &str, start: u64, spans: u8) -> ExportTraceServiceRequest {
    let spans = (0..spans)
        .map(|i| Span {
            trace_id: vec![i + 1; 16],
            span_id: vec![i + 1; 8],
            name: format!("op-{service}-{i}"),
            kind: 2,
            start_time_unix_nano: start + u64::from(i) * 1_000_000,
            end_time_unix_nano: start + u64::from(i) * 1_000_000 + 1_000,
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

/// Writes, registers and backdates one cold span L0 file.
async fn write_l0(f: &Fixture, service: &str, start: u64, spans: u8) -> FileRecord {
    let mut builder = SpanBatchBuilder::new(Limits::default());
    builder
        .append_request(&request(service, start, spans))
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
        compact_l2: false,
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

/// `(signal, service, hour, rows, bytes)` of `usage_1h`, by service.
async fn usage(f: &Fixture) -> Vec<(u8, Option<String>, i64, u64, u64)> {
    let querier = Querier::new(
        f.db.catalog.clone(),
        f.store.clone(),
        None,
        QueryLimits::default(),
    )
    .unwrap();
    let batches: Vec<RecordBatch> = querier
        .collect(
            f.tenant,
            "select signal, service_name, hour, rows, bytes from usage_1h order by service_name",
        )
        .await
        .unwrap();
    let mut out = Vec::new();
    for b in &batches {
        let service = arrow::compute::cast(b.column(1), &DataType::Utf8).unwrap();
        let service = service.as_string::<i32>();
        for i in 0..b.num_rows() {
            out.push((
                b.column(0).as_primitive::<UInt8Type>().value(i),
                service.is_valid(i).then(|| service.value(i).to_owned()),
                b.column(2)
                    .as_primitive::<TimestampNanosecondType>()
                    .value(i),
                b.column(3).as_primitive::<UInt64Type>().value(i),
                b.column(4).as_primitive::<UInt64Type>().value(i),
            ));
        }
    }
    out
}

/// What the rows of `files` count, read from the objects: `(service,
/// rows, bytes)` by service.
fn expected(f: &Fixture, files: &[FileRecord]) -> Vec<(String, u64, u64)> {
    let mut by_service = std::collections::BTreeMap::<String, (u64, u64)>::new();
    for file in files {
        let reader = ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(f.dir.path().join(&file.path)).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            let bytes = row_bytes(&batch, UsageSignal::Spans).unwrap();
            let services = arrow::compute::cast(
                batch.column_by_name("service_name").unwrap(),
                &DataType::Utf8,
            )
            .unwrap();
            let services = services.as_string::<i32>();
            for (i, size) in bytes.into_iter().enumerate() {
                let entry = by_service.entry(services.value(i).to_owned()).or_default();
                entry.0 += 1;
                entry.1 += size;
            }
        }
    }
    by_service
        .into_iter()
        .map(|(service, (rows, bytes))| (service, rows, bytes))
        .collect()
}

fn totals(rows: &[(u8, Option<String>, i64, u64, u64)]) -> Vec<(String, u64, u64)> {
    rows.iter()
        .map(|(_, service, _, r, b)| (service.clone().unwrap_or_default(), *r, *b))
        .collect()
}

async fn account_all(c: &Compactor, me: &LeaseFence) -> usize {
    let mut passes = 0;
    while c.account_once(me).await.unwrap().is_some() {
        passes += 1;
    }
    passes
}

/// The L0 to L1 pass counts its inputs once into one contribution of the
/// hour, publishes it in its swap and registers its output as counted; a
/// second pass and the accounting pass count nothing more.
#[tokio::test]
async fn a_pass_counts_the_usage_of_its_inputs_once() {
    let Some(f) = fixture(15).await else { return };
    let h = closed_hour();
    let inputs = vec![
        write_l0(&f, "web", h + 60_000_000_000, 3).await,
        write_l0(&f, "api", h + 120_000_000_000, 2).await,
        write_l0(&f, "web", h + 180_000_000_000, 4).await,
    ];
    assert!(inputs.iter().all(|x| !x.usage_accounted));
    let want = expected(&f, &inputs);
    let c = compactor(&f, config());
    let me = fence(&f).await;
    c.compact_once(&me).await.unwrap().unwrap();

    let got = usage(&f).await;
    assert_eq!(totals(&got), want);
    assert!(
        got.iter()
            .all(|(signal, _, hour, ..)| *signal == 1 && *hour == i64::try_from(h).unwrap())
    );
    assert_eq!(got.iter().map(|r| r.3).sum::<u64>(), 9);
    let visible =
        f.db.catalog
            .all_visible_files(f.tenant, Signal::Traces)
            .await
            .unwrap();
    assert!(
        visible
            .iter()
            .all(|x| x.level == Level::L1 && x.usage_accounted)
    );
    assert_eq!(
        f.db.catalog
            .all_visible_files(f.tenant, Signal::Usage1h)
            .await
            .unwrap()
            .len(),
        1,
        "one contribution for the hour"
    );
    assert!(c.compact_once(&me).await.unwrap().is_none());
    assert_eq!(account_all(&c, &me).await, 0, "nothing left to count");
    assert_eq!(totals(&usage(&f).await), want, "counted once");
    f.db.finish().await;
}

/// A pass cut short before its swap publishes no count and leaves its
/// inputs uncounted; the next pass counts them once.
#[tokio::test]
async fn a_pass_cut_short_counts_nothing_and_the_next_counts_once() {
    let Some(f) = fixture(15).await else { return };
    let h = closed_hour();
    let inputs = vec![
        write_l0(&f, "web", h, 3).await,
        write_l0(&f, "api", h + 1_000_000_000, 2).await,
    ];
    let want = expected(&f, &inputs);
    let me = fence(&f).await;
    let failing = compactor(
        &f,
        CompactionConfig {
            inject_swap_failures: 1,
            ..config()
        },
    );
    let err = failing.compact_once(&me).await.unwrap_err();
    assert_eq!(err.to_string(), CompactError::Injected("swap").to_string());
    assert_eq!(usage(&f).await.len(), 0);
    let c = compactor(&f, config());
    c.compact_once(&me).await.unwrap().unwrap();
    assert_eq!(totals(&usage(&f).await), want);
    f.db.finish().await;
}

/// L1 files written before usage was counted read uncounted: the
/// accounting pass reads them in place, publishes their count and marks
/// them, once; retention keeps them past the cut until then.
#[tokio::test]
async fn legacy_files_are_counted_in_place_before_retention_expires_them() {
    let Some(f) = fixture(1).await else { return };
    let h = closed_hour();
    let inputs = vec![
        write_l0(&f, "web", h, 3).await,
        write_l0(&f, "api", h + 1_000_000_000, 2).await,
    ];
    let want = expected(&f, &inputs);
    let c = compactor(&f, config());
    let me = fence(&f).await;
    c.compact_once(&me).await.unwrap().unwrap();
    // As written by a version before usage: no mark, no contribution.
    sqlx::query(
        "update engine_file set usage_accounted = false, confirmed_at = now() - interval '1 hour'
         where signal = 1",
    )
    .execute(f.db.catalog.pool())
    .await
    .unwrap();
    sqlx::query("delete from engine_file where signal = $1")
        .bind(Signal::Usage1h.code())
        .execute(f.db.catalog.pool())
        .await
        .unwrap();
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
    let legacy =
        f.db.catalog
            .all_visible_files(f.tenant, Signal::Traces)
            .await
            .unwrap();
    assert_eq!(legacy.len(), 1, "an uncounted L1 file stays past the cut");

    let failing = compactor(
        &f,
        CompactionConfig {
            inject_put_failure_at: Some(0),
            ..config()
        },
    );
    assert!(failing.account_once(&me).await.is_err());
    assert!(
        !f.db
            .catalog
            .all_visible_files(f.tenant, Signal::Traces)
            .await
            .unwrap()[0]
            .usage_accounted,
        "a pass that failed before its transaction marks nothing"
    );
    assert_eq!(account_all(&c, &me).await, 1);
    let counted = usage(&f).await;
    assert_eq!(totals(&counted), want);
    assert_eq!(account_all(&c, &me).await, 0, "counted once");
    assert_eq!(
        c.stats()
            .accounted
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );

    while retention.expire_once(&me).await.unwrap().is_some() {}
    assert_eq!(
        f.db.catalog
            .all_visible_files(f.tenant, Signal::Traces)
            .await
            .unwrap()
            .len(),
        0,
        "expired once counted"
    );
    f.db.finish().await;
}

/// Two passes over one hour publish two contributions; `usage_1h` sums
/// them, one row per signal, service and hour, with the declared types.
#[tokio::test]
async fn the_table_sums_every_contribution_of_a_key() {
    let Some(f) = fixture(15).await else { return };
    let h = closed_hour();
    let inputs = vec![
        write_l0(&f, "web", h, 3).await,
        write_l0(&f, "web", h + 1_000_000_000, 2).await,
    ];
    let want = expected(&f, &inputs);
    let c = compactor(
        &f,
        CompactionConfig {
            max_inputs_per_pass: 1,
            ..config()
        },
    );
    let me = fence(&f).await;
    c.compact_once(&me).await.unwrap().unwrap();
    c.compact_once(&me).await.unwrap().unwrap();
    assert_eq!(
        f.db.catalog
            .all_visible_files(f.tenant, Signal::Usage1h)
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(totals(&usage(&f).await), want, "one row for web");

    let querier = Querier::new(
        f.db.catalog.clone(),
        f.store.clone(),
        None,
        QueryLimits::default(),
    )
    .unwrap();
    let batches = querier
        .collect(f.tenant, "select * from usage_1h")
        .await
        .unwrap();
    let schema = batches[0].schema();
    let fields: Vec<(String, arrow::datatypes::DataType, bool)> = schema
        .fields()
        .iter()
        .map(|x| (x.name().clone(), x.data_type().clone(), x.is_nullable()))
        .collect();
    assert_eq!(
        fields
            .iter()
            .map(|(name, kind, _)| (name.as_str(), kind.clone()))
            .collect::<Vec<_>>(),
        [
            ("signal", DataType::UInt8),
            ("service_name", DataType::Utf8),
            (
                "hour",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
            ),
            ("rows", DataType::UInt64),
            ("bytes", DataType::UInt64),
        ]
    );
    assert!(fields[1].2, "service_name is nullable");
    f.db.finish().await;
}

/// A contribution whose footer is valid but whose rows are not, a signal
/// other than spans or logs, fails the query instead of being summed.
#[tokio::test]
async fn a_contribution_with_rows_outside_the_domain_fails_the_query() {
    use arrow::array::{StringArray, TimestampNanosecondArray, UInt8Array, UInt64Array};
    let Some(f) = fixture(15).await else { return };
    let hour = i64::try_from(closed_hour()).unwrap();
    let batch = RecordBatch::try_new(
        fs_schema::usage::schema(),
        vec![
            Arc::new(UInt8Array::from(vec![3u8])),
            Arc::new(StringArray::from(vec![Some("api")])),
            Arc::new(TimestampNanosecondArray::from(vec![hour]).with_timezone("UTC")),
            Arc::new(UInt64Array::from(vec![1u64])),
            Arc::new(UInt64Array::from(vec![1u64])),
        ],
    )
    .unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(Vec::new(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    let bytes = writer.into_inner().unwrap();
    let id = fs_catalog::FileId::new();
    let path = format!(
        "t/{}/derived/usage_1h/dt=2026-10-24/hr=14/{}.parquet",
        f.tenant.0, id.0
    );
    std::fs::create_dir_all(f.dir.path().join(&path).parent().unwrap()).unwrap();
    std::fs::write(f.dir.path().join(&path), &bytes).unwrap();
    let start = Utc.timestamp_nanos(hour);
    f.db.catalog
        .register_file(&fs_catalog::NewFile {
            id,
            origin: id,
            tenant_id: f.tenant,
            signal: Signal::Usage1h,
            level: Level::L1,
            path,
            size_bytes: i64::try_from(bytes.len()).unwrap(),
            row_count: 1,
            range: fs_catalog::TimeRange {
                start,
                end: start + chrono::Duration::minutes(59),
            },
            services: vec!["api".into()],
            summary: None,
            content_sha256: None,
            usage_accounted: true,
        })
        .await
        .unwrap();
    let querier = Querier::new(
        f.db.catalog.clone(),
        f.store.clone(),
        None,
        QueryLimits::default(),
    )
    .unwrap();
    let err = querier
        .collect(f.tenant, "select * from usage_1h")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("usage_1h"), "{err}");
    f.db.finish().await;
}

/// Usage past its byte budget is set aside as contributions of its own,
/// published in the same swap: one hour of many services gives several
/// contributions of that hour, the table sums them to one row per service,
/// and the accounting pass makes progress the same way instead of failing.
#[tokio::test]
async fn counts_past_their_budget_become_several_contributions_and_the_pass_goes_on() {
    let Some(f) = fixture(15).await else { return };
    let h = closed_hour();
    let mut inputs = Vec::new();
    for i in 0..40u64 {
        inputs.push(write_l0(&f, &format!("service-{i:03}"), h + i * 1_000_000_000, 1).await);
    }
    let want = expected(&f, &inputs);
    let budget = 4 * fs_schema::usage::UsageCounts::entry_bytes("service-000");
    let c = compactor(
        &f,
        CompactionConfig {
            usage_max_bytes: budget,
            ..config()
        },
    );
    let me = fence(&f).await;
    while c.compact_once(&me).await.unwrap().is_some() {}
    let contributions =
        f.db.catalog
            .all_visible_files(f.tenant, Signal::Usage1h)
            .await
            .unwrap();
    assert!(
        contributions.len() >= 10,
        "{} contributions",
        contributions.len()
    );
    assert!(
        contributions
            .iter()
            .all(|x| x.range.start == contributions[0].range.start)
    );
    assert_eq!(
        totals(&usage(&f).await),
        want,
        "one row per service, counted once"
    );

    // The same files counted again in place, as legacy files.
    sqlx::query(
        "update engine_file set usage_accounted = false, confirmed_at = now() - interval '1 hour'
         where signal = 1",
    )
    .execute(f.db.catalog.pool())
    .await
    .unwrap();
    sqlx::query("delete from engine_file where signal = $1")
        .bind(Signal::Usage1h.code())
        .execute(f.db.catalog.pool())
        .await
        .unwrap();
    let mut counted = 0;
    while let Some(done) = c.account_once(&me).await.unwrap() {
        counted += done.contributions;
    }
    assert!(counted >= 10, "{counted} contributions");
    assert_eq!(totals(&usage(&f).await), want);
    f.db.finish().await;
}
