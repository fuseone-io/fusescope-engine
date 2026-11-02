//! Metric files and their derived tables under the compactor: the orphan
//! sweep, compaction by file set and the accounting of sets, against a
//! real Postgres and a local bucket.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::too_many_lines,
    clippy::float_cmp
)]

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use arrow::array::AsArray;
use arrow::datatypes::{Float64Type, Int64Type, UInt64Type};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, TimeZone, Utc};
use fs_catalog::{
    COMPACTOR_LEASE, Catalog, CatalogError, FileId, FileRecord, FileSetId, LeaseFence, Level,
    MetricRole, NewFile, NewTenant, Signal, TenantId, TimeRange,
};
use fs_compactor::{CompactError, CompactionConfig, Compactor, CompactorStats};
use fs_ingest::metric_flush::{prepare_samples, prepare_series};
use fs_ingest::record::FileLayout;
use fs_schema::limits::Limits;
use fs_schema::metrics::{
    LabelMeta, MetricLimits, SeriesRow, Source, convert, convert_mapping, preflight, series_batch,
    series_rows,
};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
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

/// 2026-10-24T14:00:00Z, long closed.
fn hour() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 24, 14, 0, 0).unwrap()
}

fn ms(at: DateTime<Utc>) -> i64 {
    at.timestamp_millis()
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

/// A gauge `name` of `points`, `(host, offset ms into the hour, value)`.
fn gauge(name: &str, points: &[(&str, u64, f64)]) -> ExportMetricsServiceRequest {
    let base = u64::try_from(hour().timestamp_nanos_opt().unwrap()).unwrap();
    let data_points = points
        .iter()
        .map(|(host, offset, value)| NumberDataPoint {
            attributes: vec![kv("host", host)],
            time_unix_nano: base + offset * 1_000_000,
            value: Some(NumberValue::AsDouble(*value)),
            ..NumberDataPoint::default()
        })
        .collect();
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "api")],
                ..Resource::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: name.into(),
                    data: Some(Data::Gauge(Gauge { data_points })),
                    ..Metric::default()
                }],
                ..ScopeMetrics::default()
            }],
            schema_url: String::new(),
        }],
    }
}

fn source(instance: u8, offset: u64) -> Source {
    Source {
        instance: [instance; 16],
        segment: 1,
        offset,
    }
}

/// Converts `request` from `source` under `mapping` and writes it as one
/// L0 set of `f`'s tenant in layout `version`, registered and confirmed.
async fn l0_set(
    f: &Fixture,
    request: &ExportMetricsServiceRequest,
    at: (Source, u8, u32),
) -> FileSetId {
    l0_set_with(f, request, at, |_| {}).await
}

/// [`l0_set`] with the rows changed by `change` before they are written.
async fn l0_set_with(
    f: &Fixture,
    request: &ExportMetricsServiceRequest,
    (from, mapping, version): (Source, u8, u32),
    change: impl FnOnce(&mut fs_schema::metrics::MetricRows),
) -> FileSetId {
    let limits = Limits::default();
    let metrics = MetricLimits::default();
    let checked = preflight(request, &limits, &metrics).unwrap();
    let mut rows = convert_mapping(mapping, request, &limits, &metrics, &checked, from).unwrap();
    change(&mut rows);
    let layout = FileLayout {
        version,
        ..FileLayout::current(4)
    };
    let root = Path::from("");
    let samples = prepare_samples(
        f.tenant,
        &root,
        FileId::new(),
        hour(),
        &[rows.samples],
        layout,
    )
    .unwrap();
    let batch = series_batch(&rows.series, rows.series.len()).unwrap();
    let series = prepare_series(f.tenant, &root, FileId::new(), hour(), &batch, layout).unwrap();
    for prepared in [&samples, &series] {
        fs_ingest::flush::upload(f.store.as_ref(), prepared)
            .await
            .unwrap();
    }
    let set = FileSetId::new();
    f.db.catalog
        .register_file_set(set, &[samples.file.clone(), series.file.clone()])
        .await
        .unwrap();
    f.db.catalog.confirm_file_set(set).await.unwrap();
    set
}

fn config() -> CompactionConfig {
    CompactionConfig {
        hour_close_delay: Duration::ZERO,
        l1_target_bytes: 512 << 20,
        max_inputs_per_pass: 16,
        max_input_bytes: 2 << 30,
        trace_summary_max_traces: 1000,
        trace_summary_max_bytes: 1 << 20,
        service_graph_max_spans: 1000,
        service_graph_max_bytes: 1 << 20,
        row_group_rows: 4,
        usage_max_bytes: 64 << 20,
        compact_l2: true,
        l2_target_bytes: 1 << 30,
        l2_output_bytes: 512 << 20,
        max_l2_input_bytes: 16 << 30,
        l2_passes_per_tick: 1,
        l1_passes_per_tick: 64,
        max_trace_index_bytes: 1 << 20,
        max_services_per_file: 10_000,
        text_index: fs_index::TextIndexLimits::DEFAULT,
        compact_logs: false,
        compact_metrics: true,
        metric_pass_bytes: fs_compactor::MIN_METRIC_PASS_BYTES,
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

async fn visible(f: &Fixture, signal: Signal) -> Vec<FileRecord> {
    f.db.catalog
        .all_visible_files(f.tenant, signal)
        .await
        .unwrap()
}

async fn batches(f: &Fixture, path: &str) -> Vec<RecordBatch> {
    let bytes = f
        .store
        .get(&Path::from(path))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let schema = Arc::clone(reader.schema());
    reader
        .build()
        .unwrap()
        .map(|b| b.unwrap().with_schema(Arc::clone(&schema)).unwrap())
        .collect()
}

/// `(name, fingerprint, ms, value, source order)` of a samples file.
async fn samples_of(f: &Fixture, file: &FileRecord) -> Vec<(String, u64, i64, f64, Vec<u8>)> {
    let mut out = Vec::new();
    for batch in batches(f, &file.path).await {
        let names =
            arrow::compute::cast(batch.column(0), &arrow::datatypes::DataType::Utf8).unwrap();
        let names = names.as_string::<i32>();
        for row in 0..batch.num_rows() {
            out.push((
                names.value(row).to_owned(),
                batch.column(2).as_primitive::<UInt64Type>().value(row),
                batch.column(3).as_primitive::<Int64Type>().value(row),
                batch.column(4).as_primitive::<Float64Type>().value(row),
                batch.column(6).as_fixed_size_binary().value(row).to_vec(),
            ));
        }
    }
    out
}

async fn series_of(f: &Fixture, file: &FileRecord) -> Vec<SeriesRow> {
    batches(f, &file.path)
        .await
        .iter()
        .flat_map(|b| series_rows(b).unwrap())
        .collect()
}

/// The samples of every visible set of `level`.
async fn level_samples(f: &Fixture, level: Level) -> Vec<(String, u64, i64, f64, Vec<u8>)> {
    let mut out = Vec::new();
    for file in visible(f, Signal::MetricSamples)
        .await
        .iter()
        .filter(|r| r.level == level)
    {
        out.extend(samples_of(f, file).await);
    }
    out.sort_by_key(|s| (s.1, s.2));
    out
}

/// One sample written by two ingesters at one millisecond, in sets that
/// two L0 to L1 passes take apart, so the key lives in two L1 sets; L2
/// keeps the one of the largest source order, whichever pass came first,
/// and every other sample once.
#[tokio::test]
async fn a_duplicate_across_l0_l1_and_l2_keeps_the_largest_source_order() {
    let Some(f) = fixture().await else {
        return;
    };
    let later = l0_set(
        &f,
        &gauge("cpu", &[("a", 10, 2.0), ("a", 20, 3.0)]),
        (source(9, 1), 2, 5),
    )
    .await;
    let earlier = l0_set(
        &f,
        &gauge("cpu", &[("a", 10, 1.0), ("b", 10, 7.0)]),
        (source(1, 1), 2, 5),
    )
    .await;
    let one_at_a_time = CompactionConfig {
        max_inputs_per_pass: 1,
        ..config()
    };
    let me = fence(&f).await;
    let c = compactor(&f, one_at_a_time);
    let first = c.compact_metrics_once(&me).await.unwrap().unwrap();
    let second = c.compact_metrics_once(&me).await.unwrap().unwrap();
    assert_eq!((first.inputs, second.inputs), (1, 1));
    assert!(c.compact_metrics_once(&me).await.unwrap().is_none());
    let l1 = level_samples(&f, Level::L1).await;
    let at_10: Vec<f64> = l1
        .iter()
        .filter(|s| s.2 == ms(hour()) + 10 && s.3 != 7.0)
        .map(|s| s.3)
        .collect();
    assert_eq!(at_10.len(), 2, "the key in two L1 sets, one each: {l1:?}");
    let _ = (later, earlier);

    let merged = compactor(&f, config())
        .merge_metrics_once(&me)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(merged.inputs, 2);
    assert!(
        visible(&f, Signal::MetricSamples)
            .await
            .iter()
            .all(|r| r.level == Level::L2)
    );
    let l2 = level_samples(&f, Level::L2).await;
    let mut values: Vec<(i64, f64)> = l2.iter().map(|s| (s.2 - ms(hour()), s.3)).collect();
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(values, [(10, 2.0), (10, 7.0), (20, 3.0)]);
    assert_eq!(l2.len(), 3, "each fingerprint and millisecond once: {l2:?}");
    assert!(
        l2.iter().any(|s| s.3 == 2.0) && !l2.iter().any(|s| s.3 == 1.0),
        "the larger source order"
    );
    f.db.finish().await;
}

/// `(set, signal, path)` of the visible metric files of `level`.
async fn sets_of(f: &Fixture, level: Level) -> Vec<(uuid::Uuid, i16, String)> {
    sqlx::query_as(
        "select file_set, signal, path from engine_file
         where tenant_id = $1 and level = $2 and state = 1 and signal in (3, 4)
         order by file_set, signal",
    )
    .bind(f.tenant)
    .bind(level.code())
    .fetch_all(f.db.catalog.pool())
    .await
    .unwrap()
}

fn record(path: &str) -> FileRecord {
    FileRecord {
        id: FileId::new(),
        origin: FileId::new(),
        tenant_id: TenantId::new(),
        signal: Signal::MetricSamples,
        level: Level::L1,
        path: path.to_owned(),
        size_bytes: 0,
        row_count: 0,
        range: TimeRange {
            start: hour(),
            end: hour(),
        },
        services: Vec::new(),
        summary: None,
        content_sha256: None,
        state: fs_catalog::FileState::Visible,
        created_at: hour(),
        quarantined_at: None,
        usage_accounted: true,
    }
}

/// Outputs that close after every row group still make sets each whole:
/// every key of a set's samples has its series row in the set, no sample
/// is listed twice in one, and all of them list each input sample once.
#[tokio::test]
async fn sets_closing_often_each_hold_the_series_of_their_samples() {
    let Some(f) = fixture().await else {
        return;
    };
    let points: Vec<(String, u64, f64)> = (0..30)
        .map(|i| {
            (
                format!("h{}", i % 5),
                i,
                f64::from(u32::try_from(i).unwrap()),
            )
        })
        .collect();
    let refs: Vec<(&str, u64, f64)> = points
        .iter()
        .map(|(h, o, v)| (h.as_str(), *o, *v))
        .collect();
    l0_set(&f, &gauge("cpu", &refs[..15]), (source(1, 1), 2, 5)).await;
    l0_set(&f, &gauge("cpu", &refs[10..]), (source(1, 2), 2, 5)).await;
    let tiny = CompactionConfig {
        l1_target_bytes: 1,
        ..config()
    };
    let me = fence(&f).await;
    compactor(&f, tiny)
        .compact_metrics_once(&me)
        .await
        .unwrap()
        .unwrap();
    let files = sets_of(&f, Level::L1).await;
    assert!(files.len() > 2, "several sets: {files:?}");
    let mut all = Vec::new();
    for pair in files.chunks(2) {
        let [(set_a, 3, samples), (set_b, 4, series)] = pair else {
            panic!("a set not whole: {pair:?}");
        };
        assert_eq!(set_a, set_b);
        let samples = samples_of(&f, &record(samples)).await;
        let series = series_of(&f, &record(series)).await;
        let mut keys: Vec<(u64, i64)> = samples.iter().map(|s| (s.1, s.2)).collect();
        keys.dedup();
        assert_eq!(keys.len(), samples.len(), "a sample twice in one set");
        for s in &samples {
            assert!(
                series
                    .iter()
                    .any(|r| r.fingerprint == s.1 && r.name.as_ref() == s.0),
                "a sample without its series in its set"
            );
        }
        all.extend(samples.into_iter().map(|s| (s.1, s.2)));
    }
    all.sort_unstable();
    let before = all.len();
    all.dedup();
    assert_eq!((before, all.len()), (30, 30), "every input sample once");
    f.db.finish().await;
}

/// The same series in a set of series v1, its origins legacy, and in one
/// of v2 is one row, the known origins kept; only v1 inputs give legacy
/// origins in v2 outputs.
#[tokio::test]
async fn series_of_v1_and_v2_merge_and_v1_alone_stays_legacy() {
    let Some(f) = fixture().await else {
        return;
    };
    let request = gauge("cpu", &[("a", 1, 1.0)]);
    l0_set(&f, &request, (source(1, 1), 1, 4)).await;
    l0_set(&f, &request, (source(1, 2), 2, 5)).await;
    l0_set(&f, &gauge("mem", &[("a", 1, 1.0)]), (source(1, 3), 1, 4)).await;
    let me = fence(&f).await;
    let c = compactor(&f, config());
    c.compact_metrics_once(&me).await.unwrap().unwrap();
    let series: Vec<SeriesRow> = {
        let mut out = Vec::new();
        for file in visible(&f, Signal::MetricSeries).await {
            out.extend(series_of(&f, &file).await);
        }
        out
    };
    assert_eq!(series.len(), 2);
    let cpu = series.iter().find(|r| r.name.as_ref() == "cpu").unwrap();
    assert!(
        cpu.label_meta.iter().all(|m| m.level().is_some()),
        "known origins kept"
    );
    let mem = series.iter().find(|r| r.name.as_ref() == "mem").unwrap();
    assert!(mem.label_meta.iter().all(|m| *m == LabelMeta::LEGACY));
    assert_eq!(c.stats().metric_collisions.load(Ordering::Relaxed), 0);
    f.db.finish().await;
}

/// A crash at each put of a pass, or a swap that does not commit, leaves
/// the inputs visible and what was put an orphan; the next pass
/// completes. A lost lease refuses the swap the same way.
#[tokio::test]
async fn a_pass_that_does_not_commit_leaves_its_inputs_and_orphans() {
    let Some(f) = fixture().await else {
        return;
    };
    l0_set(
        &f,
        &gauge("cpu", &[("a", 1, 1.0), ("b", 2, 2.0)]),
        (source(1, 1), 2, 5),
    )
    .await;
    let me = fence(&f).await;
    for at in 0..5 {
        let failing = compactor(
            &f,
            CompactionConfig {
                inject_put_failure_at: Some(at),
                ..config()
            },
        );
        let err = failing.compact_metrics_once(&me).await.unwrap_err();
        assert!(matches!(err, CompactError::Injected(_)), "put {at}: {err}");
        assert_eq!(
            visible(&f, Signal::MetricSamples).await[0].level,
            Level::L0,
            "put {at}"
        );
        assert_eq!(
            failing.stats().orphaned.load(Ordering::Relaxed),
            u64::try_from(at).unwrap() + 1
        );
    }
    let swap = compactor(
        &f,
        CompactionConfig {
            inject_swap_failures: 1,
            ..config()
        },
    );
    assert!(matches!(
        swap.compact_metrics_once(&me).await,
        Err(CompactError::Injected(_))
    ));
    let stale = LeaseFence {
        generation: me.generation + 1,
        ..me.clone()
    };
    assert!(matches!(
        compactor(&f, config()).compact_metrics_once(&stale).await,
        Err(CompactError::Catalog(CatalogError::LeaseLost { .. }))
    ));
    assert!(
        visible(&f, Signal::MetricSamples)
            .await
            .iter()
            .all(|r| r.level == Level::L0)
    );
    compactor(&f, config())
        .compact_metrics_once(&me)
        .await
        .unwrap()
        .unwrap();
    assert!(
        visible(&f, Signal::MetricSamples)
            .await
            .iter()
            .all(|r| r.level == Level::L1)
    );
    f.db.finish().await;
}

/// A set in part fails the pass closed.
#[tokio::test]
async fn a_set_in_part_fails_the_pass() {
    let Some(f) = fixture().await else {
        return;
    };
    let set = l0_set(&f, &gauge("cpu", &[("a", 1, 1.0)]), (source(1, 1), 2, 5)).await;
    sqlx::query("update engine_file set state = 2 where file_set = $1 and signal = 4")
        .bind(set)
        .execute(f.db.catalog.pool())
        .await
        .unwrap();
    let me = fence(&f).await;
    assert!(matches!(
        compactor(&f, config()).compact_metrics_once(&me).await,
        Err(CompactError::Catalog(CatalogError::PartialFileSet { .. }))
    ));
    f.db.finish().await;
}

/// A pass publishes the metadata of its series over the times of their
/// samples, the usage of every input sample, duplicates included, and
/// postings of each output series file bound to it; the L2 pass after it
/// counts nothing again.
#[tokio::test]
async fn a_pass_publishes_metadata_usage_and_postings_once() {
    let Some(f) = fixture().await else {
        return;
    };
    l0_set(
        &f,
        &gauge("cpu", &[("a", 5, 1.0), ("b", 9, 2.0)]),
        (source(1, 1), 2, 5),
    )
    .await;
    l0_set(&f, &gauge("cpu", &[("a", 5, 3.0)]), (source(1, 2), 2, 5)).await;
    let me = fence(&f).await;
    let c = compactor(&f, config());
    let passed = c.compact_metrics_once(&me).await.unwrap().unwrap();
    assert!(passed.derived >= 2, "metadata and usage");

    let metadata = visible(&f, Signal::MetricMetadata).await;
    assert_eq!(metadata.len(), 1);
    let bytes = f
        .store
        .get(&Path::from(metadata[0].path.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let rows = fs_schema::metric_metadata::decode(bytes).unwrap();
    let host_a = rows.iter().find(|r| r.1 == "host" && r.4 == "a").unwrap();
    assert_eq!((host_a.2.as_str(), host_a.3.as_str()), ("point", "Str"));
    assert_eq!((host_a.5, host_a.6), (ms(hour()) + 5, ms(hour()) + 5));
    let service = rows.iter().find(|r| r.1 == "service.name").unwrap();
    assert_eq!(service.2, "resource");
    assert_eq!(
        (service.5, service.6),
        (ms(hour()) + 5, ms(hour()) + 9),
        "over every sample"
    );
    assert!(!rows.iter().any(|r| r.1 == "__name__"));

    let usage = visible(&f, Signal::Usage1h).await;
    let bytes = f
        .store
        .get(&Path::from(usage[0].path.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let counted = fs_schema::usage::decode(bytes).unwrap();
    assert_eq!(
        counted.iter().map(|r| r.3).sum::<u64>(),
        3,
        "every input sample"
    );
    assert!(
        counted
            .iter()
            .all(|r| r.0 == 3 && r.1.as_deref() == Some("api"))
    );

    let series = visible(&f, Signal::MetricSeries).await;
    let pst = fs_index::postings::sidecar_path(&series[0].path).unwrap();
    let bytes = f
        .store
        .get(&Path::from(pst.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let binding = fs_index::postings::Binding {
        path: series[0].path.clone(),
        sha256: series[0].content_sha256.unwrap(),
        schema_version: 2,
        row_count: u32::try_from(series[0].row_count).unwrap(),
    };
    let postings = fs_index::postings::Postings::decode(&bytes, &binding).unwrap();
    assert_eq!(
        postings.rows("host", "a").map(|b| b.iter().count()),
        Some(1)
    );
    assert_eq!(
        postings.rows("__name__", "cpu").map(|b| b.iter().count()),
        Some(2)
    );

    let merged = c.merge_metrics_once(&me).await.unwrap().unwrap();
    assert_eq!(merged.derived, 0);
    assert_eq!(visible(&f, Signal::Usage1h).await.len(), usage.len());
    f.db.finish().await;
}

/// With metric compaction off the sets are accounted, their usage and
/// metadata published, and left at L0; compacting them later counts
/// nothing again.
#[tokio::test]
async fn sets_accounted_with_compaction_off_are_not_counted_again() {
    let Some(f) = fixture().await else {
        return;
    };
    l0_set(&f, &gauge("cpu", &[("a", 1, 1.0)]), (source(1, 1), 2, 5)).await;
    let me = fence(&f).await;
    let off = compactor(
        &f,
        CompactionConfig {
            compact_metrics: false,
            ..config()
        },
    );
    assert!(off.compact_metrics_once(&me).await.unwrap().is_none());
    let accounted = off.account_metrics_once(&me).await.unwrap().unwrap();
    assert!(accounted.outputs.is_empty() && accounted.derived >= 2);
    assert!(
        visible(&f, Signal::MetricSamples)
            .await
            .iter()
            .all(|r| r.level == Level::L0 && r.usage_accounted)
    );
    assert!(off.account_metrics_once(&me).await.unwrap().is_none());
    let derived =
        visible(&f, Signal::Usage1h).await.len() + visible(&f, Signal::MetricMetadata).await.len();
    let on = compactor(&f, config())
        .compact_metrics_once(&me)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(on.derived, 0);
    assert_eq!(
        visible(&f, Signal::Usage1h).await.len() + visible(&f, Signal::MetricMetadata).await.len(),
        derived
    );
    f.db.finish().await;
}

/// Wide series: under a budget their rows do not fit, the hour is passed
/// over and counted; under one they fit, the pass holds no more than it,
/// every part within its share.
#[tokio::test]
async fn a_pass_of_wide_series_holds_no_more_than_its_budget() {
    let Some(f) = fixture().await else {
        return;
    };
    let wide = "v".repeat(4096);
    let mut request = gauge("cpu", &(0..200).map(|i| ("h", i, 1.0)).collect::<Vec<_>>());
    let point = &mut request.resource_metrics[0].scope_metrics[0].metrics[0];
    if let Some(Data::Gauge(g)) = point.data.as_mut() {
        for (i, p) in g.data_points.iter_mut().enumerate() {
            p.attributes = (0..64)
                .map(|k| kv(&format!("k{k}"), &format!("{wide}{i}")))
                .collect();
        }
    }
    l0_set(&f, &request, (source(1, 1), 2, 5)).await;
    let me = fence(&f).await;
    let small = compactor(&f, config());
    assert!(small.compact_metrics_once(&me).await.unwrap().is_none());
    assert_eq!(small.stats().metric_over_budget.load(Ordering::Relaxed), 1);
    let budget = 64 << 20;
    let c = compactor(
        &f,
        CompactionConfig {
            metric_pass_bytes: budget,
            ..config()
        },
    );
    c.compact_metrics_once(&me).await.unwrap().unwrap();
    let peak = usize::try_from(c.stats().metric_peak_bytes.load(Ordering::Relaxed)).unwrap();
    assert!(peak > 0);
    assert!(peak <= budget, "{peak}");
    f.db.finish().await;
}

/// The fingerprint of every row of `rows` made `fingerprint`.
fn with_fingerprint(rows: &mut fs_schema::metrics::MetricRows, fingerprint: u64) {
    let mut columns = rows.samples.columns().to_vec();
    columns[2] = Arc::new(arrow::array::UInt64Array::from(vec![
        fingerprint;
        rows.samples.num_rows()
    ]));
    rows.samples = RecordBatch::try_new(rows.samples.schema(), columns).unwrap();
    for row in &mut rows.series {
        row.fingerprint = fingerprint;
    }
}

/// Two identities under one name, fingerprint and hour, as a hash
/// collision would make, stay two series rows, counted once; one
/// fingerprint under two names is two series, each with its samples,
/// never deduplicated.
#[tokio::test]
async fn identities_of_one_key_stay_apart_and_names_never_merge() {
    let Some(f) = fixture().await else {
        return;
    };
    let request = gauge("cpu", &[("a", 1, 1.0)]);
    l0_set(&f, &request, (source(1, 1), 2, 5)).await;
    l0_set_with(&f, &request, (source(1, 2), 2, 5), |rows| {
        let row = &mut rows.series[0];
        let at = row.labels.iter().position(|(k, _)| &**k == "host").unwrap();
        row.label_meta[at] = LabelMeta::native(
            fs_schema::metrics::LabelLevel::Resource,
            fs_schema::metrics::Datatype::Str,
        );
    })
    .await;
    let fingerprint = {
        let limits = Limits::default();
        let checked = preflight(&request, &limits, &MetricLimits::default()).unwrap();
        convert(
            &request,
            &limits,
            &MetricLimits::default(),
            &checked,
            source(1, 1),
        )
        .unwrap()
        .series[0]
            .fingerprint
    };
    l0_set_with(
        &f,
        &gauge("mem", &[("a", 1, 9.0)]),
        (source(1, 3), 2, 5),
        |rows| {
            with_fingerprint(rows, fingerprint);
        },
    )
    .await;
    let me = fence(&f).await;
    let c = compactor(&f, config());
    c.compact_metrics_once(&me).await.unwrap().unwrap();
    let mut series = Vec::new();
    for file in visible(&f, Signal::MetricSeries).await {
        series.extend(series_of(&f, &file).await);
    }
    assert_eq!(
        series.iter().filter(|r| r.name.as_ref() == "cpu").count(),
        2,
        "two identities"
    );
    assert_eq!(
        series.iter().filter(|r| r.name.as_ref() == "mem").count(),
        1
    );
    assert_eq!(c.stats().metric_collisions.load(Ordering::Relaxed), 1);
    let samples = level_samples(&f, Level::L1).await;
    let names: Vec<(&str, f64)> = samples.iter().map(|s| (s.0.as_str(), s.3)).collect();
    assert!(
        names.contains(&("cpu", 1.0)) && names.contains(&("mem", 9.0)),
        "{names:?}"
    );
    assert!(samples.iter().all(|s| s.1 == fingerprint));
    f.db.finish().await;
}
