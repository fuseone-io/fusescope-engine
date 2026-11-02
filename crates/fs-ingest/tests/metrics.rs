//! Metrics end to end: OTLP metrics over gRPC → WAL → buffer → a samples
//! and a series file in a local bucket → one catalog set, with a restart
//! at every durable step.
//!
//! Needs `FS_TEST_DATABASE_URL` (`make dev-up`); skips otherwise. The bucket is
//! a temporary directory.
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

use fs_catalog::{Catalog, FileRecord, NewTenant, Signal, TenantId, TokenScope};
use fs_ingest::hot::{HotScanRequest, MetricTable};
use fs_ingest::record::RowSignal;
use fs_ingest::{IngestConfig, Ingester};
use fs_wal::{Wal, WalConfig};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile;
use opentelemetry_proto::tonic::metrics::v1::{
    ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram, HistogramDataPoint,
    Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Summary, SummaryDataPoint,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};
use sqlx::{Connection, Executor, PgConnection};
use tonic::Request;
use tonic::metadata::MetadataValue;

const T0: u64 = 1_792_678_623_000_000_000; // 2026-10-24T14:17:03Z

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
        let schema = format!("metrics_{}", uuid::Uuid::new_v4().simple());
        let mut admin = PgConnection::connect(&admin_url).await.unwrap();
        admin
            .execute(format!("create schema {schema}").as_str())
            .await
            .unwrap();
        admin.close().await.unwrap();
        let sep = if admin_url.contains('?') { '&' } else { '?' };
        let url = format!("{admin_url}{sep}options=-c%20search_path%3D{schema}");
        let catalog = Catalog::connect(&url, 4).await.unwrap();
        catalog.migrate().await.unwrap();
        Some(Self {
            admin_url,
            schema,
            catalog,
        })
    }

    async fn tenant(&self) -> (TenantId, String) {
        let tenant = self
            .catalog
            .create_tenant(&NewTenant::with_defaults("acme"))
            .await
            .unwrap()
            .id;
        let (token, _) = self
            .catalog
            .create_token(tenant, "collector", TokenScope::Ingest)
            .await
            .unwrap();
        (tenant, token)
    }

    async fn files(&self, tenant: TenantId, signal: Signal) -> Vec<FileRecord> {
        self.catalog
            .all_visible_files(tenant, signal)
            .await
            .unwrap()
    }

    /// `(file_set, signal, confirmed)` of every metric file.
    async fn sets(&self) -> Vec<(uuid::Uuid, i16, bool)> {
        sqlx::query_as(
            "select file_set, signal, confirmed_at is not null from engine_file
             where signal in (3, 4) order by file_set, signal",
        )
        .fetch_all(self.catalog.pool())
        .await
        .unwrap()
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

struct Harness {
    dir: tempfile::TempDir,
    ingester: Arc<Ingester>,
    addr: std::net::SocketAddr,
    hot_addr: std::net::SocketAddr,
    shutdown: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<(), fs_ingest::IngestError>>,
}

async fn start(catalog: &Catalog, dir: tempfile::TempDir, config: IngestConfig) -> Harness {
    std::fs::create_dir_all(dir.path().join("bucket")).unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path().join("bucket")).unwrap());
    let wal = Wal::open(WalConfig::new(dir.path().join("wal"))).unwrap();
    let ingester =
        Arc::new(Ingester::new(config, catalog.clone(), store, Path::from(""), wal).unwrap());
    let (shutdown, rx) = tokio::sync::watch::channel(false);
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let (hot_tx, hot_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn({
        let ingester = ingester.clone();
        async move { ingester.run(rx, Some(bound_tx), Some(hot_tx)).await }
    });
    let addr = tokio::time::timeout(Duration::from_secs(5), bound_rx)
        .await
        .unwrap()
        .unwrap();
    let hot_addr = tokio::time::timeout(Duration::from_secs(5), hot_rx)
        .await
        .unwrap()
        .unwrap();
    Harness {
        dir,
        ingester,
        addr,
        hot_addr,
        shutdown,
        task,
    }
}

impl Harness {
    /// Exports `request`; the points refused and why.
    async fn export(&self, request: ExportMetricsServiceRequest, token: &str) -> (i64, String) {
        let mut client = MetricsServiceClient::connect(format!("http://{}", self.addr))
            .await
            .unwrap();
        let mut r = Request::new(request);
        r.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
        );
        let response = client.export(r).await.unwrap();
        response
            .into_inner()
            .partial_success
            .map_or((0, String::new()), |p| {
                (p.rejected_data_points, p.error_message)
            })
    }

    async fn hot(&self, tenant: TenantId, table: MetricTable) -> fs_ingest::hot::HotScanResponse {
        let request = HotScanRequest {
            tenant,
            ts_min: None,
            ts_max: None,
            services: None,
            columns: None,
            trace_ids: None,
            span_ids: None,
            distinct: None,
            signal: RowSignal::Metrics,
            table: Some(table),
        };
        fs_ingest::hot::scan(&self.hot_addr.to_string(), &request, Duration::from_secs(5))
            .await
            .unwrap()
    }

    async fn crash(self) -> tempfile::TempDir {
        self.task.abort();
        let _ = self.task.await;
        self.dir
    }

    async fn stop(self) {
        self.shutdown.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    fn bucket(&self) -> std::path::PathBuf {
        self.dir.path().join("bucket")
    }
}

fn test_config() -> IngestConfig {
    IngestConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        hot_listen: Some("127.0.0.1:0".parse().unwrap()),
        flush_interval: Duration::from_secs(3600),
        persisted_retain: Duration::from_secs(2),
        ..IngestConfig::default()
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

/// A gauge of `hosts` hosts at `T0 + offset_ms`, a histogram, and an
/// exponential histogram of two points, which are refused.
fn metrics(hosts: usize, offset_ms: u64) -> ExportMetricsServiceRequest {
    let time = T0 + offset_ms * 1_000_000;
    let gauge = (0..hosts)
        .map(|h| NumberDataPoint {
            attributes: vec![kv("host", &format!("h{h}"))],
            time_unix_nano: time,
            value: Some(NumberValue::AsDouble(f64::from(u32::try_from(h).unwrap()))),
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
                metrics: vec![
                    Metric {
                        name: "cpu".into(),
                        data: Some(Data::Gauge(Gauge { data_points: gauge })),
                        ..Metric::default()
                    },
                    Metric {
                        name: "latency".into(),
                        unit: "ms".into(),
                        data: Some(Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                time_unix_nano: time,
                                count: 3,
                                sum: Some(6.0),
                                bucket_counts: vec![1, 2],
                                explicit_bounds: vec![5.0],
                                ..HistogramDataPoint::default()
                            }],
                            aggregation_temporality: 2,
                        })),
                        ..Metric::default()
                    },
                    Metric {
                        name: "exp".into(),
                        data: Some(Data::ExponentialHistogram(ExponentialHistogram {
                            data_points: vec![ExponentialHistogramDataPoint::default(); 2],
                            aggregation_temporality: 2,
                        })),
                        ..Metric::default()
                    },
                ],
                ..ScopeMetrics::default()
            }],
            schema_url: String::new(),
        }],
    }
}

fn walkdir(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

fn objects(dir: &std::path::Path, part: &str) -> Vec<std::path::PathBuf> {
    walkdir(dir)
        .into_iter()
        .filter(|p| p.to_string_lossy().contains(part))
        .collect()
}

fn sha256_of(path: &std::path::Path) -> [u8; 32] {
    Sha256::digest(std::fs::read(path).unwrap()).into()
}

fn rows_in(path: &std::path::Path) -> i64 {
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap()).unwrap();
    reader.metadata().file_metadata().num_rows()
}

fn signal_of(path: &std::path::Path) -> String {
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap()).unwrap();
    reader.schema().metadata()["fusescope.signal"].clone()
}

/// Two requests of one hour become one samples and one series file of one
/// set: duplicates deduplicated, series one a fingerprint and hour, and
/// the exponential histograms refused in the partial success.
#[tokio::test]
async fn metrics_are_flushed_as_a_samples_and_series_set() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    let (refused, why) = h.export(metrics(3, 0), &token).await;
    assert_eq!(refused, 2);
    assert!(why.contains("exponential_histogram=2"), "{why}");
    // The same points again, and a second time for two hosts.
    h.export(metrics(3, 0), &token).await;
    h.export(metrics(2, 10), &token).await;
    h.ingester.flush_all().await;

    let samples = objects(&h.bucket(), &format!("t/{}/metrics/samples/l0/", tenant.0));
    let series = objects(&h.bucket(), &format!("t/{}/metrics/series/l0/", tenant.0));
    assert_eq!((samples.len(), series.len()), (1, 1));
    // 3 hosts at T0 once, 2 at T0+10ms, and the histogram's buckets, count
    // and sum at both times.
    assert_eq!(rows_in(&samples[0]), 3 + 2 + 2 * 4);
    assert_eq!(rows_in(&series[0]), 3 + 4, "one row a series and hour");
    assert_eq!(signal_of(&samples[0]), "metric_samples");
    assert_eq!(signal_of(&series[0]), "metric_series");
    let registered = db.files(tenant, Signal::MetricSamples).await;
    assert_eq!(registered.len(), 1);
    assert_eq!(registered[0].content_sha256, Some(sha256_of(&samples[0])));
    let sets = db.sets().await;
    assert_eq!(sets.len(), 2);
    assert_eq!(sets[0].0, sets[1].0, "one set");
    let stats = h.ingester.stats();
    assert_eq!(
        stats.metric_points.load(Ordering::Relaxed),
        3 + 1 + 3 + 1 + 2 + 1
    );
    assert_eq!(stats.metric_points_rejected[0].load(Ordering::Relaxed), 6);
    h.stop().await;
    db.finish().await;
}

/// The hot window serves both tables while the rows are live and after
/// the set is registered, and names both files of the set as included.
#[tokio::test]
async fn the_hot_window_serves_samples_and_series_and_excludes_both_files() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let config = IngestConfig {
        persisted_retain: Duration::from_secs(60),
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    h.export(metrics(2, 0), &token).await;
    let rows = |r: &fs_ingest::hot::HotScanResponse| -> usize {
        r.batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum()
    };
    let live = h.hot(tenant, MetricTable::Samples).await;
    assert_eq!(rows(&live), 2 + 4);
    assert_eq!(live.persisted_files.len(), 2, "both ids of the live set");
    let live_series = h.hot(tenant, MetricTable::Series).await;
    assert_eq!(rows(&live_series), 2 + 4);
    assert_eq!(
        live_series.schema.unwrap().metadata()["fusescope.signal"],
        "metric_series"
    );
    h.ingester.flush_all().await;
    let samples = db.files(tenant, Signal::MetricSamples).await;
    let series = db.files(tenant, Signal::MetricSeries).await;
    let persisted = h.hot(tenant, MetricTable::Series).await;
    assert_eq!(rows(&persisted), 2 + 4);
    assert!(persisted.persisted_files.contains(&samples[0].id));
    assert!(persisted.persisted_files.contains(&series[0].id));
    h.stop().await;
    db.finish().await;
}

/// A crash before the intent: the replay converts the records again and
/// writes the set once.
#[tokio::test]
async fn a_crash_before_the_intent_replays_the_metric_rows() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    h.export(metrics(4, 0), &token).await;
    let dir = h.crash().await;
    assert_eq!(walkdir(&dir.path().join("bucket")).len(), 0);
    let h = start(&db.catalog, dir, test_config()).await;
    h.ingester.flush_all().await;
    assert_eq!(db.files(tenant, Signal::MetricSamples).await.len(), 1);
    assert_eq!(db.files(tenant, Signal::MetricSeries).await.len(), 1);
    assert_eq!(db.sets().await.len(), 2);
    h.stop().await;
    db.finish().await;
}

/// A copy of `from` in a new directory.
fn copy_dir(from: &std::path::Path) -> tempfile::TempDir {
    let to = tempfile::tempdir().unwrap();
    for file in walkdir(from) {
        let target = to.path().join(file.strip_prefix(from).unwrap());
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::copy(&file, &target).unwrap();
    }
    std::fs::create_dir_all(to.path().join("bucket")).unwrap();
    to
}

/// Points each conversion limit bears on: a histogram of two bounds, a
/// summary of two quantiles, a long key, a long value and seven labels.
fn bounded_request() -> ExportMetricsServiceRequest {
    let attributes = vec![
        kv("host", "h0"),
        kv("a_rather_long_key", "x"),
        kv("value", "a rather long value"),
    ];
    let summary = SummaryDataPoint {
        attributes: attributes.clone(),
        time_unix_nano: T0,
        count: 4,
        sum: 8.0,
        quantile_values: vec![
            ValueAtQuantile {
                quantile: 0.5,
                value: 1.0,
            },
            ValueAtQuantile {
                quantile: 0.9,
                value: 3.0,
            },
        ],
        ..SummaryDataPoint::default()
    };
    let mut request = metrics(1, 0);
    let scope = &mut request.resource_metrics[0].scope_metrics[0];
    for metric in &mut scope.metrics {
        match &mut metric.data {
            Some(Data::Gauge(g)) => g.data_points[0].attributes.clone_from(&attributes),
            Some(Data::Histogram(h)) => {
                let point = &mut h.data_points[0];
                point.attributes.clone_from(&attributes);
                point.explicit_bounds = vec![1.0, 5.0];
                point.bucket_counts = vec![1, 1, 1];
            }
            _ => {}
        }
    }
    scope.metrics.push(Metric {
        name: "rpc".into(),
        data: Some(Data::Summary(Summary {
            data_points: vec![summary],
        })),
        ..Metric::default()
    });
    request
}

/// Records logged, then a crash before their intent: whatever conversion
/// limit the restart is configured with, the replay converts each record
/// under the limits it was accepted with, into the same files, byte for
/// byte, under the same ids.
#[tokio::test]
async fn a_replay_converts_under_the_limits_its_records_were_accepted_with() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    let (rejected, _) = h.export(bounded_request(), &token).await;
    assert_eq!(rejected, 2, "the exponential histogram only");
    let logged = h.crash().await;
    let with = |change: fn(&mut IngestConfig)| {
        let mut config = test_config();
        change(&mut config);
        config
    };
    let restarts = [
        ("unchanged", test_config()),
        (
            "max_histogram_buckets",
            with(|c| c.metrics.max_histogram_buckets = 1),
        ),
        (
            "max_summary_quantiles",
            with(|c| c.metrics.max_summary_quantiles = 1),
        ),
        (
            "max_series_labels",
            with(|c| c.metrics.max_series_labels = 6),
        ),
        ("max_key_bytes", with(|c| c.limits.max_key_bytes = 4)),
        ("max_value_bytes", with(|c| c.limits.max_value_bytes = 4)),
    ];
    let mut first: Option<Vec<(String, [u8; 32])>> = None;
    for (change, config) in restarts {
        let h = start(&db.catalog, copy_dir(logged.path()), config).await;
        h.ingester.flush_all().await;
        let written: Vec<(String, [u8; 32])> = walkdir(&h.bucket())
            .iter()
            .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
            .map(|p| {
                (
                    p.strip_prefix(h.bucket()).unwrap().display().to_string(),
                    sha256_of(p),
                )
            })
            .collect();
        assert_eq!(written.len(), 2, "{change}: one set");
        assert_eq!(
            db.files(tenant, Signal::MetricSamples).await.len(),
            1,
            "{change}"
        );
        assert_eq!(
            db.files(tenant, Signal::MetricSeries).await.len(),
            1,
            "{change}"
        );
        match &first {
            None => first = Some(written),
            Some(first) => assert_eq!(&written, first, "{change}"),
        }
        h.stop().await;
    }
    db.finish().await;
}

/// A WAL of one metric record written as `record` makes.
fn wal_with(record: &fs_ingest::record::WalRecord) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::open(WalConfig::new(dir.path().join("wal"))).unwrap();
    wal.append(&record.encode()).unwrap();
    wal.sync().unwrap();
    dir
}

/// A request past its point cap is refused for that, counted before the
/// walk's scratch is measured and held: not for memory it never needed.
#[tokio::test]
async fn a_request_past_its_points_is_refused_before_its_walk_is_measured() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (_, token) = db.tenant().await;
    let mut config = test_config();
    config.metrics.max_points = 2;
    config.max_memory_bytes = 1024;
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut client = MetricsServiceClient::connect(format!("http://{}", h.addr))
        .await
        .unwrap();
    let mut r = Request::new(metrics(3, 0));
    r.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
    );
    let status = client.export(r).await.unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");
    assert!(status.message().contains("points"), "{status:?}");
    h.stop().await;
    db.finish().await;
}

/// The series rows of the series files under `bucket`.
fn series_in(bucket: &std::path::Path) -> Vec<fs_schema::metrics::SeriesRow> {
    objects(bucket, "/metrics/series/")
        .iter()
        .flat_map(|path| {
            let reader =
                ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap())
                    .unwrap();
            let schema = Arc::clone(reader.schema());
            reader
                .build()
                .unwrap()
                .flat_map(|batch| {
                    let batch = batch.unwrap().with_schema(Arc::clone(&schema)).unwrap();
                    fs_schema::metrics::series_rows(&batch).unwrap()
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// A record of mapping 1, logged before label origins, replays with
/// legacy origins; the same series received live under mapping 2 merges
/// with it in the buffer, no collision, the known origins kept.
#[tokio::test]
async fn a_replayed_mapping_1_record_and_live_mapping_2_rows_are_one_series() {
    use fs_ingest::metric_record::MetricConversion;
    use fs_ingest::record::{RecordSignal, VERSION_METRIC_RECORD, WalRecord};
    use prost::Message as _;

    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let config = test_config();
    let older = MetricConversion {
        mapping: 1,
        ..MetricConversion::current(&config.limits, &config.metrics)
    };
    let record = WalRecord {
        version: VERSION_METRIC_RECORD,
        tenant,
        signal: RecordSignal::Metrics,
        payload: older.payload(&metrics(2, 0).encode_to_vec()),
    };
    let h = start(&db.catalog, wal_with(&record), test_config()).await;
    h.export(metrics(2, 1), &token).await;
    h.ingester.flush_all().await;
    let series = series_in(&h.bucket());
    assert_eq!(
        series.len(),
        2 + 4,
        "two gauges and the histogram's series, once each"
    );
    assert!(
        series
            .iter()
            .all(|s| s.label_meta.iter().all(|m| m.level().is_some())),
        "every origin known"
    );
    assert_eq!(
        h.ingester
            .stats()
            .metric_series_collisions
            .load(Ordering::Relaxed),
        0
    );
    h.stop().await;
    db.finish().await;
}

/// Why the start of an ingester whose WAL holds `record` fails.
fn refused_start(db: &TestDb, record: &fs_ingest::record::WalRecord) -> String {
    let dir = wal_with(record);
    std::fs::create_dir_all(dir.path().join("bucket")).unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path().join("bucket")).unwrap());
    let wal = Wal::open(WalConfig::new(dir.path().join("wal"))).unwrap();
    match Ingester::new(
        test_config(),
        db.catalog.clone(),
        store,
        Path::from(""),
        wal,
    ) {
        Ok(_) => panic!("the start must fail"),
        Err(e) => e.to_string(),
    }
}

/// A metric record the replay cannot convert as it was accepted stops the
/// start, naming its position and why, rather than lose or change rows
/// acknowledged: one of version 1, whose limits are lost, one of a
/// mapping version this engine does not convert, and one whose payload
/// passed the log's checksum but does not parse.
#[tokio::test]
async fn a_metric_record_that_cannot_convert_as_accepted_stops_the_start() {
    use fs_ingest::metric_record::{HEADER_BYTES, MetricConversion};
    use fs_ingest::record::{RecordSignal, VERSION, VERSION_METRIC_RECORD, WalRecord};
    use prost::Message as _;

    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, _) = db.tenant().await;
    let request = metrics(3, 0).encode_to_vec();
    let config = test_config();
    let conversion = MetricConversion::current(&config.limits, &config.metrics);
    let record = |version, payload: bytes::Bytes| WalRecord {
        version,
        tenant,
        signal: RecordSignal::Metrics,
        payload,
    };
    let newer = MetricConversion {
        mapping: fs_schema::metrics::MAPPING_VERSION + 1,
        ..conversion
    };
    let short = conversion.payload(&[]).slice(..HEADER_BYTES - 1);
    // Accepted by a writer before the ceilings: 4500 bounds under 5000.
    let wide = MetricConversion {
        max_histogram_buckets: 5000,
        ..conversion
    };
    let mut buckets = metrics(1, 0);
    if let Some(Data::Histogram(h)) =
        &mut buckets.resource_metrics[0].scope_metrics[0].metrics[1].data
    {
        let point = &mut h.data_points[0];
        point.explicit_bounds = (0..4500).map(f64::from).collect();
        point.bucket_counts = vec![0; 4501];
        point.count = 0;
    }
    let past_ceiling = wide.payload(&buckets.encode_to_vec());
    let garbled = conversion.payload(&[0xff; 8]);
    for (case, record, reason) in [
        (
            "version 1",
            record(VERSION, request.clone().into()),
            "carries no conversion limits",
        ),
        (
            "newer mapping",
            record(VERSION_METRIC_RECORD, newer.payload(&request)),
            "mapping version 3",
        ),
        (
            "short header",
            record(VERSION_METRIC_RECORD, short),
            "damaged",
        ),
        (
            "garbled request",
            record(VERSION_METRIC_RECORD, garbled),
            "damaged",
        ),
        (
            "past a ceiling",
            record(VERSION_METRIC_RECORD, past_ceiling),
            "max_histogram_buckets 5000",
        ),
    ] {
        let err = refused_start(&db, &record);
        assert!(
            err.contains(reason) && err.contains("Position"),
            "{case}: {err}"
        );
    }
    db.finish().await;
}

/// The samples object is uploaded and the series one cannot be, then a
/// crash: the replay finds the samples object as it wrote it, uploads the
/// series and registers the set under the same ids.
#[tokio::test]
async fn a_crash_between_the_two_uploads_completes_the_set_with_the_same_bytes() {
    use std::os::unix::fs::PermissionsExt;

    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    h.export(metrics(3, 0), &token).await;
    let series_dir = h.bucket().join(format!("t/{}/metrics/series", tenant.0));
    std::fs::create_dir_all(&series_dir).unwrap();
    std::fs::set_permissions(&series_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    h.ingester.flush_all().await;
    let samples = objects(&h.bucket(), "/metrics/samples/l0/");
    assert_eq!(samples.len(), 1, "the samples went first");
    assert_eq!(objects(&h.bucket(), "/metrics/series/l0/").len(), 0);
    assert_eq!(db.sets().await.len(), 0, "nothing registered");
    let before = sha256_of(&samples[0]);
    std::fs::set_permissions(&series_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    let dir = h.crash().await;

    let h = start(&db.catalog, dir, test_config()).await;
    h.ingester.flush_all().await;
    assert_eq!(objects(&h.bucket(), "/metrics/samples/l0/"), samples);
    assert_eq!(sha256_of(&samples[0]), before);
    assert_eq!(objects(&h.bucket(), "/metrics/series/l0/").len(), 1);
    assert_eq!(
        h.ingester.flush_stats().superseded.load(Ordering::Relaxed),
        0
    );
    assert_eq!(db.files(tenant, Signal::MetricSamples).await.len(), 1);
    assert_eq!(db.sets().await.len(), 2);
    h.stop().await;
    db.finish().await;
}

/// Both objects uploaded, the registration fails, then a crash: the replay
/// reproduces both objects and registers the set once. Registered, never
/// confirmed, then a crash: the replay confirms both files.
#[tokio::test]
async fn crashes_around_the_registration_and_the_confirmation_end_in_one_confirmed_set() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let fails_once = IngestConfig {
        inject_register_failures: 1,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), fails_once).await;
    h.export(metrics(3, 0), &token).await;
    h.ingester.flush_all().await;
    let written = objects(&h.bucket(), "/metrics/");
    assert_eq!(written.len(), 2);
    let digests: Vec<_> = written.iter().map(|p| sha256_of(p)).collect();
    assert_eq!(db.sets().await.len(), 0);
    let dir = h.crash().await;

    let never_confirms = IngestConfig {
        inject_confirm_failures: u32::MAX,
        ..test_config()
    };
    let h = start(&db.catalog, dir, never_confirms).await;
    h.ingester.flush_all().await;
    assert_eq!(objects(&h.bucket(), "/metrics/"), written);
    assert_eq!(
        written.iter().map(|p| sha256_of(p)).collect::<Vec<_>>(),
        digests
    );
    let sets = db.sets().await;
    assert_eq!(sets.len(), 2);
    assert!(sets.iter().all(|(_, _, confirmed)| !confirmed));
    let dir = h.crash().await;

    let h = start(&db.catalog, dir, test_config()).await;
    h.ingester.flush_all().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !db.sets().await.iter().all(|(_, _, confirmed)| *confirmed) {
        assert!(tokio::time::Instant::now() < deadline, "never confirmed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(db.sets().await.len(), 2, "still one set");
    assert_eq!(db.files(tenant, Signal::MetricSeries).await.len(), 1);
    h.stop().await;
    db.finish().await;
}

/// An object of the set found with other bytes, the samples or the
/// series one: the pair takes two fresh ids and a fresh set under a new
/// intent, and the first logged of those intents may fail to sync, which
/// keeps the old pair until the next attempt supersedes it.
#[tokio::test]
async fn an_object_of_either_file_found_changed_gives_the_set_fresh_ids() {
    for tampered in ["/metrics/samples/l0/", "/metrics/series/l0/"] {
        let Some(db) = TestDb::new().await else {
            return;
        };
        let (tenant, token) = db.tenant().await;
        let fails_once = IngestConfig {
            inject_register_failures: 1,
            ..test_config()
        };
        let h = start(&db.catalog, tempfile::tempdir().unwrap(), fails_once).await;
        h.export(metrics(3, 0), &token).await;
        h.ingester.flush_all().await;
        let path = objects(&h.bucket(), tampered).pop().unwrap();
        std::fs::write(&path, b"other bytes").unwrap();
        let dir = h.crash().await;

        let supersede_fails_once = IngestConfig {
            inject_intent_failures: 1,
            ..test_config()
        };
        let h = start(&db.catalog, dir, supersede_fails_once).await;
        h.ingester.flush_all().await;
        assert_eq!(
            db.sets().await.len(),
            0,
            "{tampered}: the old pair was kept"
        );
        assert_eq!(
            h.ingester.flush_stats().superseded.load(Ordering::Relaxed),
            0
        );
        // A pass tries each flush once: the next supersedes, the one after
        // writes the set under its fresh ids.
        h.ingester.flush_all().await;
        h.ingester.flush_all().await;
        let flush = h.ingester.flush_stats();
        assert!(flush.replay_mismatch.load(Ordering::Relaxed) >= 2);
        assert_eq!(flush.superseded.load(Ordering::Relaxed), 1, "{tampered}");
        let samples = db.files(tenant, Signal::MetricSamples).await;
        let series = db.files(tenant, Signal::MetricSeries).await;
        assert_eq!((samples.len(), series.len()), (1, 1));
        assert!(
            !samples[0]
                .path
                .contains(&path.file_name().unwrap().to_string_lossy().to_string())
        );
        assert!(
            !series[0]
                .path
                .contains(&path.file_name().unwrap().to_string_lossy().to_string())
        );
        assert_ne!(
            samples[0].id, samples[0].origin,
            "the origin stays the first id"
        );
        assert_eq!(rows_in(&h.bucket().join(&samples[0].path)), 3 + 4);
        h.stop().await;
        db.finish().await;
    }
}

/// Requests whose series are all new make the buffer's series merge into
/// a new run again and again; the memory held never passes the cap.
#[tokio::test]
async fn merging_new_series_into_the_buffer_stays_under_the_memory_cap() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (_tenant, token) = db.tenant().await;
    let cap = 16 << 20;
    let config = IngestConfig {
        max_memory_bytes: cap,
        flush_bytes: usize::MAX,
        flush_rows: usize::MAX,
        ..test_config()
    };
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), config).await;
    let mut accepted = 0;
    for round in 0..60u64 {
        let mut request = metrics(0, 0);
        let points = (0..200)
            .map(|i| NumberDataPoint {
                attributes: vec![kv("pod", &format!("p{round}-{i}"))],
                time_unix_nano: T0,
                value: Some(NumberValue::AsDouble(1.0)),
                ..NumberDataPoint::default()
            })
            .collect();
        request.resource_metrics[0].scope_metrics[0].metrics[0].data = Some(Data::Gauge(Gauge {
            data_points: points,
        }));
        let mut client = MetricsServiceClient::connect(format!("http://{}", h.addr))
            .await
            .unwrap();
        let mut r = Request::new(request);
        r.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
        );
        if client.export(r).await.is_ok() {
            accepted += 1;
        }
        let held = h.ingester.memory_held().await;
        assert!(held <= cap, "round {round}: {held} held, cap {cap}");
    }
    assert!(accepted > 5, "{accepted} rounds accepted");
    h.stop().await;
    db.finish().await;
}

/// Rows of two hours become two sets, each series file with the series of
/// its own hour only.
#[tokio::test]
async fn rows_of_two_hours_become_two_sets_each_with_its_own_series() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (tenant, token) = db.tenant().await;
    let h = start(&db.catalog, tempfile::tempdir().unwrap(), test_config()).await;
    h.export(metrics(3, 0), &token).await;
    h.export(metrics(1, 3_600_000), &token).await;
    h.ingester.flush_all().await;
    let samples = db.files(tenant, Signal::MetricSamples).await;
    let series = db.files(tenant, Signal::MetricSeries).await;
    assert_eq!((samples.len(), series.len()), (2, 2));
    let mut rows: Vec<(i64, i64)> = series
        .iter()
        .map(|f| {
            (
                f.range.start.timestamp(),
                rows_in(&h.bucket().join(&f.path)),
            )
        })
        .collect();
    rows.sort_unstable();
    assert_eq!(rows.iter().map(|r| r.1).collect::<Vec<_>>(), [3 + 4, 1 + 4]);
    let sets = db.sets().await;
    assert_eq!(sets.len(), 4);
    assert_ne!(sets[0].0, sets[2].0, "a set an hour");
    h.stop().await;
    db.finish().await;
}
