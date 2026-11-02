//! The `samples` and `time_series` tables over metric files, against a
//! real Postgres and a local bucket.
#![allow(
    clippy::too_many_lines,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::AsArray;
use arrow::datatypes::{Float64Type, Int64Type};
use arrow::record_batch::RecordBatch;
use chrono::{TimeZone, Utc};
use fs_catalog::{Catalog, FileId, FileSetId, NewTenant, TenantId};
use fs_ingest::metric_flush::{prepare_samples, prepare_series};
use fs_ingest::record::FileLayout;
use fs_query::{Querier, QueryLimits};
use fs_schema::limits::Limits;
use fs_schema::metrics::{MetricLimits, Source, convert, preflight, series_batch};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use sqlx::{Connection, Executor, PgConnection};

/// 2026-10-24T14:00:00Z.
const HOUR_NS: u64 = 1_792_677_600_000_000_000;
const HOUR_MS: i64 = 1_792_677_600_000;

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
        let schema = format!("qmet_{}", uuid::Uuid::new_v4().simple());
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

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
        ..KeyValue::default()
    }
}

/// `cpu` of each host, `value` apiece, `offset_ms` into the hour.
fn request(hosts: &[&str], value: f64, offset_ms: u64) -> ExportMetricsServiceRequest {
    let data_points = hosts
        .iter()
        .map(|host| NumberDataPoint {
            attributes: vec![kv("host", host)],
            time_unix_nano: HOUR_NS + offset_ms * 1_000_000,
            value: Some(NumberValue::AsDouble(value)),
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
                    name: "cpu".into(),
                    unit: "1".into(),
                    data: Some(Data::Gauge(Gauge { data_points })),
                    ..Metric::default()
                }],
                ..ScopeMetrics::default()
            }],
            schema_url: String::new(),
        }],
    }
}

/// Writes `request` as one set of `tenant` and registers it; the set and
/// the samples and series files' ids.
async fn write_set(
    catalog: &Catalog,
    store: &LocalFileSystem,
    tenant: TenantId,
    request: &ExportMetricsServiceRequest,
) -> (FileSetId, FileId, FileId) {
    let limits = Limits::default();
    let metrics = MetricLimits::default();
    let checked = preflight(request, &limits, &metrics).unwrap();
    let source = Source {
        instance: [1; 16],
        segment: 1,
        offset: 1,
    };
    let rows = convert(request, &limits, &metrics, &checked, source).unwrap();
    let hour = Utc.timestamp_millis_opt(HOUR_MS).unwrap();
    let layout = FileLayout {
        version: FileLayout::METRIC_VERSION,
        ..FileLayout::current(1024)
    };
    let (samples_id, series_id) = (FileId::new(), FileId::new());
    let samples = prepare_samples(
        tenant,
        &Path::from(""),
        samples_id,
        hour,
        &[rows.samples],
        layout,
    )
    .unwrap();
    let batch = series_batch(&rows.series, rows.series.len()).unwrap();
    let series = prepare_series(tenant, &Path::from(""), series_id, hour, &batch, layout).unwrap();
    for prepared in [&samples, &series] {
        fs_ingest::flush::upload(store, prepared).await.unwrap();
    }
    let set = FileSetId::new();
    catalog
        .register_file_set(set, &[samples.file.clone(), series.file.clone()])
        .await
        .unwrap();
    (set, samples_id, series_id)
}

async fn query(querier: &Querier, tenant: TenantId, sql: &str) -> Vec<RecordBatch> {
    querier.collect(tenant, sql).await.unwrap()
}

fn total(batches: &[RecordBatch]) -> i64 {
    batches
        .iter()
        .map(|b| b.column(0).as_primitive::<Int64Type>().value(0))
        .sum()
}

#[tokio::test]
async fn samples_and_series_are_read_from_their_set_and_isolated_by_tenant() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let other = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("other"))
        .await
        .unwrap()
        .id;
    let (_, samples, series) =
        write_set(&db.catalog, &store, tenant, &request(&["a", "b"], 2.0, 0)).await;
    write_set(&db.catalog, &store, tenant, &request(&["c"], 3.0, 60_000)).await;
    write_set(&db.catalog, &store, other, &request(&["z"; 1], 9.0, 0)).await;
    let querier = Querier::new(
        db.catalog.clone(),
        store,
        None,
        QueryLimits {
            timeout: Duration::from_secs(30),
            ..QueryLimits::default()
        },
    )
    .unwrap();

    assert_eq!(
        total(&query(&querier, tenant, "select count(*) from samples").await),
        3
    );
    assert_eq!(
        total(&query(&querier, tenant, "select count(*) from time_series").await),
        3
    );
    let one = query(&querier, tenant, "select * from samples limit 1").await;
    let columns: Vec<String> = one[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(
        columns,
        [
            "metric_name",
            "temporality",
            "fingerprint",
            "unix_milli",
            "value",
            "flags"
        ],
        "the source order is the engine's own"
    );
    let late = query(
        &querier,
        tenant,
        &format!(
            "select value from samples where unix_milli >= {}",
            HOUR_MS + 30_000
        ),
    )
    .await;
    let values: Vec<f64> = late
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Float64Type>().values().to_vec())
        .collect();
    assert_eq!(values, [3.0]);
    let hosts = query(
        &querier,
        tenant,
        "select labels['host'] as host from time_series where metric_name = 'cpu' order by host",
    )
    .await;
    let hosts: Vec<String> = hosts
        .iter()
        .flat_map(|b| {
            let column =
                arrow::compute::cast(b.column(0), &arrow::datatypes::DataType::Utf8).unwrap();
            column
                .as_string::<i32>()
                .iter()
                .map(|v| v.unwrap_or_default().to_owned())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(hosts, ["a", "b", "c"]);
    assert_eq!(
        total(&query(&querier, other, "select count(*) from samples").await),
        1
    );

    // A file whose set's partner is gone fails the query rather than
    // answering without it, of either table, whichever file is gone.
    for gone in [samples, series] {
        for state in [2, 1] {
            sqlx::query("update engine_file set state = $2 where id = $1")
                .bind(gone)
                .bind(state)
                .execute(db.catalog.pool())
                .await
                .unwrap();
            let fresh = Querier::new(
                db.catalog.clone(),
                Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
                None,
                QueryLimits::default(),
            )
            .unwrap();
            for table in ["samples", "time_series"] {
                let outcome = fresh
                    .collect(tenant, &format!("select count(*) from {table}"))
                    .await;
                if state == 2 {
                    let err = outcome.unwrap_err().to_string();
                    assert!(err.contains("registered in part"), "{table}: {err}");
                } else {
                    assert_eq!(total(&outcome.unwrap()), 3, "{table}");
                }
            }
        }
    }
    let fresh = Querier::new(
        db.catalog.clone(),
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        None,
        QueryLimits::default(),
    )
    .unwrap();
    assert_eq!(
        total(&query(&fresh, other, "select count(*) from samples").await),
        1,
        "another tenant's sets are its own"
    );
    db.finish().await;
}

/// Registers `bytes` as a contribution of `signal` of `tenant` at `path`.
async fn contribution(
    catalog: &Catalog,
    store: &LocalFileSystem,
    (tenant, signal, path): (TenantId, fs_catalog::Signal, String),
    bytes: Vec<u8>,
) {
    use object_store::ObjectStoreExt;
    let id = FileId::new();
    let hour = Utc.timestamp_millis_opt(HOUR_MS).unwrap();
    store
        .put(&Path::from(path.as_str()), bytes.clone().into())
        .await
        .unwrap();
    let file = fs_catalog::NewFile {
        id,
        origin: id,
        tenant_id: tenant,
        signal,
        level: fs_catalog::Level::L1,
        path,
        size_bytes: i64::try_from(bytes.len()).unwrap(),
        row_count: 1,
        range: fs_catalog::TimeRange {
            start: hour,
            end: hour + chrono::Duration::minutes(59),
        },
        services: Vec::new(),
        summary: None,
        content_sha256: None,
        usage_accounted: true,
    };
    catalog.register_file(&file).await.unwrap();
    catalog.confirm_file(id).await.unwrap();
}

fn metadata_of(first: i64, last: i64) -> Vec<u8> {
    use fs_schema::metrics::{Datatype, LabelLevel, LabelMeta, MetricType, Temporality};
    let row = fs_schema::metrics::SeriesRow {
        fingerprint: 1,
        unix_milli: HOUR_MS,
        name: "cpu".into(),
        kind: MetricType::Gauge,
        temporality: Temporality::Unspecified,
        monotonic: false,
        unit: "".into(),
        description: "".into(),
        labels: vec![("host".into(), "a".into())].into(),
        label_meta: vec![LabelMeta::native(LabelLevel::Resource, Datatype::Str)].into(),
    };
    let mut counts = fs_schema::metric_metadata::MetadataCounts::default();
    counts.add_series(&row, (first, last), 0, usize::MAX);
    fs_schema::metric_metadata::encode(&counts).unwrap()
}

/// `metadata` aggregates every contribution by its key, the least first
/// and greatest last; a contribution out of its domain fails the query.
#[tokio::test]
async fn metadata_aggregates_its_contributions_and_refuses_a_damaged_one() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let path = |n: u8| {
        format!(
            "t/{}/metrics/derived/metric_metadata/dt=2026-10-24/hr=14/{}.parquet",
            tenant.0,
            uuid::Uuid::from_bytes([n; 16])
        )
    };
    let signal = fs_catalog::Signal::MetricMetadata;
    contribution(
        &db.catalog,
        &store,
        (tenant, signal, path(1)),
        metadata_of(HOUR_MS + 5, HOUR_MS + 9),
    )
    .await;
    contribution(
        &db.catalog,
        &store,
        (tenant, signal, path(2)),
        metadata_of(HOUR_MS + 1, HOUR_MS + 7),
    )
    .await;
    let querier = Querier::new(
        db.catalog.clone(),
        store.clone(),
        None,
        QueryLimits::default(),
    )
    .unwrap();
    let rows = query(
        &querier,
        tenant,
        "select attr_type, attr_datatype, first_reported_unix_milli, last_reported_unix_milli
         from metadata where metric_name = 'cpu' and attr_name = 'host'",
    )
    .await;
    assert_eq!(
        rows.iter().map(RecordBatch::num_rows).sum::<usize>(),
        1,
        "one row a key"
    );
    let batch = &rows[0];
    assert_eq!(batch.column(0).as_string::<i32>().value(0), "resource");
    assert_eq!(batch.column(1).as_string::<i32>().value(0), "Str");
    assert_eq!(
        batch.column(2).as_primitive::<Int64Type>().value(0),
        HOUR_MS + 1
    );
    assert_eq!(
        batch.column(3).as_primitive::<Int64Type>().value(0),
        HOUR_MS + 9
    );

    let other = Querier::new(
        db.catalog.clone(),
        store.clone(),
        None,
        QueryLimits::default(),
    )
    .unwrap();
    let reversed = {
        use arrow::array::{Int64Array, StringArray};
        let schema = fs_schema::metric_metadata::schema();
        let text = |v: &str| -> arrow::array::ArrayRef { Arc::new(StringArray::from(vec![v])) };
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                text("cpu"),
                text("host"),
                text("resource"),
                text("Str"),
                text("b"),
                Arc::new(Int64Array::from(vec![HOUR_MS + 9])),
                Arc::new(Int64Array::from(vec![HOUR_MS + 1])),
            ],
        )
        .unwrap();
        let mut writer =
            datafusion::parquet::arrow::ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.into_inner().unwrap()
    };
    contribution(&db.catalog, &store, (tenant, signal, path(3)), reversed).await;
    let err = other
        .collect(tenant, "select count(*) from metadata")
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("first reported after its last"), "{err}");
    db.finish().await;
}

/// A contribution of format v2 counts metric samples beside v1 ones.
#[tokio::test]
async fn usage_1h_reads_metric_samples_of_format_v2() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let mut counts = fs_schema::usage::UsageCounts::new(fs_schema::usage::UsageSignal::Samples);
    let hour = HOUR_MS * 1_000_000;
    counts
        .add_samples((hour, Some("api")), 3, 69, usize::MAX)
        .unwrap();
    let bytes = fs_schema::usage::encode_hour(&counts, hour).unwrap();
    let path = format!(
        "t/{}/derived/usage_1h/dt=2026-10-24/hr=14/{}.parquet",
        tenant.0,
        uuid::Uuid::now_v7()
    );
    contribution(
        &db.catalog,
        &store,
        (tenant, fs_catalog::Signal::Usage1h, path),
        bytes,
    )
    .await;
    let querier = Querier::new(
        db.catalog.clone(),
        store.clone(),
        None,
        QueryLimits::default(),
    )
    .unwrap();
    let rows = query(&querier, tenant, "select signal, rows, bytes from usage_1h").await;
    let batch = &rows[0];
    assert_eq!(
        batch
            .column(0)
            .as_primitive::<arrow::datatypes::UInt8Type>()
            .value(0),
        3
    );
    assert_eq!(
        batch
            .column(1)
            .as_primitive::<arrow::datatypes::UInt64Type>()
            .value(0),
        3
    );
    assert_eq!(
        batch
            .column(2)
            .as_primitive::<arrow::datatypes::UInt64Type>()
            .value(0),
        69
    );
    db.finish().await;
}
