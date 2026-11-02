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
    let layout = FileLayout::current(1024);
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
    let (_, alone, _) = write_set(&db.catalog, &store, tenant, &request(&["a", "b"], 2.0, 0)).await;
    let (_, _, unpaired) =
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
    // answering without it, of either table.
    for gone in [unpaired, alone] {
        sqlx::query("update engine_file set state = 2 where id = $1")
            .bind(gone)
            .execute(db.catalog.pool())
            .await
            .unwrap();
    }
    let fresh = Querier::new(
        db.catalog.clone(),
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        None,
        QueryLimits::default(),
    )
    .unwrap();
    for table in ["samples", "time_series"] {
        let err = fresh
            .collect(tenant, &format!("select count(*) from {table}"))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("registered in part"), "{table}: {err}");
    }
    assert_eq!(
        total(&query(&fresh, other, "select count(*) from samples").await),
        1,
        "another tenant's sets are its own"
    );
    db.finish().await;
}
