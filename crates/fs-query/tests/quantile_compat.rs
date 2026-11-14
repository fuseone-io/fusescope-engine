//! `quantile_compat` through the querier, as the backend calls it: the order
//! statistics of a group of spans, and its accumulators charged to the
//! memory pool. Needs `FS_TEST_DATABASE_URL`.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::too_many_lines
)]

use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::Float64Type;
use fs_catalog::{Catalog, FileId, NewTenant, TenantId};
use fs_ingest::flush;
use fs_query::{Querier, QueryError, QueryLimits};
use fs_schema::limits::Limits;
use fs_schema::spans::SpanBatchBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use sqlx::{Connection, Executor, PgConnection};

/// 2026-10-22T13:00:00Z.
const T0: u64 = 1_792_677_600_000_000_000;
const P99: &str = "quantile_compat(TRY_CAST(duration_nano AS DOUBLE), 0.99)";

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
        let schema = format!("quantile_{}", uuid::Uuid::new_v4().simple());
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

struct Fixture {
    db: TestDb,
    tenant: TenantId,
    store: Arc<LocalFileSystem>,
    _dir: tempfile::TempDir,
}

async fn fixture() -> Option<Fixture> {
    let db = TestDb::new().await?;
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("bucket")).unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path().join("bucket")).unwrap());
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

fn querier(f: &Fixture, limits: QueryLimits) -> Querier {
    Querier::new(f.db.catalog.clone(), f.store.clone(), None, limits).unwrap()
}

/// Span `i` of operation `name`, lasting `micros` microseconds.
fn span(i: u32, name: &str, micros: u64) -> Span {
    let start = T0 + u64::from(i) * 1_000;
    let mut trace_id = vec![7; 16];
    trace_id[12..].copy_from_slice(&i.to_be_bytes());
    let mut span_id = vec![1; 8];
    span_id[4..].copy_from_slice(&i.to_be_bytes());
    Span {
        trace_id,
        span_id,
        name: name.into(),
        kind: 2,
        start_time_unix_nano: start,
        end_time_unix_nano: start + micros * 1_000,
        ..Span::default()
    }
}

/// Writes and registers one L0 span file of `spans`.
async fn write_l0(f: &Fixture, spans: Vec<Span>) {
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("api".into())),
                    }),
                    ..KeyValue::default()
                }],
                ..Resource::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let mut builder = SpanBatchBuilder::new(Limits::default());
    builder.append_request(&request).unwrap();
    let prepared = flush::prepare(
        f.tenant,
        &Path::from(""),
        FileId::new(),
        &[builder.finish().unwrap()],
        fs_ingest::record::FileLayout::current(flush::DEFAULT_ROW_GROUP_ROWS),
    )
    .unwrap();
    flush::upload(f.store.as_ref(), &prepared).await.unwrap();
    flush::upload_index(f.store.as_ref(), &prepared)
        .await
        .unwrap();
    f.db.catalog.register_file(&prepared.file).await.unwrap();
}

#[tokio::test]
async fn the_backend_p99_lies_on_the_order_statistics() {
    let Some(f) = fixture().await else { return };
    // 1 237 spans lasting 1..=1 237 µs in shuffled order: rank 0.99·1 236 =
    // 1 223.64 lies between 1 224 and 1 225 µs.
    let spans = (0..1_237u32)
        .map(|i| span(i, "op", u64::from((i * 7_919) % 1_237 + 1)))
        .collect();
    write_l0(&f, spans).await;
    let q = querier(&f, QueryLimits::default());
    let batches = q
        .collect(f.tenant, &format!("select {P99} from spans"))
        .await
        .unwrap();
    let p99 = batches[0].column(0).as_primitive::<Float64Type>().value(0);
    assert!((p99 - 1_224_640.0).abs() < 1e-6, "{p99}");
    f.db.finish().await;
}

/// One group's accumulator grows to about 80 KB before it turns into a
/// digest; an aggregate without groups cannot spill it.
#[tokio::test]
async fn an_accumulator_past_the_memory_pool_fails_as_resources_exhausted() {
    let Some(f) = fixture().await else { return };
    let spans = (0..20_011u32)
        .map(|i| span(i, "op", u64::from(i % 9_973 + 1)))
        .collect();
    write_l0(&f, spans).await;
    let q = querier(
        &f,
        QueryLimits {
            memory_bytes: 64 * 1024,
            ..QueryLimits::default()
        },
    );
    let counted = q
        .collect(f.tenant, "select count(*) from spans")
        .await
        .unwrap();
    assert_eq!(counted[0].num_rows(), 1);
    drop(counted);
    assert_eq!(q.memory_reserved(), 0);
    let err = q
        .collect(f.tenant, &format!("select {P99} from spans"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, QueryError::DataFusion(ref e) if matches!(e.find_root(), datafusion::error::DataFusionError::ResourcesExhausted(_))),
        "{err}"
    );
    assert_eq!(q.memory_reserved(), 0, "the reservation is released");
    f.db.finish().await;
}
