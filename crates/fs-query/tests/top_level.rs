//! `top_level_operations` over every span of a tenant: the compacted table
//! with the L0 files and the hot window it does not hold yet, resolved hour
//! by hour as the compactor will resolve them, filtered only after. Needs
//! `FS_TEST_DATABASE_URL`.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::too_many_lines
)]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::AsArray;
use arrow::record_batch::RecordBatch;
use fs_catalog::{Catalog, FileId, Level, NewFile, NewTenant, Signal, TenantId, TimeRange};
use fs_ingest::flush;
use fs_query::{Querier, QueryError, QueryLimits};
use fs_schema::limits::Limits;
use fs_schema::spans::SpanBatchBuilder;
use fs_schema::top_level_operations::TopLevelOperations;
use object_store::ObjectStoreExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use sqlx::{Connection, Executor, PgConnection};

/// 2026-10-22T13:00:00Z, the start of an hour.
const H0: u64 = 1_792_677_600_000_000_000;
const HOUR: u64 = 3_600_000_000_000;
const SECOND: u64 = 1_000_000_000;

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
        let schema = format!("toplevel_{}", uuid::Uuid::new_v4().simple());
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
    dir: tempfile::TempDir,
}

async fn fixture() -> Option<Fixture> {
    fixture_of(NewTenant::with_defaults("acme")).await
}

async fn fixture_of(tenant: NewTenant) -> Option<Fixture> {
    let db = TestDb::new().await?;
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("bucket")).unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path().join("bucket")).unwrap());
    let tenant = db.catalog.create_tenant(&tenant).await.unwrap().id;
    Some(Fixture {
        db,
        tenant,
        store,
        dir,
    })
}

fn querier(f: &Fixture, limits: QueryLimits) -> Querier {
    Querier::new(f.db.catalog.clone(), f.store.clone(), None, limits).unwrap()
}

fn uncached() -> QueryLimits {
    QueryLimits {
        recent_keys_ttl: Duration::ZERO,
        ..QueryLimits::default()
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

/// `(service, trace, span id, parent id, name, start)`.
type SpanRow<'a> = (&'a str, u8, u8, Option<u8>, &'a str, u64);

/// `spans`, one resource each.
fn request(spans: &[SpanRow<'_>]) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: spans
            .iter()
            .map(|(service, trace, id, parent, name, start)| ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", service)],
                    ..Resource::default()
                }),
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: vec![*trace; 16],
                        span_id: vec![*id; 8],
                        parent_span_id: parent.map(|p| vec![p; 8]).unwrap_or_default(),
                        name: (*name).into(),
                        start_time_unix_nano: *start,
                        end_time_unix_nano: start + 1_000,
                        ..Span::default()
                    }],
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            })
            .collect(),
    }
}

fn batch(request: &ExportTraceServiceRequest) -> RecordBatch {
    let mut builder = SpanBatchBuilder::new(Limits::default());
    builder.append_request(request).unwrap();
    builder.finish().unwrap()
}

/// Writes and registers one L0 span file of `request`.
async fn write_l0(f: &Fixture, request: &ExportTraceServiceRequest) {
    let prepared = flush::prepare(
        f.tenant,
        &Path::from(""),
        FileId::new(),
        &[batch(request)],
        fs_ingest::record::FileLayout::current(flush::DEFAULT_ROW_GROUP_ROWS),
    )
    .unwrap();
    flush::upload(f.store.as_ref(), &prepared).await.unwrap();
    flush::upload_index(f.store.as_ref(), &prepared)
        .await
        .unwrap();
    f.db.catalog.register_file(&prepared.file).await.unwrap();
}

/// Registers the compacted contribution of a pass over `request`.
async fn write_derived(f: &Fixture, request: &ExportTraceServiceRequest) {
    let mut operations = TopLevelOperations::default();
    operations
        .absorb(&batch(request), usize::MAX, usize::MAX)
        .unwrap();
    operations.finish();
    let bytes = fs_schema::top_level_operations::encode(&operations).unwrap();
    let id = FileId::new();
    let path = format!(
        "t/{}/traces/derived/top_level_operations/dt=2026-10-22/hr=13/{}.parquet",
        f.tenant.0, id.0
    );
    f.store
        .put(&Path::from(path.as_str()), bytes.clone().into())
        .await
        .unwrap();
    let (first, last) = operations.range().unwrap();
    f.db.catalog
        .register_file(&NewFile {
            id,
            origin: id,
            tenant_id: f.tenant,
            signal: Signal::TopLevelOperations,
            level: Level::L1,
            path,
            size_bytes: i64::try_from(bytes.len()).unwrap(),
            row_count: i64::try_from(operations.len()).unwrap(),
            range: TimeRange {
                start: chrono::DateTime::from_timestamp_nanos(first),
                end: chrono::DateTime::from_timestamp_nanos(last),
            },
            services: vec![],
            summary: None,
            content_sha256: None,
            usage_accounted: true,
        })
        .await
        .unwrap();
}

/// `(service, name, last_seen)` that `sql` over `top_level_operations` answers.
async fn listed(
    q: &Querier,
    tenant: TenantId,
    sql: &str,
) -> Result<Vec<(String, String, i64)>, QueryError> {
    let batches = q.collect(tenant, sql).await?;
    let mut out = Vec::new();
    for batch in &batches {
        let services = batch.column(0).as_string::<i32>();
        let names = batch.column(1).as_string::<i32>();
        let seen = batch
            .column(2)
            .as_primitive::<arrow::datatypes::TimestampNanosecondType>();
        for row in 0..batch.num_rows() {
            out.push((
                services.value(row).to_owned(),
                names.value(row).to_owned(),
                seen.value(row),
            ));
        }
    }
    out.sort();
    Ok(out)
}

/// The query error a table raised, as Flight finds it under DataFusion's.
fn raised(err: &QueryError) -> &QueryError {
    match err {
        QueryError::DataFusion(datafusion::error::DataFusionError::External(inner)) => {
            inner.downcast_ref::<QueryError>().unwrap_or(err)
        }
        other => other,
    }
}

const ALL: &str = "select service_name, name, last_seen from top_level_operations";

fn at(t: u64) -> i64 {
    i64::try_from(t).unwrap()
}

#[tokio::test]
async fn a_child_of_another_service_in_l0_files_is_listed_before_compaction() {
    let Some(f) = fixture().await else { return };
    write_l0(
        &f,
        &request(&[("worker", 1, 2, Some(1), "consume", H0 + 2 * SECOND)]),
    )
    .await;
    write_l0(
        &f,
        &request(&[
            ("api", 1, 1, None, "GET /", H0 + SECOND),
            ("api", 1, 3, Some(1), "query", H0 + 3 * SECOND),
        ]),
    )
    .await;
    let rows = listed(&querier(&f, uncached()), f.tenant, ALL)
        .await
        .unwrap();
    assert_eq!(
        rows,
        [
            ("api".to_owned(), "GET /".to_owned(), at(H0 + SECOND)),
            (
                "worker".to_owned(),
                "consume".to_owned(),
                at(H0 + 2 * SECOND)
            ),
        ],
        "the child's file came first; a child of its own service is not listed"
    );
    f.db.finish().await;
}

#[tokio::test]
async fn the_compacted_table_and_recent_rows_union_by_latest_start() {
    let Some(f) = fixture().await else { return };
    write_derived(
        &f,
        &request(&[
            ("api", 1, 1, None, "GET /", H0 + SECOND),
            ("api", 1, 2, None, "compacted only", H0 + 2 * SECOND),
            ("web", 1, 3, None, "newer compacted", H0 + 3 * SECOND),
        ]),
    )
    .await;
    write_l0(
        &f,
        &request(&[
            ("api", 2, 1, None, "GET /", H0 + HOUR + SECOND),
            ("web", 2, 3, None, "newer compacted", H0 - HOUR),
        ]),
    )
    .await;
    let rows = listed(&querier(&f, uncached()), f.tenant, ALL)
        .await
        .unwrap();
    assert_eq!(
        rows,
        [
            ("api".to_owned(), "GET /".to_owned(), at(H0 + HOUR + SECOND)),
            (
                "api".to_owned(),
                "compacted only".to_owned(),
                at(H0 + 2 * SECOND)
            ),
            (
                "web".to_owned(),
                "newer compacted".to_owned(),
                at(H0 + 3 * SECOND)
            ),
        ]
    );
    f.db.finish().await;
}

#[tokio::test]
async fn a_parent_in_another_hour_does_not_make_a_boundary() {
    let Some(f) = fixture().await else { return };
    write_l0(
        &f,
        &request(&[
            ("api", 1, 1, None, "GET /", H0 + HOUR - SECOND),
            ("worker", 1, 2, Some(1), "late", H0 + HOUR + SECOND),
            ("api", 2, 1, None, "GET /", H0 + HOUR + 2 * SECOND),
            ("worker", 2, 2, Some(1), "timely", H0 + HOUR + 3 * SECOND),
        ]),
    )
    .await;
    let names: Vec<String> = listed(&querier(&f, uncached()), f.tenant, ALL)
        .await
        .unwrap()
        .into_iter()
        .map(|(_, name, _)| name)
        .collect();
    assert_eq!(
        names,
        ["GET /", "timely"],
        "as the compactor resolves each hour on its own"
    );
    f.db.finish().await;
}

#[tokio::test]
async fn filters_apply_after_the_parents_are_resolved() {
    let Some(f) = fixture().await else { return };
    write_l0(
        &f,
        &request(&[
            ("api", 1, 1, None, "GET /", H0 + SECOND),
            ("worker", 1, 2, Some(1), "consume", H0 + 10 * SECOND),
        ]),
    )
    .await;
    let q = querier(&f, uncached());
    let by_service = listed(
        &q,
        f.tenant,
        &format!("{ALL} where service_name = 'worker'"),
    )
    .await
    .unwrap();
    assert_eq!(
        by_service,
        [(
            "worker".to_owned(),
            "consume".to_owned(),
            at(H0 + 10 * SECOND)
        )]
    );
    let since = format!(
        "{ALL} where last_seen >= to_timestamp_nanos({})",
        H0 + 5 * SECOND
    );
    let recent = listed(&q, f.tenant, &since).await.unwrap();
    assert_eq!(
        recent,
        [(
            "worker".to_owned(),
            "consume".to_owned(),
            at(H0 + 10 * SECOND)
        )],
        "the parent is before the window, the child still finds it"
    );
    f.db.finish().await;
}

#[tokio::test]
async fn a_span_id_of_another_trace_is_not_a_parent() {
    let Some(f) = fixture().await else { return };
    write_l0(
        &f,
        &request(&[
            ("api", 1, 1, None, "GET /", H0 + SECOND),
            ("worker", 2, 1, None, "root", H0 + SECOND),
            ("worker", 2, 2, Some(1), "child", H0 + 2 * SECOND),
        ]),
    )
    .await;
    let names: Vec<String> = listed(&querier(&f, uncached()), f.tenant, ALL)
        .await
        .unwrap()
        .into_iter()
        .map(|(_, name, _)| name)
        .collect();
    assert_eq!(names, ["GET /", "root"]);
    f.db.finish().await;
}

#[tokio::test]
async fn a_listing_without_a_hot_window_it_needs_fails_unavailable() {
    let Some(f) = fixture().await else { return };
    write_l0(&f, &request(&[("api", 1, 1, None, "GET /", H0 + SECOND)])).await;
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = silent.local_addr().unwrap().to_string();
    drop(silent);
    f.db.catalog
        .heartbeat("ingester-gone", &address)
        .await
        .unwrap();
    let err = listed(&querier(&f, uncached()), f.tenant, ALL)
        .await
        .unwrap_err();
    assert!(
        matches!(raised(&err), QueryError::HotUnavailable(_)),
        "{err}"
    );
    f.db.finish().await;
}

#[tokio::test]
async fn a_listing_over_the_bytes_budget_fails_whole() {
    let tenant = NewTenant {
        bytes_budget: 1,
        ..NewTenant::with_defaults("acme")
    };
    let Some(f) = fixture_of(tenant).await else {
        return;
    };
    write_l0(&f, &request(&[("api", 1, 1, None, "GET /", H0 + SECOND)])).await;
    let err = listed(&querier(&f, uncached()), f.tenant, ALL)
        .await
        .unwrap_err();
    assert!(
        matches!(raised(&err), QueryError::OverBudget { .. }),
        "{err}"
    );
    f.db.finish().await;
}

#[tokio::test]
async fn a_listing_larger_than_the_memory_pool_fails_resources_exhausted() {
    let Some(f) = fixture().await else { return };
    let names: Vec<String> = (0..400).map(|i| format!("operation-{i:04}")).collect();
    let spans: Vec<SpanRow<'_>> = names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            (
                "api",
                u8::try_from(i % 250).unwrap(),
                u8::try_from(i / 250 + 1).unwrap(),
                None,
                name.as_str(),
                H0 + SECOND,
            )
        })
        .collect();
    write_l0(&f, &request(&spans)).await;
    let q = querier(
        &f,
        QueryLimits {
            memory_bytes: 16 * 1024,
            ..uncached()
        },
    );
    let err = listed(&q, f.tenant, ALL).await.unwrap_err();
    let exhausted = match &err {
        QueryError::DataFusion(e) => matches!(
            e.find_root(),
            datafusion::error::DataFusionError::ResourcesExhausted(_)
        ),
        _ => false,
    };
    assert!(
        exhausted && err.to_string().contains("top_level_operations"),
        "refused by the listing's own reservation: {err}"
    );
    assert!(
        err.to_string().contains("top_level_operations"),
        "refused by the listing's own reservation: {err}"
    );
    assert_eq!(q.memory_reserved(), 0);
    f.db.finish().await;
}

#[tokio::test]
async fn the_listings_reservation_lives_as_long_as_its_batch() {
    let Some(f) = fixture().await else { return };
    write_l0(&f, &request(&[("api", 1, 1, None, "GET /", H0 + SECOND)])).await;
    let q = querier(&f, uncached());
    let (stream, _) = q.execute(f.tenant, ALL).await.unwrap();
    let batches: Vec<RecordBatch> = futures_util::TryStreamExt::try_collect(stream)
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert!(q.memory_reserved() > 0, "held while the batch is");
    drop(batches);
    assert_eq!(q.memory_reserved(), 0);
    f.db.finish().await;
}

#[tokio::test]
async fn rows_only_the_hot_window_holds_are_listed_once() {
    use fs_ingest::{IngestConfig, Ingester};
    use fs_wal::{Wal, WalConfig};
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
    use tonic::metadata::MetadataValue;

    let Some(f) = fixture().await else { return };
    let (ingest_token, _) =
        f.db.catalog
            .create_token(f.tenant, "collector", fs_catalog::TokenScope::Ingest)
            .await
            .unwrap();
    let config = IngestConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        hot_listen: Some("127.0.0.1:0".parse().unwrap()),
        flush_interval: Duration::from_secs(3600),
        persisted_retain: Duration::from_secs(60),
        ..IngestConfig::default()
    };
    let wal = Wal::open(WalConfig::new(f.dir.path().join("wal"))).unwrap();
    let ingester = Arc::new(
        Ingester::new(
            config,
            f.db.catalog.clone(),
            f.store.clone(),
            Path::from(""),
            wal,
        )
        .unwrap(),
    );
    let (shutdown, rx) = tokio::sync::watch::channel(false);
    let (otlp_tx, otlp_rx) = tokio::sync::oneshot::channel();
    let (hot_tx, hot_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn({
        let ingester = ingester.clone();
        async move { ingester.run(rx, Some(otlp_tx), Some(hot_tx)).await }
    });
    let otlp_addr = otlp_rx.await.unwrap();
    let hot_addr = hot_rx.await.unwrap();
    f.db.catalog
        .heartbeat("ingester-test", &hot_addr.to_string())
        .await
        .unwrap();
    let now = u64::try_from(chrono::Utc::now().timestamp_nanos_opt().unwrap()).unwrap();
    let hour = now - now % HOUR;
    let (parent, child) = (hour + SECOND, hour + 2 * SECOND);
    let mut client = TraceServiceClient::connect(format!("http://{otlp_addr}"))
        .await
        .unwrap();
    let mut export = tonic::Request::new(request(&[
        ("api", 7, 1, None, "GET /", parent),
        ("worker", 7, 2, Some(1), "consume", child),
    ]));
    export.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {ingest_token}")).unwrap(),
    );
    client.export(export).await.unwrap();

    let q = querier(&f, uncached());
    let want = [
        ("api".to_owned(), "GET /".to_owned(), at(parent)),
        ("worker".to_owned(), "consume".to_owned(), at(child)),
    ];
    assert_eq!(
        listed(&q, f.tenant, ALL).await.unwrap(),
        want,
        "in the ingester alone"
    );
    ingester.flush_all().await;
    assert_eq!(
        listed(&q, f.tenant, ALL).await.unwrap(),
        want,
        "flushed and still served, listed once"
    );

    shutdown.send(true).unwrap();
    task.await.unwrap().unwrap();
    f.db.finish().await;
}
