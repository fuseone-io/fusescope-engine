//! SQL over files written by the ingest flush path, through the querier and
//! through Flight SQL. Needs `FS_TEST_DATABASE_URL`; skips otherwise.
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
use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use datafusion::logical_expr::{col, lit};
use datafusion::scalar::ScalarValue;
use fs_catalog::{Catalog, NewTenant, TenantId, TokenScope};
use fs_ingest::flush;
use fs_query::flight::FlightSql;
use fs_query::{Querier, QueryError, QueryLimits};
use fs_schema::limits::Limits;
use fs_schema::spans::SpanBatchBuilder;
use futures_util::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use sqlx::{Connection, Executor, PgConnection};
use tonic::Code;
use tonic::transport::Channel;

/// 2026-10-22T14:17:03Z
const T0: u64 = 1_792_678_623_000_000_000;
const HOUR: u64 = 3_600_000_000_000;

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
        let schema = format!("qry_{}", uuid::Uuid::new_v4().simple());
        let mut admin = PgConnection::connect(&admin_url)
            .await
            .expect("connect to test database");
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

fn request(service: &str, trace: u8, start: u64, spans: u8) -> ExportTraceServiceRequest {
    let spans = (0..spans)
        .map(|i| Span {
            trace_id: vec![trace; 16],
            span_id: vec![i + 1; 8],
            name: format!("op-{i}"),
            kind: 2,
            start_time_unix_nano: start + u64::from(i) * 1_000_000,
            end_time_unix_nano: start + u64::from(i) * 1_000_000 + 250_000,
            attributes: vec![kv(
                "http.request.method",
                if i % 2 == 0 { "GET" } else { "POST" },
            )],
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

/// Writes one L0 file for `request` and registers it, returning its size.
async fn write_file(
    catalog: &Catalog,
    store: &LocalFileSystem,
    tenant: TenantId,
    request: &ExportTraceServiceRequest,
) -> i64 {
    let mut builder = SpanBatchBuilder::new(Limits::default());
    builder.append_request(request).unwrap();
    let batch = builder.finish().unwrap();
    let prepared =
        flush::prepare(tenant, &Path::from(""), fs_catalog::FileId::new(), &[batch]).unwrap();
    flush::upload(store, &prepared).await.unwrap();
    catalog.register_file(&prepared.file).await.unwrap();
    prepared.file.size_bytes
}

struct Fixture {
    db: TestDb,
    tenant: TenantId,
    other_tenant: TenantId,
    querier: Querier,
    dir: tempfile::TempDir,
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
    let other_tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("other"))
        .await
        .unwrap()
        .id;

    // Three files: api at hour 0, worker at hour 0, api at hour 2.
    write_file(&db.catalog, &store, tenant, &request("api", 1, T0, 10)).await;
    write_file(
        &db.catalog,
        &store,
        tenant,
        &request("worker", 2, T0 + 60_000_000_000, 5),
    )
    .await;
    write_file(
        &db.catalog,
        &store,
        tenant,
        &request("api", 3, T0 + 2 * HOUR, 7),
    )
    .await;
    // Another tenant's file must never show up.
    write_file(
        &db.catalog,
        &store,
        other_tenant,
        &request("api", 9, T0, 100),
    )
    .await;

    let limits = QueryLimits {
        timeout: Duration::from_secs(30),
        ..QueryLimits::default()
    };
    let querier = Querier::new(
        db.catalog.clone(),
        store,
        Some(dir.path().join("spill")),
        limits,
    )
    .unwrap();
    Some(Fixture {
        db,
        tenant,
        other_tenant,
        querier,
        dir,
    })
}

fn count(batches: &[RecordBatch]) -> i64 {
    batches
        .iter()
        .map(|b| b.column(0).as_primitive::<Int64Type>().value(0))
        .sum()
}

#[tokio::test]
async fn counts_filters_and_isolates_tenants() {
    let Some(f) = fixture().await else { return };
    let all = f
        .querier
        .collect(f.tenant, "select count(*) from spans")
        .await
        .unwrap();
    assert_eq!(count(&all), 22);

    let api = f
        .querier
        .collect(
            f.tenant,
            "select count(*) from spans where service_name = 'api'",
        )
        .await
        .unwrap();
    assert_eq!(count(&api), 17);

    let gets = f
        .querier
        .collect(
            f.tenant,
            "select count(*) from spans where attributes_string['http.request.method'] = 'GET'",
        )
        .await
        .unwrap();
    assert_eq!(count(&gets), 12, "even indexes are GET: 5 + 3 + 4");

    let by_service = f
        .querier
        .collect(f.tenant, "select service_name, count(*) as n from spans group by service_name order by service_name")
        .await
        .unwrap();
    let names = by_service[0]
        .column(0)
        .as_dictionary::<arrow::datatypes::Int32Type>();
    assert_eq!(names.len(), 2);

    let other = f
        .querier
        .collect(f.other_tenant, "select count(*) from spans")
        .await
        .unwrap();
    assert_eq!(
        count(&other),
        100,
        "the other tenant sees only its own file"
    );

    let unknown = f.querier.collect(TenantId::new(), "select 1").await;
    assert!(matches!(unknown, Err(QueryError::UnknownTenant)));
    f.db.finish().await;
}

#[tokio::test]
async fn prunes_files_by_time_and_service_before_reading() {
    let Some(f) = fixture().await else { return };
    let cache = f.querier.cache();
    let budget = u64::MAX;
    let table = fs_query::spans_table::SpansTable::new(
        f.tenant,
        cache,
        datafusion::execution::object_store::ObjectStoreUrl::parse(fs_query::STORE_URL).unwrap(),
        budget,
    );

    let (all, _) = table.plan_files(&[]).await.unwrap();
    assert_eq!(all.len(), 3);

    let (api, _) = table
        .plan_files(&[col("service_name").eq(lit("api"))])
        .await
        .unwrap();
    assert_eq!(api.len(), 2);

    let late = lit(ScalarValue::TimestampNanosecond(
        Some(i64::try_from(T0 + HOUR).unwrap()),
        Some("UTC".into()),
    ));
    let (recent, _) = table
        .plan_files(&[col("timestamp").gt(late)])
        .await
        .unwrap();
    assert_eq!(recent.len(), 1, "only the hour-2 file");

    // The same pruning through SQL gives the right answer.
    let n = f
        .querier
        .collect(f.tenant, "select count(*) from spans where timestamp > timestamp '2026-10-22T15:17:03Z' and service_name = 'api'")
        .await
        .unwrap();
    assert_eq!(count(&n), 7);
    f.db.finish().await;
}

#[tokio::test]
async fn trace_lookup_by_hex_id() {
    let Some(f) = fixture().await else { return };
    let hex = "02".repeat(16);
    let rows = f
        .querier
        .collect(f.tenant, &format!("select name, duration_nano from spans where trace_id = decode('{hex}', 'hex') order by timestamp"))
        .await
        .unwrap();
    let total: usize = rows.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 5, "trace 2 has 5 spans in the worker file");
    f.db.finish().await;
}

#[tokio::test]
async fn sandbox_refuses_writes_and_budget_refuses_big_scans() {
    let Some(f) = fixture().await else { return };
    for sql in [
        "create table t (a int)",
        "insert into spans select * from spans",
        "create external table x stored as parquet location '/etc/passwd'",
        "copy (select 1) to '/tmp/x.parquet'",
        "set datafusion.execution.batch_size = 1",
        "drop table spans",
    ] {
        let err = f.querier.collect(f.tenant, sql).await.unwrap_err();
        assert!(
            matches!(err, QueryError::NotAllowed(_) | QueryError::DataFusion(_)),
            "{sql} must be refused, got {err}"
        );
        assert!(!matches!(err, QueryError::TooManyRows(_)));
    }
    // Table functions are gone, so a path can never be named.
    let err = f
        .querier
        .collect(f.tenant, "select * from read_parquet('/etc/passwd')")
        .await
        .unwrap_err();
    assert!(matches!(err, QueryError::DataFusion(_)), "{err}");

    // A tiny budget refuses the full scan but still allows a pruned one.
    sqlx::query("update engine_tenant set bytes_budget = 1 where id = $1")
        .bind(f.tenant)
        .execute(f.db.catalog.pool())
        .await
        .unwrap();
    let err = f
        .querier
        .collect(f.tenant, "select count(*) from spans")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("over the budget"), "{err}");
    f.db.finish().await;
}

#[tokio::test]
async fn row_limit_is_enforced() {
    let Some(mut f) = fixture().await else { return };
    let limits = QueryLimits {
        max_rows: 10,
        timeout: Duration::from_secs(30),
        ..QueryLimits::default()
    };
    f.querier = Querier::new(
        f.db.catalog.clone(),
        Arc::new(LocalFileSystem::new_with_prefix(f.dir.path().join("bucket")).unwrap()),
        None,
        limits,
    )
    .unwrap();
    let err = f
        .querier
        .collect(f.tenant, "select * from spans")
        .await
        .unwrap_err();
    assert!(matches!(err, QueryError::TooManyRows(10)), "{err}");
    let ok = f
        .querier
        .collect(f.tenant, "select * from spans limit 10")
        .await
        .unwrap();
    assert_eq!(ok.iter().map(RecordBatch::num_rows).sum::<usize>(), 10);
    f.db.finish().await;
}

#[tokio::test]
async fn flight_sql_round_trip_with_query_token() {
    let Some(f) = fixture().await else { return };
    let (token, _) =
        f.db.catalog
            .create_token(f.tenant, "backend", TokenScope::Query)
            .await
            .unwrap();
    let (ingest_token, _) =
        f.db.catalog
            .create_token(f.tenant, "collector", TokenScope::Ingest)
            .await
            .unwrap();

    let service = FlightSql::new(f.querier.clone(), f.db.catalog.clone());
    let (shutdown, rx) = tokio::sync::watch::channel(false);
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(service.serve("127.0.0.1:0".parse().unwrap(), rx, Some(bound_tx)));
    let addr = tokio::time::timeout(Duration::from_secs(5), bound_rx)
        .await
        .unwrap()
        .unwrap();

    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightSqlServiceClient::new(channel.clone());
    client.set_token(token.clone());
    let info = client
        .execute(
            "select service_name, count(*) as n from spans group by service_name order by n desc"
                .into(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(info.endpoint.len(), 1);
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let batches: Vec<RecordBatch> = client
        .do_get(ticket)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 2);
    let first_n = batches[0].column(1).as_primitive::<Int64Type>().value(0);
    assert_eq!(first_n, 17, "api first");

    // Planning errors surface at GetFlightInfo with a useful status.
    let err = client
        .execute("insert into spans select * from spans".into(), None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("only SELECT"), "{err}");

    // An ingest token cannot query.
    let mut wrong = FlightSqlServiceClient::new(channel.clone());
    wrong.set_token(ingest_token);
    let err = wrong.execute("select 1".into(), None).await.unwrap_err();
    assert!(
        matches!(err, arrow_flight::error::FlightError::Tonic(ref status) if status.code() == Code::Unauthenticated),
        "{err}"
    );

    // No token at all.
    let mut anon = FlightSqlServiceClient::new(channel);
    let err = anon.execute("select 1".into(), None).await.unwrap_err();
    assert!(
        matches!(err, arrow_flight::error::FlightError::Tonic(ref status) if status.code() == Code::Unauthenticated),
        "{err}"
    );

    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    f.db.finish().await;
}

/// Live ingester plus querier in one process: rows are visible before any
/// flush, never duplicated across a flush, and a dead ingester makes the
/// result partial instead of failing it.
#[tokio::test]
async fn hot_window_merges_unflushed_rows_without_duplicates() {
    use fs_ingest::{IngestConfig, Ingester};
    use fs_wal::{Wal, WalConfig};
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
    use tonic::metadata::MetadataValue;

    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("bucket")).unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path().join("bucket")).unwrap());
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let (ingest_token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();

    // Ingester with the hot window on a random port, long flush interval and a
    // short persisted retention so the test can watch the hand-over.
    let config = IngestConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        hot_listen: Some("127.0.0.1:0".parse().unwrap()),
        flush_interval: Duration::from_secs(3600),
        persisted_retain: Duration::from_secs(2),
        ..IngestConfig::default()
    };
    let wal = Wal::open(WalConfig::new(dir.path().join("wal"))).unwrap();
    let ingester = Arc::new(
        Ingester::new(
            config,
            db.catalog.clone(),
            store.clone(),
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
    let otlp_addr = tokio::time::timeout(Duration::from_secs(5), otlp_rx)
        .await
        .unwrap()
        .unwrap();
    let hot_addr = tokio::time::timeout(Duration::from_secs(5), hot_rx)
        .await
        .unwrap()
        .unwrap();
    db.catalog
        .heartbeat("ingester-test", &hot_addr.to_string())
        .await
        .unwrap();

    // Spans timestamped now, so they fall inside the recent window.
    let now_ns = u64::try_from(chrono::Utc::now().timestamp_nanos_opt().unwrap()).unwrap();
    let mut client = TraceServiceClient::connect(format!("http://{otlp_addr}"))
        .await
        .unwrap();
    let mut req = tonic::Request::new(request("api", 5, now_ns - 1_000_000_000, 12));
    req.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {ingest_token}")).unwrap(),
    );
    client.export(req).await.unwrap();

    let limits = QueryLimits {
        timeout: Duration::from_secs(30),
        ..QueryLimits::default()
    };
    let querier = Querier::new(db.catalog.clone(), store.clone(), None, limits).unwrap();
    let sql = "select count(*) from spans where timestamp > now() - interval '1 hour'";

    // 1. Nothing flushed: rows come from the ingester only.
    let (rows, warnings) = querier.collect_with_warnings(tenant, sql).await.unwrap();
    assert_eq!(count(&rows), 12);
    assert!(warnings.is_empty(), "{warnings:?}");

    // A trace lookup reaches the hot rows through the typed predicate.
    let lookup =
        |hex: String| format!("select count(*) from spans where trace_id = decode('{hex}', 'hex')");
    let (rows, warnings) = querier
        .collect_with_warnings(tenant, &lookup("05".repeat(16)))
        .await
        .unwrap();
    assert_eq!(count(&rows), 12);
    assert!(warnings.is_empty(), "{warnings:?}");
    let (rows, _) = querier
        .collect_with_warnings(tenant, &lookup("06".repeat(16)))
        .await
        .unwrap();
    assert_eq!(count(&rows), 0);

    // Dictionary columns travel hydrated over Flight; grouping and sorting on
    // them must still work on hot rows alone and mixed with files.
    let grouped = querier
        .collect(tenant, "select service_name, count(*) as n, max(http_method) as m from spans where timestamp > now() - interval '1 hour' group by service_name order by service_name")
        .await
        .unwrap();
    assert_eq!(grouped.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let listed = querier
        .collect(tenant, "select timestamp, name, service_name, http_method from spans where timestamp > now() - interval '1 hour' order by timestamp limit 5")
        .await
        .unwrap();
    assert_eq!(listed.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);

    // 2. Flushed and still retained in memory: the file is excluded, no duplicates.
    ingester.flush_all().await;
    querier
        .cache()
        .invalidate(tenant, fs_catalog::Signal::Traces)
        .await;
    let rows = querier.collect(tenant, sql).await.unwrap();
    assert_eq!(
        count(&rows),
        12,
        "file plus retained rows must not double count"
    );

    // Fresh hot rows so files and hot batches are unioned in one plan.
    let mut again = tonic::Request::new(request("worker", 6, now_ns - 500_000_000, 3));
    again.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {ingest_token}")).unwrap(),
    );
    client.export(again).await.unwrap();
    let grouped = querier
        .collect(tenant, "select service_name, count(*) as n from spans where timestamp > now() - interval '1 hour' group by service_name order by service_name")
        .await
        .unwrap();
    assert_eq!(
        grouped.iter().map(RecordBatch::num_rows).sum::<usize>(),
        2,
        "api from the file, worker from the hot window"
    );

    // 3. Retention passed: rows come from the files only.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    ingester.flush_all().await;
    let rows = querier.collect(tenant, sql).await.unwrap();
    assert_eq!(count(&rows), 15);

    // 4. A query outside the recent window never fans out.
    let old = querier
        .collect_with_warnings(
            tenant,
            "select count(*) from spans where timestamp < timestamp '2020-01-01T00:00:00Z'",
        )
        .await
        .unwrap();
    assert_eq!(count(&old.0), 0);

    // 5. Ingester gone but heartbeat still fresh: partial result, not an error.
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // No notification follower runs in this test, so refresh the file list by hand.
    querier
        .cache()
        .invalidate(tenant, fs_catalog::Signal::Traces)
        .await;
    let (rows, warnings) = querier.collect_with_warnings(tenant, sql).await.unwrap();
    assert_eq!(count(&rows), 15, "file rows survive the ingester");
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("ingester-test"));

    db.finish().await;
}
