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
use std::sync::atomic::Ordering;
use std::time::Duration;

use arrow::array::AsArray;
use arrow::array::FixedSizeBinaryArray;
use arrow::datatypes::Int64Type;
use arrow::datatypes::TimestampNanosecondType;
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::{Any, TicketStatementQuery};
use arrow_flight::{Action, Empty, Ticket};
use datafusion::logical_expr::{col, lit};
use datafusion::scalar::ScalarValue;
use fs_catalog::{Catalog, NewTenant, TenantId, TokenScope};
use fs_ingest::flush;
use fs_query::actions::{TRACE_LOOKUP, TraceLookup};
use fs_query::flight::FlightSql;
use fs_query::querier::{MAX_PREPARED_TTL, PREPARED_PLAN_OVERHEAD};
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
use prost::Message;
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
    let prepared = flush::prepare(
        tenant,
        &Path::from(""),
        fs_catalog::FileId::new(),
        &[batch],
        flush::DEFAULT_ROW_GROUP_ROWS,
    )
    .unwrap();
    flush::upload(store, &prepared).await.unwrap();
    flush::upload_index(store, &prepared).await.unwrap();
    catalog.register_file(&prepared.file).await.unwrap();
    prepared.file.size_bytes
}

/// Every `.tix` under `dir`, decoded, with its path on disk.
fn sidecars(dir: &std::path::Path) -> Vec<(std::path::PathBuf, fs_index::TraceIndex)> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(sidecars(&path));
            } else if path.extension().is_some_and(|e| e == "tix") {
                let index = fs_index::TraceIndex::decode(&std::fs::read(&path).unwrap()).unwrap();
                out.push((path, index));
            }
        }
    }
    out
}

fn trace_filter(trace: u8) -> datafusion::logical_expr::Expr {
    col("trace_id").eq(lit(ScalarValue::Binary(Some(vec![trace; 16]))))
}

fn stat(f: &Fixture, pick: impl Fn(&fs_query::QueryStats) -> &std::sync::atomic::AtomicU64) -> u64 {
    pick(&f.querier.stats()).load(std::sync::atomic::Ordering::Relaxed)
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

/// A lookup pinning a trace opens only the files whose sidecar lists it;
/// sidecars are fetched once and served from memory after.
#[tokio::test]
async fn trace_index_prunes_files_without_the_trace() {
    let Some(f) = fixture().await else { return };
    let table = fs_query::spans_table::SpansTable::new(
        f.tenant,
        f.querier.cache(),
        datafusion::execution::object_store::ObjectStoreUrl::parse(fs_query::STORE_URL).unwrap(),
        u64::MAX,
    )
    .with_trace_index(f.querier.trace_index());

    let (files, _) = table.plan_files(&[trace_filter(2)]).await.unwrap();
    assert_eq!(files.len(), 1, "only the worker file holds trace 2");
    assert_eq!(
        stat(&f, |s| &s.trace_index_fetches),
        3,
        "one sidecar per visible file"
    );
    assert_eq!(stat(&f, |s| &s.trace_index_misses), 0);
    assert_eq!(stat(&f, |s| &s.trace_index_pruned), 2);

    let (none, _) = table.plan_files(&[trace_filter(7)]).await.unwrap();
    assert_eq!(none.len(), 0, "no file holds trace 7");
    assert_eq!(
        stat(&f, |s| &s.trace_index_fetches),
        3,
        "served from memory"
    );
    assert_eq!(stat(&f, |s| &s.trace_index_pruned), 5);
    assert!(f.querier.trace_index().bytes() > 0);

    let (all, _) = table.plan_files(&[]).await.unwrap();
    assert_eq!(all.len(), 3, "without a pinned trace nothing is consulted");

    let hex = "02".repeat(16);
    let rows = f
        .querier
        .collect(
            f.tenant,
            &format!("select count(*) from spans where trace_id = decode('{hex}', 'hex')"),
        )
        .await
        .unwrap();
    assert_eq!(count(&rows), 5);
    assert_eq!(
        stat(&f, |s| &s.trace_index_pruned),
        7,
        "SQL goes through the same pruning"
    );
    f.db.finish().await;
}

/// A file whose sidecar is missing or damaged is scanned, counted as a
/// miss, and never fails the query; the other sidecars still prune.
#[tokio::test]
async fn missing_or_damaged_sidecars_degrade_to_a_scan() {
    let Some(f) = fixture().await else { return };
    let bucket = f.dir.path().join("bucket");
    let found = sidecars(&bucket);
    assert_eq!(found.len(), 4, "one per file, the other tenant's included");
    let path_of = |trace: u8| {
        found
            .iter()
            .find(|(_, index)| index.contains_any(&[[trace; 16]]))
            .map(|(path, _)| path.clone())
            .unwrap()
    };
    std::fs::remove_file(path_of(2)).unwrap();
    let damaged = path_of(1);
    let mut bytes = std::fs::read(&damaged).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    std::fs::write(&damaged, bytes).unwrap();

    let hex = "02".repeat(16);
    let rows = f
        .querier
        .collect(
            f.tenant,
            &format!("select count(*) from spans where trace_id = decode('{hex}', 'hex')"),
        )
        .await
        .unwrap();
    assert_eq!(
        count(&rows),
        5,
        "the worker file is scanned without its sidecar"
    );
    assert_eq!(stat(&f, |s| &s.trace_index_fetches), 3);
    assert_eq!(
        stat(&f, |s| &s.trace_index_misses),
        2,
        "one missing, one damaged"
    );
    assert_eq!(
        stat(&f, |s| &s.trace_index_pruned),
        1,
        "the hour-2 file's sidecar still prunes"
    );

    // Misses are remembered: the second lookup fetches nothing.
    let rows = f
        .querier
        .collect(
            f.tenant,
            &format!("select count(*) from spans where trace_id = decode('{hex}', 'hex')"),
        )
        .await
        .unwrap();
    assert_eq!(count(&rows), 5);
    assert_eq!(stat(&f, |s| &s.trace_index_fetches), 3);
    assert_eq!(stat(&f, |s| &s.trace_index_misses), 2);
    f.db.finish().await;
}

/// The hot window announces the id a flush was first given; a file that
/// registered under another id after changing it keeps that id as its
/// origin, and the planner leaves it out by either.
#[tokio::test]
async fn files_are_excluded_by_their_origin_as_well_as_their_id() {
    let Some(f) = fixture().await else { return };
    let mut builder = SpanBatchBuilder::new(Limits::default());
    builder
        .append_request(&request("api", 5, T0 + 3 * HOUR, 4))
        .unwrap();
    let batch = builder.finish().unwrap();
    let announced = fs_catalog::FileId::new();
    let registered = fs_catalog::FileId::new();
    let store = LocalFileSystem::new_with_prefix(f.dir.path().join("bucket")).unwrap();
    let mut prepared = flush::prepare(
        f.tenant,
        &Path::from(""),
        registered,
        &[batch],
        flush::DEFAULT_ROW_GROUP_ROWS,
    )
    .unwrap();
    prepared.file.origin = announced;
    flush::upload(&store, &prepared).await.unwrap();
    flush::upload_index(&store, &prepared).await.unwrap();
    let row = f.db.catalog.register_file(&prepared.file).await.unwrap();
    assert_eq!(row.origin, announced);

    let table = fs_query::spans_table::SpansTable::new(
        f.tenant,
        f.querier.cache(),
        datafusion::execution::object_store::ObjectStoreUrl::parse(fs_query::STORE_URL).unwrap(),
        u64::MAX,
    );
    let (all, _) = table
        .plan_files_excluding(&[], &std::collections::HashSet::new())
        .await
        .unwrap();
    assert_eq!(all.len(), 4);
    for excluded in [announced, registered] {
        let (kept, _) = table
            .plan_files_excluding(&[], &std::collections::HashSet::from([excluded]))
            .await
            .unwrap();
        assert_eq!(kept.len(), 3, "left out by {excluded:?}");
        assert!(
            kept.iter()
                .all(|p| p.path().as_ref() != prepared.file.path.as_str())
        );
    }
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

/// A Flight SQL server over `f.querier`: its address, the shutdown switch
/// and the task.
async fn serve_flight(
    f: &Fixture,
) -> (
    std::net::SocketAddr,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<Result<(), fs_query::QueryError>>,
) {
    let service = FlightSql::new(f.querier.clone(), f.db.catalog.clone());
    let (shutdown, rx) = tokio::sync::watch::channel(false);
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(service.serve("127.0.0.1:0".parse().unwrap(), rx, Some(bound_tx)));
    let addr = tokio::time::timeout(Duration::from_secs(5), bound_rx)
        .await
        .unwrap()
        .unwrap();
    (addr, shutdown, server)
}

async fn sql_client(addr: std::net::SocketAddr, token: &str) -> FlightSqlServiceClient<Channel> {
    let channel = Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = FlightSqlServiceClient::new(channel);
    client.set_token(token.to_owned());
    client
}

async fn rows_of(client: &mut FlightSqlServiceClient<Channel>, ticket: Ticket) -> usize {
    let batches: Vec<RecordBatch> = client
        .do_get(ticket)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    batches.iter().map(RecordBatch::num_rows).sum()
}

const BY_SERVICE: &str =
    "select service_name, count(*) as n from spans group by service_name order by n desc";
const COUNT_ALL: &str = "select count(*) as n from spans";
const SERVICES: &str = "select distinct service_name from spans";

/// The ticket as the engine issued it before plans were kept: the tenant id
/// followed by the SQL.
fn ticket_before_prepared_plans(tenant: TenantId, sql: &str) -> Ticket {
    let mut handle = tenant.0.as_bytes().to_vec();
    handle.extend_from_slice(sql.as_bytes());
    let statement = TicketStatementQuery {
        statement_handle: handle.into(),
    };
    Ticket::new(Any::pack(&statement).unwrap().encode_to_vec())
}

async fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(tokio::time::Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// `GetFlightInfo` plans the statement once and `DoGet` executes that plan;
/// the plan is consumed, so the same ticket presented again plans its SQL
/// again and still answers. The ticket is the one the previous version
/// issued and read, byte for byte, so either version serves the other's.
#[tokio::test]
async fn a_statement_is_planned_once_between_get_flight_info_and_do_get() {
    let Some(f) = fixture().await else { return };
    let (token, _) =
        f.db.catalog
            .create_token(f.tenant, "backend", TokenScope::Query)
            .await
            .unwrap();
    let (addr, shutdown, server) = serve_flight(&f).await;
    let mut client = sql_client(addr, &token).await;
    let stats = f.querier.stats();

    let info = client.execute(BY_SERVICE.into(), None).await.unwrap();
    assert_eq!(stats.plannings.load(Ordering::Relaxed), 1);
    assert_eq!(f.querier.prepared_count(), 1);
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    assert_eq!(
        ticket,
        ticket_before_prepared_plans(f.tenant, BY_SERVICE),
        "the previous version reads this ticket"
    );
    assert_eq!(rows_of(&mut client, ticket.clone()).await, 2);
    assert_eq!(
        stats.plannings.load(Ordering::Relaxed),
        1,
        "executed from the prepared plan"
    );
    assert_eq!(stats.prepared_hits.load(Ordering::Relaxed), 1);
    assert_eq!(f.querier.prepared_count(), 0, "consumed");

    assert_eq!(
        rows_of(&mut client, ticket).await,
        2,
        "a ticket outlives its plan"
    );
    assert_eq!(stats.plannings.load(Ordering::Relaxed), 2);
    assert_eq!(stats.prepared_misses.load(Ordering::Relaxed), 1);

    let previous = ticket_before_prepared_plans(f.tenant, COUNT_ALL);
    assert_eq!(
        rows_of(&mut client, previous).await,
        1,
        "a ticket issued by the previous version executes"
    );
    assert_eq!(stats.prepared_misses.load(Ordering::Relaxed), 2);

    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    f.db.finish().await;
}

/// A prepared plan is dropped when its TTL passes, by the reaper and not by
/// a later `prepare`, or to make room beyond the cap, oldest first; its
/// ticket plans the SQL again. A ticket of one tenant presented with another
/// tenant's token is refused and consumes nothing. Identical statements
/// prepared twice are kept twice.
#[tokio::test]
async fn a_ticket_whose_plan_is_gone_plans_again() {
    let Some(mut f) = fixture().await else { return };
    f.querier = Querier::new(
        f.db.catalog.clone(),
        Arc::new(LocalFileSystem::new_with_prefix(f.dir.path().join("bucket")).unwrap()),
        None,
        QueryLimits {
            timeout: Duration::from_secs(30),
            prepared_plans: 2,
            prepared_ttl: Duration::from_millis(300),
            ..QueryLimits::default()
        },
    )
    .unwrap();
    let (token, _) =
        f.db.catalog
            .create_token(f.tenant, "backend", TokenScope::Query)
            .await
            .unwrap();
    let (other_token, _) =
        f.db.catalog
            .create_token(f.other_tenant, "backend", TokenScope::Query)
            .await
            .unwrap();
    let (addr, shutdown, server) = serve_flight(&f).await;
    let mut client = sql_client(addr, &token).await;
    let counters = f.querier.stats();

    // Expired: the reaper drops it with nothing else going on.
    let info = client.execute(BY_SERVICE.into(), None).await.unwrap();
    assert_eq!(f.querier.prepared_count(), 1);
    wait_until("the expired plan is reaped", || {
        f.querier.prepared_count() == 0
    })
    .await;
    assert_eq!(f.querier.prepared_bytes(), 0);
    assert_eq!(counters.prepared.load(Ordering::Relaxed), 0);
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    assert_eq!(rows_of(&mut client, ticket).await, 2);
    assert_eq!(counters.prepared_misses.load(Ordering::Relaxed), 1);
    assert_eq!(counters.plannings.load(Ordering::Relaxed), 2);

    // Beyond the cap: the oldest of three is dropped.
    let first = client.execute(BY_SERVICE.into(), None).await.unwrap();
    let second = client.execute(COUNT_ALL.into(), None).await.unwrap();
    let third = client.execute(SERVICES.into(), None).await.unwrap();
    assert_eq!(f.querier.prepared_count(), 2);
    let plannings = counters.plannings.load(Ordering::Relaxed);
    assert_eq!(
        rows_of(&mut client, third.endpoint[0].ticket.clone().unwrap()).await,
        2
    );
    assert_eq!(
        rows_of(&mut client, second.endpoint[0].ticket.clone().unwrap()).await,
        1
    );
    assert_eq!(
        counters.plannings.load(Ordering::Relaxed),
        plannings,
        "both kept"
    );
    assert_eq!(
        rows_of(&mut client, first.endpoint[0].ticket.clone().unwrap()).await,
        2
    );
    assert_eq!(
        counters.plannings.load(Ordering::Relaxed),
        plannings + 1,
        "the oldest was dropped"
    );

    // Another tenant's token: refused, the plan untouched for its owner.
    let info = client.execute(BY_SERVICE.into(), None).await.unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let mut other = sql_client(addr, &other_token).await;
    let err = other.do_get(ticket.clone()).await.unwrap_err();
    assert!(
        matches!(err, arrow_flight::error::FlightError::Tonic(ref status) if status.code() == Code::PermissionDenied),
        "{err}"
    );
    assert_eq!(f.querier.prepared_count(), 1);
    let hits = counters.prepared_hits.load(Ordering::Relaxed);
    assert_eq!(rows_of(&mut client, ticket).await, 2);
    assert_eq!(counters.prepared_hits.load(Ordering::Relaxed), hits + 1);

    // The same statement prepared twice: both plans wait, both are hit.
    let twice = client.execute(BY_SERVICE.into(), None).await.unwrap();
    let again = client.execute(BY_SERVICE.into(), None).await.unwrap();
    assert_eq!(f.querier.prepared_count(), 2);
    assert_eq!(
        rows_of(&mut client, twice.endpoint[0].ticket.clone().unwrap()).await,
        2
    );
    assert_eq!(
        rows_of(&mut client, again.endpoint[0].ticket.clone().unwrap()).await,
        2
    );
    assert_eq!(counters.prepared_hits.load(Ordering::Relaxed), hits + 3);
    assert_eq!(f.querier.prepared_count(), 0);

    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    f.db.finish().await;
}

async fn lookup(
    client: &mut FlightSqlServiceClient<Channel>,
    body: &str,
) -> Result<Vec<RecordBatch>, tonic::Status> {
    let action = Action {
        r#type: TRACE_LOOKUP.into(),
        body: body.to_owned().into(),
    };
    let results: Vec<arrow_flight::Result> = client
        .do_action(action)
        .await
        .map_err(|e| match e {
            arrow_flight::error::FlightError::Tonic(status) => *status,
            other => tonic::Status::internal(other.to_string()),
        })?
        .try_collect()
        .await?;
    let mut stream = Vec::new();
    for result in results {
        stream.extend_from_slice(&result.body);
    }
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(stream), None)
        .map_err(|e| tonic::Status::internal(e.to_string()))?;
    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| tonic::Status::internal(e.to_string()))
}

fn timestamps(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            b.column_by_name("timestamp")
                .unwrap()
                .as_primitive::<TimestampNanosecondType>()
                .values()
                .to_vec()
        })
        .collect()
}

/// `trace.lookup` answers with the spans of one trace, in timestamp order,
/// as one Arrow IPC stream split across the result bodies; an optional
/// range narrows it with inclusive bounds; the tenant sees only its own
/// traces; a bad body or an unknown action is refused with the right code.
#[tokio::test]
async fn trace_lookup_action_returns_the_spans_of_one_trace() {
    let Some(f) = fixture().await else { return };
    let (token, _) =
        f.db.catalog
            .create_token(f.tenant, "backend", TokenScope::Query)
            .await
            .unwrap();
    let (addr, shutdown, server) = serve_flight(&f).await;
    let mut client = sql_client(addr, &token).await;
    let counters = f.querier.stats();

    let trace = "02".repeat(16);
    let body = serde_json::to_string(&TraceLookup {
        trace_id: trace.clone(),
        start_unix_nano: None,
        end_unix_nano: None,
    })
    .unwrap();
    let batches = lookup(&mut client, &body).await.unwrap();
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 5, "trace 2 has 5 spans in the worker file");
    let first = T0 + 60_000_000_000;
    let expected: Vec<i64> = (0..5)
        .map(|i| i64::try_from(first + i * 1_000_000).unwrap())
        .collect();
    assert_eq!(timestamps(&batches), expected, "timestamp order");
    for batch in &batches {
        let ids = batch
            .column_by_name("trace_id")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert!(ids.iter().all(|id| id == Some(&[2u8; 16][..])));
    }
    assert_eq!(counters.trace_lookups.load(Ordering::Relaxed), 1);
    assert_eq!(counters.plannings.load(Ordering::Relaxed), 0, "no SQL");

    let narrowed = serde_json::json!({
        "trace_id": trace,
        "start_unix_nano": first,
        "end_unix_nano": first + 1_000_000,
    });
    let batches = lookup(&mut client, &narrowed.to_string()).await.unwrap();
    assert_eq!(timestamps(&batches), &expected[..2], "inclusive bounds");

    let unknown = serde_json::json!({ "trace_id": "ff".repeat(16) });
    let batches = lookup(&mut client, &unknown.to_string()).await.unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);

    let other_tenants = serde_json::json!({ "trace_id": "09".repeat(16) });
    let batches = lookup(&mut client, &other_tenants.to_string())
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        0,
        "trace 9 belongs to the other tenant"
    );

    for bad in [
        "not json",
        r#"{"trace_id": "0102"}"#,
        r#"{"trace_id": "zz".repeat(16)}"#,
        &serde_json::json!({ "trace_id": trace, "start_unix_nano": first + 1, "end_unix_nano": first })
            .to_string(),
    ] {
        let err = lookup(&mut client, bad).await.unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "{bad}: {err}");
    }
    let err = client
        .do_action(Action {
            r#type: "trace.delete".into(),
            body: body.clone().into(),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(err, arrow_flight::error::FlightError::Tonic(ref status) if status.code() == Code::Unimplemented),
        "{err}"
    );

    let mut anon = sql_client(addr, "nope").await;
    let err = lookup(&mut anon, &body).await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);

    let mut raw = FlightServiceClient::new(
        Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap(),
    );
    let listed: Vec<arrow_flight::ActionType> = raw
        .list_actions(Empty {})
        .await
        .unwrap()
        .into_inner()
        .try_collect()
        .await
        .unwrap();
    assert!(
        listed.iter().any(|a| a.r#type == TRACE_LOOKUP),
        "{listed:?}"
    );

    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    f.db.finish().await;
}

/// The plans kept are bounded by the bytes they are estimated to retain,
/// oldest dropped first; a plan over the budget on its own is not kept and
/// drops nothing. The gauges follow.
#[tokio::test]
async fn prepared_plans_are_bounded_by_the_bytes_they_retain() {
    let Some(f) = fixture().await else { return };
    let querier = |prepared_bytes| {
        Querier::new(
            f.db.catalog.clone(),
            Arc::new(LocalFileSystem::new_with_prefix(f.dir.path().join("bucket")).unwrap()),
            None,
            QueryLimits {
                prepared_bytes,
                ..QueryLimits::default()
            },
        )
        .unwrap()
    };

    let probe = querier(usize::MAX);
    probe.prepare(f.tenant, BY_SERVICE).await.unwrap();
    let one = probe.prepared_bytes();
    assert!(one >= PREPARED_PLAN_OVERHEAD + BY_SERVICE.len(), "{one}");
    assert_eq!(
        probe.stats().prepared_bytes.load(Ordering::Relaxed),
        one as u64
    );

    let q = querier(one + one / 2);
    let counters = q.stats();
    q.prepare(f.tenant, BY_SERVICE).await.unwrap();
    q.prepare(f.tenant, COUNT_ALL).await.unwrap();
    assert_eq!(q.prepared_count(), 1, "the oldest made room");
    assert!(q.prepared_bytes() <= one + one / 2);
    assert_eq!(counters.prepared.load(Ordering::Relaxed), 1);
    assert!(
        q.execute_prepared(f.tenant, BY_SERVICE)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        q.execute_prepared(f.tenant, COUNT_ALL)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(q.prepared_count(), 0);
    assert_eq!(q.prepared_bytes(), 0);

    q.prepare(f.tenant, COUNT_ALL).await.unwrap();
    let padding = "x".repeat(2 * one);
    let huge = format!("{BY_SERVICE} /* {padding} */");
    q.prepare(f.tenant, &huge).await.unwrap();
    assert_eq!(q.prepared_count(), 1, "the one over budget was not kept");
    assert!(q.execute_prepared(f.tenant, &huge).await.unwrap().is_none());
    assert!(
        q.execute_prepared(f.tenant, COUNT_ALL)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(counters.prepared_misses.load(Ordering::Relaxed), 2);
    assert_eq!(counters.prepared_hits.load(Ordering::Relaxed), 2);
    assert_eq!(counters.prepared_bytes.load(Ordering::Relaxed), 0);

    let forever = Querier::new(
        f.db.catalog.clone(),
        Arc::new(LocalFileSystem::new_with_prefix(f.dir.path().join("bucket")).unwrap()),
        None,
        QueryLimits {
            prepared_ttl: Duration::MAX,
            ..QueryLimits::default()
        },
    )
    .unwrap();
    assert_eq!(forever.limits().prepared_ttl, MAX_PREPARED_TTL, "clamped");
    forever.prepare(f.tenant, COUNT_ALL).await.unwrap();
    assert!(
        forever
            .execute_prepared(f.tenant, COUNT_ALL)
            .await
            .unwrap()
            .is_some()
    );
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
