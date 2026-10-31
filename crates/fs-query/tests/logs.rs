//! The `logs` table: rows on their way from an ingester's memory to the
//! catalog, read through the querier. Needs `FS_TEST_DATABASE_URL`; skips
//! otherwise.
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
use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
use fs_catalog::{Catalog, NewTenant, Signal, TenantId, TokenScope};
use fs_ingest::record::RowSignal;
use fs_ingest::{IngestConfig, Ingester};
use fs_query::{Querier, QueryLimits};
use fs_wal::{Wal, WalConfig};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use sqlx::{Connection, Executor, PgConnection};
use tonic::metadata::MetadataValue;

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
        let schema = format!("qlog_{}", uuid::Uuid::new_v4().simple());
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

fn authed<T>(message: T, token: &str) -> tonic::Request<T> {
    let mut r = tonic::Request::new(message);
    r.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
    );
    r
}

fn resource(service: &str) -> Resource {
    Resource {
        attributes: vec![KeyValue {
            key: "service.name".into(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(service.into())),
            }),
            ..KeyValue::default()
        }],
        ..Resource::default()
    }
}

/// `count` log lines of `service` a millisecond apart from `start`, line `i`
/// in trace `i + 1`.
fn logs(service: &str, start: u64, count: u64) -> ExportLogsServiceRequest {
    let log_records = (0..count)
        .map(|i| LogRecord {
            time_unix_nano: start + i * 1_000_000,
            severity_text: if i % 2 == 0 { "INFO" } else { "ERROR" }.into(),
            severity_number: if i % 2 == 0 { 9 } else { 17 },
            body: Some(AnyValue {
                value: Some(Value::StringValue(format!("{service} line {i}"))),
            }),
            trace_id: vec![u8::try_from(i).unwrap() + 1; 16],
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

fn spans(service: &str, start: u64, count: u8) -> ExportTraceServiceRequest {
    let spans = (0..count)
        .map(|i| Span {
            trace_id: vec![1; 16],
            span_id: vec![i + 1; 8],
            name: "op".into(),
            start_time_unix_nano: start + u64::from(i),
            end_time_unix_nano: start + u64::from(i) + 1_000,
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

fn count(batches: &[RecordBatch]) -> i64 {
    batches
        .iter()
        .map(|b| b.column(0).as_primitive::<Int64Type>().value(0))
        .sum()
}

const LOGS: &str = "select count(*) from logs where timestamp > now() - interval '1 hour'";
const SPANS: &str = "select count(*) from spans where timestamp > now() - interval '1 hour'";

async fn counted(querier: &Querier, tenant: TenantId, sql: &str) -> (i64, Vec<String>) {
    let (rows, warnings) = querier.collect_with_warnings(tenant, sql).await.unwrap();
    (count(&rows), warnings)
}

/// Log rows are read live, while their file is being written, while it is
/// registered and still retained in memory, and from the catalog alone once
/// the retention passes: each row once at every step. The spans and logs
/// tables see only their own rows, and an ingester that went away makes
/// the answer partial, not wrong.
#[tokio::test]
async fn log_rows_are_read_once_from_live_to_flushing_to_persisted_to_the_catalog() {
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
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", TokenScope::Ingest)
        .await
        .unwrap();
    let config = IngestConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        hot_listen: Some("127.0.0.1:0".parse().unwrap()),
        flush_interval: Duration::from_secs(3600),
        persisted_retain: Duration::from_secs(2),
        inject_register_failures: 1,
        inject_register_delay: Some(Duration::from_secs(2)),
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
    let otlp = tokio::time::timeout(Duration::from_secs(5), otlp_rx)
        .await
        .unwrap()
        .unwrap();
    let hot = tokio::time::timeout(Duration::from_secs(5), hot_rx)
        .await
        .unwrap()
        .unwrap();
    db.catalog
        .heartbeat("ingester-logs", &hot.to_string())
        .await
        .unwrap();

    let now = u64::try_from(chrono::Utc::now().timestamp_nanos_opt().unwrap()).unwrap();
    let start = now - 2_000_000_000;
    LogsServiceClient::connect(format!("http://{otlp}"))
        .await
        .unwrap()
        .export(authed(logs("api", start, 10), &token))
        .await
        .unwrap();
    TraceServiceClient::connect(format!("http://{otlp}"))
        .await
        .unwrap()
        .export(authed(spans("api", start, 3), &token))
        .await
        .unwrap();
    let limits = QueryLimits {
        timeout: Duration::from_secs(30),
        ..QueryLimits::default()
    };
    let querier = Querier::new(db.catalog.clone(), store.clone(), None, limits).unwrap();

    // 1. Live.
    let (logs_live, warnings) = counted(&querier, tenant, LOGS).await;
    assert_eq!(logs_live, 10);
    assert!(warnings.is_empty(), "{warnings:?}");
    let other = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("other"))
        .await
        .unwrap()
        .id;
    assert_eq!(
        counted(&querier, other, LOGS).await.0,
        0,
        "another tenant's rows"
    );
    assert_eq!(counted(&querier, tenant, SPANS).await.0, 3);
    let lines = querier
        .collect(
            tenant,
            &format!(
                "select body, severity_text from logs where trace_id = decode('{}', 'hex') and timestamp > now() - interval '1 hour'",
                "04".repeat(16)
            ),
        )
        .await
        .unwrap();
    let bodies: Vec<String> = lines
        .iter()
        .flat_map(|b| {
            let body =
                arrow::compute::cast(b.column(0), &arrow::datatypes::DataType::Utf8).unwrap();
            let level =
                arrow::compute::cast(b.column(1), &arrow::datatypes::DataType::Utf8).unwrap();
            (0..b.num_rows())
                .map(|i| {
                    format!(
                        "{} {}",
                        body.as_string::<i32>().value(i),
                        level.as_string::<i32>().value(i)
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(bodies, ["api line 3 ERROR"]);
    let grouped = querier
        .collect(
            tenant,
            "select severity_text, count(*) from logs where timestamp > now() - interval '1 hour' group by severity_text order by severity_text",
        )
        .await
        .unwrap();
    assert_eq!(grouped.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);

    // 2. Flushing: the object is written and its registration is held.
    let flushing = tokio::spawn({
        let ingester = ingester.clone();
        async move { ingester.flush_all().await }
    });
    let flush = ingester.flush_stats();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while flush.registrations_held.load(Ordering::SeqCst) < 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no registration held"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        counted(&querier, tenant, LOGS).await.0,
        10,
        "while flushing"
    );
    flushing.await.unwrap();
    assert_eq!(
        counted(&querier, tenant, LOGS).await.0,
        10,
        "after a failed registration"
    );

    // 3. Persisted: registered and still retained in memory.
    ingester.flush_all().await;
    assert_eq!(
        db.catalog
            .all_visible_files(tenant, Signal::Logs)
            .await
            .unwrap()
            .len(),
        1
    );
    querier.cache().invalidate(tenant, Signal::Logs).await;
    querier.cache().invalidate(tenant, Signal::Traces).await;
    assert!(
        flush.persisted_bytes.load(Ordering::Relaxed) > 0,
        "retained"
    );
    assert_eq!(
        counted(&querier, tenant, LOGS).await.0,
        10,
        "file and memory, once"
    );

    // 4. The retention passed: the catalog alone.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    ingester.flush_all().await;
    assert_eq!(flush.persisted_bytes.load(Ordering::Relaxed), 0, "released");
    assert_eq!(counted(&querier, tenant, LOGS).await.0, 10, "from the file");
    assert_eq!(counted(&querier, tenant, SPANS).await.0, 3);

    // 5. The ingester went away with its heartbeat still fresh.
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    querier.cache().invalidate(tenant, Signal::Logs).await;
    let (rows, warnings) = counted(&querier, tenant, LOGS).await;
    assert_eq!(rows, 10, "file rows survive the ingester");
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("ingester-logs"));
    db.finish().await;
}

/// An ingester that answers every hot scan with `schema`, `batches` and
/// `persisted` as its file header, whatever the signal asked for. With the
/// span schema it is an ingester from before logs.
struct FixedIngester {
    schema: arrow::datatypes::SchemaRef,
    batches: Vec<RecordBatch>,
    persisted: &'static str,
}

type FlightStream<T> =
    std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<T, tonic::Status>> + Send>>;

#[tonic::async_trait]
impl arrow_flight::flight_service_server::FlightService for FixedIngester {
    type HandshakeStream = FlightStream<arrow_flight::HandshakeResponse>;
    type ListFlightsStream = FlightStream<arrow_flight::FlightInfo>;
    type DoGetStream = FlightStream<arrow_flight::FlightData>;
    type DoPutStream = FlightStream<arrow_flight::PutResult>;
    type DoActionStream = FlightStream<arrow_flight::Result>;
    type ListActionsStream = FlightStream<arrow_flight::ActionType>;
    type DoExchangeStream = FlightStream<arrow_flight::FlightData>;

    async fn do_get(
        &self,
        _: tonic::Request<arrow_flight::Ticket>,
    ) -> Result<tonic::Response<Self::DoGetStream>, tonic::Status> {
        use futures_util::TryStreamExt;
        let encoded = arrow_flight::encode::FlightDataEncoderBuilder::new()
            .with_schema(self.schema.clone())
            .build(futures_util::stream::iter(
                self.batches.clone().into_iter().map(Ok),
            ))
            .map_err(|e| tonic::Status::internal(e.to_string()));
        let mut response = tonic::Response::new(Box::pin(encoded) as Self::DoGetStream);
        response.metadata_mut().insert(
            fs_ingest::hot::PERSISTED_FILES_HEADER,
            self.persisted.parse().unwrap(),
        );
        Ok(response)
    }

    async fn handshake(
        &self,
        _: tonic::Request<tonic::Streaming<arrow_flight::HandshakeRequest>>,
    ) -> Result<tonic::Response<Self::HandshakeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("do_get only"))
    }
    async fn list_flights(
        &self,
        _: tonic::Request<arrow_flight::Criteria>,
    ) -> Result<tonic::Response<Self::ListFlightsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("do_get only"))
    }
    async fn get_flight_info(
        &self,
        _: tonic::Request<arrow_flight::FlightDescriptor>,
    ) -> Result<tonic::Response<arrow_flight::FlightInfo>, tonic::Status> {
        Err(tonic::Status::unimplemented("do_get only"))
    }
    async fn poll_flight_info(
        &self,
        _: tonic::Request<arrow_flight::FlightDescriptor>,
    ) -> Result<tonic::Response<arrow_flight::PollInfo>, tonic::Status> {
        Err(tonic::Status::unimplemented("do_get only"))
    }
    async fn get_schema(
        &self,
        _: tonic::Request<arrow_flight::FlightDescriptor>,
    ) -> Result<tonic::Response<arrow_flight::SchemaResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("do_get only"))
    }
    async fn do_put(
        &self,
        _: tonic::Request<tonic::Streaming<arrow_flight::FlightData>>,
    ) -> Result<tonic::Response<Self::DoPutStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("do_get only"))
    }
    async fn do_exchange(
        &self,
        _: tonic::Request<tonic::Streaming<arrow_flight::FlightData>>,
    ) -> Result<tonic::Response<Self::DoExchangeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("do_get only"))
    }
    async fn do_action(
        &self,
        _: tonic::Request<arrow_flight::Action>,
    ) -> Result<tonic::Response<Self::DoActionStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("do_get only"))
    }
    async fn list_actions(
        &self,
        _: tonic::Request<arrow_flight::Empty>,
    ) -> Result<tonic::Response<Self::ListActionsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("do_get only"))
    }
}

/// During a rollout an ingester that predates logs answers a log scan
/// with span rows: the log query leaves them out and says it is partial.
#[tokio::test]
async fn an_ingester_without_logs_makes_a_log_query_partial_not_wrong() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let now = u64::try_from(chrono::Utc::now().timestamp_nanos_opt().unwrap()).unwrap();
    let mut builder = fs_schema::spans::SpanBatchBuilder::default();
    builder
        .append_request(&spans("api", now - 1_000_000_000, 4))
        .unwrap();
    let rows = builder.finish().unwrap();
    let old = serve(
        &db.catalog,
        "ingester-old",
        FixedIngester {
            schema: rows.schema(),
            batches: vec![rows],
            persisted: "",
        },
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let querier = Querier::new(db.catalog.clone(), store, None, QueryLimits::default()).unwrap();

    let (logs, warnings) = counted(&querier, tenant, LOGS).await;
    assert_eq!(logs, 0, "span rows are not log rows");
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("ingester-old"), "{warnings:?}");
    let (spans, warnings) = counted(&querier, tenant, SPANS).await;
    assert_eq!(spans, 4, "the old ingester still serves spans");
    assert!(warnings.is_empty(), "{warnings:?}");
    old.stop().await;
    db.finish().await;
}

struct Served {
    stop: tokio::sync::oneshot::Sender<()>,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Served {
    async fn stop(self) {
        self.stop.send(()).unwrap();
        self.server.await.unwrap().unwrap();
    }
}

/// Serves `ingester` and announces it in `catalog` as `id`.
async fn serve(catalog: &Catalog, id: &str, ingester: FixedIngester) -> Served {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(arrow_flight::flight_service_server::FlightServiceServer::new(ingester))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = stopped.await;
                },
            ),
    );
    catalog.heartbeat(id, &addr.to_string()).await.unwrap();
    Served { stop, server }
}

/// An answer without rows still names its signal by the schema it sends:
/// an old ingester's empty span answer to a log scan is no answer, and the
/// files it names stay in the log scan.
#[tokio::test]
async fn an_empty_answer_is_judged_by_the_schema_it_sends() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let tenant = db
        .catalog
        .create_tenant(&NewTenant::with_defaults("acme"))
        .await
        .unwrap()
        .id;
    let hot = fs_query::hot::HotWindow::new(
        db.catalog.clone(),
        fs_query::hot::HotWindowConfig::default(),
    );
    let everything = fs_query::prune::Pruning::default();
    let scan = |signal| hot.scan(tenant, signal, &everything, None, None);
    let excluded = |ids: &[&str]| {
        ids.iter()
            .map(|id| fs_catalog::FileId(id.parse().unwrap()))
            .collect::<std::collections::HashSet<_>>()
    };

    let old = serve(
        &db.catalog,
        "ingester",
        FixedIngester {
            schema: fs_schema::spans::span_schema_v1(),
            batches: Vec::new(),
            persisted: "0195f0a0-0000-7000-8000-000000000041",
        },
    )
    .await;
    let logs = scan(RowSignal::Logs).await.unwrap();
    assert_eq!(logs.unavailable, ["ingester"]);
    assert_eq!(
        logs.excluded_files,
        excluded(&[]),
        "its files stay in the log scan"
    );
    let spans = scan(RowSignal::Traces).await.unwrap();
    assert_eq!(spans.unavailable.len(), 0, "{:?}", spans.unavailable);
    assert_eq!(
        spans.excluded_files,
        excluded(&["0195f0a0-0000-7000-8000-000000000041"])
    );

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let querier = Querier::new(db.catalog.clone(), store, None, QueryLimits::default()).unwrap();
    let (rows, warnings) = counted(&querier, tenant, LOGS).await;
    assert_eq!(rows, 0);
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].contains("ingester"), "{warnings:?}");
    let (_, warnings) = counted(&querier, tenant, SPANS).await;
    assert!(warnings.is_empty(), "{warnings:?}");
    old.stop().await;

    let new = serve(
        &db.catalog,
        "ingester",
        FixedIngester {
            schema: fs_schema::logs::log_schema_v1(),
            batches: Vec::new(),
            persisted: "0195f0a0-0000-7000-8000-000000000042",
        },
    )
    .await;
    let logs = scan(RowSignal::Logs).await.unwrap();
    assert_eq!(logs.unavailable.len(), 0, "{:?}", logs.unavailable);
    assert_eq!(
        logs.excluded_files,
        excluded(&["0195f0a0-0000-7000-8000-000000000042"])
    );
    new.stop().await;
    db.finish().await;
}
