//! The derived span columns through the querier: answered from v1 files and
//! the hot window, which hold none of them, whether projected or only
//! filtered on, charged to the memory pool while their arrays live. Needs
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

use arrow::array::{Array, AsArray};
use arrow::record_batch::RecordBatch;
use fs_catalog::{Catalog, FileId, NewTenant, TenantId};
use fs_ingest::flush;
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

/// 2026-10-22T13:00:00Z.
const T0: u64 = 1_792_677_600_000_000_000;
const DERIVED: &str = "http_url, http_host, external_http_method, external_http_url";

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
        let schema = format!("derived_{}", uuid::Uuid::new_v4().simple());
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
        dir,
    })
}

fn querier(f: &Fixture, limits: QueryLimits) -> Querier {
    Querier::new(f.db.catalog.clone(), f.store.clone(), None, limits).unwrap()
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

fn span(trace: u8, n: u8, start: u64, kind: i32, attributes: Vec<KeyValue>) -> Span {
    Span {
        trace_id: vec![trace; 16],
        span_id: vec![n; 8],
        name: format!("op-{n}"),
        kind,
        start_time_unix_nano: start + u64::from(n) * 1_000_000,
        end_time_unix_nano: start + u64::from(n) * 1_000_000 + 250_000,
        attributes,
        ..Span::default()
    }
}

/// A client call, a server span and an internal one, of trace `trace`.
fn request(trace: u8, start: u64) -> ExportTraceServiceRequest {
    let spans = vec![
        span(
            trace,
            1,
            start,
            3,
            vec![
                kv("url.full", "https://pay.d6.test/charge?x=1"),
                kv("http.request.method", "POST"),
            ],
        ),
        span(
            trace,
            2,
            start,
            2,
            vec![
                kv("server.address", "api.d6.test"),
                kv("http.method", "GET"),
                kv("url.full", "http://api.d6.test/cart"),
            ],
        ),
        span(trace, 3, start, 1, vec![]),
    ];
    requests_of(spans)
}

fn requests_of(spans: Vec<Span>) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "api")],
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

/// Writes and registers one L0 span file of `request`, returning its path.
async fn write_l0(f: &Fixture, request: &ExportTraceServiceRequest) -> String {
    let mut builder = SpanBatchBuilder::new(Limits::default());
    builder.append_request(request).unwrap();
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
    prepared.file.path.clone()
}

/// Every row of `batches` as its text columns.
fn rows(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            out.push(
                batch
                    .columns()
                    .iter()
                    .map(|c| {
                        let text =
                            arrow::compute::cast(c, &arrow::datatypes::DataType::Utf8).unwrap();
                        text.as_string::<i32>().value(row).to_owned()
                    })
                    .collect(),
            );
        }
    }
    out
}

fn expected(names: &[&str]) -> Vec<Vec<String>> {
    let all = [
        [
            "op-1",
            "https://pay.d6.test/charge?x=1",
            "pay.d6.test",
            "POST",
            "pay.d6.test",
        ],
        ["op-2", "http://api.d6.test/cart", "api.d6.test", "", ""],
        ["op-3", "", "", "", ""],
    ];
    all.iter()
        .filter(|r| names.contains(&r[0]))
        .map(|r| r.iter().map(|s| (*s).to_owned()).collect())
        .collect()
}

#[tokio::test]
async fn a_v1_file_without_the_columns_answers_them_when_projected() {
    let Some(f) = fixture().await else { return };
    let path = write_l0(&f, &request(1, T0)).await;
    let file = std::fs::File::open(f.dir.path().join("bucket").join(&path)).unwrap();
    let stored =
        datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .schema()
            .clone();
    assert_eq!(stored.metadata()["fusescope.schema_version"], "1");
    for name in [
        "http_url",
        "http_host",
        "external_http_method",
        "external_http_url",
    ] {
        assert!(
            stored.field_with_name(name).is_err(),
            "{name} is not stored"
        );
    }
    let q = querier(&f, QueryLimits::default());
    let batches = q
        .collect(
            f.tenant,
            &format!("select name, {DERIVED} from spans order by name"),
        )
        .await
        .unwrap();
    assert_eq!(rows(&batches), expected(&["op-1", "op-2", "op-3"]));
    f.db.finish().await;
}

#[tokio::test]
async fn every_row_of_every_file_is_derived_whatever_the_plan_parallelism() {
    let Some(f) = fixture().await else { return };
    for trace in 1..=4 {
        write_l0(&f, &request(trace, T0 + u64::from(trace) * 60_000_000_000)).await;
    }
    let q = querier(&f, QueryLimits::default());
    let batches = q
        .collect(f.tenant, &format!("select name, {DERIVED} from spans"))
        .await
        .unwrap();
    let mut read = rows(&batches);
    read.sort();
    let mut want: Vec<Vec<String>> = (0..4)
        .flat_map(|_| expected(&["op-1", "op-2", "op-3"]))
        .collect();
    want.sort();
    assert_eq!(read, want);
    f.db.finish().await;
}

#[tokio::test]
async fn a_derived_column_only_in_a_filter_selects_and_stays_out_of_the_file_scan() {
    let Some(f) = fixture().await else { return };
    write_l0(&f, &request(1, T0)).await;
    let q = querier(&f, QueryLimits::default());
    let sql = "select name from spans where external_http_url = 'pay.d6.test'";
    let batches = q.collect(f.tenant, sql).await.unwrap();
    assert_eq!(rows(&batches), vec![vec!["op-1".to_owned()]]);
    let host = q
        .collect(
            f.tenant,
            "select name from spans where http_host = 'api.d6.test'",
        )
        .await
        .unwrap();
    assert_eq!(rows(&host), vec![vec!["op-2".to_owned()]]);

    let plan = rows(
        &q.collect(f.tenant, &format!("explain {sql}"))
            .await
            .unwrap(),
    );
    let physical = &plan.iter().find(|r| r[0] == "physical_plan").unwrap()[1];
    let scan = physical
        .lines()
        .find(|l| l.contains("DataSourceExec"))
        .unwrap();
    assert!(!scan.contains("external_http_url"), "pushed down: {scan}");
    assert!(
        physical
            .lines()
            .any(|l| l.contains("FilterExec") && l.contains("external_http_url")),
        "{physical}"
    );
    f.db.finish().await;
}

#[tokio::test]
async fn select_star_shows_the_derived_columns_after_the_stored_ones() {
    let Some(f) = fixture().await else { return };
    write_l0(&f, &request(1, T0)).await;
    let q = querier(&f, QueryLimits::default());
    let batches = q
        .collect(f.tenant, "select * from spans where name = 'op-1'")
        .await
        .unwrap();
    let schema = batches[0].schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names[names.len() - 4..],
        [
            "http_url",
            "http_host",
            "external_http_method",
            "external_http_url"
        ]
    );
    let host = batches[0].column_by_name("http_host").unwrap();
    assert_eq!(host.as_string::<i32>().value(0), "pay.d6.test");
    f.db.finish().await;
}

#[tokio::test]
async fn rows_only_the_hot_window_holds_answer_the_derived_columns() {
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
    let mut client = TraceServiceClient::connect(format!("http://{otlp_addr}"))
        .await
        .unwrap();
    let mut export = tonic::Request::new(request(7, now - 1_000_000_000));
    export.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {ingest_token}")).unwrap(),
    );
    client.export(export).await.unwrap();

    let q = querier(&f, QueryLimits::default());
    let projected = format!("select name, {DERIVED} from spans order by name");
    let batches = q.collect(f.tenant, &projected).await.unwrap();
    assert_eq!(
        rows(&batches),
        expected(&["op-1", "op-2", "op-3"]),
        "in the ingester alone"
    );
    let filtered = q
        .collect(
            f.tenant,
            "select name from spans where external_http_method = 'POST'",
        )
        .await
        .unwrap();
    assert_eq!(rows(&filtered), vec![vec!["op-1".to_owned()]]);
    ingester.flush_all().await;
    let batches = q.collect(f.tenant, &projected).await.unwrap();
    assert_eq!(
        rows(&batches),
        expected(&["op-1", "op-2", "op-3"]),
        "flushed, counted once"
    );

    shutdown.send(true).unwrap();
    task.await.unwrap().unwrap();
    f.db.finish().await;
}

#[tokio::test]
async fn derived_arrays_stay_charged_to_the_pool_while_they_are_held() {
    let Some(f) = fixture().await else { return };
    let long = "x".repeat(4096);
    let spans = (1..=200u8)
        .map(|n| {
            span(
                1,
                n,
                T0,
                3,
                vec![kv("url.full", &format!("https://h{n}.d6.test/{long}"))],
            )
        })
        .collect();
    write_l0(&f, &requests_of(spans)).await;
    let q = querier(&f, QueryLimits::default());
    let (stream, _) = q
        .execute(f.tenant, "select http_url from spans")
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let held: usize = batches
        .iter()
        .map(|b| b.column(0).get_buffer_memory_size())
        .sum();
    assert!(held > 200 * 4096, "{held}");
    assert!(
        q.memory_reserved() >= held,
        "reserved {} while {held} are held",
        q.memory_reserved()
    );
    drop(batches);
    assert_eq!(q.memory_reserved(), 0, "released with the arrays");
    f.db.finish().await;
}

#[tokio::test]
async fn deriving_past_the_memory_pool_fails_as_resources_exhausted() {
    let Some(f) = fixture().await else { return };
    let long = "y".repeat(8192);
    let spans = (1..=100u8)
        .map(|n| {
            span(
                1,
                n,
                T0,
                3,
                vec![kv("url.full", &format!("https://h{n}.d6.test/{long}"))],
            )
        })
        .collect();
    write_l0(&f, &requests_of(spans)).await;
    let q = querier(
        &f,
        QueryLimits {
            memory_bytes: 256 * 1024,
            ..QueryLimits::default()
        },
    );
    let err = q
        .collect(f.tenant, "select http_url, external_http_url from spans")
        .await
        .unwrap_err();
    assert!(
        matches!(err, QueryError::DataFusion(ref e) if matches!(e.find_root(), datafusion::error::DataFusionError::ResourcesExhausted(_))),
        "{err}"
    );
    assert_eq!(q.memory_reserved(), 0);
    f.db.finish().await;
}
