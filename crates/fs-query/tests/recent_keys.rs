//! `metadata.keys` over the rows the derived keys table does not hold yet:
//! the L0 files of one catalog snapshot and the hot window, counted once,
//! on each hour's budget of new keys, failing whole rather than answering
//! in part, and cached only when complete. Needs `FS_TEST_DATABASE_URL`.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::too_many_lines
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::AsArray;
use arrow::datatypes::UInt64Type;
use arrow::record_batch::RecordBatch;
use fs_catalog::{Catalog, FileId, Level, NewFile, NewTenant, Signal, TenantId, TimeRange};
use fs_ingest::flush;
use fs_query::actions::KeysQuery;
use fs_query::{Querier, QueryError, QueryLimits};
use fs_schema::attribute_keys::{KeyStats, KeyTable};
use fs_schema::limits::Limits;
use fs_schema::spans::SpanBatchBuilder;
use futures_util::TryStreamExt;
use object_store::ObjectStoreExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use sqlx::{Connection, Executor, PgConnection};

const HOUR: u64 = 3_600_000_000_000;
/// The start of an hour, so a span's offsets stay within it.
const H0: u64 = 1_792_677_600_000_000_000;

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
        let schema = format!("keys_{}", uuid::Uuid::new_v4().simple());
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

async fn fixture(tenant: NewTenant) -> Option<Fixture> {
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

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
        ..KeyValue::default()
    }
}

/// `spans` spans of one trace from `start`, each with the span attributes
/// `keys`.
fn request(trace: u8, start: u64, spans: u8, keys: &[&str]) -> ExportTraceServiceRequest {
    let spans = (0..spans)
        .map(|i| Span {
            trace_id: vec![trace; 16],
            span_id: vec![i + 1; 8],
            name: format!("op-{i}"),
            kind: 2,
            start_time_unix_nano: start + u64::from(i) * 1_000_000,
            end_time_unix_nano: start + u64::from(i) * 1_000_000 + 250_000,
            attributes: keys.iter().map(|k| kv(k, "v")).collect(),
            ..Span::default()
        })
        .collect();
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

/// Registers the derived keys table counting `requests`, each on the budget
/// of its own hour, as a compaction of them would have left it.
async fn write_derived(f: &Fixture, requests: &[(&ExportTraceServiceRequest, u64)]) {
    let mut stats = KeyStats::new(KeyTable::Spans);
    for (request, hour) in requests {
        stats = stats.settled();
        stats
            .absorb(&batch(request), i64::try_from(*hour).unwrap(), usize::MAX)
            .unwrap();
    }
    let bytes = fs_schema::attribute_keys::encode(&stats).unwrap();
    let id = FileId::new();
    let path = format!(
        "t/{}/derived/attribute_keys/dt=2026-10-22/hr=13/{}.parquet",
        f.tenant.0, id.0
    );
    f.store
        .put(&Path::from(path.as_str()), bytes.clone().into())
        .await
        .unwrap();
    let (first, last) = stats.range().unwrap();
    f.db.catalog
        .register_file(&NewFile {
            id,
            origin: id,
            tenant_id: f.tenant,
            signal: Signal::AttributeKeys,
            level: Level::L1,
            path,
            size_bytes: i64::try_from(bytes.len()).unwrap(),
            row_count: i64::try_from(stats.len()).unwrap(),
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

/// The listing as key to count.
async fn keys(querier: &Querier, tenant: TenantId) -> Result<BTreeMap<String, u64>, QueryError> {
    let query = KeysQuery {
        table: KeyTable::Spans,
        prefix: None,
        limit: 1000,
    };
    let (stream, _) = querier.metadata_keys(tenant, &query).await?;
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    let mut out = BTreeMap::new();
    for b in &batches {
        let keys = b.column_by_name("key").unwrap().as_string::<i32>();
        let counts = b
            .column_by_name("count")
            .unwrap()
            .as_primitive::<UInt64Type>();
        for row in 0..b.num_rows() {
            out.insert(keys.value(row).to_owned(), counts.value(row));
        }
    }
    Ok(out)
}

fn limits(ttl: Duration) -> QueryLimits {
    QueryLimits {
        recent_keys_ttl: ttl,
        ..QueryLimits::default()
    }
}

#[tokio::test]
async fn the_keys_of_l0_files_are_listed_before_their_hour_is_compacted() {
    let Some(f) = fixture(NewTenant::with_defaults("acme")).await else {
        return;
    };
    write_l0(&f, &request(1, H0, 3, &["http.request.method"])).await;
    let listed = keys(&querier(&f, QueryLimits::default()), f.tenant)
        .await
        .unwrap();
    assert_eq!(listed.get("http.request.method"), Some(&3), "{listed:?}");
    assert_eq!(listed.get("service.name"), Some(&3));
    f.db.finish().await;
}

#[tokio::test]
async fn the_derived_table_and_the_l0_files_after_it_count_each_span_once() {
    let Some(f) = fixture(NewTenant::with_defaults("acme")).await else {
        return;
    };
    let compacted = request(1, H0, 3, &["http.request.method"]);
    write_derived(&f, &[(&compacted, H0)]).await;
    write_l0(
        &f,
        &request(2, H0 + HOUR, 2, &["http.request.method", "d9.new"]),
    )
    .await;
    let listed = keys(&querier(&f, QueryLimits::default()), f.tenant)
        .await
        .unwrap();
    assert_eq!(
        listed.get("http.request.method"),
        Some(&5),
        "3 compacted and 2 not yet: {listed:?}"
    );
    assert_eq!(listed.get("service.name"), Some(&5));
    assert_eq!(
        listed.get("d9.new"),
        Some(&2),
        "a key the derived table never saw"
    );
    f.db.finish().await;
}

#[tokio::test]
async fn recent_rows_keep_their_hours_budget_of_new_keys() {
    let tenant = NewTenant {
        max_keys_hour: 2,
        ..NewTenant::with_defaults("acme")
    };
    let Some(f) = fixture(tenant).await else {
        return;
    };
    // The derived table admitted both of H0's keys: none is left for it.
    write_derived(&f, &[(&request(1, H0, 1, &["http.request.method"]), H0)]).await;
    write_l0(&f, &request(2, H0 + 1_000_000_000, 1, &["d9.late"])).await;
    write_l0(&f, &request(3, H0 + HOUR, 1, &["d9.next"])).await;
    let listed = keys(&querier(&f, QueryLimits::default()), f.tenant)
        .await
        .unwrap();
    assert!(
        !listed.contains_key("d9.late"),
        "H0 admitted its two keys already: {listed:?}"
    );
    assert_eq!(
        listed.get("d9.next"),
        Some(&1),
        "the next hour has its own budget"
    );
    f.db.finish().await;
}

#[tokio::test]
async fn a_listing_over_the_bytes_budget_fails_and_is_not_cached() {
    let tenant = NewTenant {
        bytes_budget: 1,
        ..NewTenant::with_defaults("acme")
    };
    let Some(f) = fixture(tenant).await else {
        return;
    };
    write_l0(&f, &request(1, H0, 3, &["http.request.method"])).await;
    let q = querier(&f, limits(Duration::from_secs(3600)));
    for attempt in 0..2 {
        let err = keys(&q, f.tenant).await.unwrap_err();
        assert!(
            matches!(err, QueryError::OverBudget { .. }),
            "attempt {attempt}: {err}"
        );
    }
    f.db.finish().await;
}

#[tokio::test]
async fn a_listing_without_a_hot_window_it_needs_fails_unavailable() {
    let Some(f) = fixture(NewTenant::with_defaults("acme")).await else {
        return;
    };
    write_l0(&f, &request(1, H0, 3, &["http.request.method"])).await;
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = silent.local_addr().unwrap().to_string();
    drop(silent);
    f.db.catalog
        .heartbeat("ingester-gone", &address)
        .await
        .unwrap();
    let err = keys(&querier(&f, QueryLimits::default()), f.tenant)
        .await
        .unwrap_err();
    assert!(matches!(err, QueryError::HotUnavailable(_)), "{err}");
    f.db.finish().await;
}

#[tokio::test]
async fn the_cache_answers_within_its_ttl_and_reads_again_after() {
    let Some(f) = fixture(NewTenant::with_defaults("acme")).await else {
        return;
    };
    write_l0(&f, &request(1, H0, 1, &["d9.first"])).await;
    let q = querier(&f, limits(Duration::from_millis(300)));
    assert!(keys(&q, f.tenant).await.unwrap().contains_key("d9.first"));
    write_l0(&f, &request(2, H0, 1, &["d9.second"])).await;
    assert!(
        !keys(&q, f.tenant).await.unwrap().contains_key("d9.second"),
        "within the TTL the listing comes from the cache"
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        keys(&q, f.tenant).await.unwrap().contains_key("d9.second"),
        "visible within one TTL"
    );
    f.db.finish().await;
}

/// A key of rows the ingester holds and never flushed is listed, and stays
/// counted once after their flush, as the hot window leaves out the file it
/// still serves.
#[tokio::test]
async fn the_keys_of_rows_only_the_hot_window_holds_are_listed_once() {
    use fs_ingest::{IngestConfig, Ingester};
    use fs_wal::{Wal, WalConfig};
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
    use tonic::metadata::MetadataValue;

    let Some(f) = fixture(NewTenant::with_defaults("acme")).await else {
        return;
    };
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
    let mut export = tonic::Request::new(request(7, now - 1_000_000_000, 4, &["d9.hot"]));
    export.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {ingest_token}")).unwrap(),
    );
    client.export(export).await.unwrap();

    let q = querier(&f, limits(Duration::ZERO));
    assert_eq!(
        keys(&q, f.tenant).await.unwrap().get("d9.hot"),
        Some(&4),
        "rows in the ingester alone"
    );
    ingester.flush_all().await;
    assert_eq!(
        keys(&q, f.tenant).await.unwrap().get("d9.hot"),
        Some(&4),
        "flushed and still served: the file is left out, the rows counted once"
    );

    shutdown.send(true).unwrap();
    task.await.unwrap().unwrap();
    f.db.finish().await;
}

fn fills(q: &Querier) -> u64 {
    q.stats()
        .recent_keys_fills
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// Concurrent misses share one fill: one snapshot, one read, one answer.
#[tokio::test]
async fn concurrent_misses_share_one_fill() {
    let Some(f) = fixture(NewTenant::with_defaults("acme")).await else {
        return;
    };
    write_l0(&f, &request(1, H0, 3, &["d9.shared"])).await;
    let q = querier(&f, limits(Duration::from_secs(60)));
    let answers = futures_util::future::join_all((0..8).map(|_| keys(&q, f.tenant))).await;
    for answer in answers {
        assert_eq!(answer.unwrap().get("d9.shared"), Some(&3));
    }
    assert_eq!(fills(&q), 1, "eight misses, one fill");
    f.db.finish().await;
}

/// Concurrent misses share one failure too, and a failure is not kept: the
/// next miss fills again.
#[tokio::test]
async fn concurrent_misses_share_one_failure_that_is_not_kept() {
    let Some(f) = fixture(NewTenant::with_defaults("acme")).await else {
        return;
    };
    write_l0(&f, &request(1, H0, 3, &["d9.shared"])).await;
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = silent.local_addr().unwrap().to_string();
    drop(silent);
    f.db.catalog
        .heartbeat("ingester-gone", &address)
        .await
        .unwrap();
    let q = querier(&f, limits(Duration::from_secs(60)));
    let answers = futures_util::future::join_all((0..6).map(|_| keys(&q, f.tenant))).await;
    for answer in answers {
        let err = answer.unwrap_err();
        assert!(matches!(err, QueryError::HotUnavailable(_)), "{err}");
    }
    assert_eq!(fills(&q), 1, "six misses, one fill");
    assert!(keys(&q, f.tenant).await.is_err());
    assert_eq!(fills(&q), 2, "the failure was not kept");
    f.db.finish().await;
}

/// The derived table and the counted keys are held under the querier's
/// memory pool: a table too large for it fails the listing as resources
/// exhausted instead of growing past it.
#[tokio::test]
async fn a_listing_larger_than_the_memory_pool_fails_resources_exhausted() {
    let Some(f) = fixture(NewTenant::with_defaults("acme")).await else {
        return;
    };
    let names: Vec<String> = (0..400).map(|i| format!("d9.key.{i:04}")).collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let many = request(1, H0, 50, &refs);
    write_derived(&f, &[(&many, H0)]).await;
    let q = querier(
        &f,
        QueryLimits {
            memory_bytes: 16 * 1024,
            ..limits(Duration::ZERO)
        },
    );
    let err = keys(&q, f.tenant).await.unwrap_err();
    assert!(
        matches!(
            err,
            QueryError::DataFusion(datafusion::error::DataFusionError::ResourcesExhausted(_))
        ),
        "{err}"
    );
    assert!(
        err.to_string().contains("metadata.keys"),
        "refused by the listing's own reservation, before any operator: {err}"
    );
    let roomy = querier(&f, limits(Duration::ZERO));
    let listed = keys(&roomy, f.tenant).await.unwrap();
    assert!(
        listed.len() > 200 && listed.contains_key("service.name"),
        "the same table fits the default pool: {} keys",
        listed.len()
    );
    f.db.finish().await;
}
