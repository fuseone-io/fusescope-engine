//! Body searches over log files through their text sidecars: the same rows
//! with and without the index, the files and row groups it prunes, what a
//! search publishes, and files whose sidecar cannot be trusted. Needs
//! `FS_TEST_DATABASE_URL`; skips otherwise.
#![allow(
    clippy::too_many_lines,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::sync::Arc;
use std::sync::atomic::Ordering;

use arrow::array::AsArray;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use fs_catalog::{Catalog, FileRecord, NewTenant, TenantId};
use fs_index::TextIndexLimits;
use fs_index::text_index::Probe;
use fs_ingest::buffer::Stream;
use fs_ingest::flush;
use fs_ingest::record::{FileLayout, RowSignal};
use fs_query::file_index::{FileIndex, FileSelection, Selection};
use fs_query::{Querier, QueryLimits, QueryStats};
use fs_schema::limits::Limits;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use sqlx::{Connection, Executor, PgConnection};

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
        let schema = format!("qtext_{}", uuid::Uuid::new_v4().simple());
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
    written: usize,
}

async fn fixture() -> Option<Fixture> {
    let db = TestDb::new().await?;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
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
        written: 0,
    })
}

fn request(bodies: &[&str], start: u64) -> ExportLogsServiceRequest {
    let log_records = bodies
        .iter()
        .zip(0u64..)
        .map(|(body, i)| LogRecord {
            time_unix_nano: start + i * 1_000_000,
            severity_number: 9,
            body: Some(AnyValue {
                value: Some(Value::StringValue((*body).to_owned())),
            }),
            ..LogRecord::default()
        })
        .collect();
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
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
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Writes and registers a log L0 file of `bodies` in that order, two rows
/// per row group, with its text sidecar.
async fn write(f: &mut Fixture, bodies: &[&str]) -> FileRecord {
    let start = T0 + u64::try_from(f.written).unwrap() * 1_000_000_000;
    f.written += 1;
    let mut builder = fs_schema::logs::LogBatchBuilder::new(Limits::default());
    builder
        .append_request(&request(bodies, start), uuid::Uuid::new_v4().as_bytes())
        .unwrap();
    let prepared = flush::prepare_stream(
        Stream::logs(f.tenant),
        &Path::from(""),
        fs_catalog::FileId::new(),
        &[builder.finish().unwrap()],
        FileLayout::for_signal(RowSignal::Logs, 2, TextIndexLimits::DEFAULT),
    )
    .unwrap();
    flush::upload(f.store.as_ref(), &prepared).await.unwrap();
    flush::upload_index(f.store.as_ref(), &prepared)
        .await
        .unwrap();
    f.db.catalog.register_file(&prepared.file).await.unwrap()
}

fn querier(f: &Fixture, text_index_cache_bytes: usize) -> Querier {
    Querier::new(
        f.db.catalog.clone(),
        f.store.clone(),
        None,
        QueryLimits {
            text_index_cache_bytes,
            ..QueryLimits::default()
        },
    )
    .unwrap()
}

fn strings(batches: &[RecordBatch]) -> Vec<String> {
    let mut out: Vec<String> = batches
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
    out.sort();
    out
}

async fn bodies(q: &Querier, tenant: TenantId, condition: &str) -> Vec<String> {
    let sql = format!("select body from logs where {condition}");
    strings(&q.collect(tenant, &sql).await.unwrap())
}

const F1: [&str; 3] = [
    "connection timeout after 30s",
    "GET /api/users 200",
    "payment declined",
];
const F2: [&str; 3] = [
    "Connection RESET by peer",
    "usuário não encontrado",
    "50% off sale",
];
const F3: [&str; 3] = [
    "disk full on /var",
    "retry_count=3 exceeded",
    "Ünïcödé ERROR",
];
const F4: [&str; 3] = ["all good", "heartbeat ok", "\u{212A}elvin scale"];

async fn four_files(f: &mut Fixture) -> Vec<FileRecord> {
    let mut files = Vec::new();
    for bodies in [F1, F2, F3, F4] {
        files.push(write(f, &bodies).await);
    }
    files
}

const SEARCHES: &[&str] = &[
    "body LIKE '%timeout%'",
    "body LIKE '%Timeout%'",
    "body ILIKE '%TIMEOUT%'",
    "lower(body) LIKE '%reset%'",
    "body ILIKE '%kelvin%'",
    "lower(body) LIKE '%kelvin%'",
    r"body LIKE '%50\%%'",
    r"body LIKE '%retry\_count%'",
    "body LIKE '%retry_count%'",
    "body LIKE '%não%'",
    "body ILIKE '%NÃO%'",
    "body ILIKE '%ünïcödé%'",
    "body NOT LIKE '%timeout%'",
    "NOT (body LIKE '%timeout%')",
    "body LIKE '%timeout%' OR body LIKE '%disk%'",
    "body LIKE '%timeout%' OR severity_number > 100",
    "body LIKE '%timeout%' AND body LIKE '%30s%'",
    "body LIKE '%timeout%' AND body LIKE '%declined%'",
    "has_token(body, 'GET')",
    "has_token(body, 'get')",
    "has_token(lower(body), 'get')",
    "has_token(lower(body), lower('RESET'))",
    "has_token(body, '30')",
    "has_token(body, '30s')",
    "has_token(body, 'n')",
    "contains(body, 'declined')",
    "strpos(body, 'disk') > 0",
    "body = 'all good'",
    "starts_with(body, 'heart')",
    "body LIKE '%zzzz%'",
];

/// Every recognised form, negated, mixed with other columns, escaped or
/// outside ASCII, answers the same rows with the text index as without.
#[tokio::test]
async fn body_searches_answer_the_same_with_and_without_the_text_index() {
    let Some(mut f) = fixture().await else { return };
    four_files(&mut f).await;
    let indexed = querier(&f, 256 << 20);
    let plain = querier(&f, 0);
    for search in SEARCHES {
        assert_eq!(
            bodies(&indexed, f.tenant, search).await,
            bodies(&plain, f.tenant, search).await,
            "{search}"
        );
    }
    assert_eq!(
        bodies(&indexed, f.tenant, "body ILIKE '%kelvin%'").await,
        ["\u{212A}elvin scale"],
        "a non-ASCII row group is read for a case-insensitive search"
    );
    assert_eq!(
        bodies(&indexed, f.tenant, r"body LIKE '%50\%%'").await,
        ["50% off sale"]
    );
    let stats = indexed.stats();
    assert!(
        stats.text_files_index.load(Ordering::Relaxed)
            < stats.text_files_catalog.load(Ordering::Relaxed)
    );
    assert_eq!(plain.stats().text_index_fetches.load(Ordering::Relaxed), 0);
    f.db.finish().await;
}

fn stage(stats: &QueryStats) -> [u64; 6] {
    [
        stats.text_searches.load(Ordering::Relaxed),
        stats.text_files_catalog.load(Ordering::Relaxed),
        stats.text_files_index.load(Ordering::Relaxed),
        stats.text_files_matched.load(Ordering::Relaxed),
        stats.text_row_groups_catalog.load(Ordering::Relaxed),
        stats.text_row_groups_index.load(Ordering::Relaxed),
    ]
}

/// Mean precision observed, from the histogram's sum and count.
fn precision(stats: &QueryStats) -> f64 {
    let h = &stats.text_precision.0;
    std::time::Duration::from_nanos(h.sum_nanos()).as_secs_f64()
        / f64::from(u32::try_from(h.count()).unwrap())
}

/// A search that prunes everything, nothing, and a known fraction: files
/// and row groups per stage, the files matched and the precision.
#[tokio::test]
async fn a_search_publishes_its_files_and_row_groups_per_stage() {
    let Some(mut f) = fixture().await else { return };
    four_files(&mut f).await;

    let q = querier(&f, 256 << 20);
    assert_eq!(
        bodies(&q, f.tenant, "has_token(body, 'zqxjv')").await.len(),
        0
    );
    // Four files of two row groups each; none kept.
    assert_eq!(stage(&q.stats()), [1, 4, 0, 0, 8, 0]);
    assert!(
        (precision(&q.stats()) - 1.0).abs() < 1e-9,
        "nothing kept, nothing read in vain"
    );
    assert_eq!(q.stats().text_latency.0.count(), 1);

    let q = querier(&f, 256 << 20);
    assert_eq!(
        bodies(&q, f.tenant, "body LIKE '%declined%'").await,
        ["payment declined"]
    );
    // "payment declined" is the third row of F1: its second row group.
    assert_eq!(stage(&q.stats()), [1, 4, 1, 1, 8, 1]);
    assert!((precision(&q.stats()) - 1.0).abs() < 1e-9);

    let mut all = fixture().await.unwrap();
    for n in 0..3 {
        let one = format!("common line {n}");
        let two = format!("other line {n}");
        write(&mut all, &[one.as_str(), two.as_str()]).await;
    }
    let q = querier(&all, 256 << 20);
    assert_eq!(bodies(&q, all.tenant, "body LIKE '%line%'").await.len(), 6);
    assert_eq!(stage(&q.stats()), [1, 3, 3, 3, 3, 3], "nothing pruned");

    // Kept but not matched: the bloom keeps the file, the row filter drops it.
    let q = querier(&all, 256 << 20);
    assert_eq!(
        bodies(
            &q,
            all.tenant,
            "body LIKE '%common%' AND severity_number > 100"
        )
        .await
        .len(),
        0
    );
    assert_eq!(stage(&q.stats()), [1, 3, 3, 0, 3, 3]);
    assert!(precision(&q.stats()).abs() < 1e-9);
    all.db.finish().await;
    f.db.finish().await;
}

/// A query the index cannot prune publishes nothing; a sort with a limit
/// reads every file to its end and publishes. A scan cut short is covered
/// by the unit tests of `text_scan`, which control its partitions.
#[tokio::test]
async fn only_a_body_search_that_reads_to_the_end_publishes() {
    let Some(mut f) = fixture().await else { return };
    four_files(&mut f).await;
    let q = querier(&f, 256 << 20);
    q.collect(
        f.tenant,
        "select body from logs where body LIKE '%on%' OR body LIKE '%e%' limit 1",
    )
    .await
    .unwrap();
    q.collect(f.tenant, "select body from logs where severity_number = 9")
        .await
        .unwrap();
    assert_eq!(stage(&q.stats())[0], 0);
    let sorted = q
        .collect(
            f.tenant,
            "select body from logs where body ILIKE '%connection%' order by timestamp desc limit 1",
        )
        .await
        .unwrap();
    assert_eq!(strings(&sorted), ["Connection RESET by peer"]);
    assert_eq!(stage(&q.stats())[0], 1);
    f.db.finish().await;
}

/// A file whose sidecar is missing, damaged, bound to another file or to
/// other bytes, or whose catalog row has no digest, is read whole: the
/// rows are all there and each counts as a miss.
#[tokio::test]
async fn a_file_without_a_trustworthy_sidecar_is_read_whole() {
    let Some(mut f) = fixture().await else { return };
    let files = four_files(&mut f).await;
    let fifth = write(&mut f, &["timeout five", "plain"]).await;
    let sidecar = |file: &FileRecord| {
        f.dir
            .path()
            .join(fs_index::text_index::sidecar_path(&file.path).unwrap())
    };
    std::fs::remove_file(sidecar(&files[0])).unwrap();
    let mut damaged = std::fs::read(sidecar(&files[1])).unwrap();
    let middle = damaged.len() / 2;
    damaged[middle] ^= 0xff;
    std::fs::write(sidecar(&files[1]), damaged).unwrap();
    std::fs::copy(sidecar(&files[3]), sidecar(&files[2])).unwrap();
    sqlx::query("update engine_file set content_sha256 = null where id = $1")
        .bind(fifth.id.0)
        .execute(f.db.catalog.pool())
        .await
        .unwrap();

    let q = querier(&f, 256 << 20);
    let plain = querier(&f, 0);
    for search in [
        "has_token(body, 'zqxjv')",
        "body LIKE '%timeout%'",
        "body LIKE '%disk%'",
    ] {
        assert_eq!(
            bodies(&q, f.tenant, search).await,
            bodies(&plain, f.tenant, search).await,
            "{search}"
        );
    }
    let stats = q.stats();
    let first = stage(&stats);
    assert_eq!(first[1], 15, "five files per search");
    assert_eq!(
        stats.text_index_misses.load(Ordering::Relaxed),
        4,
        "each file without a trustworthy sidecar is a miss once, then remembered"
    );
    // Only the fourth file's sidecar is sound: the absent token keeps the
    // other four in every search.
    assert!(first[2] >= 12, "{first:?}");
    f.db.finish().await;
}

/// An index that answers what it likes: `Skip` everywhere, or nonsense.
#[derive(Debug)]
struct Double {
    skip: bool,
}

#[async_trait]
impl FileIndex for Double {
    async fn select(&self, files: &[FileRecord], _: &Probe) -> Vec<FileSelection> {
        files
            .iter()
            .flat_map(|file| {
                if self.skip {
                    vec![FileSelection {
                        file: file.id,
                        selection: Selection::Skip,
                        row_groups: None,
                    }]
                } else {
                    vec![
                        FileSelection {
                            file: file.id,
                            selection: Selection::RowGroups(vec![7].into()),
                            row_groups: Some(2),
                        },
                        FileSelection {
                            file: file.id,
                            selection: Selection::Skip,
                            row_groups: Some(2),
                        },
                    ]
                }
            })
            .collect()
    }
}

/// The querier obeys the index contract: a double that skips every file
/// leaves no file row, one that answers nonsense reads every file.
#[tokio::test]
async fn the_querier_follows_a_test_double_of_the_index() {
    let Some(mut f) = fixture().await else { return };
    four_files(&mut f).await;
    let plain = querier(&f, 0);
    let skipping = querier(&f, 0).with_text_index(Arc::new(Double { skip: true }));
    assert_eq!(
        bodies(&skipping, f.tenant, "body LIKE '%timeout%'")
            .await
            .len(),
        0
    );
    assert_eq!(stage(&skipping.stats())[2], 0);
    let nonsense = querier(&f, 0).with_text_index(Arc::new(Double { skip: false }));
    assert_eq!(
        bodies(&nonsense, f.tenant, "body LIKE '%timeout%'").await,
        bodies(&plain, f.tenant, "body LIKE '%timeout%'").await
    );
    assert_eq!(stage(&nonsense.stats())[1..3], [4, 4], "read whole");
    f.db.finish().await;
}

/// An index that takes its time before reading every file whole, as a
/// cold cache downloading sidecars and footers would.
#[derive(Debug)]
struct Slow;

#[async_trait]
impl FileIndex for Slow {
    async fn select(&self, files: &[FileRecord], _: &Probe) -> Vec<FileSelection> {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        files
            .iter()
            .map(|file| FileSelection {
                file: file.id,
                selection: Selection::Skip,
                row_groups: None,
            })
            .collect()
    }
}

/// The time of a search includes the index lookup, even when the index
/// then prunes every file.
#[tokio::test]
async fn the_time_of_a_search_includes_its_index_lookup() {
    let Some(mut f) = fixture().await else { return };
    four_files(&mut f).await;
    let q = querier(&f, 0).with_text_index(Arc::new(Slow));
    assert_eq!(bodies(&q, f.tenant, "body LIKE '%timeout%'").await.len(), 0);
    let latency = &q.stats().text_latency.0;
    assert_eq!(latency.count(), 1);
    assert!(
        latency.sum_nanos() >= 300_000_000,
        "{} ns leaves the index lookup out",
        latency.sum_nanos()
    );
    f.db.finish().await;
}
