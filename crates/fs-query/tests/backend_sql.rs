//! The trace statements the Go backend renders in its DataFusion dialect
//! plan and run against a tenant session. The fixture is a copy written by
//! `scripts/backend-sql-fixture.py`, with the backend commit it came from.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::too_many_lines
)]

use std::fmt::Write;
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use fs_catalog::{Catalog, NewTenant, TenantId};
use fs_ingest::flush;
use fs_query::{Querier, QueryLimits};
use fs_schema::limits::Limits;
use fs_schema::spans::SpanBatchBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Connection, Executor, PgConnection};

const FORMAT_VERSION: u32 = 1;
/// 2025-05-22T21:40:00Z, inside the window every backend case queries.
const T0: u64 = 1_747_950_000_000_000_000;
const FIXTURE: &str = include_str!("fixtures/backend_trace_sql.json");

#[derive(Deserialize)]
struct Fixture {
    format_version: u32,
    backend_commit: String,
    sha256: String,
    cases: Vec<Case>,
}

#[derive(Serialize, Deserialize)]
struct Case {
    name: String,
    sql: String,
}

fn fixture() -> Fixture {
    let fixture: Fixture = serde_json::from_str(FIXTURE).unwrap();
    assert_eq!(
        fixture.format_version, FORMAT_VERSION,
        "unknown fixture format"
    );
    let digest = Sha256::digest(serde_json::to_vec(&fixture.cases).unwrap());
    let hex = digest.iter().fold(String::new(), |mut hex, byte| {
        let _ = write!(hex, "{byte:02x}");
        hex
    });
    assert_eq!(
        hex, fixture.sha256,
        "the cases do not match their sha256: regenerate the fixture, never edit it"
    );
    assert_eq!(fixture.backend_commit.len(), 40, "a full backend commit");
    assert!(!fixture.cases.is_empty());
    fixture
}

fn kv(key: &str, value: Value) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue { value: Some(value) }),
        ..KeyValue::default()
    }
}

/// A frontend root calling cart across a remote parent, and a worker span
/// with no status code, so empty and present values are both read.
fn spans() -> ExportTraceServiceRequest {
    let span =
        |id: u8, parent: Option<u8>, flags: u32, name: &str, attributes: Vec<KeyValue>| Span {
            trace_id: vec![7; 16],
            span_id: vec![id; 8],
            parent_span_id: parent.map(|p| vec![p; 8]).unwrap_or_default(),
            flags,
            name: name.into(),
            kind: 2,
            start_time_unix_nano: T0 + u64::from(id) * 1_000_000,
            end_time_unix_nano: T0 + u64::from(id) * 1_000_000 + 500_000,
            attributes,
            status: (id == 2).then_some(Status {
                code: 2,
                ..Status::default()
            }),
            ..Span::default()
        };
    let resource = |service: &str, spans: Vec<Span>| ResourceSpans {
        resource: Some(Resource {
            attributes: vec![
                kv("service.name", Value::StringValue(service.into())),
                kv("deployment.environment", Value::StringValue("prod".into())),
            ],
            ..Resource::default()
        }),
        scope_spans: vec![ScopeSpans {
            spans,
            ..ScopeSpans::default()
        }],
        ..ResourceSpans::default()
    };
    ExportTraceServiceRequest {
        resource_spans: vec![
            resource(
                "frontend",
                vec![span(
                    1,
                    None,
                    0,
                    "GET /cart",
                    vec![
                        kv("http.route", Value::StringValue("/cart".into())),
                        kv("http.response.status_code", Value::IntValue(200)),
                    ],
                )],
            ),
            resource(
                "cart",
                vec![span(
                    2,
                    Some(1),
                    0x300,
                    "serve",
                    vec![kv("http.response.status_code", Value::IntValue(404))],
                )],
            ),
            resource("worker", vec![span(3, Some(2), 0x100, "work", Vec::new())]),
        ],
    }
}

async fn write_spans(catalog: &Catalog, store: &LocalFileSystem, tenant: TenantId) {
    let mut builder = SpanBatchBuilder::new(Limits::default());
    builder.append_request(&spans()).unwrap();
    let prepared = flush::prepare(
        tenant,
        &Path::from(""),
        fs_catalog::FileId::new(),
        &[builder.finish().unwrap()],
        fs_ingest::record::FileLayout::current(flush::DEFAULT_ROW_GROUP_ROWS),
    )
    .unwrap();
    flush::upload(store, &prepared).await.unwrap();
    flush::upload_index(store, &prepared).await.unwrap();
    catalog.register_file(&prepared.file).await.unwrap();
}

#[test]
fn the_fixture_is_the_generated_copy() {
    fixture();
}

#[tokio::test]
async fn every_backend_trace_statement_plans_and_runs() {
    let fixture = fixture();
    let Ok(admin_url) = std::env::var("FS_TEST_DATABASE_URL") else {
        eprintln!("skipped: FS_TEST_DATABASE_URL is not set (run `make dev-up`)");
        return;
    };
    let schema = format!("bsql_{}", uuid::Uuid::new_v4().simple());
    let mut admin = PgConnection::connect(&admin_url).await.unwrap();
    admin
        .execute(format!("create schema {schema}").as_str())
        .await
        .unwrap();
    let sep = if admin_url.contains('?') { '&' } else { '?' };
    let catalog = Catalog::connect(
        &format!("{admin_url}{sep}options=-c%20search_path%3D{schema}"),
        4,
    )
    .await
    .unwrap();
    catalog.migrate().await.unwrap();
    let tenant = catalog
        .create_tenant(&NewTenant::with_defaults("backend"))
        .await
        .unwrap()
        .id;
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("bucket")).unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path().join("bucket")).unwrap());
    write_spans(&catalog, &store, tenant).await;
    let querier = Querier::new(
        catalog.clone(),
        store,
        Some(dir.path().join("spill")),
        QueryLimits {
            timeout: Duration::from_secs(30),
            ..QueryLimits::default()
        },
    )
    .unwrap();

    let mut failures = Vec::new();
    let mut rows = 0;
    for case in &fixture.cases {
        match querier.collect(tenant, &case.sql).await {
            Ok(batches) => rows += batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            Err(e) => failures.push(format!("{}: {e}\n  {}", case.name, case.sql)),
        }
    }

    catalog.pool().close().await;
    admin
        .execute(format!("drop schema {schema} cascade").as_str())
        .await
        .unwrap();
    assert!(
        failures.is_empty(),
        "{} of {} statements from backend {} fail:\n{}",
        failures.len(),
        fixture.cases.len(),
        fixture.backend_commit,
        failures.join("\n")
    );
    assert!(rows > 0, "the statements read the spans written");
}
