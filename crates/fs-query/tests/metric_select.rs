//! `metrics.select`, `metrics.label_values` and `metrics.label_names` over
//! metric files of every level, their postings and the hot window,
//! against a real Postgres and a local bucket.
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

use arrow::array::{
    Array, AsArray, FixedSizeBinaryBuilder, Float64Builder, Int64Builder, StringDictionaryBuilder,
    UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{Float64Type, Int32Type, Int64Type};
use arrow::record_batch::RecordBatch;
use chrono::{TimeZone, Utc};
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use fs_catalog::{Catalog, FileId, FileSetId, Level, NewFile, NewTenant, TenantId};
use fs_index::postings::{Binding, PostingsBuilder, sidecar_path};
use fs_ingest::metric_flush::{prepare_samples, prepare_series};
use fs_ingest::record::FileLayout;
use fs_query::metric_select::{LabelLookup, STALE_NAN_BITS, SelectLookup};
use fs_query::{Querier, QueryError, QueryLimits};
use fs_schema::metrics::{
    LabelMeta, MetricType, SeriesRow, Temporality, samples_schema_v1, series_batch, series_rows,
};
use futures_util::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::json;
use sqlx::{Connection, Executor, PgConnection};

/// 2026-10-24T14:00:00Z.
const T0: i64 = 1_792_677_600_000;
const HOUR: i64 = 3_600_000;

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
        let schema = format!("qsel_{}", uuid::Uuid::new_v4().simple());
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

    async fn tenant(&self, name: &str) -> TenantId {
        self.catalog
            .create_tenant(&NewTenant::with_defaults(name))
            .await
            .unwrap()
            .id
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

/// A series in the hour starting at `hour`, labelled `__name__` and `pairs`.
fn series(name: &str, fingerprint: u64, hour: i64, pairs: &[(&str, &str)]) -> SeriesRow {
    let mut labels: Vec<(Box<str>, Box<str>)> = pairs
        .iter()
        .map(|(k, v)| ((*k).into(), (*v).into()))
        .chain([("__name__".into(), name.into())])
        .collect();
    labels.sort();
    SeriesRow {
        fingerprint,
        unix_milli: hour,
        name: name.into(),
        kind: MetricType::Gauge,
        temporality: Temporality::Unspecified,
        monotonic: false,
        unit: "1".into(),
        description: String::new().into(),
        label_meta: vec![LabelMeta::LEGACY; labels.len()].into(),
        labels: labels.into(),
    }
}

/// One sample: name, fingerprint, time, value, flags and the byte its
/// source order repeats.
type Sample = (&'static str, u64, i64, f64, u32, u8);

fn samples(rows: &[Sample]) -> RecordBatch {
    let mut names = StringDictionaryBuilder::<Int32Type>::new();
    let mut temporality = StringDictionaryBuilder::<Int32Type>::new();
    let (mut fps, mut times) = (UInt64Builder::new(), Int64Builder::new());
    let (mut values, mut flags) = (Float64Builder::new(), UInt32Builder::new());
    let mut orders = FixedSizeBinaryBuilder::new(36);
    for (name, fp, time, value, flag, order) in rows {
        names.append_value(name);
        temporality.append_value("Unspecified");
        fps.append_value(*fp);
        times.append_value(*time);
        values.append_value(*value);
        flags.append_value(*flag);
        orders.append_value([*order; 36]).unwrap();
    }
    RecordBatch::try_new(
        samples_schema_v1(),
        vec![
            Arc::new(names.finish()),
            Arc::new(temporality.finish()),
            Arc::new(fps.finish()),
            Arc::new(times.finish()),
            Arc::new(values.finish()),
            Arc::new(flags.finish()),
            Arc::new(orders.finish()),
        ],
    )
    .unwrap()
}

/// Writes a set of `level` of the hour at `hour` and registers it; the
/// series file.
async fn write_set(
    catalog: &Catalog,
    store: &dyn ObjectStore,
    (tenant, level, hour): (TenantId, Level, i64),
    rows: &[Sample],
    series: &[SeriesRow],
) -> NewFile {
    let layout = FileLayout {
        version: FileLayout::METRIC_VERSION,
        ..FileLayout::current(1024)
    };
    let at = Utc.timestamp_millis_opt(hour).unwrap();
    let mut prepared = [
        prepare_samples(
            tenant,
            &Path::from(""),
            FileId::new(),
            at,
            &[samples(rows)],
            layout,
        )
        .unwrap(),
        prepare_series(
            tenant,
            &Path::from(""),
            FileId::new(),
            at,
            &series_batch(series, series.len()).unwrap(),
            layout,
        )
        .unwrap(),
    ];
    let dir = match level {
        Level::L0 => "/l0/",
        Level::L1 => "/l1/",
        Level::L2 => "/l2/",
    };
    for file in &mut prepared {
        file.file.level = level;
        file.file.usage_accounted = level != Level::L0;
        file.file.path = file.file.path.replace("/l0/", dir);
        store
            .put(
                &Path::from(file.file.path.as_str()),
                file.bytes.clone().into(),
            )
            .await
            .unwrap();
    }
    catalog
        .register_file_set(
            FileSetId::new(),
            &[prepared[0].file.clone(), prepared[1].file.clone()],
        )
        .await
        .unwrap();
    prepared[1].file.clone()
}

/// Writes the postings of the series `file`, read back in file order.
async fn write_postings(store: &dyn ObjectStore, file: &NewFile) -> Vec<u8> {
    let bytes = store
        .get(&Path::from(file.path.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let schema = Arc::clone(reader.schema());
    let mut builder = PostingsBuilder::new(usize::MAX);
    let mut ordinal = 0u64;
    for batch in reader.build().unwrap() {
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), batch.unwrap().columns().to_vec()).unwrap();
        for row in series_rows(&batch).unwrap() {
            builder
                .add(ordinal, row.labels.iter().map(|(k, v)| (&**k, &**v)))
                .unwrap();
            ordinal += 1;
        }
    }
    let binding = Binding {
        path: file.path.clone(),
        sha256: file.content_sha256.unwrap(),
        schema_version: 2,
        row_count: u32::try_from(ordinal).unwrap(),
    };
    let encoded = builder.finish(&binding).unwrap();
    put_postings(store, file, encoded.clone()).await;
    encoded
}

async fn put_postings(store: &dyn ObjectStore, file: &NewFile, bytes: Vec<u8>) {
    store
        .put(&Path::from(sidecar_path(&file.path).unwrap()), bytes.into())
        .await
        .unwrap();
}

fn querier(db: &TestDb, dir: &std::path::Path, limits: QueryLimits) -> Querier {
    Querier::new(
        db.catalog.clone(),
        Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap()),
        None,
        QueryLimits {
            timeout: Duration::from_secs(30),
            ..limits
        },
    )
    .unwrap()
}

/// One series of an answer: labels, times and values.
type Answered = (Vec<(String, String)>, Vec<i64>, Vec<f64>);

/// An [`Answered`] with its values as bits, so stale markers compare.
type Bits = (Vec<(String, String)>, Vec<i64>, Vec<u64>);

async fn select(
    q: &Querier,
    tenant: TenantId,
    body: serde_json::Value,
) -> Result<Vec<Answered>, QueryError> {
    let query = SelectLookup::parse(body.to_string().as_bytes())?.query()?;
    let (stream, warnings) = q.metrics_select(tenant, &query).await?;
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    assert!(warnings.list().is_empty(), "a select is never partial");
    let mut out = Vec::new();
    for batch in &batches {
        let (map, times, values) = (
            batch.column(0).as_map(),
            batch.column(1).as_list::<i32>(),
            batch.column(2).as_list::<i32>(),
        );
        for i in 0..batch.num_rows() {
            let entries = map.value(i);
            let (keys, vals) = (
                entries.column(0).as_string::<i32>(),
                entries.column(1).as_string::<i32>(),
            );
            let labels = (0..keys.len())
                .map(|j| (keys.value(j).to_owned(), vals.value(j).to_owned()))
                .collect();
            out.push((
                labels,
                times.value(i).as_primitive::<Int64Type>().values().to_vec(),
                values
                    .value(i)
                    .as_primitive::<Float64Type>()
                    .values()
                    .to_vec(),
            ));
        }
    }
    Ok(out)
}

async fn labels(
    q: &Querier,
    tenant: TenantId,
    body: serde_json::Value,
    values: bool,
) -> (Vec<String>, Vec<String>) {
    let query = LabelLookup::parse(body.to_string().as_bytes())
        .unwrap()
        .query(values)
        .unwrap();
    let (stream, warnings) = q.metrics_labels(tenant, &query).await.unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let listed = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_string::<i32>()
                .iter()
                .map(|v| v.unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .collect();
    (listed, warnings.list())
}

fn named(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

fn window(matchers: &serde_json::Value) -> serde_json::Value {
    json!({"matchers": matchers, "start_unix_milli": T0, "end_unix_milli": T0 + HOUR - 1})
}

fn m(name: &str, kind: &str, value: &str) -> serde_json::Value {
    json!({"name": name, "type": kind, "value": value})
}

#[tokio::test]
async fn a_select_answers_as_the_go_backend_does() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let tenant = db.tenant("acme").await;
    let other = db.tenant("other").await;
    let mut delta = series("cpu", 3, T0, &[("host", "c")]);
    delta.temporality = Temporality::Delta;
    let mut cumulative = series("cpu", 2, T0, &[("host", "b"), ("zone", "eu")]);
    cumulative.temporality = Temporality::Cumulative;
    write_set(
        &db.catalog,
        &store,
        (tenant, Level::L0, T0),
        &[
            ("cpu", 1, T0 + 1000, 1.0, 0, 1),
            ("cpu", 1, T0 + 2000, 2.0, 1, 1),
            ("cpu", 2, T0 + 1000, 5.0, 0, 1),
            ("cpu", 3, T0 + 1000, 7.0, 0, 1),
            ("mem", 4, T0 + 1500, 9.0, 0, 1),
        ],
        &[
            series("cpu", 1, T0, &[("host", "a"), ("zone", "")]),
            cumulative,
            delta,
            series("mem", 4, T0, &[("host", "a")]),
        ],
    )
    .await;
    write_set(
        &db.catalog,
        &store,
        (other, Level::L0, T0),
        &[("cpu", 9, T0 + 1000, 99.0, 0, 1)],
        &[series("cpu", 9, T0, &[("host", "a")])],
    )
    .await;
    let q = querier(&db, dir.path(), QueryLimits::default());
    let stale = f64::from_bits(STALE_NAN_BITS);
    let a = (
        named(&[("__name__", "cpu"), ("host", "a")]),
        vec![T0 + 1000, T0 + 2000],
        vec![1.0, stale],
    );
    let b = (
        named(&[("__name__", "cpu"), ("host", "b"), ("zone", "eu")]),
        vec![T0 + 1000],
        vec![5.0],
    );
    let c = (
        named(&[("__name__", "cpu"), ("host", "c")]),
        vec![T0 + 1000],
        vec![7.0],
    );
    let mem = (
        named(&[("__name__", "mem"), ("host", "a")]),
        vec![T0 + 1500],
        vec![9.0],
    );
    let bits = |answer: Vec<Answered>| -> Vec<Bits> {
        answer
            .into_iter()
            .map(|(l, t, v)| (l, t, v.iter().map(|v| v.to_bits()).collect()))
            .collect()
    };
    let cases = [
        (
            "by name, Delta left out, empty labels dropped",
            json!([m("__name__", "=", "cpu")]),
            vec![a.clone(), b.clone()],
        ),
        (
            "an absent label is empty",
            json!([m("__name__", "=", "cpu"), m("zone", "=", "")]),
            vec![a.clone()],
        ),
        (
            "not equal and a name regex",
            json!([m("__name__", "=~", "c.*"), m("host", "!=", "a")]),
            vec![b.clone()],
        ),
        (
            "a label across names, in labelset order",
            json!([m("host", "=~", "a")]),
            vec![a.clone(), mem.clone()],
        ),
        ("anchored", json!([m("__name__", "=~", "cp")]), vec![]),
        (
            "negative regex on the name",
            json!([m("__name__", "!~", "cpu")]),
            vec![mem.clone()],
        ),
        (
            "no matchers selects every series",
            json!([]),
            vec![a.clone(), b.clone(), mem.clone()],
        ),
    ];
    for (what, matchers, expected) in cases {
        let answer = select(&q, tenant, window(&matchers)).await.unwrap();
        assert_eq!(bits(answer), bits(expected), "{what}");
    }
    let delta_only = json!({
        "matchers": [m("__name__", "=", "cpu")],
        "start_unix_milli": T0, "end_unix_milli": T0 + HOUR - 1,
        "temporalities": ["Delta"],
    });
    assert_eq!(
        bits(select(&q, tenant, delta_only).await.unwrap()),
        bits(vec![c])
    );
    let early = json!({"matchers": [m("host", "=", "a")], "start_unix_milli": T0 + 1000, "end_unix_milli": T0 + 1500});
    assert_eq!(
        bits(select(&q, tenant, early).await.unwrap()),
        bits(vec![
            (a.0.clone(), vec![T0 + 1000], vec![1.0]),
            (mem.0.clone(), vec![T0 + 1500], vec![9.0]),
        ]),
        "both bounds inclusive"
    );
    let theirs = select(&q, other, window(&json!([m("__name__", "=", "cpu")])))
        .await
        .unwrap();
    assert_eq!(theirs.len(), 1, "another tenant sees only its own");
    assert_eq!(theirs[0].2, [99.0]);
    db.finish().await;
}

#[tokio::test]
async fn fragments_of_every_level_answer_once_the_last_written_winning() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let tenant = db.tenant("acme").await;
    let key = || vec![series("cpu", 1, T0, &[("host", "a")])];
    let sets: [(Level, Vec<Sample>); 4] = [
        (
            Level::L0,
            vec![
                ("cpu", 1, T0 + 1000, 1.0, 0, 1),
                ("cpu", 1, T0 + 3000, 30.0, 0, 1),
            ],
        ),
        (Level::L1, vec![("cpu", 1, T0 + 1000, 3.0, 0, 3)]),
        (Level::L1, vec![("cpu", 1, T0 + 1000, 2.0, 0, 2)]),
        (
            Level::L2,
            vec![
                ("cpu", 1, T0 + 1000, 0.5, 0, 0),
                ("cpu", 1, T0 + 2000, 20.0, 0, 0),
            ],
        ),
    ];
    for (level, rows) in sets {
        let file = write_set(&db.catalog, &store, (tenant, level, T0), &rows, &key()).await;
        if level != Level::L0 {
            write_postings(&store, &file).await;
        }
    }
    let q = querier(&db, dir.path(), QueryLimits::default());
    let answer = select(&q, tenant, window(&json!([m("host", "=", "a")])))
        .await
        .unwrap();
    assert_eq!(
        answer,
        vec![(
            named(&[("__name__", "cpu"), ("host", "a")]),
            vec![T0 + 1000, T0 + 2000, T0 + 3000],
            vec![3.0, 20.0, 30.0]
        )]
    );
    db.finish().await;
}

#[tokio::test]
async fn an_identity_hidden_in_another_file_fails_the_select() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let tenant = db.tenant("acme").await;
    let shown = write_set(
        &db.catalog,
        &store,
        (tenant, Level::L1, T0),
        &[
            ("cpu", 7, T0 + 1000, 1.0, 0, 1),
            ("cpu", 8, T0 + 1000, 8.0, 0, 1),
        ],
        &[
            series("cpu", 7, T0, &[("host", "a")]),
            series("cpu", 8, T0, &[("host", "c")]),
        ],
    )
    .await;
    write_postings(&store, &shown).await;
    // The same key, another identity, in a file whose postings hold no
    // row of `host="a"`.
    let hidden = write_set(
        &db.catalog,
        &store,
        (tenant, Level::L1, T0),
        &[("cpu", 7, T0 + 2000, 2.0, 0, 1)],
        &[series("cpu", 7, T0, &[("host", "b")])],
    )
    .await;
    write_postings(&store, &hidden).await;
    // A unit that changes across hours.
    let mut seconds = series("mem", 9, T0 + HOUR, &[("host", "x")]);
    seconds.unit = "s".into();
    write_set(
        &db.catalog,
        &store,
        (tenant, Level::L0, T0),
        &[("mem", 9, T0 + 5, 1.0, 0, 1)],
        &[series("mem", 9, T0, &[("host", "x")])],
    )
    .await;
    write_set(
        &db.catalog,
        &store,
        (tenant, Level::L0, T0 + HOUR),
        &[("mem", 9, T0 + HOUR + 5, 1.0, 0, 1)],
        &[seconds],
    )
    .await;

    let q = querier(&db, dir.path(), QueryLimits::default());
    let err = select(&q, tenant, window(&json!([m("host", "=", "a")])))
        .await
        .unwrap_err();
    assert!(
        matches!(err, QueryError::Ambiguous { ref name, fingerprint: 7 } if name == "cpu"),
        "{err}"
    );
    assert_eq!(
        q.stats().metric_index_pruned.load(Ordering::Relaxed),
        1,
        "the hidden file was pruned in step 1 only"
    );
    let fine = select(&q, tenant, window(&json!([m("host", "=", "c")])))
        .await
        .unwrap();
    assert_eq!(fine.len(), 1, "a key without a collision still answers");
    let two_hours = json!({"matchers": [m("__name__", "=", "mem")], "start_unix_milli": T0, "end_unix_milli": T0 + 2 * HOUR});
    let err = select(&q, tenant, two_hours).await.unwrap_err();
    assert!(
        matches!(err, QueryError::Ambiguous { fingerprint: 9, .. }),
        "{err}"
    );
    let one_hour = json!({"matchers": [m("__name__", "=", "mem")], "start_unix_milli": T0, "end_unix_milli": T0 + HOUR - 1});
    assert_eq!(
        select(&q, tenant, one_hour).await.unwrap().len(),
        1,
        "only the hours of the window count"
    );
    assert_eq!(q.stats().metric_ambiguous.load(Ordering::Relaxed), 2);
    db.finish().await;
}

#[tokio::test]
async fn one_fingerprint_under_two_names_is_two_series() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let tenant = db.tenant("acme").await;
    write_set(
        &db.catalog,
        &store,
        (tenant, Level::L0, T0),
        &[("cpu", 5, T0 + 1, 1.0, 0, 1), ("mem", 5, T0 + 1, 2.0, 0, 1)],
        &[
            series("cpu", 5, T0, &[("host", "a")]),
            series("mem", 5, T0, &[("host", "a")]),
        ],
    )
    .await;
    let q = querier(&db, dir.path(), QueryLimits::default());
    let answer = select(&q, tenant, window(&json!([m("host", "=", "a")])))
        .await
        .unwrap();
    let values: Vec<(String, f64)> = answer
        .iter()
        .map(|(l, _, v)| (l[0].1.clone(), v[0]))
        .collect();
    assert_eq!(values, [("cpu".to_owned(), 1.0), ("mem".to_owned(), 2.0)]);
    db.finish().await;
}

/// The last sample of each bucket of the Go backend's formula.
fn reduced(raw: &[(i64, f64)], first_eval: i64, step: i64) -> Vec<(i64, f64)> {
    let bucket = |t: i64| {
        if t <= first_eval {
            0
        } else {
            (t - first_eval - 1) / step + 1
        }
    };
    let mut out: Vec<(i64, f64)> = Vec::new();
    for &(t, v) in raw {
        match out.last_mut() {
            Some(last) if bucket(last.0) == bucket(t) => *last = (t, v),
            _ => out.push((t, v)),
        }
    }
    out
}

#[tokio::test]
async fn last_per_step_is_the_raw_answer_reduced() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let tenant = db.tenant("acme").await;
    let rows: Vec<Sample> = (0..300i32)
        .map(|k| {
            (
                "cpu",
                1,
                T0 + i64::from(k) * 137 % 40_000,
                f64::from(k),
                0,
                1,
            )
        })
        .collect();
    // A duplicate time written later, so dedupe precedes the reduction.
    write_set(
        &db.catalog,
        &store,
        (tenant, Level::L0, T0),
        &rows,
        &[series("cpu", 1, T0, &[])],
    )
    .await;
    write_set(
        &db.catalog,
        &store,
        (tenant, Level::L0, T0),
        &[("cpu", 1, T0 + 137, -1.0, 0, 9)],
        &[series("cpu", 1, T0, &[])],
    )
    .await;
    let q = querier(&db, dir.path(), QueryLimits::default());
    let matchers = json!([m("__name__", "=", "cpu")]);
    let raw = select(&q, tenant, window(&matchers)).await.unwrap();
    let raw: Vec<(i64, f64)> = raw[0]
        .1
        .iter()
        .copied()
        .zip(raw[0].2.iter().copied())
        .collect();
    assert!(
        raw.windows(2).all(|w| w[0].0 < w[1].0),
        "one sample a time, in order"
    );
    assert_eq!(raw.iter().find(|(t, _)| *t == T0 + 137).unwrap().1, -1.0);
    for (first_eval, step) in [
        (T0 - 5, 1),
        (T0, 7),
        (T0 + 999, 1000),
        (T0 + 30_000, 60_000),
        (T0 + HOUR, 5),
        (T0 + 20_000, 3_333),
    ] {
        let body = json!({
            "matchers": matchers, "start_unix_milli": T0, "end_unix_milli": T0 + HOUR - 1,
            "last_per_step": {"first_eval_unix_milli": first_eval, "step_ms": step},
        });
        let answer = select(&q, tenant, body).await.unwrap();
        let got: Vec<(i64, f64)> = answer[0]
            .1
            .iter()
            .copied()
            .zip(answer[0].2.iter().copied())
            .collect();
        assert_eq!(
            got,
            reduced(&raw, first_eval, step),
            "first eval {first_eval}, step {step}"
        );
    }
    db.finish().await;
}

#[tokio::test]
async fn postings_change_what_is_read_not_the_answer() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let tenant = db.tenant("acme").await;
    let a = write_set(
        &db.catalog,
        &store,
        (tenant, Level::L1, T0),
        &[("cpu", 1, T0 + 1, 1.0, 0, 1)],
        &[series("cpu", 1, T0, &[("host", "a")])],
    )
    .await;
    let b = write_set(
        &db.catalog,
        &store,
        (tenant, Level::L1, T0),
        &[("cpu", 2, T0 + 1, 2.0, 0, 1)],
        &[series("cpu", 2, T0, &[("host", "b")])],
    )
    .await;
    let of_a = write_postings(&store, &a).await;
    let of_b = write_postings(&store, &b).await;
    let mut damaged = of_b.clone();
    *damaged.last_mut().unwrap() ^= 1;
    let selector = window(&json!([m("host", "=~", "a|z")]));
    let expected = vec![(
        named(&[("__name__", "cpu"), ("host", "a")]),
        vec![T0 + 1],
        vec![1.0],
    )];
    for (what, b_postings, pruned) in [
        ("usable", Some(of_b.clone()), 1),
        ("damaged", Some(damaged), 0),
        ("of another file", Some(of_a.clone()), 0),
        ("missing", None, 0),
    ] {
        match b_postings {
            Some(bytes) => put_postings(&store, &b, bytes).await,
            None => store
                .delete(&Path::from(sidecar_path(&b.path).unwrap()))
                .await
                .unwrap(),
        }
        let q = querier(&db, dir.path(), QueryLimits::default());
        assert_eq!(
            select(&q, tenant, selector.clone()).await.unwrap(),
            expected,
            "{what}"
        );
        assert_eq!(
            q.stats().metric_index_pruned.load(Ordering::Relaxed),
            pruned,
            "{what}"
        );
        let unnarrowed = querier(&db, dir.path(), QueryLimits::default());
        let names = json!({"matchers": [m("host", "!=", "z")], "start_unix_milli": T0, "end_unix_milli": T0 + HOUR - 1});
        assert_eq!(
            labels(&unnarrowed, tenant, names, false).await.0,
            ["__name__", "host"]
        );
        assert_eq!(
            unnarrowed
                .stats()
                .metric_index_fetches
                .load(Ordering::Relaxed),
            0,
            "{what}: no matcher narrows by postings, none is fetched"
        );
        let disabled = querier(
            &db,
            dir.path(),
            QueryLimits {
                metric_index_cache_bytes: 0,
                ..QueryLimits::default()
            },
        );
        assert_eq!(
            select(&disabled, tenant, selector.clone()).await.unwrap(),
            expected,
            "{what}, no cache"
        );
    }
    db.finish().await;
}

#[tokio::test]
async fn the_limits_refuse_before_the_work() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let tenant = db.tenant("acme").await;
    let rows = [
        ("cpu", 1, T0 + 1, 1.0, 0, 1),
        ("cpu", 1, T0 + 2, 1.0, 0, 1),
        ("cpu", 2, T0 + 1, 2.0, 0, 1),
    ];
    write_set(
        &db.catalog,
        &store,
        (tenant, Level::L0, T0),
        &rows,
        &[
            series("cpu", 1, T0, &[("host", "a")]),
            series("cpu", 2, T0, &[("host", "b")]),
        ],
    )
    .await;
    let selector = window(&json!([m("__name__", "=", "cpu")]));
    let few_rows = querier(
        &db,
        dir.path(),
        QueryLimits {
            max_rows: 2,
            ..QueryLimits::default()
        },
    );
    assert!(matches!(
        select(&few_rows, tenant, selector.clone()).await,
        Err(QueryError::TooManyRows(2))
    ));
    let regex = querier(
        &db,
        dir.path(),
        QueryLimits {
            metrics_max_regex_bytes: 3,
            ..QueryLimits::default()
        },
    );
    let long = window(&json!([m("host", "=~", "abcd")]));
    assert!(matches!(
        select(&regex, tenant, long).await,
        Err(QueryError::InvalidAction(_))
    ));
    let matchers = querier(
        &db,
        dir.path(),
        QueryLimits {
            metrics_max_matchers: 1,
            ..QueryLimits::default()
        },
    );
    let two = window(&json!([m("host", "=", "a"), m("__name__", "=", "cpu")]));
    assert!(matches!(
        select(&matchers, tenant, two).await,
        Err(QueryError::InvalidAction(_))
    ));

    // With the samples gone, a select that read them would fail on the
    // store: the series cap refuses first.
    let mut listed = store.list(Some(&Path::from(format!("t/{}/metrics/samples", tenant.0))));
    while let Some(meta) = futures_util::StreamExt::next(&mut listed).await {
        store.delete(&meta.unwrap().location).await.unwrap();
    }
    let one_series = querier(
        &db,
        dir.path(),
        QueryLimits {
            metrics_max_series: 1,
            ..QueryLimits::default()
        },
    );
    assert!(matches!(
        select(&one_series, tenant, selector.clone()).await,
        Err(QueryError::TooManySeries(1))
    ));
    let read = querier(&db, dir.path(), QueryLimits::default());
    assert!(
        select(&read, tenant, selector).await.is_err(),
        "the samples are gone indeed"
    );
    db.finish().await;
}

#[tokio::test]
async fn label_listings_are_sorted_truncated_and_scoped() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let tenant = db.tenant("acme").await;
    let other = db.tenant("other").await;
    let mut delta = series("cpu", 4, T0, &[("host", "d"), ("rack", "r1")]);
    delta.temporality = Temporality::Delta;
    write_set(
        &db.catalog,
        &store,
        (tenant, Level::L0, T0),
        &[("cpu", 1, T0 + 1, 1.0, 0, 1)],
        &[
            series("cpu", 1, T0, &[("host", "c"), ("zone", "")]),
            series("cpu", 2, T0, &[("host", "a")]),
            series("mem", 3, T0, &[("host", "b"), ("dc", "x")]),
            delta,
        ],
    )
    .await;
    write_set(
        &db.catalog,
        &store,
        (other, Level::L0, T0),
        &[("cpu", 9, T0 + 1, 1.0, 0, 1)],
        &[series("cpu", 9, T0, &[("host", "z"), ("secret", "s")])],
    )
    .await;
    let q = querier(&db, dir.path(), QueryLimits::default());
    let body = |label: Option<&str>, matchers: serde_json::Value, limit: Option<usize>| {
        let mut body =
            json!({"matchers": matchers, "start_unix_milli": T0, "end_unix_milli": T0 + HOUR - 1});
        if let Some(label) = label {
            body["label"] = json!(label);
        }
        if let Some(limit) = limit {
            body["limit"] = json!(limit);
        }
        body
    };
    let values =
        |label, matchers, limit| labels(&q, tenant, body(Some(label), matchers, limit), true);
    assert_eq!(
        values("host", json!([]), None).await.0,
        ["a", "b", "c"],
        "a Delta series is not listed"
    );
    assert_eq!(
        values("host", json!([]), Some(2)).await.0,
        ["a", "b"],
        "the first in order"
    );
    assert_eq!(
        values("host", json!([m("__name__", "=", "cpu")]), None)
            .await
            .0,
        ["a", "c"]
    );
    assert_eq!(values("__name__", json!([]), None).await.0, ["cpu", "mem"]);
    assert_eq!(
        values("zone", json!([]), None).await.0,
        Vec::<String>::new(),
        "empty values are absent"
    );
    let names = labels(
        &q,
        tenant,
        body(None, json!([m("host", "=~", "b|c")]), None),
        false,
    )
    .await
    .0;
    assert_eq!(names, ["__name__", "dc", "host"]);
    let names = labels(&q, tenant, body(None, json!([]), Some(1)), false)
        .await
        .0;
    assert_eq!(names, ["__name__"]);
    let theirs = labels(&q, other, body(None, json!([]), None), false)
        .await
        .0;
    assert_eq!(theirs, ["__name__", "host", "secret"]);
    db.finish().await;
}

#[tokio::test]
async fn an_ingester_that_does_not_answer_fails_a_select_but_not_a_listing() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let tenant = db.tenant("acme").await;
    let now = Utc::now().timestamp_millis();
    let hour = now.div_euclid(HOUR) * HOUR;
    write_set(
        &db.catalog,
        &store,
        (tenant, Level::L0, hour),
        &[("cpu", 1, now - 10, 1.0, 0, 1)],
        &[series("cpu", 1, hour, &[("host", "a")])],
    )
    .await;
    let q = querier(&db, dir.path(), QueryLimits::default());
    let recent = |matchers: serde_json::Value| json!({"matchers": matchers, "start_unix_milli": now - 60_000, "end_unix_milli": now + 60_000});
    assert_eq!(
        select(&q, tenant, recent(json!([]))).await.unwrap().len(),
        1,
        "no ingester, nothing to wait for"
    );

    // Alive by its heartbeat, but nothing listens.
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = silent.local_addr().unwrap().to_string();
    drop(silent);
    db.catalog
        .heartbeat("ingester-gone", &address)
        .await
        .unwrap();
    let err = select(&q, tenant, recent(json!([]))).await.unwrap_err();
    assert!(
        matches!(err, QueryError::HotUnavailable(ref ids) if ids == &["ingester-gone".to_owned()]),
        "{err}"
    );
    let mut body = recent(json!([]));
    body["label"] = json!("host");
    let (listed, warnings) = labels(&q, tenant, body, true).await;
    assert_eq!(listed, ["a"]);
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    db.finish().await;
}

fn gauge(
    host: &str,
    points: &[(i64, f64)],
) -> opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest {
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::metrics::v1::metric::Data;
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as Number;
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    };
    let kv = |key: &str, value: &str| KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
        ..KeyValue::default()
    };
    let data_points = points
        .iter()
        .map(|(ms, value)| NumberDataPoint {
            attributes: vec![kv("host", host)],
            time_unix_nano: u64::try_from(*ms).unwrap() * 1_000_000,
            value: Some(Number::AsDouble(*value)),
            ..NumberDataPoint::default()
        })
        .collect();
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
                attributes: vec![kv("service.name", "api")],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "hot_cpu".into(),
                    data: Some(Data::Gauge(Gauge { data_points })),
                    ..Metric::default()
                }],
                ..ScopeMetrics::default()
            }],
            schema_url: String::new(),
        }],
    }
}

#[tokio::test]
async fn the_hot_window_and_files_answer_together() {
    use fs_ingest::{IngestConfig, Ingester};
    use fs_schema::limits::Limits;
    use fs_schema::metrics::{MetricLimits, Source, convert, preflight};
    use fs_wal::{Wal, WalConfig};
    use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;

    let Some(db) = TestDb::new().await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("bucket")).unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path().join("bucket")).unwrap());
    let tenant = db.tenant("acme").await;
    let (token, _) = db
        .catalog
        .create_token(tenant, "collector", fs_catalog::TokenScope::Ingest)
        .await
        .unwrap();
    let now = Utc::now().timestamp_millis();
    let hour = now.div_euclid(HOUR) * HOUR;
    let (old, recent) = (hour.max(now - 50_000), now - 1000);

    // A flushed file holds an older point and, written earlier, the recent one.
    let request = gauge("h", &[(old, 1.0), (recent, -1.0)]);
    let (limits, metrics) = (Limits::default(), MetricLimits::default());
    let checked = preflight(&request, &limits, &metrics).unwrap();
    let source = Source {
        instance: [0; 16],
        segment: 0,
        offset: 0,
    };
    let rows = convert(&request, &limits, &metrics, &checked, source).unwrap();
    let layout = FileLayout {
        version: FileLayout::METRIC_VERSION,
        ..FileLayout::current(1024)
    };
    let at = Utc.timestamp_millis_opt(hour).unwrap();
    let files = [
        prepare_samples(
            tenant,
            &Path::from(""),
            FileId::new(),
            at,
            &[rows.samples],
            layout,
        )
        .unwrap(),
        prepare_series(
            tenant,
            &Path::from(""),
            FileId::new(),
            at,
            &series_batch(&rows.series, rows.series.len()).unwrap(),
            layout,
        )
        .unwrap(),
    ];
    for file in &files {
        fs_ingest::flush::upload(store.as_ref(), file)
            .await
            .unwrap();
    }
    db.catalog
        .register_file_set(
            FileSetId::new(),
            &[files[0].file.clone(), files[1].file.clone()],
        )
        .await
        .unwrap();

    let config = IngestConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        hot_listen: Some("127.0.0.1:0".parse().unwrap()),
        flush_interval: Duration::from_secs(3600),
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
        .heartbeat("ingester-hot", &hot.to_string())
        .await
        .unwrap();
    let mut client = MetricsServiceClient::connect(format!("http://{otlp}"))
        .await
        .unwrap();
    let mut export = tonic::Request::new(gauge("h", &[(recent, 5.0)]));
    export
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    client.export(export).await.unwrap();

    let q = Querier::new(
        db.catalog.clone(),
        store,
        None,
        QueryLimits {
            timeout: Duration::from_secs(30),
            ..QueryLimits::default()
        },
    )
    .unwrap();
    let body = json!({
        "matchers": [m("__name__", "=", "hot_cpu"), m("host", "=", "h")],
        "start_unix_milli": now - 60_000, "end_unix_milli": now + 60_000,
    });
    let answer = select(&q, tenant, body).await.unwrap();
    assert_eq!(answer.len(), 1, "{answer:?}");
    assert_eq!(answer[0].1, [old, recent]);
    assert_eq!(
        answer[0].2,
        [1.0, 5.0],
        "the hot window's later write wins its time"
    );
    shutdown.send(true).unwrap();
    task.await.unwrap().unwrap();
    db.finish().await;
}
