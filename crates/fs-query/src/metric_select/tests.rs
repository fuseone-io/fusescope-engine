#![allow(clippy::indexing_slicing, clippy::type_complexity)]

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation,
};
use fs_schema::metrics::{LabelLevel, LabelMeta, MetricType};

use super::matchers::{MatchType, RawMatcher, compile};
use super::output::{Series, batch, fuse};
use super::*;

fn reservation(bytes: usize) -> MemoryReservation {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
    MemoryConsumer::new("test").register(&pool)
}

fn raw(name: &str, kind: MatchType, value: &str) -> RawMatcher {
    RawMatcher {
        name: name.into(),
        kind,
        value: value.into(),
    }
}

const LIMITS: MatcherLimits = MatcherLimits {
    max_matchers: 8,
    max_regex_bytes: 64,
};

fn labels(pairs: &[(&str, &str)]) -> Box<[(Box<str>, Box<str>)]> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).into(), (*v).into()))
        .collect()
}

#[test]
fn matchers_follow_prometheus_with_an_absent_label_empty() {
    let set = labels(&[("__name__", "cpu"), ("host", "web-1")]);
    let cases = [
        (raw("host", MatchType::Equal, "web-1"), true),
        (raw("host", MatchType::Equal, "web"), false),
        (raw("zone", MatchType::Equal, ""), true),
        (raw("host", MatchType::NotEqual, "web-1"), false),
        (raw("zone", MatchType::NotEqual, ""), false),
        (raw("zone", MatchType::NotEqual, "eu"), true),
        (raw("host", MatchType::Regex, "web-.*"), true),
        (raw("host", MatchType::Regex, "web"), false),
        (raw("host", MatchType::Regex, "eb-1"), false),
        (raw("zone", MatchType::Regex, ".*"), true),
        (raw("zone", MatchType::Regex, ".+"), false),
        (raw("host", MatchType::NotRegex, "web-.*"), false),
        (raw("zone", MatchType::NotRegex, ".+"), true),
        (raw("zone", MatchType::NotRegex, ""), false),
        (raw("__name__", MatchType::Equal, "cpu"), true),
        (raw("host", MatchType::Regex, "web-1|(?m:^)x"), true),
    ];
    for (matcher, expected) in cases {
        let what = format!("{matcher:?}");
        let compiled = compile(&[matcher], LIMITS, &mut reservation(1 << 24)).unwrap();
        assert_eq!(all_match(&compiled, &set), expected, "{what}");
    }
    let anchored = compile(
        &[raw("v", MatchType::Regex, "a|b")],
        LIMITS,
        &mut reservation(1 << 24),
    )
    .unwrap();
    assert!(
        !all_match(&anchored, &labels(&[("v", "ab")])),
        "the alternation is grouped before anchoring"
    );
    assert!(
        !all_match(&anchored, &labels(&[("v", "a\n")])),
        "the end is the end of the text"
    );
}

#[test]
fn only_a_positive_matcher_that_refuses_empty_requires_its_label() {
    let cases = [
        (raw("h", MatchType::Equal, "x"), true),
        (raw("h", MatchType::Equal, ""), false),
        (raw("h", MatchType::Regex, ".+"), true),
        (raw("h", MatchType::Regex, "x|"), false),
        (raw("h", MatchType::NotEqual, ""), false),
        (raw("h", MatchType::NotRegex, ""), false),
    ];
    for (matcher, expected) in cases {
        let what = format!("{matcher:?}");
        let compiled = compile(&[matcher], LIMITS, &mut reservation(1 << 24)).unwrap();
        assert_eq!(compiled[0].requires_label(), expected, "{what}");
    }
}

#[test]
fn matchers_are_bounded_before_they_compile() {
    let many: Vec<RawMatcher> = (0..9)
        .map(|n| raw(&format!("k{n}"), MatchType::Equal, "v"))
        .collect();
    let refused = |e: QueryError| matches!(e, QueryError::InvalidAction(_));
    assert!(refused(
        compile(&many, LIMITS, &mut reservation(1 << 24)).unwrap_err()
    ));
    let long = [raw("k", MatchType::Regex, &"a".repeat(65))];
    assert!(refused(
        compile(&long, LIMITS, &mut reservation(1 << 24)).unwrap_err()
    ));
    let broken = [raw("k", MatchType::Regex, "(")];
    assert!(refused(
        compile(&broken, LIMITS, &mut reservation(1 << 24)).unwrap_err()
    ));
    let huge = [raw("k", MatchType::Regex, "\\w{1000}\\w{1000}")];
    let big = MatcherLimits {
        max_regex_bytes: 1 << 20,
        ..LIMITS
    };
    assert!(
        refused(compile(&huge, big, &mut reservation(1 << 24)).unwrap_err()),
        "past its compile limit"
    );

    let mut tight = reservation(2 * REGEX_SIZE_LIMIT + 1);
    let two = [
        raw("a", MatchType::Regex, "x"),
        raw("b", MatchType::NotRegex, "y"),
    ];
    let err = compile(&two, LIMITS, &mut tight).unwrap_err();
    assert!(
        matches!(
            err,
            QueryError::DataFusion(datafusion::error::DataFusionError::ResourcesExhausted(_))
        ),
        "each regex reserves its limits first: {err}"
    );
    assert_eq!(tight.size(), 2 * REGEX_SIZE_LIMIT);
    let clamped = MetricLimits {
        max_series: usize::MAX,
        max_rows: 1,
        matchers: MatcherLimits {
            max_matchers: usize::MAX,
            max_regex_bytes: usize::MAX,
        },
    }
    .clamped();
    assert_eq!(
        (
            clamped.max_series,
            clamped.matchers.max_matchers,
            clamped.matchers.max_regex_bytes
        ),
        (MAX_SERIES, MAX_MATCHERS, MAX_REGEX_BYTES)
    );
}

fn buckets(first_eval: i64, step_ms: i64, end: i64) -> Buckets {
    SelectLookup {
        matchers: vec![],
        start_unix_milli: end.min(first_eval) - 1,
        end_unix_milli: end,
        temporalities: None,
        last_per_step: Some(LastPerStep {
            first_eval_unix_milli: first_eval,
            step_ms,
        }),
    }
    .query()
    .unwrap()
    .buckets
    .unwrap()
}

/// The Go backend's `if(t <= first_eval, 0, intDiv(t - first_eval - 1, step) + 1)`.
#[test]
fn step_buckets_match_the_go_backend() {
    let golden: [(i64, i64, &[(i64, i64)]); 3] = [
        (
            1000,
            60,
            &[
                (999, 0),
                (1000, 0),
                (1001, 1),
                (1060, 1),
                (1061, 2),
                (1120, 2),
                (1121, 3),
            ],
        ),
        (
            -100,
            50,
            &[
                (-150, 0),
                (-100, 0),
                (-99, 1),
                (-50, 1),
                (-49, 2),
                (0, 2),
                (1, 3),
            ],
        ),
        (
            299_999,
            60_000,
            &[
                (0, 0),
                (299_999, 0),
                (300_000, 1),
                (359_999, 1),
                (360_000, 2),
            ],
        ),
    ];
    for (first_eval, step, vectors) in golden {
        let b = buckets(first_eval, step, 400_000);
        for &(t, expected) in vectors {
            assert_eq!(
                b.of(t),
                Some(expected),
                "t {t}, first eval {first_eval}, step {step}"
            );
        }
    }
    let instant = buckets(5000, 1, 5000);
    assert_eq!(instant.of(-7), Some(0));
    assert_eq!(
        instant.of(5000),
        Some(0),
        "an instant query anchored at its end is one bucket"
    );
}

#[test]
fn a_select_checks_its_bounds_step_and_temporalities() {
    let lookup = |body: &str| SelectLookup::parse(body.as_bytes()).and_then(|l| l.query());
    let invalid =
        |r: Result<SelectQuery, QueryError>| matches!(r, Err(QueryError::InvalidAction(_)));
    assert!(invalid(lookup(
        r#"{"start_unix_milli":2,"end_unix_milli":1}"#
    )));
    assert!(invalid(lookup(
        r#"{"start_unix_milli":1,"end_unix_milli":2,"last_per_step":{"first_eval_unix_milli":1,"step_ms":0}}"#
    )));
    assert!(
        invalid(lookup(&format!(
            r#"{{"start_unix_milli":0,"end_unix_milli":{},"last_per_step":{{"first_eval_unix_milli":{},"step_ms":1}}}}"#,
            i64::MAX,
            i64::MIN
        ))),
        "a bucket that overflows"
    );
    assert!(invalid(lookup(
        r#"{"start_unix_milli":1,"end_unix_milli":2,"temporalities":["Weekly"]}"#
    )));
    assert!(invalid(lookup(
        r#"{"start_unix_milli":1,"end_unix_milli":2,"temporalities":[]}"#
    )));
    assert!(
        invalid(lookup(
            r#"{"start_unix_milli":1,"end_unix_milli":2,"step":5}"#
        )),
        "an unknown field"
    );
    assert!(invalid(lookup(
        r#"{"start_unix_milli":1,"end_unix_milli":2,"matchers":[{"name":"a","type":"==","value":"b"}]}"#
    )));
    let query = lookup(r#"{"start_unix_milli":-1,"end_unix_milli":3600000}"#).unwrap();
    assert_eq!(
        query.temporalities,
        [Temporality::Cumulative, Temporality::Unspecified]
    );
    assert_eq!(
        query.window.first_hour, -3_600_000,
        "the hour of a negative start is the one before"
    );
    let labels =
        |body: &str, values| LabelLookup::parse(body.as_bytes()).and_then(|l| l.query(values));
    assert!(
        labels(r#"{"start_unix_milli":1,"end_unix_milli":2}"#, true).is_err(),
        "values need a label"
    );
    assert!(
        labels(
            r#"{"label":"a","start_unix_milli":1,"end_unix_milli":2}"#,
            false
        )
        .is_err()
    );
    assert!(
        labels(
            r#"{"start_unix_milli":1,"end_unix_milli":2,"limit":0}"#,
            false
        )
        .is_err()
    );
    assert_eq!(
        labels(r#"{"start_unix_milli":1,"end_unix_milli":2}"#, false)
            .unwrap()
            .limit,
        1000
    );
}

fn row(name: &str, fingerprint: u64, hour: i64) -> SeriesRow {
    let labels = labels(&[("__name__", name), ("host", "a")]);
    SeriesRow {
        fingerprint,
        unix_milli: hour * 3_600_000,
        name: name.into(),
        kind: MetricType::Gauge,
        temporality: Temporality::Unspecified,
        monotonic: false,
        unit: "1".into(),
        description: String::new().into(),
        label_meta: vec![LabelMeta::LEGACY; labels.len()].into(),
        labels,
    }
}

fn level(level: LabelLevel) -> Box<[LabelMeta]> {
    vec![LabelMeta::native(level, fs_schema::metrics::Datatype::Str); 2].into()
}

#[test]
fn a_key_resolves_to_one_identity_or_fails_closed() {
    let one = resolve(vec![row("cpu", 1, 0), row("cpu", 1, 1), row("cpu", 1, 0)]).unwrap();
    assert_eq!(one.len(), 1, "one series across hours and copies");
    let two = resolve(vec![row("cpu", 1, 0), row("mem", 1, 0)]).unwrap();
    assert_eq!(two.len(), 2, "the fingerprint alone is not the key");
    assert_eq!((&*two[0].name, &*two[1].name), ("cpu", "mem"));

    let mut legacy_then_native = row("cpu", 1, 1);
    legacy_then_native.label_meta = level(LabelLevel::Resource);
    assert_eq!(
        resolve(vec![row("cpu", 1, 0), legacy_then_native])
            .unwrap()
            .len(),
        1,
        "legacy agrees"
    );

    let changed: [(&str, fn(&mut SeriesRow)); 6] = [
        ("unit", |r| r.unit = "s".into()),
        ("temporality", |r| r.temporality = Temporality::Delta),
        ("type", |r| r.kind = MetricType::Sum),
        ("monotonicity", |r| r.monotonic = true),
        ("labels", |r| {
            r.labels = labels(&[("__name__", "cpu"), ("host", "b")]);
        }),
        ("levels", |r| r.label_meta = level(LabelLevel::Point)),
    ];
    for (what, change) in changed {
        for same_hour in [false, true] {
            let mut first = row("cpu", 1, 0);
            first.label_meta = level(LabelLevel::Resource);
            let mut other = row("cpu", 1, i64::from(!same_hour));
            change(&mut other);
            let err = resolve(vec![first, other, row("mem", 2, 0)]).unwrap_err();
            assert!(
                matches!(err, QueryError::Ambiguous { ref name, fingerprint: 1 } if name == "cpu"),
                "{what}, same hour {same_hour}: {err}"
            );
        }
    }
    let mut point = row("cpu", 1, 2);
    point.label_meta = level(LabelLevel::Point);
    let mut resource = row("cpu", 1, 0);
    resource.label_meta = level(LabelLevel::Resource);
    assert!(
        resolve(vec![resource, row("cpu", 1, 1), point]).is_err(),
        "a legacy hour between two levels does not reconcile them"
    );
}

fn series(pairs: &[(&str, &str)], samples: &[(i64, f64)]) -> Series {
    Series {
        labels: labels(pairs),
        samples: samples.to_vec(),
    }
}

#[test]
fn equal_labelsets_merge_the_first_winning_and_empty_series_go() {
    let input = vec![
        series(&[("a", "2")], &[(1, 1.0)]),
        series(&[("a", "1")], &[(1, 10.0), (3, 30.0)]),
        series(&[("a", "3")], &[]),
        series(&[("a", "1")], &[(1, 99.0), (2, 20.0)]),
    ];
    let fused = fuse(input, 100, &mut reservation(1 << 20)).unwrap();
    assert_eq!(
        fused,
        vec![
            series(&[("a", "1")], &[(1, 10.0), (2, 20.0), (3, 30.0)]),
            series(&[("a", "2")], &[(1, 1.0)]),
        ]
    );
    let overlapping = vec![
        series(&[("a", "1")], &[(1, 1.0), (2, 2.0), (3, 3.0)]),
        series(&[("a", "1")], &[(4, 4.0), (5, 5.0), (6, 6.0)]),
    ];
    assert!(
        matches!(
            fuse(overlapping, 5, &mut reservation(1 << 20)),
            Err(QueryError::TooManyRows(5))
        ),
        "the cap holds while merging"
    );
}

#[test]
fn the_answer_is_a_sorted_non_null_map_and_aligned_lists() {
    let stale = f64::from_bits(STALE_NAN_BITS);
    assert_eq!(stale.to_bits(), 0x7ff0_0000_0000_0002);
    let rows = [
        series(
            &[("__name__", "cpu"), ("host", "a")],
            &[(1, 1.5), (2, stale)],
        ),
        series(&[("__name__", "cpu"), ("host", "b")], &[(5, 2.5)]),
    ];
    let answer = batch(&rows, &mut reservation(1 << 20)).unwrap();
    assert_eq!(answer.schema(), select_schema());
    let DataType::Map(entries, sorted) = answer.schema().field(0).data_type().clone() else {
        panic!("labels are a map");
    };
    assert!(sorted, "keys sorted");
    let DataType::Struct(fields) = entries.data_type() else {
        panic!("entries are a struct");
    };
    assert!(answer.schema().fields().iter().all(|f| !f.is_nullable()));
    assert!(fields.iter().all(|f| !f.is_nullable()) && !entries.is_nullable());
    let times = answer.column(1).as_list::<i32>();
    let values = answer.column(2).as_list::<i32>();
    assert_eq!(times.value_offsets(), values.value_offsets());
    assert_eq!(times.value(0).as_primitive::<Int64Type>().values(), &[1, 2]);
    assert_eq!(
        values
            .value(0)
            .as_primitive::<Float64Type>()
            .value(1)
            .to_bits(),
        STALE_NAN_BITS
    );
    let map = answer.column(0).as_map();
    assert_eq!(map.value(1).column(1).as_string::<i32>().value(1), "b");
    assert_eq!(map.null_count(), 0);
}

fn postings_of(hosts: usize) -> fs_index::postings::Postings {
    let binding = fs_index::postings::Binding {
        path: "t/x/metrics/series/l1/dt=2026-10-24/hr=14/a.parquet".into(),
        sha256: [1; 32],
        schema_version: 2,
        row_count: u32::try_from(hosts * 2).unwrap(),
    };
    let mut builder = fs_index::postings::PostingsBuilder::new(usize::MAX);
    for row in 0..hosts * 2 {
        let host = format!("h{:06}", row / 2);
        let zone = if row % 2 == 0 { "eu" } else { "us" };
        builder
            .add(
                row as u64,
                [("__name__", "cpu"), ("host", host.as_str()), ("zone", zone)],
            )
            .unwrap();
    }
    fs_index::postings::Postings::decode(&builder.finish(&binding).unwrap(), &binding).unwrap()
}

#[test]
fn postings_rows_are_charged_until_they_are_dropped() {
    let postings = postings_of(20_000);
    let matchers: Arc<[Matcher]> = compile(
        &[
            raw("host", MatchType::Regex, "h0.*"),
            raw("zone", MatchType::Equal, "us"),
        ],
        LIMITS,
        &mut reservation(1 << 24),
    )
    .unwrap()
    .into();
    let narrow = series::Narrow::Matchers(matchers);

    let tight = reservation(64 * 1024);
    let err = narrow.rows(&postings, &tight).unwrap_err();
    assert!(
        matches!(
            err,
            QueryError::DataFusion(datafusion::error::DataFusionError::ResourcesExhausted(_))
        ),
        "a broad regex over many values is refused before its union is built: {err}"
    );
    assert_eq!(tight.size(), 0);

    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
    let roomy = MemoryConsumer::new("test").register(&pool);
    let (rows, held) = narrow.rows(&postings, &roomy).unwrap().unwrap();
    assert_eq!(rows.len(), 20_000, "every odd row");
    assert!(rows.iter().all(|r| r % 2 == 1));
    assert!(
        held.size() >= 2 * rows.serialized_size(),
        "{} for {}",
        held.size(),
        rows.serialized_size()
    );
    assert_eq!(pool.reserved(), held.size(), "held while the rows are");
    drop((rows, held));
    assert_eq!(pool.reserved(), 0);

    let none: Arc<[Matcher]> = compile(
        &[raw("host", MatchType::NotEqual, "x")],
        LIMITS,
        &mut reservation(1 << 24),
    )
    .unwrap()
    .into();
    assert!(
        series::Narrow::Matchers(none)
            .rows(&postings, &roomy)
            .unwrap()
            .is_none()
    );
}

#[test]
fn the_answer_charges_what_its_arrays_hold() {
    let keys = "k".repeat(300);
    let values = "v".repeat(5000);
    let rows: Vec<Series> = (0..50)
        .map(|n| {
            let value = format!("{values}{n}");
            series(
                &[("__name__", &keys), ("host", &value)],
                &[(1, 1.0), (2, 2.0)],
            )
        })
        .collect();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
    let mut charged = MemoryConsumer::new("test").register(&pool);
    let answer = batch(&rows, &mut charged).unwrap();
    assert!(
        charged.size() >= answer.get_array_memory_size(),
        "charged {} for arrays of {}",
        charged.size(),
        answer.get_array_memory_size()
    );
    let listed: Vec<Box<str>> = (0..300).map(|n| format!("{values}{n}").into()).collect();
    let mut charged = MemoryConsumer::new("test").register(&pool);
    let answer = super::output::text_batch("value", &listed, &mut charged).unwrap();
    assert!(
        charged.size() >= answer.get_array_memory_size(),
        "charged {} for a listing of {}",
        charged.size(),
        answer.get_array_memory_size()
    );
}

#[test]
fn merging_charges_the_merged_list() {
    let input: Vec<Series> = (0..1000)
        .map(|n| series(&[("a", &n.to_string())], &[(1, 1.0)]))
        .collect();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
    let mut charged = MemoryConsumer::new("test").register(&pool);
    let fused = fuse(input, 10_000, &mut charged).unwrap();
    assert!(
        charged.size() >= fused.capacity() * std::mem::size_of::<Series>(),
        "charged {} for a list of {}",
        charged.size(),
        fused.capacity() * std::mem::size_of::<Series>()
    );
}
