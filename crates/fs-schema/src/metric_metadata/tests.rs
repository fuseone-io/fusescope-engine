use super::*;
use crate::metrics::{LabelMeta, MetricType, Temporality};

/// 2026-10-24T14:00:00Z.
const T0_MS: i64 = 1_792_677_600_000;

fn row(name: &str, labels: &[(&str, &str, LabelMeta)]) -> SeriesRow {
    SeriesRow {
        fingerprint: 1,
        unix_milli: T0_MS,
        name: name.into(),
        kind: MetricType::Gauge,
        temporality: Temporality::Unspecified,
        monotonic: false,
        unit: "".into(),
        description: "".into(),
        labels: labels
            .iter()
            .map(|(k, v, _)| ((*k).into(), (*v).into()))
            .collect(),
        label_meta: labels.iter().map(|(_, _, m)| *m).collect(),
    }
}

fn meta(level: LabelLevel, types: &[Datatype]) -> LabelMeta {
    types
        .iter()
        .map(|d| LabelMeta::native(level, *d))
        .reduce(|a, b| a.merge(b).unwrap())
        .unwrap()
}

fn listed(counts: &MetadataCounts) -> Vec<MetadataRow> {
    decode(bytes::Bytes::from(encode(counts).unwrap())).unwrap()
}

/// A label is listed under its level, once a type it was received as;
/// `__name__` and a legacy origin are not; the times widen.
#[test]
fn labels_are_listed_by_level_and_type_and_their_times_widen() {
    let point = meta(LabelLevel::Point, &[Datatype::Str, Datatype::Int]);
    let series = row(
        "cpu",
        &[
            (NAME, "cpu", meta(LabelLevel::Point, &[Datatype::Str])),
            ("host", "a", meta(LabelLevel::Resource, &[Datatype::Str])),
            ("old", "x", LabelMeta::LEGACY),
            ("port", "1", point),
        ],
    );
    let mut counts = MetadataCounts::default();
    assert_eq!(
        counts
            .add_series(&series, (T0_MS + 5, T0_MS + 9), 0, usize::MAX)
            .unwrap(),
        None
    );
    assert_eq!(
        counts
            .add_series(&series, (T0_MS + 1, T0_MS + 7), 0, usize::MAX)
            .unwrap(),
        None
    );
    let rows = listed(&counts);
    let row = |attr: &str, level: &str, datatype: &str, value: &str| {
        (
            "cpu".to_owned(),
            attr.to_owned(),
            level.to_owned(),
            datatype.to_owned(),
            value.to_owned(),
            T0_MS + 1,
            T0_MS + 9,
        )
    };
    assert_eq!(
        rows,
        [
            row("host", "resource", "Str", "a"),
            row("port", "point", "Int", "1"),
            row("port", "point", "Str", "1"),
        ]
    );
}

/// Listing stops before an entry that would pass the budget and resumes
/// there: the chunks together list what one unbounded count does.
#[test]
fn a_full_count_is_set_aside_and_listing_resumes_at_its_entry() {
    let labels: Vec<(String, String)> = (0..20)
        .map(|i| (format!("k{i:02}"), format!("v{i}")))
        .collect();
    let refs: Vec<(&str, &str, LabelMeta)> = labels
        .iter()
        .map(|(k, v)| {
            (
                k.as_str(),
                v.as_str(),
                meta(LabelLevel::Point, &[Datatype::Str]),
            )
        })
        .collect();
    let series = row("m", &refs);
    let mut whole = MetadataCounts::default();
    assert_eq!(
        whole
            .add_series(&series, (T0_MS, T0_MS), 0, usize::MAX)
            .unwrap(),
        None
    );
    let budget = 3 * MetadataCounts::entry_bytes("m", "k00", "v0");
    let mut counts = MetadataCounts::default();
    let mut chunks = Vec::new();
    let mut from = 0;
    while let Some(next) = counts
        .add_series(&series, (T0_MS, T0_MS), from, budget)
        .unwrap()
    {
        assert!(counts.estimated_bytes() <= budget);
        chunks.push(counts.take());
        from = next;
    }
    chunks.push(counts.take());
    assert!(chunks.len() > 1);
    let joined: Vec<MetadataRow> = chunks.iter().flat_map(listed).collect();
    assert_eq!(joined, listed(&whole));
}

/// A file listing a key twice, out of order, of an unknown level or type,
/// or first reported after its last is refused.
#[test]
fn a_contribution_outside_the_domain_is_refused() {
    let text = |v: &[&str]| -> ArrayRef { Arc::new(StringArray::from(v.to_vec())) };
    let batch = |level: &str, datatype: &str, values: &[&str], first: i64| {
        let n = values.len();
        RecordBatch::try_new(
            schema(),
            vec![
                text(&vec!["m"; n]),
                text(&vec!["k"; n]),
                text(&vec![level; n]),
                text(&vec![datatype; n]),
                text(values),
                Arc::new(Int64Array::from(vec![first; n])),
                Arc::new(Int64Array::from(vec![5i64; n])),
            ],
        )
        .unwrap()
    };
    assert!(
        RowOrder::default()
            .check(&batch("point", "Str", &["a", "b"], 1))
            .is_ok()
    );
    for (bad, why) in [
        (batch("point", "Str", &["b", "a"], 1), "out of order"),
        (batch("point", "Str", &["a", "a"], 1), "twice"),
        (batch("label", "Str", &["a"], 1), "level"),
        (batch("point", "Text", &["a"], 1), "type"),
        (batch("point", "Str", &["a"], 9), "first after last"),
    ] {
        assert!(RowOrder::default().check(&bad).is_err(), "{why}");
    }
}

/// The committed fixture is what this encoder writes for its input, byte
/// for byte, and decodes to it.
#[test]
fn the_v1_fixture_is_written_byte_for_byte() {
    let series = row(
        "http.server.duration",
        &[
            ("host", "a", meta(LabelLevel::Resource, &[Datatype::Str])),
            ("le", "+Inf", meta(LabelLevel::Point, &[Datatype::Str])),
            (
                "port",
                "8080",
                meta(LabelLevel::Point, &[Datatype::Int, Datatype::Str]),
            ),
        ],
    );
    let mut counts = MetadataCounts::default();
    counts
        .add_series(&series, (T0_MS, T0_MS + 60_000), 0, usize::MAX)
        .unwrap();
    let fixture = bytes::Bytes::from_static(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/metric_metadata_v1.parquet"
    )));
    assert_eq!(encode(&counts).unwrap(), fixture.to_vec());
    assert_eq!(decode(fixture).unwrap().len(), 4);
}

/// Keys compare field by field whatever bytes the fields hold, a zero
/// byte included, and decode to their fields.
#[test]
fn keys_encode_in_the_order_of_their_fields_and_decode_back() {
    let key = |fields: [&str; 5]| {
        let mut out = Vec::new();
        encode_key(&mut out, fields.map(str::as_bytes));
        out
    };
    let a = key(["m", "a", "point", "Str", "x"]);
    let b = key(["m", "a\u{0}b", "point", "Str", "x"]);
    let c = key(["m", "ab", "point", "Str", "x"]);
    let d = key(["m\u{0}", "", "point", "Str", "x"]);
    assert!(a < b && b < c, "a < a\\0b < ab");
    assert!(
        key(["m", "zzz", "scope", "Str", "x"]) < d,
        "the first field first"
    );
    assert_eq!(
        decode_key(&b),
        ["m", "a\u{0}b", "point", "Str", "x"].map(String::from)
    );
}

/// What a count holds, its lookup key included, never passes its budget
/// but for one entry, however long the texts.
#[test]
fn a_count_admits_its_lookup_key_and_entries_within_its_budget() {
    let long = "v".repeat(10_000);
    let labels: Vec<(String, String)> = (0..8)
        .map(|i| (format!("k{i}"), format!("{long}{i}")))
        .collect();
    let refs: Vec<(&str, &str, LabelMeta)> = labels
        .iter()
        .map(|(k, v)| {
            (
                k.as_str(),
                v.as_str(),
                meta(LabelLevel::Point, &[Datatype::Str]),
            )
        })
        .collect();
    let series = row("m", &refs);
    let budget = 3 * MetadataCounts::entry_bytes("m", "k0", &labels.first().unwrap().1);
    let mut counts = MetadataCounts::default();
    let mut from = 0;
    let mut chunks = 0;
    while let Some(next) = counts
        .add_series(&series, (T0_MS, T0_MS), from, budget)
        .unwrap()
    {
        assert!(
            counts.estimated_bytes() <= budget,
            "{}",
            counts.estimated_bytes()
        );
        drop(counts.take());
        chunks += 1;
        from = next;
    }
    assert!(chunks >= 2);
    assert!(counts.estimated_bytes() <= budget);
}

/// One entry larger than the budget, or a lookup key that would grow past
/// it, is refused, even into empty counts: the budget is a ceiling.
#[test]
fn an_entry_past_the_budget_is_refused_even_alone() {
    let long = "v".repeat(10_000);
    let series = row(
        "m",
        &[(
            "k",
            long.as_str(),
            meta(LabelLevel::Point, &[Datatype::Str]),
        )],
    );
    let budget = MetadataCounts::entry_bytes("m", "k", &long) - 1;
    let mut counts = MetadataCounts::default();
    assert!(
        counts
            .add_series(&series, (T0_MS, T0_MS), 0, budget)
            .is_err()
    );
    assert!(counts.is_empty());
    assert!(counts.estimated_bytes() <= budget);
}
