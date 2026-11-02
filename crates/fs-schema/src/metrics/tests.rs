use arrow::array::AsArray;
use arrow::datatypes::{Float64Type, Int32Type, Int64Type, UInt32Type, UInt64Type};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile;
use opentelemetry_proto::tonic::metrics::v1::{
    ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram, HistogramDataPoint,
    Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Summary, SummaryDataPoint,
};
use opentelemetry_proto::tonic::resource::v1::Resource;

use super::*;
use crate::limits::Limits;

/// 2026-10-30T00:00:00Z.
const T0: u64 = 1_793_318_400_000_000_000;
const T0_MS: i64 = 1_793_318_400_000;

const SOURCE: Source = Source {
    instance: [9; 16],
    segment: 3,
    offset: 40,
};

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
        ..KeyValue::default()
    }
}

fn request(
    resource: Vec<KeyValue>,
    scope: Vec<KeyValue>,
    metrics: Vec<Metric>,
) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: resource,
                ..Resource::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "lib".into(),
                    version: "2".into(),
                    attributes: scope,
                    ..InstrumentationScope::default()
                }),
                metrics,
                schema_url: "https://s".into(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn number(value: f64, attributes: Vec<KeyValue>) -> NumberDataPoint {
    NumberDataPoint {
        attributes,
        time_unix_nano: T0,
        value: Some(NumberValue::AsDouble(value)),
        ..NumberDataPoint::default()
    }
}

fn gauge(name: &str, points: Vec<NumberDataPoint>) -> Metric {
    Metric {
        name: name.into(),
        unit: "ms".into(),
        description: "d".into(),
        data: Some(Data::Gauge(Gauge {
            data_points: points,
        })),
        ..Metric::default()
    }
}

fn histogram(name: &str, point: HistogramDataPoint) -> Metric {
    Metric {
        name: name.into(),
        unit: "ms".into(),
        data: Some(Data::Histogram(Histogram {
            data_points: vec![point],
            aggregation_temporality: 2,
        })),
        ..Metric::default()
    }
}

fn bounded(bounds: Vec<f64>, counts: Vec<u64>) -> HistogramDataPoint {
    HistogramDataPoint {
        time_unix_nano: T0,
        count: counts.iter().fold(0, |sum, c| sum.wrapping_add(*c)),
        sum: Some(9.0),
        bucket_counts: counts,
        explicit_bounds: bounds,
        ..HistogramDataPoint::default()
    }
}

fn convert_all(request: &ExportMetricsServiceRequest) -> (Preflight, MetricRows) {
    let limits = Limits::default();
    let metrics = MetricLimits::default();
    let checked = preflight(request, &limits, &metrics).unwrap();
    let rows = convert(request, &limits, &metrics, &checked, SOURCE).unwrap();
    (checked, rows)
}

fn names(rows: &MetricRows) -> Vec<String> {
    let names = rows.samples.column(0).as_dictionary::<Int32Type>();
    let values = names.values().as_string::<i32>();
    names
        .keys()
        .values()
        .iter()
        .map(|k| values.value(usize::try_from(*k).unwrap()).to_owned())
        .collect()
}

fn values(rows: &MetricRows) -> Vec<f64> {
    rows.samples
        .column(4)
        .as_primitive::<Float64Type>()
        .values()
        .to_vec()
}

fn labels_of(series: &SeriesRow) -> Vec<(&str, &str)> {
    series.labels.iter().map(|(k, v)| (&**k, &**v)).collect()
}

#[test]
fn a_gauge_is_one_sample_and_one_series_with_its_generated_labels() {
    let req = request(
        vec![kv("service.name", "api"), kv("zone", "resource")],
        vec![kv("zone", "scope"), kv("region", "scope")],
        vec![gauge(
            "m",
            vec![number(
                2.5,
                vec![kv("region", "point"), kv("host", "h"), kv("__name__", "x")],
            )],
        )],
    );
    let (checked, rows) = convert_all(&req);
    assert_eq!((checked.samples, checked.series), (1, 1));
    assert_eq!(rows.samples.num_rows(), 1);
    assert_eq!(rows.samples.schema(), samples_schema_v1());
    let series = rows.series.first().unwrap();
    assert_eq!(
        labels_of(series),
        vec![
            ("__name__", "m"),
            ("__scope.name__", "lib"),
            ("__scope.schema_url__", "https://s"),
            ("__scope.version__", "2"),
            ("__temporality__", "Unspecified"),
            ("host", "h"),
            ("region", "scope"),
            ("service.name", "api"),
            ("zone", "resource"),
        ]
    );
    assert_eq!(
        (
            series.kind,
            series.temporality,
            series.monotonic,
            &*series.unit,
            series.unix_milli
        ),
        (
            MetricType::Gauge,
            Temporality::Unspecified,
            false,
            "ms",
            T0_MS
        )
    );
    let fingerprints = rows.samples.column(2).as_primitive::<UInt64Type>();
    assert_eq!(fingerprints.value(0), series.fingerprint);
    assert_eq!(
        rows.samples.column(3).as_primitive::<Int64Type>().value(0),
        T0_MS
    );
    let order = rows.samples.column(6).as_fixed_size_binary();
    assert_eq!(order.value(0), SOURCE.order(0));
}

#[test]
fn a_histogram_becomes_cumulative_buckets_with_count_sum_min_and_max() {
    let mut point = bounded(vec![0.000_01, 1.0, 1_000_000.0], vec![1, 2, 3, 4]);
    point.min = Some(0.5);
    point.max = Some(f64::NAN);
    point.attributes = vec![kv("le", "custom")];
    let (_, rows) = convert_all(&request(vec![], vec![], vec![histogram("h", point)]));
    assert_eq!(
        names(&rows),
        vec![
            "h.bucket", "h.bucket", "h.bucket", "h.bucket", "h.count", "h.min", "h.sum"
        ]
    );
    let buckets: Vec<(&str, f64)> = rows
        .series
        .iter()
        .filter(|s| &*s.name == "h.bucket")
        .map(|s| {
            let le = s.labels.iter().find(|(k, _)| &**k == "le").unwrap();
            (&*le.1, 0.0)
        })
        .collect();
    let mut les: Vec<&str> = buckets.iter().map(|b| b.0).collect();
    les.sort_unstable();
    assert_eq!(les, vec!["+Inf", "0.00001", "1", "1000000"]);
    let mut bucket_values: Vec<f64> = values(&rows).into_iter().take(4).collect();
    bucket_values.sort_by(f64::total_cmp);
    assert_eq!(bucket_values, vec![1.0, 3.0, 6.0, 10.0]);
    let kinds: Vec<(&str, MetricType, Temporality, bool, &str)> = rows
        .series
        .iter()
        .filter(|s| &*s.name != "h.bucket")
        .map(|s| (&*s.name, s.kind, s.temporality, s.monotonic, &*s.unit))
        .collect();
    assert_eq!(
        kinds,
        vec![
            (
                "h.count",
                MetricType::Sum,
                Temporality::Cumulative,
                true,
                "ms"
            ),
            (
                "h.min",
                MetricType::Gauge,
                Temporality::Unspecified,
                false,
                "ms"
            ),
            (
                "h.sum",
                MetricType::Sum,
                Temporality::Cumulative,
                true,
                "ms"
            ),
        ]
    );
}

#[test]
fn a_histogram_without_buckets_has_one_whole_bucket_and_a_count_of_unit_one() {
    let point = HistogramDataPoint {
        time_unix_nano: T0,
        count: 0,
        ..HistogramDataPoint::default()
    };
    let mut metric = histogram("e", point);
    metric.unit = String::new();
    let (_, rows) = convert_all(&request(vec![], vec![], vec![metric]));
    assert_eq!(names(&rows), vec!["e.bucket", "e.count"]);
    let count = rows.series.iter().find(|s| &*s.name == "e.count").unwrap();
    assert_eq!(&*count.unit, "1");
}

#[test]
fn a_summary_becomes_quantiles_count_and_sum() {
    let metric = Metric {
        name: "s".into(),
        unit: "s".into(),
        data: Some(Data::Summary(Summary {
            data_points: vec![SummaryDataPoint {
                time_unix_nano: T0,
                count: 3,
                sum: 1.5,
                quantile_values: vec![
                    ValueAtQuantile {
                        quantile: 0.000_01,
                        value: 0.1,
                    },
                    ValueAtQuantile {
                        quantile: 1.0,
                        value: 2.0,
                    },
                ],
                ..SummaryDataPoint::default()
            }],
        })),
        ..Metric::default()
    };
    let (_, rows) = convert_all(&request(vec![], vec![], vec![metric]));
    let kinds: Vec<(&str, MetricType, Temporality, &str)> = rows
        .series
        .iter()
        .map(|s| (&*s.name, s.kind, s.temporality, &*s.unit))
        .collect();
    assert_eq!(kinds.len(), 4);
    assert!(kinds.contains(&("s.count", MetricType::Sum, Temporality::Cumulative, "1")));
    assert!(kinds.contains(&("s.sum", MetricType::Sum, Temporality::Cumulative, "s")));
    let quantiles: Vec<&str> = rows
        .series
        .iter()
        .filter(|s| s.kind == MetricType::Summary)
        .map(|s| &*s.labels.iter().find(|(k, _)| &**k == "quantile").unwrap().1)
        .collect();
    assert_eq!(quantiles.len(), 2);
    assert!(quantiles.contains(&"0.00001") && quantiles.contains(&"1"));
}

#[test]
fn malformed_histograms_are_refused_whole_before_any_row() {
    let bad_histograms = [
        (bounded(vec![1.0], vec![1]), Rejection::MalformedHistogram),
        (
            bounded(vec![2.0, 1.0], vec![1, 1, 1]),
            Rejection::MalformedHistogram,
        ),
        (
            bounded(vec![1.0, 1.0], vec![1, 1, 1]),
            Rejection::MalformedHistogram,
        ),
        (
            bounded(vec![f64::INFINITY], vec![1, 1]),
            Rejection::MalformedHistogram,
        ),
        (
            bounded(vec![f64::NAN], vec![1, 1]),
            Rejection::MalformedHistogram,
        ),
        (
            HistogramDataPoint {
                count: 5,
                ..bounded(vec![1.0], vec![1, 1])
            },
            Rejection::MalformedHistogram,
        ),
        (
            HistogramDataPoint {
                count: 0,
                ..bounded(vec![1.0], vec![u64::MAX, 2])
            },
            Rejection::MalformedHistogram,
        ),
        (
            bounded((0..161).map(f64::from).collect(), vec![0; 162]),
            Rejection::TooManyBuckets,
        ),
        (
            HistogramDataPoint {
                flags: 1,
                ..bounded(vec![1.0], vec![1, 1])
            },
            Rejection::StaleHistogram,
        ),
    ];
    for (point, reason) in bad_histograms {
        let req = request(
            vec![],
            vec![],
            vec![
                histogram("h", point),
                gauge("ok", vec![number(1.0, vec![])]),
            ],
        );
        let (checked, rows) = convert_all(&req);
        assert_eq!(checked.rejected.of(reason), 1, "{reason:?}");
        assert_eq!(checked.rejected.total(), 1);
        assert_eq!(
            names(&rows),
            vec!["ok"],
            "{reason:?}: nothing of the refused point"
        );
    }
}

#[test]
fn malformed_summaries_and_unsupported_points_are_refused() {
    let summary = |quantiles: Vec<(f64, f64)>| Metric {
        name: "s".into(),
        data: Some(Data::Summary(Summary {
            data_points: vec![SummaryDataPoint {
                time_unix_nano: T0,
                quantile_values: quantiles
                    .into_iter()
                    .map(|(quantile, value)| ValueAtQuantile { quantile, value })
                    .collect(),
                ..SummaryDataPoint::default()
            }],
        })),
        ..Metric::default()
    };
    for quantiles in [
        vec![(1.5, 1.0)],
        vec![(-0.1, 1.0)],
        vec![(0.5, 1.0), (0.5, 2.0)],
        vec![(0.9, 1.0), (0.5, 2.0)],
        vec![(0.5, f64::NAN)],
        vec![(0.5, -1.0)],
    ] {
        let (checked, _) = convert_all(&request(vec![], vec![], vec![summary(quantiles.clone())]));
        assert_eq!(
            checked.rejected.of(Rejection::MalformedSummary),
            1,
            "{quantiles:?}"
        );
    }
    let exponential = Metric {
        name: "x".into(),
        data: Some(Data::ExponentialHistogram(ExponentialHistogram {
            data_points: vec![ExponentialHistogramDataPoint::default(); 3],
            aggregation_temporality: 2,
        })),
        ..Metric::default()
    };
    let unnamed = gauge("", vec![number(1.0, vec![])]);
    let untimed = gauge(
        "t",
        vec![NumberDataPoint {
            time_unix_nano: 0,
            ..number(1.0, vec![])
        }],
    );
    let valueless = gauge(
        "v",
        vec![NumberDataPoint {
            value: None,
            ..number(1.0, vec![])
        }],
    );
    let crowded = gauge(
        "c",
        vec![number(
            1.0,
            (0..400).map(|i| kv(&format!("k{i}"), "v")).collect(),
        )],
    );
    let (checked, rows) = convert_all(&request(
        vec![],
        vec![],
        vec![exponential, unnamed, untimed, valueless, crowded],
    ));
    assert_eq!(checked.rejected.of(Rejection::ExponentialHistogram), 3);
    assert_eq!(checked.rejected.of(Rejection::NoName), 1);
    assert_eq!(checked.rejected.of(Rejection::NoTimestamp), 1);
    assert_eq!(checked.rejected.of(Rejection::NoValue), 1);
    assert_eq!(checked.rejected.of(Rejection::TooManyLabels), 1);
    assert_eq!(rows.samples.num_rows(), 0);
    assert_eq!(
        checked.rejected.summary(),
        "exponential_histogram=3, too_many_labels=1, no_timestamp=1, no_value=1, no_name=1"
    );
}

#[test]
fn nan_writes_nothing_and_no_recorded_value_writes_zero_flagged() {
    let stale = NumberDataPoint {
        flags: 1,
        value: Some(NumberValue::AsDouble(7.0)),
        ..number(0.0, vec![kv("k", "stale")])
    };
    let req = request(
        vec![],
        vec![],
        vec![gauge("g", vec![number(f64::NAN, vec![]), stale])],
    );
    let (checked, rows) = convert_all(&req);
    assert_eq!(checked.rejected.total(), 0);
    assert_eq!(values(&rows), vec![0.0]);
    assert_eq!(
        rows.samples.column(5).as_primitive::<UInt32Type>().value(0),
        1
    );
}

#[test]
fn the_preflight_counts_what_convert_builds_and_bounds_what_it_holds() {
    let req = request(
        vec![kv("service.name", "api")],
        vec![],
        vec![
            histogram("h", bounded(vec![1.0, 2.0, 3.0], vec![1, 1, 1, 1])),
            gauge(
                "g",
                (0..50)
                    .map(|i| number(f64::from(i), vec![kv("i", &i.to_string())]))
                    .collect(),
            ),
        ],
    );
    let (checked, rows) = convert_all(&req);
    assert_eq!(rows.samples.num_rows(), checked.samples);
    assert_eq!(checked.samples, 6 + 50);
    assert_eq!(
        rows.series.len(),
        checked.series,
        "no duplicate series in this request"
    );
    let held = rows.samples.get_array_memory_size()
        + rows.series.iter().map(SeriesRow::bytes).sum::<usize>();
    assert!(
        held <= checked.bytes_bound(),
        "{held} held, bound {}",
        checked.bytes_bound()
    );
}

/// What the traversal is seen to hold, by the capacities it allocated,
/// never passes the bound found before it from the request's sizes, for
/// wide levels, nested values, buckets and quantiles alike.
#[test]
fn the_traversal_holds_no_more_than_its_bound_found_before_it() {
    use opentelemetry_proto::tonic::common::v1::{ArrayValue, KeyValueList};

    let any = |value| AnyValue { value: Some(value) };
    let nested = |i: usize| KeyValue {
        key: format!("nested{i}"),
        value: Some(any(Value::KvlistValue(KeyValueList {
            values: (0..200)
                .map(|j| KeyValue {
                    key: format!("k{}", j % 50),
                    value: Some(any(Value::ArrayValue(ArrayValue {
                        values: vec![
                            any(Value::DoubleValue(0.1)),
                            any(Value::BytesValue(vec![1; 99])),
                        ],
                    }))),
                    ..KeyValue::default()
                })
                .collect(),
        }))),
        ..KeyValue::default()
    };
    let wide: Vec<KeyValue> = (0..300)
        .map(|i| kv(&format!("attr{i}"), &"v\u{1}".repeat(i * 9)))
        .chain((0..3).map(nested))
        .collect();
    let summary = Metric {
        name: "s".repeat(500),
        data: Some(Data::Summary(Summary {
            data_points: vec![SummaryDataPoint {
                attributes: wide.clone(),
                time_unix_nano: T0,
                count: 2,
                sum: 1.0,
                quantile_values: vec![ValueAtQuantile {
                    quantile: f64::from_bits(1),
                    value: 1.0,
                }],
                ..SummaryDataPoint::default()
            }],
        })),
        ..Metric::default()
    };
    let mut point = bounded(vec![1.0, 2.0], vec![1, 1, 1]);
    point.attributes = wide.iter().take(40).cloned().collect();
    let req = request(
        wide.clone(),
        wide.iter().rev().take(9).cloned().collect(),
        vec![
            histogram("h", point),
            gauge("g", vec![number(1.0, wide.clone())]),
            summary,
        ],
    );
    let limits = Limits::default();
    for metrics in [
        MetricLimits::default(),
        MetricLimits {
            max_series_labels: 1000,
            ..MetricLimits::default()
        },
    ] {
        let bound = walk_bound(&req, &limits, &metrics);
        let measured = super::expand::measure(&req, &limits, &metrics);
        assert!(measured > 0);
        assert!(measured <= bound, "{measured} held, bound {bound}");
        assert_eq!(preflight(&req, &limits, &metrics).unwrap().scratch, bound);
    }
}

#[test]
fn requests_past_their_point_or_sample_cap_are_refused_whole() {
    let points = |n: usize| {
        request(
            vec![],
            vec![],
            vec![gauge("g", vec![number(1.0, vec![]); n])],
        )
    };
    let metrics = MetricLimits {
        max_points: 10,
        max_samples: 15,
        ..MetricLimits::default()
    };
    let limits = Limits::default();
    assert_eq!(
        preflight(&points(11), &limits, &metrics),
        Err(TooLarge::Points(11))
    );
    assert!(preflight(&points(10), &limits, &metrics).is_ok());
    let wide = request(
        vec![],
        vec![],
        vec![histogram(
            "h",
            bounded((0..20).map(f64::from).collect(), vec![0; 21]),
        )],
    );
    assert!(matches!(
        preflight(&wide, &limits, &metrics),
        Err(TooLarge::Samples(_))
    ));
    let clamped = MetricLimits {
        max_samples: usize::MAX,
        ..MetricLimits::default()
    }
    .clamped();
    assert_eq!(clamped.max_samples, u32::MAX as usize);
}

#[test]
fn samples_sort_by_name_fingerprint_time_and_source_order() {
    let mut late = number(1.0, vec![]);
    late.time_unix_nano = T0 + 2_000_000;
    let req = request(
        vec![],
        vec![],
        vec![
            gauge("b", vec![late, number(2.0, vec![])]),
            gauge("a", vec![number(3.0, vec![])]),
        ],
    );
    let (_, rows) = convert_all(&req);
    assert_eq!(names(&rows), vec!["a", "b", "b"]);
    let times = rows.samples.column(3).as_primitive::<Int64Type>();
    assert!(times.value(1) < times.value(2));
    let order = rows.samples.column(6).as_fixed_size_binary();
    assert_eq!(order.value(0), SOURCE.order(2));
    assert_eq!(order.value(1), SOURCE.order(1));
    assert_eq!(order.value(2), SOURCE.order(0));
    assert!(SOURCE.order(1) > SOURCE.order(0));
    let other = Source {
        instance: [1; 16],
        ..SOURCE
    };
    assert!(
        other.order(u32::MAX) < SOURCE.order(0),
        "the instance compares first"
    );
}

#[test]
fn of_one_series_and_hour_the_longest_description_wins_then_the_least_bytes() {
    let row = |description: &str| SeriesRow {
        fingerprint: 1,
        unix_milli: T0_MS,
        name: "m".into(),
        kind: MetricType::Gauge,
        temporality: Temporality::Unspecified,
        monotonic: false,
        unit: "".into(),
        description: description.into(),
        labels: Box::new([]),
    };
    // "é" and "ab" are two bytes each; "ab" sorts before "é".
    let mut rows = vec![row("é"), row("a"), row("ab"), row("")];
    dedupe_series(&mut rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(&*rows.first().unwrap().description, "ab");
}

#[test]
fn series_batches_have_the_series_schema() {
    let (_, rows) = convert_all(&request(
        vec![],
        vec![],
        vec![gauge("g", vec![number(1.0, vec![kv("a", "b")])])],
    ));
    let batch = series_batch(&rows.series, rows.series.len()).unwrap();
    assert_eq!(batch.schema(), series_schema_v1());
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch.column(4).as_primitive::<UInt64Type>().value(0),
        rows.series.first().unwrap().fingerprint
    );
}

#[test]
fn a_request_converted_before_its_position_was_known_takes_it_after() {
    let req = request(
        vec![],
        vec![],
        vec![
            gauge(
                "b",
                vec![number(1.0, vec![]), number(2.0, vec![kv("k", "v")])],
            ),
            histogram("h", bounded(vec![1.0], vec![1, 1])),
        ],
    );
    let limits = Limits::default();
    let metrics = MetricLimits::default();
    let checked = preflight(&req, &limits, &metrics).unwrap();
    let unknown = Source {
        instance: [0; 16],
        segment: 0,
        offset: 0,
    };
    let early = convert(&req, &limits, &metrics, &checked, unknown).unwrap();
    let known = convert(&req, &limits, &metrics, &checked, SOURCE).unwrap();
    assert_eq!(with_source(&early.samples, SOURCE).unwrap(), known.samples);
    assert_eq!(checked.hours, vec![T0_MS]);
}
