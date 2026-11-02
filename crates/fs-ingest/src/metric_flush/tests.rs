use arrow::array::AsArray;
use arrow::datatypes::{Float64Type, Int64Type};
use fs_schema::limits::Limits;
use fs_schema::metrics::{MetricLimits, Source, convert, preflight, series_batch};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;

use super::*;

/// 2026-10-30T10:00:00Z.
const T0: u64 = 1_793_354_400_000_000_000;

fn hour() -> DateTime<Utc> {
    Utc.timestamp_nanos(i64::try_from(T0).unwrap())
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

/// Gauge `name` at `offsets` milliseconds past `T0`, one point each, of
/// values `values`, and a histogram.
fn request(name: &str, points: &[(u64, f64, &str)]) -> ExportMetricsServiceRequest {
    let data_points = points
        .iter()
        .map(|(offset, value, host)| NumberDataPoint {
            attributes: vec![kv("host", host)],
            time_unix_nano: T0 + offset * 1_000_000,
            value: Some(NumberValue::AsDouble(*value)),
            ..NumberDataPoint::default()
        })
        .collect();
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "api")],
                ..Resource::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![
                    Metric {
                        name: name.into(),
                        data: Some(Data::Gauge(Gauge { data_points })),
                        ..Metric::default()
                    },
                    Metric {
                        name: "latency".into(),
                        unit: "ms".into(),
                        data: Some(Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                time_unix_nano: T0,
                                count: 3,
                                sum: Some(6.0),
                                bucket_counts: vec![1, 2],
                                explicit_bounds: vec![5.0],
                                ..HistogramDataPoint::default()
                            }],
                            aggregation_temporality: 2,
                        })),
                        ..Metric::default()
                    },
                ],
                ..ScopeMetrics::default()
            }],
            schema_url: String::new(),
        }],
    }
}

fn rows(request: &ExportMetricsServiceRequest, offset: u64) -> fs_schema::metrics::MetricRows {
    let limits = Limits::default();
    let metrics = MetricLimits::default();
    let checked = preflight(request, &limits, &metrics).unwrap();
    let source = Source {
        instance: [5; 16],
        segment: 1,
        offset,
    };
    convert(request, &limits, &metrics, &checked, source).unwrap()
}

fn values(prepared: &Prepared) -> Vec<(String, i64, f64)> {
    prepared
        .rows
        .iter()
        .flat_map(|r| {
            let batch = &r.batch;
            let names = batch.column(0).as_dictionary::<Int32Type>();
            let text = names.values().as_string::<i32>();
            let keys: Vec<i32> = names.keys().values().to_vec();
            let times = batch
                .column(3)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec();
            let values = batch
                .column(4)
                .as_primitive::<Float64Type>()
                .values()
                .to_vec();
            keys.into_iter()
                .zip(times)
                .zip(values)
                .map(|((k, t), v)| (text.value(usize::try_from(k).unwrap()).to_owned(), t, v))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn of_a_fingerprint_and_millisecond_the_largest_source_order_is_kept() {
    let first = rows(&request("cpu", &[(0, 1.0, "a"), (1, 2.0, "a")]), 100);
    let second = rows(&request("cpu", &[(0, 9.0, "a"), (2, 3.0, "b")]), 200);
    let prepared = prepare_samples(
        TenantId::new(),
        &Path::from(""),
        FileId::new(),
        hour(),
        &[second.samples.clone(), first.samples.clone()],
        FileLayout::current(3),
    )
    .unwrap();
    let got = values(&prepared);
    let cpu: Vec<_> = got.iter().filter(|(n, _, _)| n == "cpu").cloned().collect();
    let t0 = T0_MS;
    assert!(
        cpu.contains(&("cpu".into(), t0, 9.0)),
        "the later request wins: {cpu:?}"
    );
    assert!(!cpu.contains(&("cpu".into(), t0, 1.0)));
    assert_eq!(cpu.len(), 3);
    assert_eq!(
        got.iter()
            .filter(|(n, _, _)| n.starts_with("latency"))
            .count(),
        4,
        "one histogram point each request, deduplicated: buckets, count, sum"
    );
    assert!(prepared.rows.iter().all(|r| r.batch.num_rows() <= 3));
    let mut sorted = got.clone();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        got.iter().map(|g| g.0.clone()).collect::<Vec<_>>(),
        sorted.iter().map(|g| g.0.clone()).collect::<Vec<_>>(),
        "by name"
    );
}

const T0_MS: i64 = 1_793_354_400_000;

#[test]
fn chunks_and_the_requests_they_merge_give_the_same_bytes() {
    let a = rows(&request("cpu", &[(0, 1.0, "a"), (5, 2.0, "b")]), 10);
    let b = rows(&request("mem", &[(0, 4.0, "a"), (0, 5.0, "a")]), 20);
    let id = FileId::new();
    let tenant = TenantId::new();
    let one_by_one = prepare_samples(
        tenant,
        &Path::from(""),
        id,
        hour(),
        &[a.samples.clone(), b.samples.clone()],
        FileLayout::current(4),
    )
    .unwrap();
    let chunk =
        arrow::compute::concat_batches(&samples_schema_v1(), [&a.samples, &b.samples]).unwrap();
    let merged = prepare_samples(
        tenant,
        &Path::from(""),
        id,
        hour(),
        &[chunk],
        FileLayout::current(4),
    )
    .unwrap();
    assert_eq!(one_by_one.bytes, merged.bytes);
    assert_eq!(one_by_one.file, merged.file);
}

/// The bytes of a canonical input, pinned for every layout version this
/// engine writes for metrics, samples and series.
#[test]
fn canonical_metric_input_gives_the_pinned_bytes_for_each_version() {
    let input = rows(&request("cpu", &[(0, 1.5, "a"), (1, -0.0, "b")]), 7);
    let tenant = TenantId(uuid::Uuid::from_bytes([1; 16]));
    let id = FileId(uuid::Uuid::from_bytes([2; 16]));
    let pinned = [
        (4, PINNED_SAMPLES, PINNED_SERIES),
        (5, PINNED_SAMPLES, PINNED_SERIES_V2),
    ];
    assert_eq!(
        pinned.map(|(version, _, _)| version),
        *FileLayout::written_versions(RowSignal::Metrics)
    );
    for (version, samples, series) in pinned {
        let layout = FileLayout {
            version,
            ..FileLayout::current(4)
        };
        let s = prepare_samples(
            tenant,
            &Path::from(""),
            id,
            hour(),
            std::slice::from_ref(&input.samples),
            layout,
        )
        .unwrap();
        let batch = series_batch(&input.series, input.series.len()).unwrap();
        let r = prepare_series(tenant, &Path::from(""), id, hour(), &batch, layout).unwrap();
        assert_eq!(
            hex(&crate::sha256::digest(&s.bytes)),
            samples,
            "samples v{version}"
        );
        assert_eq!(
            hex(&crate::sha256::digest(&r.bytes)),
            series,
            "series v{version}"
        );
        assert_eq!(r.file.signal, fs_catalog::Signal::MetricSeries);
        assert!(r.file.path.contains("/metrics/series/l0/"));
        assert!(s.file.path.contains("/metrics/samples/l0/"));
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

const PINNED_SAMPLES: &str = "f96c050d0dda0449e2a2850fbf5ddb58b317d851831598245bdc23b00403b86e";
const PINNED_SERIES: &str = "4f0fd1e3582f66a43a79cb1b950aa4eb1a0d3fd364a73cd9cfc2e9273de16eb2";
const PINNED_SERIES_V2: &str = "790b8c744b400276cfd67cd2636323fb1fc5feda45e960724e126f96220de4da";

/// Two ingesters at the same WAL positions write the same sample: the one
/// whose instance sorts last wins, whichever input comes first, so any
/// later merge of their files picks the same.
#[test]
fn of_two_ingesters_at_the_same_position_the_winner_is_stable() {
    let limits = Limits::default();
    let metrics = MetricLimits::default();
    let at = |instance: u8, value: f64| {
        let request = request("cpu", &[(0, value, "a")]);
        let checked = preflight(&request, &limits, &metrics).unwrap();
        let source = Source {
            instance: [instance; 16],
            segment: 1,
            offset: 10,
        };
        convert(&request, &limits, &metrics, &checked, source)
            .unwrap()
            .samples
    };
    let (low, high) = (at(1, 1.0), at(2, 2.0));
    let tenant = TenantId::new();
    let id = FileId::new();
    let write = |inputs: &[RecordBatch]| {
        values(
            &prepare_samples(
                tenant,
                &Path::from(""),
                id,
                hour(),
                inputs,
                FileLayout::current(8),
            )
            .unwrap(),
        )
    };
    let forward = write(&[low.clone(), high.clone()]);
    let backward = write(&[high, low]);
    assert_eq!(forward, backward);
    assert!(forward.contains(&("cpu".into(), T0_MS, 2.0)));
    assert!(!forward.contains(&("cpu".into(), T0_MS, 1.0)));
}
