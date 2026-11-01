//! One traversal of an OTLP metrics request, run twice: to count and
//! validate ([`preflight`]) and to build ([`convert`]).

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    HistogramDataPoint, Metric, NumberDataPoint, SummaryDataPoint,
};

use super::canonical::{Capped, double};
use super::identity::{
    ALWAYS_GENERATED, LE, Level, NAME, QUANTILE, SCOPE_NAME, SCOPE_SCHEMA_URL, SCOPE_VERSION,
    SeriesKey, TEMPORALITY, TooManyLabels, fingerprint, labels,
};
use super::rows::{Builder, MetricRows};
use super::{MetricLimits, MetricType, Rejection, Rejections, Source, Temporality};
use crate::SchemaError;
use crate::limits::{DropStats, Limits};

/// OTLP `DataPointFlags.FLAG_NO_RECORDED_VALUE`.
const NO_RECORDED_VALUE: u32 = 1;

const MILLIS_PER_HOUR: i64 = 3_600_000;

/// What a request converts into, counted without building it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Preflight {
    /// Data points the request carries.
    pub points: usize,
    /// Points refused, by reason.
    pub rejected: Rejections,
    /// Samples, one per series of each accepted point.
    pub samples: usize,
    /// Series rows before deduplication, one per sample.
    pub series: usize,
    /// Labels over every series row.
    pub labels: usize,
    /// Bytes of those labels' keys and values.
    pub label_bytes: usize,
    /// Bytes of the series' names, units and descriptions.
    pub text_bytes: usize,
    /// Bytes of the samples' names.
    pub name_bytes: usize,
    /// Attributes dropped or truncated on the way.
    pub stats: DropStats,
    /// The most bytes the traversal holds for the levels and labels of one
    /// point, whatever the request.
    pub scratch: usize,
}

/// A request over a request-wide bound, refused whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TooLarge {
    /// More data points than `max_points`.
    Points(usize),
    /// More samples than `max_samples`.
    Samples(usize),
}

impl Preflight {
    /// The most bytes [`convert`] holds at once for this request: the
    /// samples, sorted in place, and their batch; the series rows, their
    /// labels and texts.
    pub fn bytes_bound(&self) -> usize {
        super::rows::bytes_bound(self)
    }
}

/// Counts and validates `request` under `limits` and `metrics`, building
/// nothing but one level of attributes at a time.
///
/// # Errors
/// A request over `max_points` or `max_samples`, counted before the rest
/// is walked.
pub fn preflight(
    request: &ExportMetricsServiceRequest,
    limits: &Limits,
    metrics: &MetricLimits,
) -> Result<Preflight, TooLarge> {
    let metrics = metrics.clamped();
    let points = count_points(request);
    if points > metrics.max_points {
        return Err(TooLarge::Points(points));
    }
    let mut count = Count {
        preflight: Preflight {
            points,
            ..Preflight::default()
        },
        max_samples: metrics.max_samples,
    };
    let mut stats = DropStats::default();
    walk(request, limits, &metrics, &mut count, &mut stats).map_err(|e| match e {
        Stop::TooLarge(t) => t,
        Stop::Schema(_) => TooLarge::Samples(usize::MAX),
    })?;
    count.preflight.stats = stats;
    count.preflight.scratch = scratch_bound(limits, &metrics);
    Ok(count.preflight)
}

/// Builds `request`'s samples and series, as `checked` counted them; the
/// `n`th sample gets `source`'s order with ordinal `n`.
///
/// # Errors
/// An Arrow failure, or a request that converts into more than `checked`
/// counted.
pub fn convert(
    request: &ExportMetricsServiceRequest,
    limits: &Limits,
    metrics: &MetricLimits,
    checked: &Preflight,
    source: Source,
) -> Result<MetricRows, SchemaError> {
    let metrics = metrics.clamped();
    let mut builder = Builder::with_capacity(checked);
    let mut stats = DropStats::default();
    walk(request, limits, &metrics, &mut builder, &mut stats).map_err(|e| match e {
        Stop::Schema(e) => e,
        Stop::TooLarge(_) => SchemaError::Layout("metrics: conversion past its preflight"),
    })?;
    builder.finish(source, checked.rejected, stats)
}

/// Three levels of at most `max_series_labels` attributes, each a key and
/// a value whose text may grow to twice its cap, and the fused labels.
fn scratch_bound(limits: &Limits, metrics: &MetricLimits) -> usize {
    let cap = metrics.max_series_labels;
    let pair =
        2 * std::mem::size_of::<String>() + limits.max_key_bytes + 2 * limits.max_value_bytes;
    let fused = 2 * std::mem::size_of::<(&str, &str)>();
    cap.saturating_mul(3 * pair + 2 * fused)
}

fn count_points(request: &ExportMetricsServiceRequest) -> usize {
    request
        .resource_metrics
        .iter()
        .flat_map(|r| &r.scope_metrics)
        .flat_map(|s| &s.metrics)
        .map(points_of)
        .fold(0, usize::saturating_add)
}

fn points_of(metric: &Metric) -> usize {
    match &metric.data {
        Some(Data::Gauge(g)) => g.data_points.len(),
        Some(Data::Sum(s)) => s.data_points.len(),
        Some(Data::Histogram(h)) => h.data_points.len(),
        Some(Data::ExponentialHistogram(e)) => e.data_points.len(),
        Some(Data::Summary(s)) => s.data_points.len(),
        None => 0,
    }
}

/// Why a traversal stopped.
pub(crate) enum Stop {
    TooLarge(TooLarge),
    Schema(SchemaError),
}

impl From<SchemaError> for Stop {
    fn from(e: SchemaError) -> Self {
        Self::Schema(e)
    }
}

/// One series of an accepted point, with its sample.
pub(crate) struct Emit<'a> {
    pub(crate) key: SeriesKey<'a>,
    pub(crate) description: &'a str,
    pub(crate) fingerprint: u64,
    pub(crate) labels: &'a [(&'a str, &'a str)],
    pub(crate) unix_milli: i64,
    pub(crate) value: f64,
    pub(crate) flags: u32,
}

/// What a traversal hands its results to.
pub(crate) trait Sink {
    /// A point refused for `reason`.
    fn reject(&mut self, reason: Rejection, points: usize);
    /// A new metric: the names its series take next are new.
    fn metric(&mut self);
    /// One series and sample of an accepted point.
    fn emit(&mut self, emit: &Emit<'_>) -> Result<(), Stop>;
}

struct Count {
    preflight: Preflight,
    max_samples: usize,
}

impl Sink for Count {
    fn reject(&mut self, reason: Rejection, points: usize) {
        self.preflight.rejected.add(reason, points as u64);
    }

    fn metric(&mut self) {}

    fn emit(&mut self, emit: &Emit<'_>) -> Result<(), Stop> {
        let p = &mut self.preflight;
        p.samples += 1;
        if p.samples > self.max_samples {
            return Err(Stop::TooLarge(TooLarge::Samples(p.samples)));
        }
        p.series += 1;
        p.labels += emit.labels.len();
        p.label_bytes += emit
            .labels
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>();
        p.text_bytes += emit.key.name.len() + emit.key.unit.len() + emit.description.len();
        p.name_bytes += emit.key.name.len();
        Ok(())
    }
}

/// The levels and generated labels shared by the series of one scope.
struct Context<'a> {
    resource: &'a Level,
    scope: &'a Level,
    name: &'a str,
    version: &'a str,
    schema_url: &'a str,
}

fn walk(
    request: &ExportMetricsServiceRequest,
    limits: &Limits,
    metrics: &MetricLimits,
    sink: &mut impl Sink,
    stats: &mut DropStats,
) -> Result<(), Stop> {
    let cap = metrics.max_series_labels;
    for resource_metrics in &request.resource_metrics {
        let attributes = resource_metrics
            .resource
            .as_ref()
            .map_or(&[][..], |r| r.attributes.as_slice());
        let resource = Level::new(
            "resource",
            &[resource_metrics.schema_url.as_str()],
            attributes,
            limits,
            cap,
            stats,
        );
        for scope_metrics in &resource_metrics.scope_metrics {
            let (name, version, attributes) =
                scope_metrics.scope.as_ref().map_or(("", "", &[][..]), |s| {
                    (s.name.as_str(), s.version.as_str(), s.attributes.as_slice())
                });
            let schema_url = scope_metrics.schema_url.as_str();
            let scope = Level::new(
                "scope",
                &[name, version, schema_url],
                attributes,
                limits,
                cap,
                stats,
            );
            let (Ok(resource), Ok(scope)) = (resource.as_ref(), scope) else {
                for metric in &scope_metrics.metrics {
                    sink.reject(Rejection::TooManyLabels, points_of(metric));
                }
                continue;
            };
            let at = Context {
                resource,
                scope: &scope,
                name,
                version,
                schema_url,
            };
            for metric in &scope_metrics.metrics {
                sink.metric();
                metric_points(&at, metric, limits, metrics, sink, stats)?;
            }
        }
    }
    Ok(())
}

fn metric_points(
    at: &Context<'_>,
    metric: &Metric,
    limits: &Limits,
    metrics: &MetricLimits,
    sink: &mut impl Sink,
    stats: &mut DropStats,
) -> Result<(), Stop> {
    if metric.name.is_empty() {
        sink.reject(Rejection::NoName, points_of(metric));
        return Ok(());
    }
    match &metric.data {
        Some(Data::Gauge(g)) => {
            for point in &g.data_points {
                let kind = (MetricType::Gauge, Temporality::Unspecified, false);
                number(at, metric, point, kind, limits, metrics, sink, stats)?;
            }
        }
        Some(Data::Sum(s)) => {
            let temporality = Temporality::from_otlp(s.aggregation_temporality);
            for point in &s.data_points {
                let kind = (MetricType::Sum, temporality, s.is_monotonic);
                number(at, metric, point, kind, limits, metrics, sink, stats)?;
            }
        }
        Some(Data::Histogram(h)) => {
            let temporality = Temporality::from_otlp(h.aggregation_temporality);
            for point in &h.data_points {
                histogram(at, metric, point, temporality, limits, metrics, sink, stats)?;
            }
        }
        Some(Data::Summary(s)) => {
            for point in &s.data_points {
                summary(at, metric, point, limits, metrics, sink, stats)?;
            }
        }
        Some(Data::ExponentialHistogram(e)) => {
            sink.reject(Rejection::ExponentialHistogram, e.data_points.len());
        }
        None => {}
    }
    Ok(())
}

/// Text of every generated label but `le` and `quantile`.
fn generated<'a>(
    at: &Context<'a>,
    name: &'a str,
    temporality: Temporality,
) -> [(&'static str, &'a str); ALWAYS_GENERATED] {
    [
        (NAME, name),
        (TEMPORALITY, temporality.as_str()),
        (SCOPE_NAME, at.name),
        (SCOPE_VERSION, at.version),
        (SCOPE_SCHEMA_URL, at.schema_url),
    ]
}

/// The point's attributes, and whether its series fit the label cap with
/// `extra` generated labels.
fn point_level(
    at: &Context<'_>,
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
    extra: usize,
    limits: &Limits,
    cap: usize,
    stats: &mut DropStats,
) -> Result<Level, TooManyLabels> {
    let point = Level::new("point", &[], attributes, limits, cap, stats)?;
    let mut scratch = Vec::new();
    let mut generated: Vec<(&'static str, &str)> = generated(at, "", Temporality::Unspecified)
        .into_iter()
        .collect();
    if extra > 0 {
        generated.push((LE, ""));
    }
    labels(
        [at.resource, at.scope, &point],
        &generated,
        cap,
        &mut scratch,
    )
    .ok_or(TooManyLabels)?;
    Ok(point)
}

fn unix_milli(time_unix_nano: u64) -> Option<i64> {
    (time_unix_nano > 0).then(|| i64::try_from(time_unix_nano / 1_000_000).unwrap_or(i64::MAX))
}

/// Emits the series named `name` of a point, its labels built from the
/// levels and `extra`, `le` or `quantile`.
#[allow(clippy::too_many_arguments)]
fn emit_series(
    at: &Context<'_>,
    point: &Level,
    key: SeriesKey<'_>,
    description: &str,
    unix_milli: i64,
    (value, flags): (f64, u32),
    cap: usize,
    sink: &mut impl Sink,
) -> Result<(), Stop> {
    let mut generated: Vec<(&'static str, &str)> = generated(at, key.name, key.temporality)
        .into_iter()
        .collect();
    if let Some(extra) = key.generated {
        generated.push(extra);
    }
    let mut fused = Vec::new();
    labels([at.resource, at.scope, point], &generated, cap, &mut fused)
        .ok_or(SchemaError::Layout("metrics: labels past a checked cap"))?;
    let fingerprint = fingerprint([at.resource, at.scope, point], &key);
    sink.emit(&Emit {
        key,
        description,
        fingerprint,
        labels: &fused,
        unix_milli,
        value,
        flags,
    })
}

#[allow(clippy::too_many_arguments)]
fn number(
    at: &Context<'_>,
    metric: &Metric,
    point: &NumberDataPoint,
    (kind, temporality, monotonic): (MetricType, Temporality, bool),
    limits: &Limits,
    metrics: &MetricLimits,
    sink: &mut impl Sink,
    stats: &mut DropStats,
) -> Result<(), Stop> {
    let cap = metrics.max_series_labels;
    let Some(unix_milli) = unix_milli(point.time_unix_nano) else {
        sink.reject(Rejection::NoTimestamp, 1);
        return Ok(());
    };
    let stale = point.flags & NO_RECORDED_VALUE != 0;
    #[allow(clippy::cast_precision_loss)]
    let value = match point.value {
        _ if stale => 0.0,
        Some(NumberValue::AsDouble(d)) => d,
        Some(NumberValue::AsInt(i)) => i as f64,
        None => {
            sink.reject(Rejection::NoValue, 1);
            return Ok(());
        }
    };
    if value.is_nan() {
        return Ok(());
    }
    let Ok(level) = point_level(at, &point.attributes, 0, limits, cap, stats) else {
        sink.reject(Rejection::TooManyLabels, 1);
        return Ok(());
    };
    let key = SeriesKey {
        name: &metric.name,
        kind,
        unit: &metric.unit,
        temporality,
        monotonic,
        generated: None,
    };
    let value = (value, point.flags);
    emit_series(
        at,
        &level,
        key,
        &metric.description,
        unix_milli,
        value,
        cap,
        sink,
    )
}

/// The bucket counts of `point` made cumulative over the bounds, or why
/// the point is refused.
fn cumulative_buckets(
    point: &HistogramDataPoint,
    metrics: &MetricLimits,
) -> Result<Vec<u64>, Rejection> {
    let bounds = &point.explicit_bounds;
    if bounds.len() > metrics.max_histogram_buckets {
        return Err(Rejection::TooManyBuckets);
    }
    // The proto's one exception: no bounds and no counts, one `+Inf`
    // bucket holding the whole count.
    if bounds.is_empty() && point.bucket_counts.is_empty() {
        return Ok(vec![point.count]);
    }
    if point.bucket_counts.len() != bounds.len() + 1
        || bounds.iter().any(|b| !b.is_finite())
        || bounds.windows(2).any(|w| matches!(w, [a, b] if a >= b))
    {
        return Err(Rejection::MalformedHistogram);
    }
    let mut total = 0u64;
    let mut cumulative = Vec::with_capacity(point.bucket_counts.len());
    for count in &point.bucket_counts {
        total = total
            .checked_add(*count)
            .ok_or(Rejection::MalformedHistogram)?;
        cumulative.push(total);
    }
    if total != point.count {
        return Err(Rejection::MalformedHistogram);
    }
    Ok(cumulative)
}

#[allow(clippy::too_many_arguments, clippy::cast_precision_loss)]
fn histogram(
    at: &Context<'_>,
    metric: &Metric,
    point: &HistogramDataPoint,
    temporality: Temporality,
    limits: &Limits,
    metrics: &MetricLimits,
    sink: &mut impl Sink,
    stats: &mut DropStats,
) -> Result<(), Stop> {
    let cap = metrics.max_series_labels;
    if point.flags & NO_RECORDED_VALUE != 0 {
        sink.reject(Rejection::StaleHistogram, 1);
        return Ok(());
    }
    let Some(unix_milli) = unix_milli(point.time_unix_nano) else {
        sink.reject(Rejection::NoTimestamp, 1);
        return Ok(());
    };
    let cumulative = match cumulative_buckets(point, metrics) {
        Ok(c) => c,
        Err(reason) => {
            sink.reject(reason, 1);
            return Ok(());
        }
    };
    let Ok(level) = point_level(at, &point.attributes, 1, limits, cap, stats) else {
        sink.reject(Rejection::TooManyLabels, 1);
        return Ok(());
    };
    let names = Names::new(&metric.name);
    let unit = metric.unit.as_str();
    let description = metric.description.as_str();
    let mut bound = String::new();
    for (i, count) in cumulative.iter().enumerate() {
        bound.clear();
        match point.explicit_bounds.get(i) {
            Some(b) => double(*b, &mut Capped::new(&mut bound, usize::MAX)),
            None => bound.push_str("+Inf"),
        }
        let key = SeriesKey {
            name: &names.bucket,
            kind: MetricType::Histogram,
            unit,
            temporality,
            monotonic: true,
            generated: Some((LE, &bound)),
        };
        let sample = (*count as f64, point.flags);
        emit_series(at, &level, key, description, unix_milli, sample, cap, sink)?;
    }
    let count_unit = if unit.is_empty() { "1" } else { unit };
    let count = (point.count as f64, point.flags);
    let key = sum_key(&names.count, temporality, count_unit);
    emit_series(at, &level, key, description, unix_milli, count, cap, sink)?;
    if let Some(s) = point.sum.filter(|s| !s.is_nan()) {
        let key = sum_key(&names.sum, temporality, unit);
        emit_series(
            at,
            &level,
            key,
            description,
            unix_milli,
            (s, point.flags),
            cap,
            sink,
        )?;
    }
    for (name, value) in [(&names.min, point.min), (&names.max, point.max)] {
        if let Some(v) = value.filter(|v| !v.is_nan()) {
            let key = SeriesKey {
                name,
                kind: MetricType::Gauge,
                unit,
                temporality: Temporality::Unspecified,
                monotonic: false,
                generated: None,
            };
            emit_series(
                at,
                &level,
                key,
                description,
                unix_milli,
                (v, point.flags),
                cap,
                sink,
            )?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::cast_precision_loss)]
fn summary(
    at: &Context<'_>,
    metric: &Metric,
    point: &SummaryDataPoint,
    limits: &Limits,
    metrics: &MetricLimits,
    sink: &mut impl Sink,
    stats: &mut DropStats,
) -> Result<(), Stop> {
    let cap = metrics.max_series_labels;
    if point.flags & NO_RECORDED_VALUE != 0 {
        sink.reject(Rejection::StaleHistogram, 1);
        return Ok(());
    }
    let Some(unix_milli) = unix_milli(point.time_unix_nano) else {
        sink.reject(Rejection::NoTimestamp, 1);
        return Ok(());
    };
    let quantiles = &point.quantile_values;
    if quantiles.len() > metrics.max_summary_quantiles {
        sink.reject(Rejection::TooManyQuantiles, 1);
        return Ok(());
    }
    let malformed = quantiles
        .iter()
        .any(|q| !(0.0..=1.0).contains(&q.quantile) || q.value.is_nan() || q.value < 0.0)
        || quantiles
            .windows(2)
            .any(|w| matches!(w, [a, b] if a.quantile >= b.quantile));
    if malformed {
        sink.reject(Rejection::MalformedSummary, 1);
        return Ok(());
    }
    let Ok(level) = point_level(at, &point.attributes, 1, limits, cap, stats) else {
        sink.reject(Rejection::TooManyLabels, 1);
        return Ok(());
    };
    let names = Names::new(&metric.name);
    let unit = metric.unit.as_str();
    let description = metric.description.as_str();
    let mut text = String::new();
    for q in quantiles {
        text.clear();
        double(q.quantile, &mut Capped::new(&mut text, usize::MAX));
        let key = SeriesKey {
            name: &names.quantile,
            kind: MetricType::Summary,
            unit,
            temporality: Temporality::Unspecified,
            monotonic: false,
            generated: Some((QUANTILE, &text)),
        };
        emit_series(
            at,
            &level,
            key,
            description,
            unix_milli,
            (q.value, point.flags),
            cap,
            sink,
        )?;
    }
    let count = (point.count as f64, point.flags);
    emit_series(
        at,
        &level,
        sum_key(&names.count, Temporality::Cumulative, "1"),
        description,
        unix_milli,
        count,
        cap,
        sink,
    )?;
    if !point.sum.is_nan() {
        let key = sum_key(&names.sum, Temporality::Cumulative, unit);
        emit_series(
            at,
            &level,
            key,
            description,
            unix_milli,
            (point.sum, point.flags),
            cap,
            sink,
        )?;
    }
    Ok(())
}

/// The key of a histogram's or summary's monotonic `Sum` series.
fn sum_key<'a>(name: &'a str, temporality: Temporality, unit: &'a str) -> SeriesKey<'a> {
    SeriesKey {
        name,
        kind: MetricType::Sum,
        unit,
        temporality,
        monotonic: true,
        generated: None,
    }
}

/// The names of a histogram's or summary's series.
struct Names {
    bucket: String,
    count: String,
    sum: String,
    min: String,
    max: String,
    quantile: String,
}

impl Names {
    fn new(name: &str) -> Self {
        let with = |suffix: &str| format!("{name}.{suffix}");
        Self {
            bucket: with("bucket"),
            count: with("count"),
            sum: with("sum"),
            min: with("min"),
            max: with("max"),
            quantile: with("quantile"),
        }
    }
}

/// Hours of `unix_milli`, as series rows key them.
pub(crate) fn hour_of(unix_milli: i64) -> i64 {
    unix_milli - unix_milli.rem_euclid(MILLIS_PER_HOUR)
}
