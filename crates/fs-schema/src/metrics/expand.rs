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
    ALWAYS_GENERATED, HASHER_BYTES, LE, Label, Level, NAME, QUANTILE, SCOPE_NAME, SCOPE_SCHEMA_URL,
    SCOPE_VERSION, SeriesKey, TEMPORALITY, TooManyLabels, fingerprint, labels,
};
use super::label_meta::{Datatype, LabelLevel, LabelMeta};
use super::rows::{Builder, MetricRows};
use super::{
    MAPPING_VERSION, MetricLimits, MetricType, Rejection, Rejections, Source, Temporality,
};
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
    /// At least the bytes either traversal holds at once for the levels,
    /// labels and names of one point of this request: [`walk_bound`].
    pub scratch: usize,
    /// Starts of the hours the samples fall in, in Unix milliseconds, in
    /// order; at most [`MAX_TRACKED_HOURS`], `hours_over` past them.
    pub hours: Vec<i64>,
    /// The samples fall in more hours than [`MAX_TRACKED_HOURS`].
    pub hours_over: bool,
}

/// Most distinct hours a preflight lists.
pub const MAX_TRACKED_HOURS: usize = 1024;

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

    /// What the rows hold once converted, the samples' batch and the
    /// series: what a buffer keeps.
    pub fn rows_bound(&self) -> usize {
        super::rows::rows_bound(self)
    }

    /// What converting holds besides the rows, only while it runs.
    pub fn transient_bound(&self) -> usize {
        super::rows::transient_bound(self)
    }
}

/// Counts and validates `request` under `limits` and `metrics`, building
/// nothing but one point's levels at a time, within [`walk_bound`].
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
    let points = check_points(request, &metrics)?;
    let mut count = Count {
        preflight: Preflight {
            points,
            scratch: walk_bound(request, limits, &metrics),
            ..Preflight::default()
        },
        max_samples: metrics.max_samples,
        #[cfg(test)]
        measured: 0,
    };
    let mut stats = DropStats::default();
    walk(request, limits, &metrics, &mut count, &mut stats).map_err(|e| match e {
        Stop::TooLarge(t) => t,
        Stop::Schema(_) => TooLarge::Samples(usize::MAX),
    })?;
    count.preflight.stats = stats;
    Ok(count.preflight)
}

/// The most bytes a preflight of `request` was seen to hold for its levels
/// and labels, by their capacities.
#[cfg(test)]
pub(crate) fn measure(
    request: &ExportMetricsServiceRequest,
    limits: &Limits,
    metrics: &MetricLimits,
) -> usize {
    let metrics = metrics.clamped();
    let mut count = Count {
        preflight: Preflight::default(),
        max_samples: metrics.max_samples,
        measured: 0,
    };
    let _ = walk(
        request,
        limits,
        &metrics,
        &mut count,
        &mut DropStats::default(),
    );
    count.measured
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
    convert_mapping(MAPPING_VERSION, request, limits, metrics, checked, source)
}

/// [`convert`] under `mapping`, a version this engine converts by: under
/// 1, every label origin is legacy.
///
/// # Errors
/// As [`convert`], or a mapping this engine does not convert by.
pub fn convert_mapping(
    mapping: u8,
    request: &ExportMetricsServiceRequest,
    limits: &Limits,
    metrics: &MetricLimits,
    checked: &Preflight,
    source: Source,
) -> Result<MetricRows, SchemaError> {
    if !super::converts(mapping) {
        return Err(SchemaError::Layout(
            "metrics: a mapping this engine does not convert by",
        ));
    }
    let metrics = metrics.clamped();
    let mut builder = Builder::with_capacity(checked, mapping);
    let mut stats = DropStats::default();
    walk(request, limits, &metrics, &mut builder, &mut stats).map_err(|e| match e {
        Stop::Schema(e) => e,
        Stop::TooLarge(_) => SchemaError::Layout("metrics: conversion past its preflight"),
    })?;
    builder.finish(source, checked.rejected, stats)
}

/// Bytes of one label of a series being built.
const LABEL: usize = std::mem::size_of::<Label<'static>>();

/// Bytes of the text of a bucket's bound or a quantile.
const GENERATED_TEXT: usize = 1024;

/// Found without allocating, at least the bytes either traversal of
/// `request` holds at once besides what its sink keeps: the levels of a
/// point, built and being built, its labels and their sort, the names and
/// cumulative buckets of its metric, a bound's text, the hasher, and the
/// hours a preflight lists.
pub fn walk_bound(
    request: &ExportMetricsServiceRequest,
    limits: &Limits,
    metrics: &MetricLimits,
) -> usize {
    let metrics = metrics.clamped();
    let cap = metrics.max_series_labels;
    let mut peak = 0usize;
    for resource_metrics in &request.resource_metrics {
        let attributes = resource_metrics
            .resource
            .as_ref()
            .map_or(&[][..], |r| r.attributes.as_slice());
        let resource = Level::bound(attributes, limits, cap);
        peak = peak.max(resource.building);
        for scope_metrics in &resource_metrics.scope_metrics {
            let attributes = scope_metrics
                .scope
                .as_ref()
                .map_or(&[][..], |s| s.attributes.as_slice());
            let scope = Level::bound(attributes, limits, cap);
            peak = peak.max(resource.held.saturating_add(scope.building));
            let levels = (
                resource.held.saturating_add(scope.held),
                resource.pairs + scope.pairs,
            );
            for metric in &scope_metrics.metrics {
                peak = peak.max(metric_bound(metric, levels, limits, &metrics));
            }
        }
    }
    peak.saturating_add(HASHER_BYTES)
        .saturating_add(MAX_TRACKED_HOURS * std::mem::size_of::<i64>())
}

/// [`walk_bound`] of the points of `metric`, beside `levels`, the bytes
/// and pairs of their resource and scope.
fn metric_bound(
    metric: &Metric,
    (held, pairs): (usize, usize),
    limits: &Limits,
    metrics: &MetricLimits,
) -> usize {
    let cap = metrics.max_series_labels;
    let point = |attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue], extra: usize| {
        let level = Level::bound(attributes, limits, cap);
        let labels = (pairs + level.pairs + ALWAYS_GENERATED + 1)
            .saturating_mul(3)
            .saturating_add(2 * (ALWAYS_GENERATED + 1))
            .saturating_mul(LABEL);
        held.saturating_add(
            level
                .building
                .max(level.held.saturating_add(labels).saturating_add(extra)),
        )
    };
    // Six names of the metric's and a suffix, each up to twice its text.
    let names = metric.name.len().saturating_add(16).saturating_mul(12);
    match &metric.data {
        Some(Data::Gauge(g)) => g.data_points.iter().map(|p| point(&p.attributes, 0)).max(),
        Some(Data::Sum(s)) => s.data_points.iter().map(|p| point(&p.attributes, 0)).max(),
        Some(Data::Histogram(h)) => h
            .data_points
            .iter()
            .map(|p| {
                let buckets = p
                    .bucket_counts
                    .len()
                    .clamp(1, metrics.max_histogram_buckets.saturating_add(1))
                    .saturating_mul(std::mem::size_of::<u64>());
                let extra = names.saturating_add(buckets).saturating_add(GENERATED_TEXT);
                point(&p.attributes, extra)
            })
            .max(),
        Some(Data::Summary(s)) => s
            .data_points
            .iter()
            .map(|p| point(&p.attributes, names.saturating_add(GENERATED_TEXT)))
            .max(),
        Some(Data::ExponentialHistogram(_)) | None => None,
    }
    .unwrap_or(0)
}

/// The data points `request` carries, counted without walking them.
///
/// # Errors
/// More than `max_points`.
pub fn check_points(
    request: &ExportMetricsServiceRequest,
    metrics: &MetricLimits,
) -> Result<usize, TooLarge> {
    let points = count_points(request);
    if points > metrics.clamped().max_points {
        return Err(TooLarge::Points(points));
    }
    Ok(points)
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
    pub(crate) labels: &'a [Label<'a>],
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
    /// The bytes the traversal holds for the point being expanded.
    fn scratch(&mut self, _bytes: usize) {}
}

struct Count {
    preflight: Preflight,
    max_samples: usize,
    /// The most bytes the traversal was seen to hold.
    #[cfg(test)]
    measured: usize,
}

impl Sink for Count {
    fn reject(&mut self, reason: Rejection, points: usize) {
        self.preflight.rejected.add(reason, points as u64);
    }

    fn metric(&mut self) {}

    #[cfg(test)]
    fn scratch(&mut self, bytes: usize) {
        self.measured = self.measured.max(bytes);
    }

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
            .map(|(k, v, _)| k.len() + v.len())
            .sum::<usize>();
        p.text_bytes += emit.key.name.len() + emit.key.unit.len() + emit.description.len();
        p.name_bytes += emit.key.name.len();
        let hour = hour_of(emit.unix_milli);
        if let Err(at) = p.hours.binary_search(&hour) {
            if p.hours.len() < MAX_TRACKED_HOURS {
                p.hours.insert(at, hour);
            } else {
                p.hours_over = true;
            }
        }
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
) -> [Label<'a>; ALWAYS_GENERATED] {
    let (point, scope) = (
        generated_meta(LabelLevel::Point),
        generated_meta(LabelLevel::Scope),
    );
    [
        (NAME, name, point),
        (TEMPORALITY, temporality.as_str(), point),
        (SCOPE_NAME, at.name, scope),
        (SCOPE_VERSION, at.version, scope),
        (SCOPE_SCHEMA_URL, at.schema_url, scope),
    ]
}

/// The origin of a label the engine generates at `level`: a string.
fn generated_meta(level: LabelLevel) -> LabelMeta {
    LabelMeta::native(level, Datatype::Str)
}

/// The point's attributes, and whether its series fit the label cap with
/// `extra` generated labels.
fn point_level(
    at: &Context<'_>,
    attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
    extra: usize,
    (limits, cap): (&Limits, usize),
    stats: &mut DropStats,
    sink: &mut impl Sink,
) -> Result<Level, TooManyLabels> {
    let point = Level::new("point", &[], attributes, limits, cap, stats)?;
    let mut scratch = Vec::new();
    let mut generated: Vec<Label<'_>> = generated(at, "", Temporality::Unspecified)
        .into_iter()
        .collect();
    if extra > 0 {
        generated.push((LE, "", generated_meta(LabelLevel::Point)));
    }
    let fits = labels(
        [at.resource, at.scope, &point],
        &generated,
        cap,
        &mut scratch,
    );
    sink.scratch(
        at.resource.held()
            + at.scope.held()
            + point.held()
            + (scratch.capacity() + generated.capacity()) * LABEL,
    );
    fits.ok_or(TooManyLabels)?;
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
    let mut generated: Vec<Label<'_>> = generated(at, key.name, key.temporality)
        .into_iter()
        .collect();
    if let Some((label, value)) = key.generated {
        generated.push((label, value, generated_meta(LabelLevel::Point)));
    }
    let mut fused = Vec::new();
    labels([at.resource, at.scope, point], &generated, cap, &mut fused)
        .ok_or(SchemaError::Layout("metrics: labels past a checked cap"))?;
    sink.scratch(
        at.resource.held()
            + at.scope.held()
            + point.held()
            + (fused.capacity() + generated.capacity()) * LABEL,
    );
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
    let Ok(level) = point_level(at, &point.attributes, 0, (limits, cap), stats, sink) else {
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
    let Ok(level) = point_level(at, &point.attributes, 1, (limits, cap), stats, sink) else {
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
    let Ok(level) = point_level(at, &point.attributes, 1, (limits, cap), stats, sink) else {
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
