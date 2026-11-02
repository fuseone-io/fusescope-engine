//! Metric samples, version 1, series, versions 1 and 2, and the OTLP metrics to Arrow
//! conversion (`MAPPING.md`, "Metrics").
//!
//! A request converts in two passes over one traversal: [`preflight`]
//! validates every point and counts what it would produce, samples,
//! series and bytes, without keeping any; [`convert`] then builds exactly
//! that, so a caller reserves memory for the bound before anything is
//! allocated.

mod canonical;
mod decode;
mod expand;
mod identity;
mod label_meta;
mod rows;
mod schema;

pub use expand::{
    MAX_TRACKED_HOURS, Preflight, TooLarge, check_points, convert, convert_mapping, preflight,
    walk_bound,
};
pub use identity::{LE, NAME, QUANTILE, SCOPE_NAME, SCOPE_SCHEMA_URL, SCOPE_VERSION, TEMPORALITY};
pub use label_meta::{Conflict, Datatype, LabelLevel, LabelMeta};
pub use rows::{
    MetricRows, SeriesRow, dedupe_series, series_batch, series_batch_of, series_row_sizes,
    series_rows, with_source,
};
pub use schema::{
    SAMPLES_SCHEMA_VERSION, SERIES_SCHEMA_VERSION, check_samples_schema, col, samples_schema_v1,
    series_schema_v1, series_schema_v2, series_schema_version,
};

/// Version of the rules a request converts by: `MAPPING.md` ("Metrics"),
/// the canonical text, fingerprint version 1 and the schemas. Any change
/// to the rows a request converts into takes a new version. Version 1
/// knew no label origin; version 2 records each label's level and type.
pub const MAPPING_VERSION: u8 = 2;

/// Whether this engine converts under `mapping`.
pub fn converts(mapping: u8) -> bool {
    (1..=MAPPING_VERSION).contains(&mapping)
}

/// Bytes of a sample's source order: the WAL instance, segment and offset
/// of its record and its ordinal in the request.
pub const SOURCE_ORDER_BYTES: usize = 36;

/// [`SOURCE_ORDER_BYTES`] as Arrow sizes a fixed-size binary.
pub const SOURCE_ORDER_WIDTH: i32 = 36;

/// The kind of series, as the Go backend reads `type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MetricType {
    /// `Gauge`.
    Gauge,
    /// `Sum`.
    Sum,
    /// `Histogram`: the bucket series of a histogram.
    Histogram,
    /// `Summary`: the quantile series of a summary.
    Summary,
}

impl MetricType {
    /// The type `text` stores, `None` for another.
    pub fn parse(text: &str) -> Option<Self> {
        [Self::Gauge, Self::Sum, Self::Histogram, Self::Summary]
            .into_iter()
            .find(|t| t.as_str() == text)
    }

    /// The stored text.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gauge => "Gauge",
            Self::Sum => "Sum",
            Self::Histogram => "Histogram",
            Self::Summary => "Summary",
        }
    }
}

/// Aggregation temporality, as the Go backend reads `temporality`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Temporality {
    /// `Unspecified`: gauges, summaries, and sums that name none.
    Unspecified,
    /// `Delta`.
    Delta,
    /// `Cumulative`.
    Cumulative,
}

impl Temporality {
    /// The temporality `text` stores, `None` for another.
    pub fn parse(text: &str) -> Option<Self> {
        [Self::Unspecified, Self::Delta, Self::Cumulative]
            .into_iter()
            .find(|t| t.as_str() == text)
    }

    /// The stored text.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "Unspecified",
            Self::Delta => "Delta",
            Self::Cumulative => "Cumulative",
        }
    }

    fn from_otlp(code: i32) -> Self {
        match code {
            1 => Self::Delta,
            2 => Self::Cumulative,
            _ => Self::Unspecified,
        }
    }
}

/// Bounds of one request's conversion, applied before anything is built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricLimits {
    /// Most data points a request carries.
    pub max_points: usize,
    /// Most samples a request expands into, buckets and quantiles
    /// included; at most `u32::MAX`, a sample's ordinal.
    pub max_samples: usize,
    /// Most explicit bounds of a histogram point.
    pub max_histogram_buckets: usize,
    /// Most quantiles of a summary point.
    pub max_summary_quantiles: usize,
    /// Most labels of a series, generated ones included, after the levels
    /// fuse; also the most attributes of one level.
    pub max_series_labels: usize,
}

impl Default for MetricLimits {
    fn default() -> Self {
        Self {
            max_points: 100_000,
            max_samples: 1_000_000,
            max_histogram_buckets: 160,
            max_summary_quantiles: 64,
            max_series_labels: 320,
        }
    }
}

impl MetricLimits {
    /// The least `max_series_labels`: the generated labels every series
    /// carries and one more.
    pub const MIN_SERIES_LABELS: usize = identity::ALWAYS_GENERATED + 1;
    /// The most `max_histogram_buckets` takes.
    pub const MAX_HISTOGRAM_BUCKETS: usize = 4096;
    /// The most `max_summary_quantiles` takes.
    pub const MAX_SUMMARY_QUANTILES: usize = 4096;
    /// The most `max_series_labels` takes.
    pub const MAX_SERIES_LABELS: usize = 4096;

    /// The limits with `max_samples` within `1..=u32::MAX`, every other
    /// bound at least 1 and the per-point ones at most their ceilings,
    /// whatever the caller validated.
    #[must_use]
    pub fn clamped(self) -> Self {
        let max_ordinal = usize::try_from(u32::MAX).unwrap_or(usize::MAX);
        Self {
            max_points: self.max_points.max(1),
            max_samples: self.max_samples.clamp(1, max_ordinal),
            max_histogram_buckets: self
                .max_histogram_buckets
                .clamp(1, Self::MAX_HISTOGRAM_BUCKETS),
            max_summary_quantiles: self
                .max_summary_quantiles
                .clamp(1, Self::MAX_SUMMARY_QUANTILES),
            max_series_labels: self
                .max_series_labels
                .clamp(Self::MIN_SERIES_LABELS, Self::MAX_SERIES_LABELS),
        }
    }
}

/// Why a data point was refused. Every refused point is reported in the
/// request's partial success and counted by its reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Rejection {
    /// An exponential histogram, not stored by this version.
    ExponentialHistogram,
    /// A histogram or summary point flagged with no recorded value.
    StaleHistogram,
    /// Buckets that do not match their bounds or count, bounds not finite
    /// and strictly increasing, or a cumulative count past `u64`.
    MalformedHistogram,
    /// Quantiles outside `[0, 1]` or not strictly increasing, or a value
    /// that is NaN or negative.
    MalformedSummary,
    /// More bounds than `max_histogram_buckets`.
    TooManyBuckets,
    /// More quantiles than `max_summary_quantiles`.
    TooManyQuantiles,
    /// More labels than `max_series_labels`.
    TooManyLabels,
    /// No time.
    NoTimestamp,
    /// A number point with no value and not flagged as such.
    NoValue,
    /// A metric without a name.
    NoName,
}

impl Rejection {
    /// Every reason, in a fixed order.
    pub const ALL: [Self; 10] = [
        Self::ExponentialHistogram,
        Self::StaleHistogram,
        Self::MalformedHistogram,
        Self::MalformedSummary,
        Self::TooManyBuckets,
        Self::TooManyQuantiles,
        Self::TooManyLabels,
        Self::NoTimestamp,
        Self::NoValue,
        Self::NoName,
    ];

    /// The label of its counter.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ExponentialHistogram => "exponential_histogram",
            Self::StaleHistogram => "stale_histogram",
            Self::MalformedHistogram => "malformed_histogram",
            Self::MalformedSummary => "malformed_summary",
            Self::TooManyBuckets => "too_many_buckets",
            Self::TooManyQuantiles => "too_many_quantiles",
            Self::TooManyLabels => "too_many_labels",
            Self::NoTimestamp => "no_timestamp",
            Self::NoValue => "no_value",
            Self::NoName => "no_name",
        }
    }
}

/// Points refused, by reason.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Rejections([u64; Rejection::ALL.len()]);

impl Rejections {
    fn add(&mut self, reason: Rejection, points: u64) {
        if let Some(n) = self.0.get_mut(reason as usize) {
            *n = n.saturating_add(points);
        }
    }

    /// Points refused for `reason`.
    pub fn of(&self, reason: Rejection) -> u64 {
        self.0.get(reason as usize).copied().unwrap_or(0)
    }

    /// Points refused.
    pub fn total(&self) -> u64 {
        self.0.iter().fold(0, |sum, n| sum.saturating_add(*n))
    }

    /// The reasons with points, in a fixed order: a message of at most one
    /// entry a reason, whatever the points.
    pub fn summary(&self) -> String {
        let mut out = String::new();
        for reason in Rejection::ALL {
            let n = self.of(reason);
            if n > 0 {
                if !out.is_empty() {
                    out.push_str(", ");
                }
                out.push_str(reason.as_str());
                out.push('=');
                out.push_str(&n.to_string());
            }
        }
        out
    }
}

/// Where a request's samples come from: the WAL instance, segment and
/// offset of its record. A sample's source order appends its ordinal, so
/// orders compare as bytes and the larger wins a duplicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Source {
    /// The WAL instance id.
    pub instance: [u8; 16],
    /// The segment of the record.
    pub segment: u64,
    /// The offset of the record in its segment.
    pub offset: u64,
}

impl Source {
    /// The source order of the `ordinal`th sample of the request.
    pub fn order(&self, ordinal: u32) -> [u8; SOURCE_ORDER_BYTES] {
        let mut out = [0u8; SOURCE_ORDER_BYTES];
        let (instance, rest) = out.split_at_mut(16);
        instance.copy_from_slice(&self.instance);
        let (segment, rest) = rest.split_at_mut(8);
        segment.copy_from_slice(&self.segment.to_be_bytes());
        let (offset, ordinal_bytes) = rest.split_at_mut(8);
        offset.copy_from_slice(&self.offset.to_be_bytes());
        ordinal_bytes.copy_from_slice(&ordinal.to_be_bytes());
        out
    }
}

#[cfg(test)]
mod tests;
