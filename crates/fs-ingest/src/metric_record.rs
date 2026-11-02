//! The payload of a metric record of
//! [`VERSION_METRIC_RECORD`](crate::record::VERSION_METRIC_RECORD): what
//! converting the request depends on besides itself, then the request.
//!
//! Layout, integers little-endian: the mapping version (`u8`), then
//! `max_key_bytes`, `max_value_bytes`, `max_histogram_buckets`,
//! `max_summary_quantiles` and `max_series_labels` (`u64` each), as the
//! live conversion applied them, then the `ExportMetricsServiceRequest`
//! as received. A replay converts under these, not under the engine's
//! configuration, so a record keeps its rows whatever limits a restart
//! brings. A mapping version this engine does not convert stops the
//! replay. The bounds of a whole request are not kept: the request was
//! accepted under them, and they do not change its rows.

use bytes::Bytes;
use fs_schema::limits::Limits;
use fs_schema::metrics::{MAPPING_VERSION, MetricLimits};

/// Bytes ahead of the request.
pub const HEADER_BYTES: usize = 1 + 5 * 8;

/// What converting a metric request depends on besides the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricConversion {
    /// The rules, [`MAPPING_VERSION`] when written by this engine.
    pub mapping: u8,
    /// [`Limits::max_key_bytes`].
    pub max_key_bytes: usize,
    /// [`Limits::max_value_bytes`].
    pub max_value_bytes: usize,
    /// [`MetricLimits::max_histogram_buckets`].
    pub max_histogram_buckets: usize,
    /// [`MetricLimits::max_summary_quantiles`].
    pub max_summary_quantiles: usize,
    /// [`MetricLimits::max_series_labels`].
    pub max_series_labels: usize,
}

/// Why a metric payload cannot be converted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricPayloadError {
    /// Shorter than its header, or a limit past this platform's sizes.
    Damaged,
    /// Written under rules this engine does not convert by.
    Mapping(u8),
}

impl MetricConversion {
    /// What this engine converts by under `limits` and `metrics`, clamped
    /// as conversion clamps them.
    pub fn current(limits: &Limits, metrics: &MetricLimits) -> Self {
        let metrics = metrics.clamped();
        Self {
            mapping: MAPPING_VERSION,
            max_key_bytes: limits.max_key_bytes,
            max_value_bytes: limits.max_value_bytes,
            max_histogram_buckets: metrics.max_histogram_buckets,
            max_summary_quantiles: metrics.max_summary_quantiles,
            max_series_labels: metrics.max_series_labels,
        }
    }

    /// The first limit this engine would convert under other than as
    /// recorded, clamping it into its domain, and its recorded value; a
    /// record of an engine before the ceilings may carry one.
    pub fn outside_domain(&self) -> Option<(&'static str, usize)> {
        let clamped = MetricLimits {
            max_histogram_buckets: self.max_histogram_buckets,
            max_summary_quantiles: self.max_summary_quantiles,
            max_series_labels: self.max_series_labels,
            ..MetricLimits::default()
        }
        .clamped();
        [
            (
                "max_histogram_buckets",
                self.max_histogram_buckets,
                clamped.max_histogram_buckets,
            ),
            (
                "max_summary_quantiles",
                self.max_summary_quantiles,
                clamped.max_summary_quantiles,
            ),
            (
                "max_series_labels",
                self.max_series_labels,
                clamped.max_series_labels,
            ),
        ]
        .into_iter()
        .find(|(_, recorded, applied)| recorded != applied)
        .map(|(name, recorded, _)| (name, recorded))
    }

    /// `limits` and `metrics` with this conversion's values in place of
    /// theirs; the bounds of a whole request stay `metrics`'.
    pub fn apply(&self, limits: &Limits, metrics: &MetricLimits) -> (Limits, MetricLimits) {
        (
            Limits {
                max_key_bytes: self.max_key_bytes,
                max_value_bytes: self.max_value_bytes,
                ..*limits
            },
            MetricLimits {
                max_histogram_buckets: self.max_histogram_buckets,
                max_summary_quantiles: self.max_summary_quantiles,
                max_series_labels: self.max_series_labels,
                ..*metrics
            },
        )
    }

    /// The payload of `request`, the protobuf as received, under this
    /// conversion.
    pub fn payload(&self, request: &[u8]) -> Bytes {
        let mut out = Vec::with_capacity(HEADER_BYTES + request.len());
        out.push(self.mapping);
        for n in [
            self.max_key_bytes,
            self.max_value_bytes,
            self.max_histogram_buckets,
            self.max_summary_quantiles,
            self.max_series_labels,
        ] {
            out.extend_from_slice(&u64::try_from(n).unwrap_or(u64::MAX).to_le_bytes());
        }
        out.extend_from_slice(request);
        Bytes::from(out)
    }

    /// The conversion and the request of a payload written by
    /// [`MetricConversion::payload`].
    ///
    /// # Errors
    /// A payload shorter than its header or a limit this platform cannot
    /// hold, or a mapping version other than [`MAPPING_VERSION`].
    pub fn split(payload: &[u8]) -> Result<(Self, &[u8]), MetricPayloadError> {
        let (header, request) = payload
            .split_at_checked(HEADER_BYTES)
            .ok_or(MetricPayloadError::Damaged)?;
        let (&mapping, limits) = header.split_first().ok_or(MetricPayloadError::Damaged)?;
        if mapping != MAPPING_VERSION {
            return Err(MetricPayloadError::Mapping(mapping));
        }
        let mut values = limits.chunks_exact(8).map(|b| {
            let n = u64::from_le_bytes(b.try_into().map_err(|_| MetricPayloadError::Damaged)?);
            usize::try_from(n).map_err(|_| MetricPayloadError::Damaged)
        });
        let mut next = || values.next().unwrap_or(Err(MetricPayloadError::Damaged));
        Ok((
            Self {
                mapping,
                max_key_bytes: next()?,
                max_value_bytes: next()?,
                max_histogram_buckets: next()?,
                max_summary_quantiles: next()?,
                max_series_labels: next()?,
            },
            request,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_payload_keeps_its_conversion_and_refuses_another_mapping() {
        let limits = Limits {
            max_key_bytes: 7,
            ..Limits::default()
        };
        let metrics = MetricLimits {
            max_histogram_buckets: 3,
            max_series_labels: 1,
            ..MetricLimits::default()
        };
        let conversion = MetricConversion::current(&limits, &metrics);
        assert_eq!(
            conversion.max_series_labels,
            MetricLimits::MIN_SERIES_LABELS,
            "as conversion clamps it"
        );
        let payload = conversion.payload(b"otlp");
        assert_eq!(payload.len(), HEADER_BYTES + 4);
        assert_eq!(
            MetricConversion::split(&payload),
            Ok((conversion, &b"otlp"[..]))
        );
        let (applied, applied_metrics) =
            conversion.apply(&Limits::default(), &MetricLimits::default());
        assert_eq!(applied.max_key_bytes, 7);
        assert_eq!(applied.max_body_bytes, Limits::default().max_body_bytes);
        assert_eq!(applied_metrics.max_histogram_buckets, 3);
        assert_eq!(
            applied_metrics.max_points,
            MetricLimits::default().max_points
        );

        assert_eq!(conversion.outside_domain(), None);
        let wide = MetricConversion {
            max_series_labels: MetricLimits::MAX_SERIES_LABELS + 1,
            ..conversion
        };
        assert_eq!(
            wide.outside_domain(),
            Some(("max_series_labels", MetricLimits::MAX_SERIES_LABELS + 1))
        );

        let mut newer = payload.to_vec();
        *newer.first_mut().unwrap() = MAPPING_VERSION + 1;
        assert_eq!(
            MetricConversion::split(&newer),
            Err(MetricPayloadError::Mapping(MAPPING_VERSION + 1))
        );
        assert_eq!(
            MetricConversion::split(payload.get(..HEADER_BYTES - 1).unwrap()),
            Err(MetricPayloadError::Damaged)
        );
    }
}
