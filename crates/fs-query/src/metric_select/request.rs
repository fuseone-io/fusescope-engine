//! Bodies of the metric actions and the queries they decode into.

use fs_schema::metrics::Temporality;
use serde::{Deserialize, Serialize};

use super::matchers::RawMatcher;
use crate::QueryError;
use crate::actions::DEFAULT_LIST_LIMIT;

const MILLIS_PER_HOUR: i64 = 3_600_000;

/// Body of a [`METRICS_SELECT`](crate::actions::METRICS_SELECT) action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectLookup {
    /// The selector's matchers.
    #[serde(default)]
    pub matchers: Vec<RawMatcher>,
    /// Inclusive lower bound on sample time, Unix milliseconds.
    pub start_unix_milli: i64,
    /// Inclusive upper bound on sample time, Unix milliseconds.
    pub end_unix_milli: i64,
    /// Temporalities of the series selected; `Cumulative` and
    /// `Unspecified` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temporalities: Option<Vec<String>>,
    /// Keep only the last sample of each step bucket.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_per_step: Option<LastPerStep>,
}

/// Step buckets anchored at the first evaluation: bucket 0 holds the
/// samples at or before `first_eval_unix_milli`, bucket `i` those in
/// `(first_eval + (i - 1)·step, first_eval + i·step]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LastPerStep {
    /// The first evaluation, Unix milliseconds.
    pub first_eval_unix_milli: i64,
    /// The step, milliseconds, positive.
    pub step_ms: i64,
}

/// Body of a [`METRICS_LABEL_VALUES`](crate::actions::METRICS_LABEL_VALUES)
/// or, without `label`, [`METRICS_LABEL_NAMES`](crate::actions::METRICS_LABEL_NAMES)
/// action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LabelLookup {
    /// The label whose values to list; absent to list label names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// The series' matchers.
    #[serde(default)]
    pub matchers: Vec<RawMatcher>,
    /// Inclusive lower bound, Unix milliseconds.
    pub start_unix_milli: i64,
    /// Inclusive upper bound, Unix milliseconds.
    pub end_unix_milli: i64,
    /// Most values to answer, the first in order; [`DEFAULT_LIST_LIMIT`]
    /// when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// A select with its fields checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectQuery {
    /// The matchers, compiled when the query runs.
    pub matchers: Vec<RawMatcher>,
    /// The time bounds.
    pub window: Window,
    /// The temporalities selected.
    pub temporalities: Vec<Temporality>,
    /// The step buckets, when only the last sample of each is kept.
    pub buckets: Option<Buckets>,
}

/// A label listing with its fields checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelQuery {
    /// The label whose values to list; `None` lists label names.
    pub label: Option<String>,
    /// The matchers, compiled when the query runs.
    pub matchers: Vec<RawMatcher>,
    /// The time bounds.
    pub window: Window,
    /// Most values.
    pub limit: usize,
}

/// Inclusive time bounds, Unix milliseconds, and the start of the hour of
/// the first: series rows stand for their hour.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    /// Lower bound.
    pub start: i64,
    /// Upper bound.
    pub end: i64,
    /// Start of the hour of `start`.
    pub first_hour: i64,
}

/// Step buckets of [`LastPerStep`], checked so that no sample of the
/// window overflows its bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Buckets {
    first_eval: i64,
    step: i64,
}

impl Buckets {
    /// The bucket of a sample at `t`; `None` only past the window checked.
    pub fn of(self, t: i64) -> Option<i64> {
        if t <= self.first_eval {
            return Some(0);
        }
        let since = t.checked_sub(self.first_eval)?.checked_sub(1)?;
        since.div_euclid(self.step).checked_add(1)
    }
}

impl SelectLookup {
    /// Decodes a JSON body.
    ///
    /// # Errors
    /// Not JSON, not the shape of a select, or a field it does not know.
    pub fn parse(body: &[u8]) -> Result<Self, QueryError> {
        parse(body)
    }

    /// Checks the bounds, temporalities and buckets.
    ///
    /// # Errors
    /// A start after the end, a temporality the engine does not know, a
    /// step that is not positive, or a window whose buckets overflow.
    pub fn query(&self) -> Result<SelectQuery, QueryError> {
        let window = window_of(self.start_unix_milli, self.end_unix_milli)?;
        let temporalities = match &self.temporalities {
            None => vec![Temporality::Cumulative, Temporality::Unspecified],
            Some(names) if names.is_empty() => return Err(invalid("temporalities is empty")),
            Some(names) => names
                .iter()
                .map(|name| {
                    Temporality::parse(name)
                        .ok_or_else(|| invalid(&format!("unknown temporality {name:?}")))
                })
                .collect::<Result<_, _>>()?,
        };
        let buckets = self
            .last_per_step
            .map(|step| {
                if step.step_ms <= 0 {
                    return Err(invalid("step_ms must be positive"));
                }
                if window.end > step.first_eval_unix_milli
                    && window.end.checked_sub(step.first_eval_unix_milli).is_none()
                {
                    return Err(invalid("first_eval_unix_milli is out of range"));
                }
                Ok(Buckets {
                    first_eval: step.first_eval_unix_milli,
                    step: step.step_ms,
                })
            })
            .transpose()?;
        Ok(SelectQuery {
            matchers: self.matchers.clone(),
            window,
            temporalities,
            buckets,
        })
    }
}

impl LabelLookup {
    /// Decodes a JSON body.
    ///
    /// # Errors
    /// Not JSON, not the shape of a listing, or a field it does not know.
    pub fn parse(body: &[u8]) -> Result<Self, QueryError> {
        parse(body)
    }

    /// Checks the bounds and the limit; `label` must be present exactly
    /// when `values` asks for label values.
    ///
    /// # Errors
    /// A start after the end, a zero limit, or a label missing or
    /// unexpected.
    pub fn query(&self, values: bool) -> Result<LabelQuery, QueryError> {
        match (&self.label, values) {
            (None, true) => return Err(invalid("label is missing")),
            (Some(_), false) => return Err(invalid("label names take no label")),
            _ => {}
        }
        let limit = match self.limit {
            Some(0) => return Err(invalid("limit is zero")),
            Some(limit) => limit,
            None => DEFAULT_LIST_LIMIT,
        };
        Ok(LabelQuery {
            label: self.label.clone(),
            matchers: self.matchers.clone(),
            window: window_of(self.start_unix_milli, self.end_unix_milli)?,
            limit,
        })
    }
}

fn window_of(start: i64, end: i64) -> Result<Window, QueryError> {
    if start > end {
        return Err(invalid("start_unix_milli is after end_unix_milli"));
    }
    let first_hour = start
        .div_euclid(MILLIS_PER_HOUR)
        .checked_mul(MILLIS_PER_HOUR)
        .ok_or_else(|| invalid("start_unix_milli is out of range"))?;
    Ok(Window {
        start,
        end,
        first_hour,
    })
}

fn invalid(what: &str) -> QueryError {
    QueryError::InvalidAction(what.into())
}

fn parse<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, QueryError> {
    serde_json::from_slice(body).map_err(|e| QueryError::InvalidAction(e.to_string()))
}
