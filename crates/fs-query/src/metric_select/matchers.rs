//! Label matchers of a metric selector, with Prometheus semantics: an
//! absent label is `""` for every type, and a regex matches the whole
//! value, anchored as `\A(?:…)\z`.

use datafusion::execution::memory_pool::MemoryReservation;
use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};

use crate::QueryError;

/// Bytes the compiled program of one regex may take, and its lazy DFA
/// again.
pub const REGEX_SIZE_LIMIT: usize = 1 << 20;

/// How a matcher compares a label's value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MatchType {
    /// The value equals.
    #[serde(rename = "=")]
    Equal,
    /// The value differs.
    #[serde(rename = "!=")]
    NotEqual,
    /// The value matches the regex.
    #[serde(rename = "=~")]
    Regex,
    /// The value does not match the regex.
    #[serde(rename = "!~")]
    NotRegex,
}

/// A matcher as a request names it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawMatcher {
    /// The label; `__name__` is the metric's name.
    pub name: String,
    /// The comparison.
    #[serde(rename = "type")]
    pub kind: MatchType,
    /// The value, or the regex.
    pub value: String,
}

/// Bounds on the matchers of one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MatcherLimits {
    /// Most matchers.
    pub max_matchers: usize,
    /// Most bytes of regex patterns, summed.
    pub max_regex_bytes: usize,
}

/// A matcher ready to evaluate.
#[derive(Debug)]
pub struct Matcher {
    /// The label.
    pub name: Box<str>,
    kind: MatchType,
    value: Box<str>,
    regex: Option<Regex>,
}

impl Matcher {
    /// Whether a label of `value`, `""` when absent, passes.
    pub fn matches(&self, value: &str) -> bool {
        match (&self.regex, self.kind) {
            (Some(regex), MatchType::Regex) => regex.is_match(value),
            (Some(regex), _) => !regex.is_match(value),
            (None, MatchType::Equal) => *self.value == *value,
            (None, _) => *self.value != *value,
        }
    }

    /// Whether only a series carrying the label can pass: a positive
    /// matcher that refuses `""`, the only kind that may choose rows by
    /// postings.
    pub fn requires_label(&self) -> bool {
        matches!(self.kind, MatchType::Equal | MatchType::Regex) && !self.matches("")
    }

    /// The one value an equality matcher requires, if it is one.
    pub fn equal_to(&self) -> Option<&str> {
        (self.kind == MatchType::Equal).then_some(&*self.value)
    }
}

/// Compiles `raw` within `limits`, each regex's compile limits reserved
/// in `reservation` before it is compiled.
///
/// # Errors
/// [`QueryError::InvalidAction`] for too many matchers, too many regex
/// bytes, or a regex that does not compile within its limits; a
/// reservation the pool cannot grant.
pub fn compile(
    raw: &[RawMatcher],
    limits: MatcherLimits,
    reservation: &mut MemoryReservation,
) -> Result<Vec<Matcher>, QueryError> {
    if raw.len() > limits.max_matchers {
        return Err(QueryError::InvalidAction(format!(
            "{} matchers, more than {}",
            raw.len(),
            limits.max_matchers
        )));
    }
    let regex_bytes: usize = raw
        .iter()
        .filter(|m| matches!(m.kind, MatchType::Regex | MatchType::NotRegex))
        .map(|m| m.value.len())
        .sum();
    if regex_bytes > limits.max_regex_bytes {
        return Err(QueryError::InvalidAction(format!(
            "{regex_bytes} bytes of regex, more than {}",
            limits.max_regex_bytes
        )));
    }
    raw.iter()
        .map(|m| {
            let regex = match m.kind {
                MatchType::Regex | MatchType::NotRegex => {
                    reservation.try_grow(2 * REGEX_SIZE_LIMIT)?;
                    let anchored = format!(r"\A(?:{})\z", m.value);
                    let regex = RegexBuilder::new(&anchored)
                        .size_limit(REGEX_SIZE_LIMIT)
                        .dfa_size_limit(REGEX_SIZE_LIMIT)
                        .build()
                        .map_err(|e| {
                            QueryError::InvalidAction(format!("regex of {}: {e}", m.name))
                        })?;
                    Some(regex)
                }
                MatchType::Equal | MatchType::NotEqual => None,
            };
            Ok(Matcher {
                name: m.name.as_str().into(),
                kind: m.kind,
                value: m.value.as_str().into(),
                regex,
            })
        })
        .collect()
}

/// Whether `labels`, sorted by key, pass every matcher.
pub fn all_match(matchers: &[Matcher], labels: &[(Box<str>, Box<str>)]) -> bool {
    matchers
        .iter()
        .all(|m| m.matches(value_of(labels, &m.name)))
}

/// The value of `key` in `labels`, sorted by key; `""` when absent.
pub fn value_of<'a>(labels: &'a [(Box<str>, Box<str>)], key: &str) -> &'a str {
    labels
        .binary_search_by(|(k, _)| k.as_bytes().cmp(key.as_bytes()))
        .ok()
        .and_then(|at| labels.get(at))
        .map_or("", |(_, v)| v)
}
