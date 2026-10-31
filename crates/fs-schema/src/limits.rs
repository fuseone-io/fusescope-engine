//! Ingest limits applied during conversion and the counters of what they dropped.
//!
//! Limits protect the engine from cardinality and size attacks. A record that
//! exceeds them is kept with the excess removed; only structurally invalid
//! records (bad id lengths) are rejected. The counters let the ingester expose
//! per-tenant drop metrics.

/// Size limits applied to every record during conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Maximum attributes kept per attribute set (span, resource, scope, event, link).
    pub max_attributes: usize,
    /// Maximum attribute key length in bytes. Longer keys are dropped.
    pub max_key_bytes: usize,
    /// Maximum attribute value length in bytes. Longer values are truncated on a
    /// UTF-8 boundary.
    pub max_value_bytes: usize,
    /// Maximum events per span. Extra events are dropped.
    pub max_events: usize,
    /// Maximum links per span. Extra links are dropped.
    pub max_links: usize,
    /// Maximum log body length in bytes, after rendering to text. Longer bodies
    /// are truncated on a UTF-8 boundary.
    pub max_body_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_attributes: 256,
            max_key_bytes: 256,
            max_value_bytes: 8 * 1024,
            max_events: 128,
            max_links: 128,
            max_body_bytes: 64 * 1024,
        }
    }
}

/// Counters of what the limits removed. Monotonic for the life of a builder.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DropStats {
    /// Attributes dropped for exceeding `max_attributes`, a bad key, or an empty value.
    pub attributes_dropped: u64,
    /// Attribute values truncated to `max_value_bytes`.
    pub values_truncated: u64,
    /// Events dropped for exceeding `max_events`.
    pub events_dropped: u64,
    /// Links dropped for exceeding `max_links` or carrying ids of the wrong length.
    pub links_dropped: u64,
    /// Spans rejected for carrying a trace or span id of the wrong length.
    pub spans_rejected: u64,
    /// Log records rejected for carrying neither an event nor an observed time.
    pub logs_rejected: u64,
    /// Log bodies truncated to `max_body_bytes`.
    pub bodies_truncated: u64,
}

impl DropStats {
    /// True when nothing was dropped.
    pub fn is_clean(&self) -> bool {
        *self == Self::default()
    }
}

/// Truncates `value` to at most `max_bytes` without splitting a UTF-8 sequence.
pub(crate) fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value.get(..end).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncation_respects_char_boundaries() {
        // "é" is two bytes; cutting at 3 must not split it.
        assert_eq!(truncate_utf8("aéb", 3), "aé");
        assert_eq!(truncate_utf8("aéb", 2), "a");
        assert_eq!(truncate_utf8("abc", 10), "abc");
        assert_eq!(truncate_utf8("é", 1), "");
    }

    #[test]
    fn default_stats_are_clean() {
        assert!(DropStats::default().is_clean());
        let s = DropStats {
            spans_rejected: 1,
            ..DropStats::default()
        };
        assert!(!s.is_clean());
    }
}
