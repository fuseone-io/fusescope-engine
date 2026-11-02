//! The identity of a metric series: its attributes at each level, the
//! labels they fuse into and the fingerprint (`MAPPING.md`, "Metrics").

use opentelemetry_proto::tonic::common::v1::KeyValue;
use twox_hash::XxHash3_64;

use super::canonical::{Capped, render, render_scratch, rendered_len};
use super::{MetricType, Temporality};
use crate::limits::{DropStats, Limits};

/// Domain of fingerprint version 1.
const FINGERPRINT_DOMAIN: &[u8] = b"fs.metric.v1";

/// Labels the engine generates, which no attribute overrides.
pub const NAME: &str = "__name__";
/// The series' temporality.
pub const TEMPORALITY: &str = "__temporality__";
/// The instrumentation scope's name.
pub const SCOPE_NAME: &str = "__scope.name__";
/// The instrumentation scope's version.
pub const SCOPE_VERSION: &str = "__scope.version__";
/// The instrumentation scope's schema URL.
pub const SCOPE_SCHEMA_URL: &str = "__scope.schema_url__";
/// A histogram bucket's upper bound.
pub const LE: &str = "le";
/// A summary quantile.
pub const QUANTILE: &str = "quantile";

/// Generated labels every series has.
pub(crate) const ALWAYS_GENERATED: usize = 5;

/// The attributes of one level, resource, scope or point: valid keys,
/// canonical values, a repeated key keeping its last value, in key byte
/// order, and their hash.
#[derive(Debug, Default)]
pub(crate) struct Level {
    pub(crate) pairs: Vec<(String, String)>,
    pub(crate) hash: u64,
}

/// Bytes of one pair of a level.
const PAIR: usize = std::mem::size_of::<(String, String)>();

/// What building a level holds, bounded before it is built.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LevelBound {
    /// At most while it is built.
    pub(crate) building: usize,
    /// At most once built.
    pub(crate) held: usize,
    /// At most its pairs.
    pub(crate) pairs: usize,
}

/// Why a level cannot form series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TooManyLabels;

impl Level {
    /// The level `kind` of `attributes`, hashed with `extra`, the strings
    /// that are part of the level besides its attributes. More than `cap`
    /// attributes, before repeats are resolved, refuse it.
    pub(crate) fn new(
        kind: &str,
        extra: &[&str],
        attributes: &[KeyValue],
        limits: &Limits,
        cap: usize,
        stats: &mut DropStats,
    ) -> Result<Self, TooManyLabels> {
        if attributes.len() > cap {
            return Err(TooManyLabels);
        }
        let mut pairs: Vec<(String, String)> = Vec::with_capacity(attributes.len());
        for kv in attributes {
            if kv.key.is_empty() || kv.key.len() > limits.max_key_bytes {
                stats.attributes_dropped += 1;
                continue;
            }
            let Some(value) = kv.value.as_ref().and_then(|v| v.value.as_ref()) else {
                stats.attributes_dropped += 1;
                continue;
            };
            let mut text = String::with_capacity(rendered_len(value, limits.max_value_bytes));
            let mut out = Capped::new(&mut text, limits.max_value_bytes);
            if render(value, &mut out).is_none() {
                stats.attributes_dropped += 1;
                continue;
            }
            if out.truncated {
                stats.values_truncated += 1;
            }
            pairs.push((kv.key.clone(), text));
        }
        // Stable: of a run of one key the last is the last received.
        pairs.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        let mut kept: Vec<(String, String)> = Vec::with_capacity(pairs.len());
        for pair in pairs {
            match kept.last_mut() {
                Some(last) if last.0 == pair.0 => *last = pair,
                _ => kept.push(pair),
            }
        }
        let mut hash = Hasher::new();
        hash.str(kind);
        for s in extra {
            hash.str(s);
        }
        hash.u64(kept.len() as u64);
        for (k, v) in &kept {
            hash.str(k);
            hash.str(v);
        }
        Ok(Self {
            pairs: kept,
            hash: hash.finish(),
        })
    }
}

impl Level {
    /// Found without allocating, at least the bytes [`Level::new`] holds
    /// for `attributes` at once, and the bytes the level holds then: its
    /// pairs before and after their repeats are resolved, the sort's
    /// scratch, the texts and the rendering of one value.
    pub(crate) fn bound(attributes: &[KeyValue], limits: &Limits, cap: usize) -> LevelBound {
        if attributes.len() > cap {
            return LevelBound::default();
        }
        let mut texts = 0usize;
        let mut render = 0usize;
        for kv in attributes {
            let Some(value) = kv.value.as_ref().and_then(|v| v.value.as_ref()) else {
                continue;
            };
            if kv.key.is_empty() || kv.key.len() > limits.max_key_bytes {
                continue;
            }
            let text = rendered_len(value, limits.max_value_bytes);
            texts = texts.saturating_add(kv.key.len()).saturating_add(text);
            render = render.max(render_scratch(value));
        }
        let pairs = attributes.len().saturating_mul(PAIR);
        let held = pairs.saturating_add(texts);
        LevelBound {
            building: held.saturating_add(pairs.saturating_mul(2).max(render)),
            held,
            pairs: attributes.len(),
        }
    }

    /// Bytes it holds: its pairs as allocated and their texts.
    pub(crate) fn held(&self) -> usize {
        self.pairs.capacity() * std::mem::size_of::<(String, String)>()
            + self
                .pairs
                .iter()
                .map(|(k, v)| k.capacity() + v.capacity())
                .sum::<usize>()
    }
}

/// What makes one series of a point: everything its fingerprint hashes
/// besides the levels.
pub(crate) struct SeriesKey<'a> {
    pub(crate) name: &'a str,
    pub(crate) kind: MetricType,
    pub(crate) unit: &'a str,
    pub(crate) temporality: Temporality,
    pub(crate) monotonic: bool,
    /// `le` or `quantile` and its value.
    pub(crate) generated: Option<(&'static str, &'a str)>,
}

/// The fingerprint, version 1: the domain, the three level hashes, then
/// the series' name, type, unit, temporality, monotonicity and generated
/// label, each string length-prefixed, counts and lengths little-endian.
pub(crate) fn fingerprint(levels: [&Level; 3], key: &SeriesKey<'_>) -> u64 {
    let mut hash = Hasher::new();
    hash.bytes(FINGERPRINT_DOMAIN);
    for level in levels {
        hash.u64(level.hash);
    }
    hash.str(key.name);
    hash.str(key.kind.as_str());
    hash.str(key.unit);
    hash.str(key.temporality.as_str());
    hash.u64(u64::from(key.monotonic));
    match key.generated {
        Some((label, value)) => {
            hash.u64(1);
            hash.str(label);
            hash.str(value);
        }
        None => hash.u64(0),
    }
    hash.finish()
}

/// The labels of a series of the point whose levels are `levels`: the
/// generated ones, then the resource's, the scope's and the point's, the
/// first of a key winning, in key byte order. `None` past `cap` labels.
pub(crate) fn labels<'a>(
    levels: [&'a Level; 3],
    generated: &[(&'static str, &'a str)],
    cap: usize,
    out: &mut Vec<(&'a str, &'a str)>,
) -> Option<()> {
    out.clear();
    out.extend(generated.iter().copied());
    for level in levels {
        for (k, v) in &level.pairs {
            out.push((k.as_str(), v.as_str()));
        }
    }
    // Stable, so of one key the first pushed, the most preferred, leads.
    out.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    out.dedup_by(|later, first| later.0 == first.0);
    (out.len() <= cap).then_some(())
}

/// xxh3-64 over a length-prefixed encoding, streamed.
struct Hasher(XxHash3_64);

impl Hasher {
    fn new() -> Self {
        Self(XxHash3_64::new())
    }

    fn u64(&mut self, n: u64) {
        std::hash::Hasher::write(&mut self.0, &n.to_le_bytes());
    }

    fn bytes(&mut self, b: &[u8]) {
        self.u64(b.len() as u64);
        std::hash::Hasher::write(&mut self.0, b);
    }

    fn str(&mut self, s: &str) {
        self.bytes(s.as_bytes());
    }

    fn finish(&self) -> u64 {
        std::hash::Hasher::finish(&self.0)
    }
}

/// Bytes a [`Hasher`] holds: its buffer and secret.
pub(crate) const HASHER_BYTES: usize = 1024;

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::AnyValue;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;

    use super::*;

    fn kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(value.into())),
            }),
            ..KeyValue::default()
        }
    }

    fn level(kind: &str, attributes: &[KeyValue]) -> Level {
        let mut stats = DropStats::default();
        Level::new(kind, &[], attributes, &Limits::default(), 320, &mut stats).unwrap()
    }

    fn key(name: &str) -> SeriesKey<'_> {
        SeriesKey {
            name,
            kind: MetricType::Gauge,
            unit: "",
            temporality: Temporality::Unspecified,
            monotonic: false,
            generated: None,
        }
    }

    #[test]
    fn a_repeated_key_keeps_its_last_value_and_order_does_not_count() {
        let a = level("point", &[kv("b", "1"), kv("a", "x"), kv("b", "2")]);
        assert_eq!(
            a.pairs,
            vec![("a".into(), "x".into()), ("b".into(), "2".into())]
        );
        let b = level("point", &[kv("a", "x"), kv("b", "2")]);
        assert_eq!(a.hash, b.hash);
    }

    #[test]
    fn generated_labels_then_resource_then_scope_then_point_win() {
        let resource = level("resource", &[kv("zone", "resource"), kv("__name__", "r")]);
        let scope = level("scope", &[kv("zone", "scope"), kv("region", "scope")]);
        let point = level(
            "point",
            &[kv("region", "point"), kv("host", "point"), kv("le", "9")],
        );
        let mut out = Vec::new();
        labels(
            [&resource, &scope, &point],
            &[("__name__", "m.bucket"), ("le", "1")],
            320,
            &mut out,
        )
        .unwrap();
        assert_eq!(
            out,
            vec![
                ("__name__", "m.bucket"),
                ("host", "point"),
                ("le", "1"),
                ("region", "scope"),
                ("zone", "resource"),
            ]
        );
        assert!(labels([&resource, &scope, &point], &[], 3, &mut out).is_none());
    }

    #[test]
    fn a_value_shadowed_by_another_level_keeps_its_own_fingerprint() {
        let resource = level("resource", &[kv("host.name", "gateway")]);
        let scope = level("scope", &[]);
        let a = level("point", &[kv("host.name", "node-a")]);
        let b = level("point", &[kv("host.name", "node-b")]);
        let (mut la, mut lb) = (Vec::new(), Vec::new());
        labels([&resource, &scope, &a], &[], 320, &mut la).unwrap();
        labels([&resource, &scope, &b], &[], 320, &mut lb).unwrap();
        assert_eq!(la, lb, "the same labels");
        assert_ne!(
            fingerprint([&resource, &scope, &a], &key("m")),
            fingerprint([&resource, &scope, &b], &key("m"))
        );
        // The same attribute at another level is another series.
        let moved = level("resource", &[]);
        let point = level("point", &[kv("host.name", "gateway")]);
        assert_ne!(
            fingerprint([&resource, &scope, &level("point", &[])], &key("m")),
            fingerprint([&moved, &scope, &point], &key("m"))
        );
    }

    #[test]
    fn type_unit_temporality_and_monotonicity_are_part_of_the_identity() {
        let none = level("x", &[]);
        let base = fingerprint([&none, &none, &none], &key("m"));
        let variants = [
            SeriesKey {
                kind: MetricType::Sum,
                ..key("m")
            },
            SeriesKey {
                unit: "ms",
                ..key("m")
            },
            SeriesKey {
                temporality: Temporality::Delta,
                ..key("m")
            },
            SeriesKey {
                monotonic: true,
                ..key("m")
            },
            SeriesKey {
                generated: Some(("le", "1")),
                ..key("m")
            },
        ];
        for variant in &variants {
            assert_ne!(fingerprint([&none, &none, &none], variant), base);
        }
    }

    #[test]
    fn fingerprint_version_one_is_pinned() {
        let resource = level("resource", &[kv("service.name", "api")]);
        let scope = level("scope", &[]);
        let point = level("point", &[kv("route", "/")]);
        assert_eq!(
            fingerprint([&resource, &scope, &point], &key("http.requests")),
            PINNED_FINGERPRINT
        );
    }

    const PINNED_FINGERPRINT: u64 = 15_322_634_958_810_982_456;
}
