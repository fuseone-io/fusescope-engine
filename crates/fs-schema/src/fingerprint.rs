//! Resource fingerprint: a stable 64-bit hash of a resource's attributes.
//!
//! The value is engine internal. It groups rows that share a resource so the
//! query layer can filter by resource cheaply. It is never compared with
//! fingerprints produced by other systems.

use twox_hash::XxHash3_64;

/// Hashes `(key, value)` pairs in key order, each string length-prefixed.
///
/// Length prefixes make the encoding unambiguous whatever bytes keys and
/// values contain. The input is sorted by key so attribute order on the wire
/// does not change the result. Keys are expected to be unique; if a key
/// repeats, every occurrence takes part in the hash in input order.
pub fn resource_fingerprint<'a, I>(pairs: I) -> u64
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let mut sorted: Vec<(&str, &str)> = pairs.into_iter().collect();
    sorted.sort_unstable();
    let mut buffer = Vec::with_capacity(sorted.iter().map(|(k, v)| k.len() + v.len() + 16).sum());
    for (key, value) in sorted {
        write_str(&mut buffer, key);
        write_str(&mut buffer, value);
    }
    XxHash3_64::oneshot(&buffer)
}

fn write_str(buffer: &mut Vec<u8>, s: &str) {
    buffer.extend_from_slice(&(s.len() as u64).to_le_bytes());
    buffer.extend_from_slice(s.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_independent() {
        let a = resource_fingerprint([("service.name", "api"), ("host.name", "h1")]);
        let b = resource_fingerprint([("host.name", "h1"), ("service.name", "api")]);
        assert_eq!(a, b);
    }

    #[test]
    fn separators_prevent_collisions() {
        let shifted_key = resource_fingerprint([("ab", "c")]);
        let shifted_value = resource_fingerprint([("a", "bc")]);
        assert_ne!(shifted_key, shifted_value);
        let two_pairs = resource_fingerprint([("a", "b"), ("c", "d")]);
        let embedded_separators = resource_fingerprint([("a", "b\u{1}c\u{0}d")]);
        assert_ne!(two_pairs, embedded_separators);
        let one_value = resource_fingerprint([("a", "bc")]);
        let split_with_empty_key = resource_fingerprint([("a", "b"), ("", "c")]);
        assert_ne!(one_value, split_with_empty_key);
    }

    #[test]
    fn empty_is_stable() {
        assert_eq!(resource_fingerprint([]), resource_fingerprint([]));
    }
}
