//! Sidecar indexes: xor and bloom filters, ngram bloom, roaring postings, `trace_id` index
//!
//! See DESIGN.md at the repository root for the role of this crate.

/// Crate version, exposed for diagnostics and the `--version` flag.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    #[test]
    fn version_is_set() {
        assert_ne!(super::VERSION, "");
    }
}
