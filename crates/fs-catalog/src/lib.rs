//! File catalog in Postgres with in-memory cache, change notification and Iceberg export
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
