//! File catalog in Postgres with change notification.
//!
//! The catalog is the single source of truth for which Parquet files exist in
//! the bucket (DESIGN.md section 3.2, ADR 0001). Ingesters register files
//! after a flush, queriers list visible files for a time range, the compactor
//! swaps inputs for outputs atomically and purges quarantined files.
//!
//! In-memory caching belongs to the querier; this crate exposes the change
//! stream ([`Catalog::watch`]) that keeps such a cache fresh.

mod catalog;
mod types;
mod watch;

pub use catalog::Catalog;
pub use types::{
    Compaction, FileId, FileRecord, FileState, Ingester, Level, NewFile, NewTenant, PurgedFile,
    Signal, Tenant, TenantId, TimeRange,
};
pub use watch::{FileChange, Watcher};

use thiserror::Error;

/// Crate version, exposed for diagnostics and the `--version` flag.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Errors from the catalog.
#[derive(Debug, Error)]
pub enum CatalogError {
    /// Database failure.
    #[error("database: {0}")]
    Database(#[from] sqlx::Error),
    /// Migration failure.
    #[error("migration: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    /// A compaction swap found inputs that are not visible any more. Another
    /// compactor won; the caller discards its outputs.
    #[error("compaction conflict: {expected} inputs expected, {found} visible")]
    CompactionConflict {
        /// Inputs the caller passed.
        expected: usize,
        /// Inputs still visible at swap time.
        found: usize,
    },
    /// A value read from the database is outside the domain, for example an
    /// unknown signal code.
    #[error("invalid value in column {column}: {value}")]
    InvalidValue {
        /// Column name.
        column: &'static str,
        /// Offending value.
        value: i64,
    },
}

#[cfg(test)]
mod tests {
    #[test]
    fn version_is_set() {
        assert_ne!(super::VERSION, "");
    }
}
