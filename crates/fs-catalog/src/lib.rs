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
mod compaction;
mod lease;
mod orphans;
mod token;
mod types;
mod watch;

pub use catalog::{ACQUIRE_TIMEOUT, Catalog};
pub use compaction::CompactionInputs;
pub use lease::{COMPACTOR_LEASE, Lease, LeaseFence};
pub use token::{
    TOKEN_PREFIX, TenantToken, TokenScope, generate_token, hash_token, looks_like_token,
};
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
    /// The lease a write was fenced by is no longer this holder's at this
    /// generation: nothing was written.
    #[error("lease {name} is no longer held by {holder} at generation {generation}")]
    LeaseLost {
        /// Lease name.
        name: String,
        /// Holder that lost it.
        holder: String,
        /// Generation it quoted.
        generation: i64,
    },
    /// A compaction swap found inputs that are not visible any more. Another
    /// compactor won; the caller discards its outputs.
    #[error("compaction conflict: {expected} inputs expected, {found} visible")]
    CompactionConflict {
        /// Inputs the caller passed.
        expected: usize,
        /// Inputs still visible at swap time.
        found: usize,
    },
    /// A file was registered again under the same id with different content.
    /// The id is derived from what a flush contains, so this means a retry
    /// grouped rows differently from the original; the caller must not proceed.
    #[error("file {id} already registered with a different {field}")]
    FileMismatch {
        /// File id.
        id: uuid::Uuid,
        /// First differing field.
        field: &'static str,
    },
    /// The operating system could not provide randomness for a token.
    #[error("randomness unavailable: {0}")]
    Randomness(#[from] getrandom::Error),
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
