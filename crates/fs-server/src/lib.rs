//! FuseScope Engine server: configuration, bootstrap and process roles.
//!
//! The binary in `main.rs` is a thin wrapper over [`server::run`] and
//! [`server::check`] so integration tests can drive the same code in-process.

pub mod bucket;
pub mod config;
pub mod role;
pub mod roles;
pub mod server;

use std::path::PathBuf;

use thiserror::Error;

/// Crate version, exposed for diagnostics and the `--version` flag.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Errors from loading configuration or running the server.
#[derive(Debug, Error)]
pub enum ServerError {
    /// Configuration file could not be read.
    #[error("read config {path}: {source}")]
    ReadConfig {
        /// File path.
        path: PathBuf,
        /// Underlying error.
        #[source]
        source: std::io::Error,
    },
    /// Configuration file does not parse.
    #[error("parse config: {0}")]
    ParseConfig(#[from] toml::de::Error),
    /// Configuration is inconsistent.
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    /// Bucket client could not be built or reached.
    #[error("bucket: {0}")]
    Bucket(#[from] object_store::Error),
    /// Catalog failure.
    #[error("catalog: {0}")]
    Catalog(#[from] fs_catalog::CatalogError),
    /// Write-ahead log failure.
    #[error("wal: {0}")]
    Wal(#[from] fs_wal::WalError),
    /// Ingest failure.
    #[error("ingest: {0}")]
    Ingest(#[from] fs_ingest::IngestError),
    /// Query engine failure.
    #[error("query: {0}")]
    Query(#[from] fs_query::QueryError),
    /// A role task panicked or was cancelled.
    #[error("role {role} stopped unexpectedly: {reason}")]
    RoleStopped {
        /// Role name.
        role: &'static str,
        /// Why.
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    #[test]
    fn version_is_set() {
        assert_ne!(super::VERSION, "");
    }
}
