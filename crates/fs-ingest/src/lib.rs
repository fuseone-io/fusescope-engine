//! OTLP ingest: gRPC server, limits, write-ahead log, per-tenant buffers and
//! Parquet L0 flush with catalog registration.
//!
//! Request path (DESIGN.md section 2.2):
//!
//! 1. authenticate the bearer token against the catalog (cached);
//! 2. enforce message size and per-tenant throughput limits;
//! 3. append the raw protobuf to the WAL and fsync per request;
//! 4. convert to Arrow with `fs-schema` and add to the tenant's buffer;
//! 5. acknowledge.
//!
//! A flusher writes buffers that reach the row, byte or age threshold as one
//! sorted Parquet file to the bucket, registers it in the catalog and then
//! releases the WAL segments every tenant has flushed past.

pub mod auth;
pub mod buffer;
pub mod config;
pub mod flush;
pub mod hot;
pub mod ingester;
pub mod ratelimit;
pub mod record;
pub mod server;
pub mod walwriter;

pub use config::IngestConfig;
pub use ingester::{IngestStats, Ingester};

use thiserror::Error;

/// Crate version, exposed for diagnostics and the `--version` flag.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Errors from the ingest pipeline.
#[derive(Debug, Error)]
pub enum IngestError {
    /// Write-ahead log failure.
    #[error("wal: {0}")]
    Wal(#[from] fs_wal::WalError),
    /// Catalog failure.
    #[error("catalog: {0}")]
    Catalog(#[from] fs_catalog::CatalogError),
    /// Bucket failure.
    #[error("bucket: {0}")]
    Bucket(#[from] object_store::Error),
    /// Arrow or Parquet failure while building a file.
    #[error("encode: {0}")]
    Encode(String),
    /// A WAL record could not be decoded during replay.
    #[error("wal record: {0}")]
    Record(String),
    /// Listening socket could not be bound.
    #[error("bind {addr}: {source}")]
    Bind {
        /// Address.
        addr: String,
        /// Underlying error.
        #[source]
        source: std::io::Error,
    },
    /// gRPC transport failure.
    #[error("grpc: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// A failure injected by test configuration.
    #[error("injected failure: {0}")]
    Injected(&'static str),
    /// Hot-window scan against an ingester failed.
    #[error("hot scan {ingester}: {reason}")]
    HotScan {
        /// Ingester address.
        ingester: String,
        /// Why.
        reason: String,
    },
    /// A blocking task panicked or was cancelled.
    #[error("task: {0}")]
    Join(#[from] tokio::task::JoinError),
}

impl From<arrow::error::ArrowError> for IngestError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self::Encode(e.to_string())
    }
}

impl From<parquet::errors::ParquetError> for IngestError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Self::Encode(e.to_string())
    }
}

impl From<fs_schema::SchemaError> for IngestError {
    fn from(e: fs_schema::SchemaError) -> Self {
        Self::Encode(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn version_is_set() {
        assert_ne!(super::VERSION, "");
    }
}
