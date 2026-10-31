//! Versioned Arrow schemas for traces, logs and metrics, and OTLP to Arrow conversion.
//!
//! Every mapping rule in this crate is specified from the OpenTelemetry protocol,
//! the semantic conventions and observed data. See `MAPPING.md` next to this
//! crate and ADR 0008.

pub mod attribute_keys;
pub mod attrs;
pub mod bucket;
mod columns;
pub mod fingerprint;
pub mod limits;
pub mod parquet;
pub mod service_graph;
pub mod spans;
pub mod trace_summary;

use thiserror::Error;

/// Crate version, exposed for diagnostics and the `--version` flag.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Errors produced while building Arrow batches.
#[derive(Debug, Error)]
pub enum SchemaError {
    /// An Arrow builder refused data, for example a dictionary key overflow.
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// A nested builder did not have the layout the schema declares. Programming error.
    #[error("builder layout mismatch at {0}")]
    Layout(&'static str),
    /// The Parquet writer refused its settings.
    #[error("parquet: {0}")]
    Parquet(#[from] ::parquet::errors::ParquetError),
    /// A row group size outside `1..=MAX_ROW_GROUP_ROWS`.
    #[error("row group of {0} rows is outside the supported range")]
    RowGroupRows(usize),
}

#[cfg(test)]
mod tests {
    #[test]
    fn version_is_set() {
        assert_ne!(super::VERSION, "");
    }
}
