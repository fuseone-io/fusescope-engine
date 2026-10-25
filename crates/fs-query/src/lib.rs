//! Query engine: DataFusion over the catalog's Parquet files, served through
//! Arrow Flight SQL.
//!
//! Every session is scoped to one tenant and sees only that tenant's tables.
//! Pruning happens before any object is opened: the catalog narrows the file
//! list by time range and service, then Parquet statistics, bloom filters and
//! page indexes narrow further inside DataFusion. A per-tenant byte budget
//! refuses scans that would read too much.

pub mod files;
pub mod flight;
pub mod hot;
pub mod prune;
pub mod querier;
pub mod session;
pub mod spans_table;

pub use querier::{Querier, QueryLimits, QueryStats, STORE_URL};

use thiserror::Error;

/// Crate version, exposed for diagnostics and the `--version` flag.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Errors from planning or executing a query.
#[derive(Debug, Error)]
pub enum QueryError {
    /// DataFusion planning or execution failure.
    #[error("{0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    /// Catalog failure.
    #[error("catalog: {0}")]
    Catalog(#[from] fs_catalog::CatalogError),
    /// The statement is not a read-only query.
    #[error("only SELECT statements are allowed: {0}")]
    NotAllowed(String),
    /// The scan would exceed the tenant's byte budget.
    #[error("query would scan {bytes} bytes, over the budget of {budget}")]
    OverBudget {
        /// Bytes the pruned file list would read.
        bytes: u64,
        /// Tenant budget.
        budget: u64,
    },
    /// Execution exceeded the time limit.
    #[error("query exceeded {0:?}")]
    Timeout(std::time::Duration),
    /// The result exceeded the row limit.
    #[error("result exceeded {0} rows")]
    TooManyRows(usize),
    /// Unknown tenant.
    #[error("unknown tenant")]
    UnknownTenant,
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
}

#[cfg(test)]
mod tests {
    #[test]
    fn version_is_set() {
        assert_ne!(super::VERSION, "");
    }
}
