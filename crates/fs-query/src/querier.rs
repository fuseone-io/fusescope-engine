//! The querier facade: sessions, limits and execution.

use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::logical_expr::LogicalPlan;
use fs_catalog::{Catalog, TenantId};
use futures_util::StreamExt;
use object_store::ObjectStore;
use tracing::info;

use crate::QueryError;
use crate::files::FileCache;
use crate::session::{plan_read_only, tenant_session};

/// Resource limits applied to every query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryLimits {
    /// Memory the whole querier may use for execution before spilling or failing.
    pub memory_bytes: usize,
    /// Wall-clock limit per query.
    pub timeout: Duration,
    /// Maximum rows returned per query.
    pub max_rows: usize,
    /// Rows per output batch.
    pub batch_size: usize,
}

impl Default for QueryLimits {
    fn default() -> Self {
        Self {
            memory_bytes: 2 * 1024 * 1024 * 1024,
            timeout: Duration::from_secs(60),
            max_rows: 1_000_000,
            batch_size: 8192,
        }
    }
}

/// URL under which the engine bucket is registered with DataFusion.
pub const STORE_URL: &str = "fusescope://bucket/";

/// Shared query engine. Cheap to clone.
#[derive(Clone)]
pub struct Querier {
    catalog: Catalog,
    cache: Arc<FileCache>,
    runtime: Arc<RuntimeEnv>,
    store_url: ObjectStoreUrl,
    limits: QueryLimits,
}

impl Querier {
    /// Builds the engine over `store`, with a memory-limited runtime that
    /// spills to `spill_dir`.
    ///
    /// # Errors
    /// Runtime construction failure.
    pub fn new(
        catalog: Catalog,
        store: Arc<dyn ObjectStore>,
        spill_dir: Option<std::path::PathBuf>,
        limits: QueryLimits,
    ) -> Result<Self, QueryError> {
        let store_url = ObjectStoreUrl::parse(STORE_URL)?;
        let mut builder = RuntimeEnvBuilder::new().with_memory_limit(limits.memory_bytes, 1.0);
        if let Some(dir) = spill_dir {
            builder = builder.with_temp_file_path(dir);
        }
        let runtime = builder.build_arc()?;
        runtime.register_object_store(store_url.as_ref(), store);
        let cache = Arc::new(FileCache::new(catalog.clone()));
        info!(memory_bytes = limits.memory_bytes, timeout = ?limits.timeout, "querier ready");
        Ok(Self {
            catalog,
            cache,
            runtime,
            store_url,
            limits,
        })
    }

    /// The file cache, for the notification follower.
    pub fn cache(&self) -> Arc<FileCache> {
        Arc::clone(&self.cache)
    }

    /// Limits in force.
    pub fn limits(&self) -> QueryLimits {
        self.limits
    }

    /// Plans `sql` for `tenant` without executing it. Returns the plan and
    /// the tenant's byte budget.
    ///
    /// # Errors
    /// Unknown tenant, disallowed statements, planning failures.
    pub async fn plan(
        &self,
        tenant: TenantId,
        sql: &str,
    ) -> Result<(datafusion::execution::context::SessionContext, LogicalPlan), QueryError> {
        let budget = self
            .catalog
            .tenant(tenant)
            .await?
            .ok_or(QueryError::UnknownTenant)?
            .bytes_budget;
        let budget = u64::try_from(budget).unwrap_or(u64::MAX);
        let ctx = tenant_session(
            tenant,
            Arc::clone(&self.cache),
            Arc::clone(&self.runtime),
            &self.store_url,
            budget,
            self.limits.batch_size,
        )?;
        let plan = plan_read_only(&ctx, sql).await?;
        Ok((ctx, plan))
    }

    /// Executes `sql` for `tenant` as a stream of batches. The stream enforces
    /// the row limit; the caller wraps it in the timeout from [`QueryLimits`]
    /// or uses [`Querier::collect`].
    ///
    /// # Errors
    /// See [`Querier::plan`].
    pub async fn execute(
        &self,
        tenant: TenantId,
        sql: &str,
    ) -> Result<SendableRecordBatchStream, QueryError> {
        let (ctx, plan) = self.plan(tenant, sql).await?;
        let frame = ctx.execute_logical_plan(plan).await?;
        Ok(frame.execute_stream().await?)
    }

    /// Executes `sql` and collects every batch, enforcing timeout and row limit.
    ///
    /// # Errors
    /// See [`Querier::plan`], plus timeout and row limit.
    pub async fn collect(
        &self,
        tenant: TenantId,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, QueryError> {
        let limits = self.limits;
        let run = async {
            let mut stream = self.execute(tenant, sql).await?;
            let mut out = Vec::new();
            let mut rows = 0usize;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                rows += batch.num_rows();
                if rows > limits.max_rows {
                    return Err(QueryError::TooManyRows(limits.max_rows));
                }
                out.push(batch);
            }
            Ok(out)
        };
        tokio::time::timeout(limits.timeout, run)
            .await
            .map_err(|_| QueryError::Timeout(limits.timeout))?
    }
}
