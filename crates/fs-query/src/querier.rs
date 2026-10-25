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

/// Notes a query collected while planning, for example ingesters that did
/// not answer. Stored as a session extension so table providers can reach it.
#[derive(Debug, Default)]
pub struct QueryWarnings {
    items: std::sync::Mutex<Vec<String>>,
}

impl QueryWarnings {
    /// Records a warning.
    pub fn push(&self, warning: impl Into<String>) {
        if let Ok(mut items) = self.items.lock() {
            items.push(warning.into());
        }
    }

    /// Everything recorded so far.
    pub fn list(&self) -> Vec<String> {
        self.items
            .lock()
            .map(|items| items.clone())
            .unwrap_or_default()
    }
}

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
    /// Hot-window fan-out settings.
    pub hot_window: crate::hot::HotWindowConfig,
}

impl Default for QueryLimits {
    fn default() -> Self {
        Self {
            memory_bytes: 2 * 1024 * 1024 * 1024,
            timeout: Duration::from_secs(60),
            max_rows: 1_000_000,
            batch_size: 8192,
            hot_window: crate::hot::HotWindowConfig::default(),
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
    hot: Arc<crate::hot::HotWindow>,
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
        let hot = Arc::new(crate::hot::HotWindow::new(
            catalog.clone(),
            limits.hot_window,
        ));
        info!(
            memory_bytes = limits.memory_bytes,
            timeout = ?limits.timeout,
            hot_window = limits.hot_window.enabled,
            "querier ready"
        );
        Ok(Self {
            catalog,
            cache,
            hot,
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

    /// Plans `sql` for `tenant` without executing it. Returns the session, the
    /// plan and the warnings collected while planning.
    ///
    /// # Errors
    /// Unknown tenant, disallowed statements, planning failures.
    pub async fn plan(
        &self,
        tenant: TenantId,
        sql: &str,
    ) -> Result<
        (
            datafusion::execution::context::SessionContext,
            LogicalPlan,
            Arc<QueryWarnings>,
        ),
        QueryError,
    > {
        let budget = self
            .catalog
            .tenant(tenant)
            .await?
            .ok_or(QueryError::UnknownTenant)?
            .bytes_budget;
        let budget = u64::try_from(budget).unwrap_or(u64::MAX);
        let warnings = Arc::new(QueryWarnings::default());
        let ctx = tenant_session(
            tenant,
            Arc::clone(&self.cache),
            Arc::clone(&self.hot),
            Arc::clone(&warnings),
            Arc::clone(&self.runtime),
            &self.store_url,
            budget,
            self.limits.batch_size,
        )?;
        let plan = plan_read_only(&ctx, sql).await?;
        Ok((ctx, plan, warnings))
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
    ) -> Result<(SendableRecordBatchStream, Arc<QueryWarnings>), QueryError> {
        let (ctx, plan, warnings) = self.plan(tenant, sql).await?;
        let frame = ctx.execute_logical_plan(plan).await?;
        // Physical planning runs the table scans, and with them the hot-window
        // fan-out, so warnings are complete once the stream exists.
        let stream = frame.execute_stream().await?;
        Ok((stream, warnings))
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
        self.collect_with_warnings(tenant, sql)
            .await
            .map(|(batches, _)| batches)
    }

    /// Like [`Querier::collect`], also returning the warnings.
    ///
    /// # Errors
    /// See [`Querier::collect`].
    pub async fn collect_with_warnings(
        &self,
        tenant: TenantId,
        sql: &str,
    ) -> Result<(Vec<RecordBatch>, Vec<String>), QueryError> {
        let limits = self.limits;
        let run = async {
            let (mut stream, warnings) = self.execute(tenant, sql).await?;
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
            Ok((out, warnings.list()))
        };
        tokio::time::timeout(limits.timeout, run)
            .await
            .map_err(|_| QueryError::Timeout(limits.timeout))?
    }
}
