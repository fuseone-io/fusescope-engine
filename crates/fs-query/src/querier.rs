//! The querier facade: sessions, limits and execution.

use std::collections::HashMap;
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

/// Counters exposed for metrics.
#[derive(Debug, Default)]
pub struct QueryStats {
    /// Statements planned.
    pub queries: std::sync::atomic::AtomicU64,
    /// Times a statement went through the planner: once per statement
    /// when its ticket is executed where it was prepared, twice when the
    /// prepared plan was gone and the ticket's SQL was planned again.
    pub plannings: std::sync::atomic::AtomicU64,
    /// Tickets executed from their prepared plan.
    pub prepared_hits: std::sync::atomic::AtomicU64,
    /// Tickets whose prepared plan was gone: expired, evicted, consumed, or
    /// prepared by another querier; their SQL was planned again.
    pub prepared_misses: std::sync::atomic::AtomicU64,
    /// Statements that failed to plan or were refused.
    pub errors: std::sync::atomic::AtomicU64,
    /// Rows returned by `collect`.
    pub rows: std::sync::atomic::AtomicU64,
    /// Hot-window fan-outs performed.
    pub hot_fanouts: std::sync::atomic::AtomicU64,
    /// Ingesters that did not answer a fan-out.
    pub hot_unavailable: std::sync::atomic::AtomicU64,
    /// `trace_id` sidecars fetched from the bucket.
    pub trace_index_fetches: std::sync::atomic::AtomicU64,
    /// Sidecars missing, unreadable or unreachable; their files were scanned.
    pub trace_index_misses: std::sync::atomic::AtomicU64,
    /// Files a trace lookup skipped because their sidecar lacked the trace.
    pub trace_index_pruned: std::sync::atomic::AtomicU64,
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
    /// Decoded `trace_id` sidecars kept in memory for trace lookups. Zero
    /// disables the sidecars.
    pub trace_index_cache_bytes: usize,
    /// Most plans kept between `GetFlightInfo` and `DoGet`; beyond it the
    /// oldest is dropped and its ticket plans again.
    pub prepared_plans: usize,
    /// How long a prepared plan waits for its `DoGet` before it is dropped.
    pub prepared_ttl: Duration,
}

impl Default for QueryLimits {
    fn default() -> Self {
        Self {
            memory_bytes: 2 * 1024 * 1024 * 1024,
            timeout: Duration::from_secs(60),
            max_rows: 1_000_000,
            batch_size: 8192,
            hot_window: crate::hot::HotWindowConfig::default(),
            trace_index_cache_bytes: 256 * 1024 * 1024,
            prepared_plans: 1024,
            prepared_ttl: Duration::from_secs(60),
        }
    }
}

/// A statement planned on `GetFlightInfo`, waiting for its `DoGet`.
struct Prepared {
    tenant: TenantId,
    ctx: datafusion::execution::context::SessionContext,
    plan: LogicalPlan,
    warnings: Arc<QueryWarnings>,
    at: std::time::Instant,
}

/// Opaque handle of a prepared plan, carried by the ticket.
pub type PreparedHandle = uuid::Uuid;

/// URL under which the engine bucket is registered with DataFusion.
pub const STORE_URL: &str = "fusescope://bucket/";

/// Shared query engine. Cheap to clone.
#[derive(Clone)]
pub struct Querier {
    catalog: Catalog,
    cache: Arc<FileCache>,
    hot: Arc<crate::hot::HotWindow>,
    trace_index: Arc<crate::trace_index::TraceIndexCache>,
    runtime: Arc<RuntimeEnv>,
    store_url: ObjectStoreUrl,
    limits: QueryLimits,
    stats: Arc<QueryStats>,
    prepared: Arc<std::sync::Mutex<HashMap<PreparedHandle, Prepared>>>,
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
        runtime.register_object_store(store_url.as_ref(), Arc::clone(&store));
        let cache = Arc::new(FileCache::new(catalog.clone()));
        let stats = Arc::new(QueryStats::default());
        let trace_index = Arc::new(
            crate::trace_index::TraceIndexCache::new(store, limits.trace_index_cache_bytes)
                .with_stats(Arc::clone(&stats)),
        );
        let hot = Arc::new(
            crate::hot::HotWindow::new(catalog.clone(), limits.hot_window)
                .with_stats(Arc::clone(&stats)),
        );
        info!(
            memory_bytes = limits.memory_bytes,
            timeout = ?limits.timeout,
            hot_window = limits.hot_window.enabled,
            trace_index_cache_bytes = limits.trace_index_cache_bytes,
            "querier ready"
        );
        Ok(Self {
            catalog,
            cache,
            hot,
            trace_index,
            runtime,
            store_url,
            limits,
            stats,
            prepared: Arc::default(),
        })
    }

    /// The file cache, for the notification follower.
    pub fn cache(&self) -> Arc<FileCache> {
        Arc::clone(&self.cache)
    }

    /// The sidecar cache trace lookups consult.
    pub fn trace_index(&self) -> Arc<crate::trace_index::TraceIndexCache> {
        Arc::clone(&self.trace_index)
    }

    /// Limits in force.
    pub fn limits(&self) -> QueryLimits {
        self.limits
    }

    /// Shared handle to the counters, for a metrics exporter.
    pub fn stats(&self) -> Arc<QueryStats> {
        Arc::clone(&self.stats)
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
        self.stats
            .queries
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let warnings = Arc::new(QueryWarnings::default());
        let ctx = tenant_session(
            tenant,
            Arc::clone(&self.cache),
            Arc::clone(&self.hot),
            Arc::clone(&self.trace_index),
            Arc::clone(&warnings),
            Arc::clone(&self.runtime),
            &self.store_url,
            budget,
            self.limits.batch_size,
        )?;
        self.stats
            .plannings
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let plan = match plan_read_only(&ctx, sql).await {
            Ok(plan) => plan,
            Err(e) => {
                self.stats
                    .errors
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(e);
            }
        };
        Ok((ctx, plan, warnings))
    }

    /// Plans `sql` for `tenant` and keeps the plan for one execution under
    /// the returned handle, for `prepared_ttl` at most. Returns the handle
    /// and the result schema. When `prepared_plans` plans wait already, the
    /// oldest is dropped: its ticket plans again when executed.
    ///
    /// # Errors
    /// See [`Querier::plan`].
    pub async fn prepare(
        &self,
        tenant: TenantId,
        sql: &str,
    ) -> Result<(PreparedHandle, arrow::datatypes::SchemaRef), QueryError> {
        let (ctx, plan, warnings) = self.plan(tenant, sql).await?;
        let schema: arrow::datatypes::SchemaRef = Arc::new(plan.schema().as_arrow().clone());
        let handle = uuid::Uuid::new_v4();
        let now = std::time::Instant::now();
        let mut prepared = self.prepared.lock().map_err(|_| QueryError::Poisoned)?;
        prepared.retain(|_, p| now.duration_since(p.at) < self.limits.prepared_ttl);
        while prepared.len() >= self.limits.prepared_plans.max(1) {
            let Some(oldest) = prepared.iter().min_by_key(|(_, p)| p.at).map(|(h, _)| *h) else {
                break;
            };
            prepared.remove(&oldest);
        }
        prepared.insert(
            handle,
            Prepared {
                tenant,
                ctx,
                plan,
                warnings,
                at: now,
            },
        );
        Ok((handle, schema))
    }

    /// Executes the plan prepared under `handle` for `tenant`, consuming
    /// it, or `None` when it is gone: expired, dropped for room, executed
    /// already, prepared by another querier, or prepared for another
    /// tenant, which is never served across tenants.
    ///
    /// # Errors
    /// Physical planning or execution failure.
    pub async fn execute_prepared(
        &self,
        tenant: TenantId,
        handle: PreparedHandle,
    ) -> Result<Option<(SendableRecordBatchStream, Arc<QueryWarnings>)>, QueryError> {
        let taken = {
            let mut prepared = self.prepared.lock().map_err(|_| QueryError::Poisoned)?;
            match prepared.get(&handle) {
                Some(p) if p.tenant == tenant && p.at.elapsed() < self.limits.prepared_ttl => {
                    prepared.remove(&handle)
                }
                _ => None,
            }
        };
        let Some(Prepared {
            ctx,
            plan,
            warnings,
            ..
        }) = taken
        else {
            self.stats
                .prepared_misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(None);
        };
        self.stats
            .prepared_hits
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let frame = ctx.execute_logical_plan(plan).await?;
        let stream = frame.execute_stream().await?;
        Ok(Some((stream, warnings)))
    }

    /// Prepared plans waiting for their `DoGet`.
    pub fn prepared_count(&self) -> usize {
        self.prepared.lock().map_or(0, |p| p.len())
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
            self.stats
                .rows
                .fetch_add(rows as u64, std::sync::atomic::Ordering::Relaxed);
            Ok((out, warnings.list()))
        };
        tokio::time::timeout(limits.timeout, run)
            .await
            .map_err(|_| QueryError::Timeout(limits.timeout))?
    }
}
