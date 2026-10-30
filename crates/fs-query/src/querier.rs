//! The querier facade: sessions, limits and execution.

use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Weak};
use std::time::Duration;

use arrow::array::{AsArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::context::SessionContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::functions::expr_fn::starts_with;
use datafusion::functions_aggregate::count::count_all;
use datafusion::logical_expr::{LogicalPlan, cast, col, lit};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::scalar::ScalarValue;
use fs_catalog::{Catalog, Signal, TenantId};
use fs_schema::attribute_keys::{self, col_names as key_col};
use fs_schema::spans::col as span_col;
use futures_util::StreamExt;
use object_store::ObjectStore;
use tracing::info;

use crate::QueryError;
use crate::actions::{KeysQuery, OperationsQuery, ServicesQuery, TraceQuery};
use crate::files::FileCache;
use crate::prune::Pruning;
use crate::session::{SPANS_TABLE, plan_read_only, tenant_session};

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
    /// Plans waiting for their `DoGet` right now.
    pub prepared: std::sync::atomic::AtomicU64,
    /// Bytes the waiting plans are estimated to retain.
    pub prepared_bytes: std::sync::atomic::AtomicU64,
    /// Statements that failed to plan or were refused.
    pub errors: std::sync::atomic::AtomicU64,
    /// `trace.lookup` actions served.
    pub trace_lookups: std::sync::atomic::AtomicU64,
    /// `metadata.*` actions served.
    pub metadata_lookups: std::sync::atomic::AtomicU64,
    /// Rows returned by `collect`.
    pub rows: std::sync::atomic::AtomicU64,
    /// Hot-window fan-outs performed.
    pub hot_fanouts: std::sync::atomic::AtomicU64,
    /// Ingesters that did not answer a fan-out.
    pub hot_unavailable: std::sync::atomic::AtomicU64,
    /// Flight requests refused for a missing, unknown or invalid token.
    pub auth_refused: std::sync::atomic::AtomicU64,
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
    /// Most bytes the kept plans may retain together, as estimated by
    /// [`retained_bytes`]; the oldest are dropped to make room, and a plan
    /// that does not fit on its own is not kept at all.
    pub prepared_bytes: usize,
    /// How long a prepared plan waits for its `DoGet` before it is dropped;
    /// clamped to [`MAX_PREPARED_TTL`].
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
            prepared_bytes: 64 * 1024 * 1024,
            prepared_ttl: Duration::from_secs(60),
        }
    }
}

/// URL under which the engine bucket is registered with DataFusion.
pub const STORE_URL: &str = "fusescope://bucket/";

/// Allowance for the session state a prepared plan keeps alongside its SQL
/// and plan text, in bytes.
pub const PREPARED_PLAN_OVERHEAD: usize = 64 * 1024;

/// Longest `prepared_ttl` honoured, one day, so adding it to the instant a
/// plan was kept cannot overflow.
pub const MAX_PREPARED_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Estimate of what a plan kept for `sql` retains: the SQL, the plan with
/// its schemas as displayed, and [`PREPARED_PLAN_OVERHEAD`]. The heap is not
/// measured; this is the figure `prepared_bytes` bounds.
pub fn retained_bytes(sql: &str, plan: &LogicalPlan) -> usize {
    PREPARED_PLAN_OVERHEAD + sql.len() + plan.display_indent_schema().to_string().len()
}

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
    prepared: Arc<std::sync::Mutex<PreparedPlans>>,
    verified: crate::derived_table::VerifiedPaths,
}

impl Querier {
    /// Asks the ingesters for the hot window over mTLS with `tls`.
    #[must_use]
    pub fn with_hot_tls(mut self, tls: fs_auth::ClientTls) -> Self {
        self.hot = Arc::new(
            crate::hot::HotWindow::new(self.catalog.clone(), self.limits.hot_window)
                .with_stats(Arc::clone(&self.stats))
                .with_tls(tls),
        );
        self
    }

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
        let limits = QueryLimits {
            prepared_ttl: limits.prepared_ttl.min(MAX_PREPARED_TTL),
            ..limits
        };
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
            prepared: Arc::new(std::sync::Mutex::new(PreparedPlans::new(Arc::clone(
                &stats,
            )))),
            stats,
            verified: Arc::default(),
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
    ) -> Result<(SessionContext, LogicalPlan, Arc<QueryWarnings>), QueryError> {
        let (ctx, warnings) = self.session(tenant).await?;
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

    /// A session scoped to `tenant`, with its byte budget, and the warnings
    /// its table providers report into. Counts as a query.
    async fn session(
        &self,
        tenant: TenantId,
    ) -> Result<(SessionContext, Arc<QueryWarnings>), QueryError> {
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
            Arc::clone(&self.verified),
        )?;
        Ok((ctx, warnings))
    }

    /// The spans of one trace for `tenant`, in `timestamp` order, within
    /// the query's bounds when it has them. Goes through the spans table
    /// like SQL would, so the sidecars, the hot window and the budget all
    /// apply, without parsing or planning SQL.
    ///
    /// # Errors
    /// Unknown tenant, planning or execution failure.
    pub async fn trace_lookup(
        &self,
        tenant: TenantId,
        query: TraceQuery,
    ) -> Result<(SendableRecordBatchStream, Arc<QueryWarnings>), QueryError> {
        let (ctx, warnings) = self.session(tenant).await?;
        self.stats
            .trace_lookups
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut frame = ctx
            .table(SPANS_TABLE)
            .await?
            .filter(col(span_col::TRACE_ID).eq(lit(ScalarValue::FixedSizeBinary(
                16,
                Some(query.trace_id.to_vec()),
            ))))?;
        if let Some(start) = query.start_ns {
            frame = frame.filter(col(span_col::TIMESTAMP).gt_eq(timestamp(start)))?;
        }
        if let Some(end) = query.end_ns {
            frame = frame.filter(col(span_col::TIMESTAMP).lt_eq(timestamp(end)))?;
        }
        let frame = frame.sort(vec![col(span_col::TIMESTAMP).sort(true, false)])?;
        let stream = frame.execute_stream().await?;
        Ok((stream, warnings))
    }

    /// The distinct `service_name` of `tenant`'s spans within the query's
    /// range, sorted, as one batch of one `service_name` column: the
    /// services of every file the catalog holds that overlaps the range,
    /// so a range answers at file granularity, and the services the
    /// ingesters still hold when the range touches the recent window. No
    /// file is read. More services than the query's limit, or than
    /// `max_rows`, is [`QueryError::TooManyRows`] as soon as it is known,
    /// so the answer never holds more than that.
    ///
    /// # Errors
    /// Unknown tenant, catalog failure, or too many services.
    pub async fn metadata_services(
        &self,
        tenant: TenantId,
        query: ServicesQuery,
    ) -> Result<(SendableRecordBatchStream, Arc<QueryWarnings>), QueryError> {
        let range = query.range;
        let limit = query.limit.min(self.limits.max_rows);
        self.catalog
            .tenant(tenant)
            .await?
            .ok_or(QueryError::UnknownTenant)?;
        let relaxed = std::sync::atomic::Ordering::Relaxed;
        self.stats.queries.fetch_add(1, relaxed);
        self.stats.metadata_lookups.fetch_add(1, relaxed);
        let warnings = Arc::new(QueryWarnings::default());
        let mut services = BTreeSet::new();
        for file in self.cache.files(tenant, Signal::Traces).await?.iter() {
            let start = file.range.start.timestamp_nanos_opt();
            let end = file.range.end.timestamp_nanos_opt();
            let overlaps = range
                .start_ns
                .is_none_or(|from| end.is_none_or(|end| end >= from))
                && range
                    .end_ns
                    .is_none_or(|to| start.is_none_or(|start| start <= to));
            if overlaps {
                for service in &file.services {
                    admit(&mut services, service, limit)?;
                }
            }
        }
        let pruning = Pruning {
            ts_min: range.start_ns,
            ts_max: range.end_ns,
            ..Pruning::default()
        };
        if self.hot.touches_recent(&pruning) {
            let hot = self
                .hot
                .scan(
                    tenant,
                    &pruning,
                    Some(vec![span_col::SERVICE_NAME.to_owned()]),
                    Some(limit.saturating_add(1)),
                )
                .await?;
            for ingester in &hot.unavailable {
                warnings.push(format!(
                    "ingester {ingester} did not answer; recent services may be missing"
                ));
            }
            for batch in &hot.batches {
                let names = service_names(batch)?;
                for service in names.as_string::<i32>().iter().flatten() {
                    admit(&mut services, service, limit)?;
                }
            }
        }
        let batch = RecordBatch::try_new(
            services_schema(),
            vec![Arc::new(StringArray::from_iter_values(services))],
        )?;
        Ok((one_batch(batch), warnings))
    }

    /// The span names of `query.service_name` within its range, with how
    /// many spans carry each, most frequent first, `query.limit` at most.
    /// Goes through the spans table like SQL would.
    ///
    /// # Errors
    /// Unknown tenant, planning or execution failure.
    pub async fn metadata_operations(
        &self,
        tenant: TenantId,
        query: &OperationsQuery,
    ) -> Result<(SendableRecordBatchStream, Arc<QueryWarnings>), QueryError> {
        let (ctx, warnings) = self.session(tenant).await?;
        self.stats
            .metadata_lookups
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut frame = ctx
            .table(SPANS_TABLE)
            .await?
            .filter(col(span_col::SERVICE_NAME).eq(lit(query.service_name.as_str())))?;
        if let Some(start) = query.range.start_ns {
            frame = frame.filter(col(span_col::TIMESTAMP).gt_eq(timestamp(start)))?;
        }
        if let Some(end) = query.range.end_ns {
            frame = frame.filter(col(span_col::TIMESTAMP).lt_eq(timestamp(end)))?;
        }
        let frame = frame
            .aggregate(vec![col(span_col::NAME)], vec![count_all().alias("spans")])?
            .select(vec![
                cast(col(span_col::NAME), DataType::Utf8).alias("name"),
                col("spans"),
            ])?
            .sort(vec![
                col("spans").sort(false, false),
                col("name").sort(true, false),
            ])?
            .limit(0, Some(query.limit.min(self.limits.max_rows)))?;
        Ok((frame.execute_stream().await?, warnings))
    }

    /// The rows of the tenant's `attribute_keys` table whose key starts
    /// with `query.prefix`, most frequent first, `query.limit` at most.
    ///
    /// # Errors
    /// Unknown tenant, planning or execution failure.
    pub async fn metadata_keys(
        &self,
        tenant: TenantId,
        query: &KeysQuery,
    ) -> Result<(SendableRecordBatchStream, Arc<QueryWarnings>), QueryError> {
        let (ctx, warnings) = self.session(tenant).await?;
        self.stats
            .metadata_lookups
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut frame = ctx.table(attribute_keys::TABLE).await?;
        if let Some(prefix) = &query.prefix {
            frame = frame.filter(starts_with(col(key_col::KEY), lit(prefix.as_str())))?;
        }
        let frame = frame
            .sort(vec![
                col(key_col::COUNT).sort(false, false),
                col(key_col::KEY).sort(true, false),
                col(key_col::SOURCE).sort(true, false),
                col(key_col::TYPE).sort(true, false),
            ])?
            .limit(0, Some(query.limit.min(self.limits.max_rows)))?;
        Ok((frame.execute_stream().await?, warnings))
    }

    /// Plans `sql` for `tenant`, returns the result schema and keeps the
    /// plan for one execution of the same `sql` by the same tenant, for
    /// `prepared_ttl` at most. Beyond `prepared_plans` or `prepared_bytes`
    /// the oldest plans are dropped and their tickets plan again; a plan
    /// over `prepared_bytes` on its own is not kept. Identical statements
    /// prepared concurrently are kept apart and executed oldest first.
    ///
    /// # Errors
    /// See [`Querier::plan`].
    pub async fn prepare(
        &self,
        tenant: TenantId,
        sql: &str,
    ) -> Result<arrow::datatypes::SchemaRef, QueryError> {
        let (ctx, plan, warnings) = self.plan(tenant, sql).await?;
        let schema: arrow::datatypes::SchemaRef = Arc::new(plan.schema().as_arrow().clone());
        let prepared = Prepared {
            tenant,
            sql: sql.to_owned(),
            bytes: retained_bytes(sql, &plan),
            ctx,
            plan,
            warnings,
        };
        let mut plans = self.prepared.lock().map_err(|_| QueryError::Poisoned)?;
        let now = std::time::Instant::now();
        plans.drop_expired(now, self.limits.prepared_ttl);
        if plans.keep(prepared, now, &self.limits) && !plans.reaping {
            plans.reaping = true;
            tokio::spawn(reap_expired(
                Arc::downgrade(&self.prepared),
                self.limits.prepared_ttl,
            ));
        }
        Ok(schema)
    }

    /// Executes the oldest plan kept for `sql` by `tenant`, consuming it,
    /// or `None` when none waits: expired, dropped for room, executed
    /// already or prepared by another querier. A plan is only ever served
    /// to the tenant that prepared it.
    ///
    /// # Errors
    /// Physical planning or execution failure.
    pub async fn execute_prepared(
        &self,
        tenant: TenantId,
        sql: &str,
    ) -> Result<Option<(SendableRecordBatchStream, Arc<QueryWarnings>)>, QueryError> {
        let taken = {
            let mut plans = self.prepared.lock().map_err(|_| QueryError::Poisoned)?;
            plans.drop_expired(std::time::Instant::now(), self.limits.prepared_ttl);
            plans.take(tenant, sql)
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
        self.prepared.lock().map_or(0, |p| p.queue.len())
    }

    /// Derived table files whose footer passed the version check so far.
    pub fn verified_derived_files(&self) -> usize {
        self.verified.lock().map_or(0, |paths| {
            paths
                .values()
                .filter(|slot| slot.try_lock().is_ok_and(|done| *done))
                .count()
        })
    }

    /// Bytes the waiting plans are estimated to retain.
    pub fn prepared_bytes(&self) -> usize {
        self.prepared.lock().map_or(0, |p| p.bytes)
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

/// A statement planned on `GetFlightInfo`, waiting for its `DoGet`.
struct Prepared {
    tenant: TenantId,
    sql: String,
    bytes: usize,
    ctx: datafusion::execution::context::SessionContext,
    plan: LogicalPlan,
    warnings: Arc<QueryWarnings>,
}

/// A plan in the queue, stamped by [`PreparedPlans::keep`] under the lock
/// so that queue order is `at` order.
struct Kept {
    at: std::time::Instant,
    plan: Prepared,
}

/// Plans waiting for their `DoGet`, oldest first, so the expired and the
/// ones dropped for room both leave from the front. `reaping` says a
/// [`reap_expired`] task is alive for the queue.
struct PreparedPlans {
    queue: VecDeque<Kept>,
    bytes: usize,
    reaping: bool,
    stats: Arc<QueryStats>,
}

impl PreparedPlans {
    fn new(stats: Arc<QueryStats>) -> Self {
        Self {
            queue: VecDeque::new(),
            bytes: 0,
            reaping: false,
            stats,
        }
    }

    fn drop_expired(&mut self, now: std::time::Instant, ttl: Duration) {
        while self
            .queue
            .front()
            .is_some_and(|p| now.duration_since(p.at) >= ttl)
        {
            self.pop_front();
        }
        self.publish();
    }

    /// Keeps `plan` as of `now`, dropping the oldest until it fits; `false`
    /// when it does not fit on its own. `now` must not precede the last
    /// `now` kept, which holding the lock across clock and push guarantees.
    fn keep(&mut self, plan: Prepared, now: std::time::Instant, limits: &QueryLimits) -> bool {
        if plan.bytes > limits.prepared_bytes {
            self.publish();
            return false;
        }
        while self.queue.len() >= limits.prepared_plans.max(1)
            || self.bytes + plan.bytes > limits.prepared_bytes
        {
            self.pop_front();
        }
        self.bytes += plan.bytes;
        self.queue.push_back(Kept { at: now, plan });
        self.publish();
        true
    }

    fn take(&mut self, tenant: TenantId, sql: &str) -> Option<Prepared> {
        let index = self
            .queue
            .iter()
            .position(|k| k.plan.tenant == tenant && k.plan.sql == sql)?;
        let kept = self.queue.remove(index)?;
        self.bytes -= kept.plan.bytes;
        self.publish();
        Some(kept.plan)
    }

    fn pop_front(&mut self) {
        if let Some(kept) = self.queue.pop_front() {
            self.bytes -= kept.plan.bytes;
        }
    }

    fn next_expiry(&self, ttl: Duration) -> Option<std::time::Instant> {
        self.queue.front().map(|p| p.at + ttl)
    }

    fn publish(&self) {
        let relaxed = std::sync::atomic::Ordering::Relaxed;
        self.stats.prepared.store(self.queue.len() as u64, relaxed);
        self.stats.prepared_bytes.store(self.bytes as u64, relaxed);
    }
}

/// Drops the plans of `weak` as they expire, sleeping until the oldest is
/// due, and ends once nothing waits; the next plan kept starts it again.
/// Holds no strong reference, so it ends with the querier too.
async fn reap_expired(weak: Weak<std::sync::Mutex<PreparedPlans>>, ttl: Duration) {
    loop {
        let due = {
            let Some(shared) = weak.upgrade() else { return };
            let Ok(mut plans) = shared.lock() else { return };
            plans.drop_expired(std::time::Instant::now(), ttl);
            let Some(at) = plans.next_expiry(ttl) else {
                plans.reaping = false;
                return;
            };
            at
        };
        tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await;
    }
}

/// A `timestamp` literal of the span schema's type.
fn timestamp(ns: i64) -> datafusion::logical_expr::Expr {
    lit(ScalarValue::TimestampNanosecond(
        Some(ns),
        Some("UTC".into()),
    ))
}

/// Schema of a `metadata.services` answer.
fn services_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        span_col::SERVICE_NAME,
        DataType::Utf8,
        false,
    )]))
}

/// The `service_name` column of a hot-window batch as plain strings,
/// whatever encoding it arrived in; read in place, never copied out.
fn service_names(batch: &RecordBatch) -> Result<arrow::array::ArrayRef, QueryError> {
    let index = batch.schema().index_of(span_col::SERVICE_NAME)?;
    Ok(arrow::compute::cast(batch.column(index), &DataType::Utf8)?)
}

/// Adds `service` to `services` unless that would exceed `limit`.
fn admit(services: &mut BTreeSet<String>, service: &str, limit: usize) -> Result<(), QueryError> {
    if services.contains(service) {
        return Ok(());
    }
    if services.len() >= limit {
        return Err(QueryError::TooManyRows(limit));
    }
    services.insert(service.to_owned());
    Ok(())
}

fn one_batch(batch: RecordBatch) -> SendableRecordBatchStream {
    let schema = batch.schema();
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures_util::stream::iter(vec![Ok(batch)]),
    ))
}

#[cfg(test)]
mod tests {
    use datafusion::common::DFSchema;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::EmptyRelation;

    use super::*;

    fn plan(tenant: TenantId, sql: &str, bytes: usize) -> Prepared {
        Prepared {
            tenant,
            sql: sql.to_owned(),
            bytes,
            ctx: SessionContext::new(),
            plan: LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(DFSchema::empty()),
            }),
            warnings: Arc::default(),
        }
    }

    #[test]
    fn plans_expire_and_make_room_in_the_order_they_were_kept() {
        let tenant = TenantId::new();
        let limits = QueryLimits {
            prepared_plans: 2,
            prepared_bytes: 100,
            ..QueryLimits::default()
        };
        let ttl = Duration::from_secs(10);
        let t0 = std::time::Instant::now();
        let mut plans = PreparedPlans::new(Arc::default());
        assert!(plans.keep(plan(tenant, "a", 40), t0, &limits));
        assert!(plans.keep(plan(tenant, "b", 40), t0 + Duration::from_secs(1), &limits));
        assert_eq!(plans.next_expiry(ttl), Some(t0 + ttl));

        plans.drop_expired(t0 + Duration::from_secs(10), ttl);
        assert!(plans.take(tenant, "a").is_none(), "a expired");
        assert_eq!(plans.next_expiry(ttl), Some(t0 + Duration::from_secs(11)));
        assert_eq!(plans.bytes, 40);

        assert!(plans.keep(plan(tenant, "c", 40), t0 + Duration::from_secs(2), &limits));
        assert!(plans.keep(plan(tenant, "d", 40), t0 + Duration::from_secs(3), &limits));
        assert!(
            plans.take(tenant, "b").is_none(),
            "b made room, oldest first"
        );
        assert_eq!(plans.queue.len(), 2);
        assert!(!plans.keep(plan(tenant, "e", 101), t0 + Duration::from_secs(4), &limits));
        assert_eq!(plans.queue.len(), 2, "over budget alone, kept nothing out");
        assert!(plans.take(tenant, "c").is_some());
        assert!(plans.take(tenant, "d").is_some());
        assert_eq!(plans.bytes, 0);
        assert_eq!(plans.next_expiry(ttl), None);
    }
}
