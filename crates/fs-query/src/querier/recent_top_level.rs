//! `top_level_operations` over every span of a tenant, not only the hours
//! the compactor closed.
//!
//! The compacted contributions hold the passes already run. The spans they
//! do not hold yet are the visible L0 files and the hot window, listed with
//! those contributions in one catalog snapshot, and read as the `spans`
//! table reads them: the hot window required, the files it still serves
//! left out, under the tenant's byte budget. Their spans are resolved hour
//! by hour, an hour's spans together as its compaction pass will take them,
//! with the compactor's own rule, then united with the contributions by
//! `max(last_seen)`. Filters run above the listing, after every parent was
//! found. A read that exceeds the budget, the memory pool or misses an
//! ingester fails whole; only a complete listing is kept, for
//! `recent_keys_ttl`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result as DfResult;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use fs_catalog::{Catalog, FileRecord, Signal, TenantId};
use fs_schema::top_level_operations::{self, TopLevelOperations};
use futures_util::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt};

use super::QueryStats;
use super::recent_keys::{Joined, KeysCache, Listed, Listing, split_by_hour, typed, unshared};
use crate::QueryError;
use crate::files::FileCache;
use crate::signal_table::SignalTable;

/// The name the listing's reservation is charged under.
const CONSUMER: &str = "top_level_operations";

/// The span columns the rule reads.
const SPAN_COLUMNS: [&str; 6] = [
    "trace_id",
    "span_id",
    "parent_span_id",
    "timestamp",
    "service_name",
    "name",
];

/// What a listing is read with: the querier's catalog, bucket, hot window,
/// memory pool and cache of complete listings.
pub(crate) struct TopLevelSource {
    pub(crate) catalog: Catalog,
    pub(crate) cache: Arc<FileCache>,
    pub(crate) hot: Arc<crate::hot::HotWindow>,
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) store_url: ObjectStoreUrl,
    pub(crate) pool: Arc<dyn MemoryPool>,
    pub(crate) listings: Arc<KeysCache>,
    pub(crate) stats: Arc<QueryStats>,
}

/// The session's `top_level_operations`: one tenant's complete listing.
pub(crate) struct TopLevelTable {
    tenant: TenantId,
    source: Arc<TopLevelSource>,
}

impl TopLevelTable {
    pub(crate) fn new(tenant: TenantId, source: Arc<TopLevelSource>) -> Self {
        Self { tenant, source }
    }
}

impl std::fmt::Debug for TopLevelTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopLevelTable")
            .field("tenant", &self.tenant.0)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for TopLevelTable {
    fn schema(&self) -> SchemaRef {
        top_level_operations::schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let listing = self
            .source
            .listing(self.tenant, state)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![listing.batch]],
            self.schema(),
            projection.cloned(),
        )?)
    }
}

impl TopLevelSource {
    /// The complete listing of `tenant`, from the cache or a fill shared by
    /// concurrent misses.
    async fn listing(&self, tenant: TenantId, state: &dyn Session) -> Result<Listing, QueryError> {
        let flight = match self.listings.join(tenant, Listed::TopLevel) {
            Joined::Cached(listing) => return Ok(listing),
            Joined::Fill(flight) => flight,
        };
        let filled = flight
            .get_or_init(|| async {
                let filled = self.fill(tenant, state).await;
                if let Ok(listing) = &filled {
                    self.listings.put(tenant, Listed::TopLevel, listing.clone());
                }
                filled.map_err(Arc::new)
            })
            .await
            .clone();
        self.listings.landed(tenant, Listed::TopLevel, &flight);
        filled.map_err(|e| unshared(&e))
    }

    /// One fill: the snapshot, the contributions and the recent spans
    /// resolved by hour. Every growth is charged before it happens; the
    /// listing's buffers keep the charge for their batch.
    async fn fill(&self, tenant: TenantId, state: &dyn Session) -> Result<Listing, QueryError> {
        self.stats
            .recent_top_level_fills
            .fetch_add(1, Ordering::Relaxed);
        let record = self
            .catalog
            .tenant(tenant)
            .await?
            .ok_or(QueryError::UnknownTenant)?;
        let budget = u64::try_from(record.bytes_budget).unwrap_or(u64::MAX);
        let snapshot = self
            .catalog
            .keys_snapshot(tenant, Signal::Traces, Signal::TopLevelOperations)
            .await?;
        let mut held = MemoryConsumer::new(CONSUMER).register(&self.pool);
        let mut total = self.compacted(&snapshot.keys, &mut held).await?;
        let mut hours: BTreeMap<i64, TopLevelOperations> = BTreeMap::new();
        let mut recent = self
            .recent_spans(tenant, snapshot.l0, budget, state)
            .await?;
        let resolving = |source| QueryError::DerivedTable {
            path: "top_level_operations of recent spans".into(),
            source,
        };
        while let Some(batch) = recent.next().await {
            let batch = batch.map_err(typed)?;
            for (hour, part) in split_by_hour(&batch)? {
                let bound = TopLevelOperations::growth_bound(&part).map_err(resolving)?;
                held.try_resize(charged(&total, &hours).saturating_add(bound))?;
                hours
                    .entry(hour)
                    .or_default()
                    .absorb(&part, usize::MAX, usize::MAX)
                    .map_err(resolving)?;
                held.try_resize(charged(&total, &hours))?;
            }
        }
        for (_, mut operations) in std::mem::take(&mut hours) {
            operations.finish();
            total.merge(operations);
        }
        held.try_resize(total.estimated_bytes())?;
        let batch = total.to_batch().map_err(resolving)?;
        drop(total);
        held.try_resize(batch.get_array_memory_size())?;
        crate::claim::claim(batch.columns(), held.take());
        Ok(Listing::new(batch, held))
    }

    /// The compacted contributions of `files`, decoded and united; each
    /// object and the union as it grows are charged to `held` first.
    async fn compacted(
        &self,
        files: &[FileRecord],
        held: &mut MemoryReservation,
    ) -> Result<TopLevelOperations, QueryError> {
        let mut total = TopLevelOperations::default();
        for file in files {
            let object = usize::try_from(file.size_bytes).unwrap_or(usize::MAX);
            let rows = usize::try_from(file.row_count).unwrap_or(usize::MAX);
            held.try_resize(
                total
                    .estimated_bytes()
                    .saturating_add(object)
                    .saturating_add(
                        rows.saturating_mul(top_level_operations::OPERATION_ENTRY_BYTES),
                    ),
            )?;
            let bytes = self
                .store
                .get(&object_store::path::Path::from(file.path.as_str()))
                .await
                .map_err(|e| QueryError::DataFusion(DataFusionError::ObjectStore(Box::new(e))))?
                .bytes()
                .await
                .map_err(|e| QueryError::DataFusion(DataFusionError::ObjectStore(Box::new(e))))?;
            let decoded =
                top_level_operations::decode(bytes).map_err(|source| QueryError::DerivedTable {
                    path: file.path.clone(),
                    source,
                })?;
            held.try_resize(
                total
                    .estimated_bytes()
                    .saturating_add(decoded.estimated_bytes()),
            )?;
            total.merge(decoded);
        }
        held.try_resize(total.estimated_bytes())?;
        Ok(total)
    }

    /// The rule's columns of `l0` and of the hot window, streamed as the
    /// `spans` table reads them.
    async fn recent_spans(
        &self,
        tenant: TenantId,
        l0: Vec<FileRecord>,
        budget: u64,
        state: &dyn Session,
    ) -> Result<datafusion::execution::SendableRecordBatchStream, QueryError> {
        let spans = SignalTable::spans(
            tenant,
            Arc::clone(&self.cache),
            self.store_url.clone(),
            budget,
        )
        .with_hot_window(
            Arc::clone(&self.hot),
            Arc::new(super::QueryWarnings::default()),
        )
        .requiring_hot_window()
        .over_files(Arc::new(l0));
        let schema = spans.schema();
        let projection = SPAN_COLUMNS
            .iter()
            .map(|name| schema.index_of(name))
            .collect::<Result<Vec<_>, _>>()?;
        let plan = spans
            .scan(state, Some(&projection), &[], None)
            .await
            .map_err(typed)?;
        datafusion::physical_plan::execute_stream(plan, state.task_ctx()).map_err(typed)
    }
}

/// What the union and the hours being resolved hold, as charged.
fn charged(total: &TopLevelOperations, hours: &BTreeMap<i64, TopLevelOperations>) -> usize {
    hours.values().fold(total.estimated_bytes(), |bytes, hour| {
        bytes.saturating_add(hour.estimated_bytes())
    })
}
