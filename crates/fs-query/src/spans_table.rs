//! DataFusion table over the spans files of one tenant.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{DFSchema, Result as DfResult};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, utils::conjunction};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::table_schema::TableSchema;
use fs_catalog::{Signal, TenantId};
use fs_schema::spans::span_schema_v1;
use tracing::debug;

use crate::QueryError;
use crate::files::FileCache;
use crate::prune::Pruning;

/// The `spans` table of one tenant.
pub struct SpansTable {
    tenant: TenantId,
    cache: Arc<FileCache>,
    store_url: ObjectStoreUrl,
    bytes_budget: u64,
    hot: Option<(
        Arc<crate::hot::HotWindow>,
        Arc<crate::querier::QueryWarnings>,
    )>,
    trace_index: Option<Arc<crate::trace_index::TraceIndexCache>>,
}

impl SpansTable {
    /// Table over `tenant`'s visible span files, reading through `store_url`.
    pub fn new(
        tenant: TenantId,
        cache: Arc<FileCache>,
        store_url: ObjectStoreUrl,
        bytes_budget: u64,
    ) -> Self {
        Self {
            tenant,
            cache,
            store_url,
            bytes_budget,
            hot: None,
            trace_index: None,
        }
    }

    /// Consults `trace_id` sidecars when the filters pin trace ids.
    #[must_use]
    pub fn with_trace_index(mut self, cache: Arc<crate::trace_index::TraceIndexCache>) -> Self {
        self.trace_index = Some(cache);
        self
    }

    /// Adds the hot-window fan-out; ingesters that fail are noted in `warnings`.
    #[must_use]
    pub fn with_hot_window(
        mut self,
        hot: Arc<crate::hot::HotWindow>,
        warnings: Arc<crate::querier::QueryWarnings>,
    ) -> Self {
        self.hot = Some((hot, warnings));
        self
    }

    /// Files the scan would open after catalog-level pruning, with their total size.
    ///
    /// # Errors
    /// Catalog failure.
    pub async fn plan_files(
        &self,
        filters: &[Expr],
    ) -> Result<(Vec<PartitionedFile>, u64), QueryError> {
        self.plan_files_excluding(filters, &std::collections::HashSet::new())
            .await
    }

    /// Files the scan would open after catalog-level pruning, leaving out
    /// the ones the hot window still serves: `excluded` holds the ids the
    /// ingesters announced, matched against a file's id and its origin, the
    /// id its rows were first assigned, so a flush that changed id between
    /// the hot scan and the catalog read is still left out.
    ///
    /// # Errors
    /// Catalog failure.
    pub async fn plan_files_excluding(
        &self,
        filters: &[Expr],
        excluded: &std::collections::HashSet<fs_catalog::FileId>,
    ) -> Result<(Vec<PartitionedFile>, u64), QueryError> {
        let pruning = Pruning::from_filters(filters);
        let all = self.cache.files(self.tenant, Signal::Traces).await?;
        let candidates: Vec<&fs_catalog::FileRecord> = all
            .iter()
            .filter(|f| {
                pruning.keeps(f) && !excluded.contains(&f.id) && !excluded.contains(&f.origin)
            })
            .collect();
        let after_catalog = candidates.len();
        let candidates = match (&self.trace_index, pinned_traces(&pruning)) {
            (Some(index), Some(traces)) => index.files_holding(candidates, &traces).await,
            _ => candidates,
        };
        let mut bytes = 0u64;
        let files: Vec<PartitionedFile> = candidates
            .into_iter()
            .map(|f| {
                let size = u64::try_from(f.size_bytes).unwrap_or(0);
                bytes = bytes.saturating_add(size);
                PartitionedFile::new(f.path.clone(), size)
            })
            .collect();
        debug!(
            tenant = %self.tenant.0,
            candidates = all.len(),
            after_catalog,
            kept = files.len(),
            excluded = excluded.len(),
            bytes,
            ?pruning,
            "catalog pruning"
        );
        Ok((files, bytes))
    }
}

impl std::fmt::Debug for SpansTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpansTable")
            .field("tenant", &self.tenant.0)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for SpansTable {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        span_schema_v1()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // Pruning is a hint; DataFusion keeps evaluating every filter.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let schema = span_schema_v1();
        let external = |e: QueryError| datafusion::error::DataFusionError::External(Box::new(e));

        // Hot window first: ingesters report which recently flushed files they
        // still serve, and those leave the file scan.
        let pruning = Pruning::from_filters(filters);
        let projected_names: Option<Vec<String>> = projection.map(|indices| {
            indices
                .iter()
                .map(|i| schema.field(*i).name().clone())
                .collect()
        });
        let mut hot_batches = Vec::new();
        let mut excluded = std::collections::HashSet::new();
        if let Some((hot, warnings)) = &self.hot {
            if hot.touches_recent(&pruning) {
                let result = hot
                    .scan(self.tenant, &pruning, projected_names.clone())
                    .await
                    .map_err(|e| external(e.into()))?;
                for ingester in &result.unavailable {
                    warnings.push(format!(
                        "ingester {ingester} did not answer; recent rows may be missing"
                    ));
                }
                hot_batches = result.batches;
                excluded = result.excluded_files;
            }
        }

        let (files, bytes) = self
            .plan_files_excluding(filters, &excluded)
            .await
            .map_err(external)?;
        if bytes > self.bytes_budget {
            return Err(external(QueryError::OverBudget {
                bytes,
                budget: self.bytes_budget,
            }));
        }
        let projected_schema = match projection {
            Some(indices) => Arc::new(schema.project(indices)?),
            None => Arc::clone(&schema),
        };
        let hot_exec: Option<Arc<dyn ExecutionPlan>> = if hot_batches.is_empty() {
            None
        } else {
            // Flight hydrates dictionaries in transit; bring every column back
            // to the declared type so the union sees one schema.
            let coerced = hot_batches
                .iter()
                .map(|b| coerce_to_schema(b, &projected_schema))
                .collect::<Result<Vec<_>, _>>()?;
            Some(MemorySourceConfig::try_new_exec(
                &[coerced],
                Arc::clone(&projected_schema),
                None,
            )?)
        };
        if files.is_empty() {
            return Ok(hot_exec.unwrap_or_else(|| Arc::new(EmptyExec::new(projected_schema))));
        }

        let mut source = ParquetSource::new(TableSchema::from(Arc::clone(&schema)))
            .with_pushdown_filters(true)
            .with_enable_page_index(true)
            .with_bloom_filter_on_read(true);
        if let Some(predicate) = conjunction(filters.iter().cloned()) {
            let df_schema = DFSchema::try_from(Arc::clone(&schema))?;
            let physical = state.create_physical_expr(predicate, &df_schema)?;
            source = source.with_predicate(physical);
        }
        let config = FileScanConfigBuilder::new(self.store_url.clone(), Arc::new(source))
            .with_file_group(FileGroup::new(files))
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .build();
        let files_exec: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
        Ok(match hot_exec {
            Some(hot) => UnionExec::try_new(vec![files_exec, hot])?,
            None => files_exec,
        })
    }
}

/// The 16-byte trace ids the filters pin; `None` when they pin none.
fn pinned_traces(pruning: &Pruning) -> Option<Vec<[u8; 16]>> {
    let ids: Vec<[u8; 16]> = pruning
        .trace_ids
        .as_ref()?
        .iter()
        .filter_map(|id| id.as_slice().try_into().ok())
        .collect();
    (!ids.is_empty()).then_some(ids)
}

/// Casts every column whose type differs from `schema` to the declared type.
fn coerce_to_schema(
    batch: &arrow::record_batch::RecordBatch,
    schema: &arrow::datatypes::SchemaRef,
) -> DfResult<arrow::record_batch::RecordBatch> {
    if batch.schema().fields() == schema.fields() {
        return Ok(batch.clone());
    }
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let index = batch.schema().index_of(field.name())?;
            let column = batch.column(index);
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                Ok(arrow::compute::cast(column, field.data_type())?)
            }
        })
        .collect::<DfResult<Vec<_>>>()?;
    Ok(arrow::record_batch::RecordBatch::try_new(
        Arc::clone(schema),
        columns,
    )?)
}
