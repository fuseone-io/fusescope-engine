//! DataFusion table over the spans files of one tenant.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{DFSchema, Result as DfResult};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, utils::conjunction};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
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
        }
    }

    /// Files the scan would open after catalog-level pruning, with their total size.
    ///
    /// # Errors
    /// Catalog failure.
    pub async fn plan_files(
        &self,
        filters: &[Expr],
    ) -> Result<(Vec<PartitionedFile>, u64), QueryError> {
        let pruning = Pruning::from_filters(filters);
        let all = self.cache.files(self.tenant, Signal::Traces).await?;
        let mut bytes = 0u64;
        let files: Vec<PartitionedFile> = all
            .iter()
            .filter(|f| pruning.keeps(f))
            .map(|f| {
                let size = u64::try_from(f.size_bytes).unwrap_or(0);
                bytes = bytes.saturating_add(size);
                PartitionedFile::new(f.path.clone(), size)
            })
            .collect();
        debug!(tenant = %self.tenant.0, candidates = all.len(), kept = files.len(), bytes, ?pruning, "catalog pruning");
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
        let (files, bytes) = self
            .plan_files(filters)
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        if bytes > self.bytes_budget {
            return Err(datafusion::error::DataFusionError::External(Box::new(
                QueryError::OverBudget {
                    bytes,
                    budget: self.bytes_budget,
                },
            )));
        }
        if files.is_empty() {
            let projected = match projection {
                Some(indices) => Arc::new(schema.project(indices)?),
                None => schema,
            };
            return Ok(Arc::new(EmptyExec::new(projected)));
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
        Ok(DataSourceExec::from_data_source(config))
    }
}
