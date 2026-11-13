//! DataFusion table over the span or log rows, or the metric samples or
//! series, of one tenant: its files in the catalog and, for a recent range,
//! the ingesters' memory.

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
use fs_catalog::TenantId;
use tracing::debug;

use fs_catalog::Signal;
use fs_ingest::flush::{catalog_signal, schema_of};
use fs_ingest::hot::MetricTable;
use fs_ingest::record::RowSignal;

use crate::QueryError;
use crate::file_index::FileIndex;
use crate::files::FileCache;
use crate::prune::Pruning;
use crate::querier::QueryStats;
use crate::text_scan::{TextScanExec, TextTally};
use fs_index::text_index::Probe;

/// The `spans` or `logs` table of one tenant, bound to the files and
/// schema of its signal.
pub struct SignalTable {
    tenant: TenantId,
    signal: RowSignal,
    /// For metrics: the samples or the series.
    table: Option<MetricTable>,
    cache: Arc<FileCache>,
    store_url: ObjectStoreUrl,
    bytes_budget: u64,
    hot: Option<(
        Arc<crate::hot::HotWindow>,
        Arc<crate::querier::QueryWarnings>,
    )>,
    trace_index: Option<Arc<crate::trace_index::TraceIndexCache>>,
    text_index: Option<(Arc<dyn FileIndex>, Arc<QueryStats>)>,
    /// For samples: the source order is a column.
    source_order: bool,
    /// An ingester that does not answer fails the scan.
    hot_required: bool,
    /// What the hot window's answers are charged to before they are held.
    hot_charge: Option<crate::hot::Charge>,
    /// The files to read, fixed by the caller's snapshot, in place of the
    /// cache's visible files.
    files: Option<Arc<Vec<fs_catalog::FileRecord>>>,
}

/// The files a scan opens and what the text index made of them.
struct Planned {
    files: Vec<PartitionedFile>,
    bytes: u64,
    /// Counts of an eligible body search; `None` for any other scan.
    text: Option<TextTally>,
}

impl SignalTable {
    /// Table over `tenant`'s visible span files, reading through `store_url`.
    pub fn spans(
        tenant: TenantId,
        cache: Arc<FileCache>,
        store_url: ObjectStoreUrl,
        bytes_budget: u64,
    ) -> Self {
        Self::of(RowSignal::Traces, tenant, cache, store_url, bytes_budget)
    }

    /// Table over `tenant`'s visible log files, reading through `store_url`.
    pub fn logs(
        tenant: TenantId,
        cache: Arc<FileCache>,
        store_url: ObjectStoreUrl,
        bytes_budget: u64,
    ) -> Self {
        Self::of(RowSignal::Logs, tenant, cache, store_url, bytes_budget)
    }

    /// Table over `tenant`'s visible metric samples or series files,
    /// reading through `store_url`.
    pub fn metrics(
        table: MetricTable,
        tenant: TenantId,
        cache: Arc<FileCache>,
        store_url: ObjectStoreUrl,
        bytes_budget: u64,
    ) -> Self {
        Self {
            table: Some(table),
            ..Self::of(RowSignal::Metrics, tenant, cache, store_url, bytes_budget)
        }
    }

    fn of(
        signal: RowSignal,
        tenant: TenantId,
        cache: Arc<FileCache>,
        store_url: ObjectStoreUrl,
        bytes_budget: u64,
    ) -> Self {
        Self {
            tenant,
            signal,
            table: None,
            cache,
            store_url,
            bytes_budget,
            hot: None,
            trace_index: None,
            text_index: None,
            source_order: false,
            hot_required: false,
            hot_charge: None,
            files: None,
        }
    }

    /// Reads `files` alone, a listing the caller took from one catalog
    /// snapshot, instead of the cache's visible files; pruning and the hot
    /// window's exclusions still apply to them.
    #[must_use]
    pub fn over_files(mut self, files: Arc<Vec<fs_catalog::FileRecord>>) -> Self {
        self.files = Some(files);
        self
    }

    /// Shows the samples' source order, for a reader that deduplicates
    /// them; no session table does.
    #[must_use]
    pub fn with_source_order(mut self) -> Self {
        self.source_order = self.table == Some(MetricTable::Samples);
        self
    }

    /// Charges the hot window's answers to `charge` as they arrive, and
    /// the copies that bring them to the table's types before those are
    /// made; a refused charge fails the scan as resources exhausted.
    #[must_use]
    pub fn with_hot_charge(mut self, charge: crate::hot::Charge) -> Self {
        self.hot_charge = Some(charge);
        self
    }

    /// Fails the scan with [`QueryError::HotUnavailable`] when an ingester
    /// does not answer, instead of noting it in the warnings.
    #[must_use]
    pub fn requiring_hot_window(mut self) -> Self {
        self.hot_required = true;
        self
    }

    /// Consults `trace_id` sidecars when the filters pin trace ids. Only
    /// span files have them, so a log table ignores it.
    #[must_use]
    pub fn with_trace_index(mut self, cache: Arc<crate::trace_index::TraceIndexCache>) -> Self {
        if self.signal == RowSignal::Traces {
            self.trace_index = Some(cache);
        }
        self
    }

    /// Consults `index` when the filters search the body, publishing what
    /// it pruned to `stats`. Only log files have text sidecars, so a span
    /// table ignores it.
    #[must_use]
    pub fn with_text_index(mut self, index: Arc<dyn FileIndex>, stats: Arc<QueryStats>) -> Self {
        if self.signal == RowSignal::Logs {
            self.text_index = Some((index, stats));
        }
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
        let planned = self.plan(filters, excluded).await?;
        Ok((planned.files, planned.bytes))
    }

    async fn plan(
        &self,
        filters: &[Expr],
        excluded: &std::collections::HashSet<fs_catalog::FileId>,
    ) -> Result<Planned, QueryError> {
        let pruning = self.pruning(filters);
        let all = match &self.files {
            Some(files) => Arc::clone(files),
            None => self.cache.files(self.tenant, self.catalog_signal()).await?,
        };
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
        let (files, text) = if let Some((index, probe)) = self.text_search(filters) {
            let owned: Vec<fs_catalog::FileRecord> =
                candidates.iter().map(|f| (*f).clone()).collect();
            let answers = index.select(&owned, &probe).await;
            let selections = crate::file_index::resolve(&owned, answers);
            let (files, tally) = crate::text_scan::apply(&candidates, &selections);
            (files, Some(tally))
        } else {
            let files = candidates
                .into_iter()
                .map(|f| {
                    PartitionedFile::new(f.path.clone(), u64::try_from(f.size_bytes).unwrap_or(0))
                })
                .collect();
            (files, None)
        };
        let bytes = files
            .iter()
            .fold(0u64, |sum, f| sum.saturating_add(f.object_meta.size));
        debug!(
            tenant = %self.tenant.0,
            signal = ?self.signal,
            candidates = all.len(),
            after_catalog,
            kept = files.len(),
            excluded = excluded.len(),
            bytes,
            ?pruning,
            ?text,
            "catalog pruning"
        );
        Ok(Planned { files, bytes, text })
    }

    /// The schema the table shows: the stored one, for spans followed by
    /// the derived columns.
    fn table_schema(&self) -> arrow::datatypes::SchemaRef {
        if self.derives() {
            crate::derived_columns::table_schema(&self.stored_schema())
        } else {
            self.stored_schema()
        }
    }

    /// True for the spans table, which shows the derived span columns.
    fn derives(&self) -> bool {
        self.signal == RowSignal::Traces && self.table.is_none()
    }

    /// The schema of the table's files and hot rows: its signal's, for
    /// metrics its table's, without the samples' source order unless it was
    /// asked for.
    fn stored_schema(&self) -> arrow::datatypes::SchemaRef {
        match self.table {
            Some(MetricTable::Series) => fs_schema::metrics::series_schema_v1(),
            Some(MetricTable::Samples) if self.source_order => {
                fs_schema::metrics::samples_schema_v1()
            }
            Some(MetricTable::Samples) => {
                let full = fs_schema::metrics::samples_schema_v1();
                let shown: Vec<usize> = (0..full.fields().len())
                    .filter(|i| full.field(*i).name() != fs_schema::metrics::col::SOURCE_ORDER)
                    .collect();
                full.project(&shown).map(Arc::new).unwrap_or(full)
            }
            None => schema_of(self.signal),
        }
    }

    /// The catalog's signal for the table's files.
    fn catalog_signal(&self) -> Signal {
        match self.table {
            Some(MetricTable::Series) => Signal::MetricSeries,
            Some(MetricTable::Samples) => Signal::MetricSamples,
            None => catalog_signal(self.signal),
        }
    }

    /// What `filters` say about the table's files: metric rows are timed
    /// by `unix_milli`.
    fn pruning(&self, filters: &[Expr]) -> Pruning {
        if self.table.is_some() {
            Pruning::from_metric_filters(filters)
        } else {
            Pruning::from_filters(filters)
        }
    }

    /// The text index and the probe of a body search it can prune.
    fn text_search(&self, filters: &[Expr]) -> Option<(&Arc<dyn FileIndex>, Probe)> {
        let (index, _) = self.text_index.as_ref()?;
        let probe = crate::text_probe::probe(filters);
        probe.prunes().then_some((index, probe))
    }
}

impl std::fmt::Debug for SignalTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignalTable")
            .field("tenant", &self.tenant.0)
            .field("signal", &self.signal)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for SignalTable {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.table_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // Pruning is a hint; DataFusion keeps evaluating every filter.
        Ok(filters
            .iter()
            .map(|f| {
                if self.derives() && crate::derived_columns::reads_derived(f) {
                    TableProviderFilterPushDown::Unsupported
                } else {
                    TableProviderFilterPushDown::Inexact
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let stored = self.stored_schema();
        let split = if self.derives() {
            crate::derived_columns::Split::of(&stored, projection)?
        } else {
            None
        };
        let Some(split) = split else {
            return self.scan_stored(state, projection, filters, limit).await;
        };
        let table = self.table_schema();
        let shown = match projection {
            Some(indices) => Arc::new(table.project(indices)?),
            None => table,
        };
        let input = self
            .scan_stored(state, Some(&split.stored.clone()), filters, limit)
            .await?;
        Ok(Arc::new(crate::derived_columns::DeriveExec::new(
            input, split, shown,
        )))
    }
}

impl SignalTable {
    /// The scan of the stored columns `projection` names, indices into
    /// [`SignalTable::stored_schema`].
    async fn scan_stored(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let started = std::time::Instant::now();
        let schema = self.stored_schema();
        let external = |e: QueryError| datafusion::error::DataFusionError::External(Box::new(e));

        // Hot window first: ingesters report which recently flushed files they
        // still serve, and those leave the file scan.
        let pruning = self.pruning(filters);
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
                let free: crate::hot::Charge = Arc::new(|_| true);
                let result = hot
                    .scan_charged(
                        self.tenant,
                        (self.signal, self.table),
                        &pruning,
                        (projected_names.clone(), None),
                        self.hot_charge.as_ref().unwrap_or(&free),
                    )
                    .await
                    .map_err(|e| external(e.into()))?;
                if !result.exhausted.is_empty() {
                    return Err(hot_exhausted(&result.exhausted));
                }
                if self.hot_required && !result.unavailable.is_empty() {
                    return Err(external(QueryError::HotUnavailable(result.unavailable)));
                }
                for ingester in &result.unavailable {
                    warnings.push(format!(
                        "ingester {ingester} did not answer; recent rows may be missing"
                    ));
                }
                hot_batches = result.batches;
                excluded = result.excluded_files;
            }
        }

        let planned = self.plan(filters, &excluded).await.map_err(external)?;
        if planned.bytes > self.bytes_budget {
            return Err(external(QueryError::OverBudget {
                bytes: planned.bytes,
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
            let coerced =
                coerce_charged(&hot_batches, &projected_schema, self.hot_charge.as_ref())?;
            Some(MemorySourceConfig::try_new_exec(
                &[coerced],
                Arc::clone(&projected_schema),
                None,
            )?)
        };
        let files_exec = self.files_exec(state, projection, filters, limit, planned, started)?;
        Ok(match (files_exec, hot_exec) {
            (Some(files), Some(hot)) => UnionExec::try_new(vec![files, hot])?,
            (Some(files), None) => files,
            (None, Some(hot)) => hot,
            (None, None) => Arc::new(EmptyExec::new(projected_schema)),
        })
    }
}

impl SignalTable {
    /// The scan of the planned files; `None` when there is none to read
    /// and no body search to account for. A body search reads them with
    /// the hidden file column, under a [`TextScanExec`] that drops it.
    fn files_exec(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        planned: Planned,
        started: std::time::Instant,
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        let schema = self.stored_schema();
        let text = planned
            .text
            .zip(self.text_index.as_ref().map(|(_, stats)| Arc::clone(stats)));
        if planned.files.is_empty() && text.is_none() {
            return Ok(None);
        }
        let mut projection = projection.cloned();
        let table_schema = if text.is_some() {
            let all = schema.fields().len();
            projection
                .get_or_insert_with(|| (0..all).collect())
                .push(all);
            TableSchema::builder(Arc::clone(&schema))
                .with_table_partition_cols(vec![crate::text_scan::file_field()])
                .build()
        } else {
            TableSchema::from(Arc::clone(&schema))
        };
        let scanned: Arc<dyn ExecutionPlan> = if planned.files.is_empty() {
            let full = table_schema.table_schema();
            let indices = projection.clone().unwrap_or_default();
            Arc::new(EmptyExec::new(Arc::new(full.project(&indices)?)))
        } else {
            let mut source = ParquetSource::new(table_schema)
                .with_pushdown_filters(true)
                .with_enable_page_index(true)
                .with_bloom_filter_on_read(true);
            if let Some(predicate) = conjunction(filters.iter().cloned()) {
                let df_schema = DFSchema::try_from(Arc::clone(&schema))?;
                let physical = state.create_physical_expr(predicate, &df_schema)?;
                source = source.with_predicate(physical);
            }
            let config = FileScanConfigBuilder::new(self.store_url.clone(), Arc::new(source))
                .with_file_group(FileGroup::new(planned.files))
                .with_projection_indices(projection)?
                .with_limit(limit)
                .build();
            DataSourceExec::from_data_source(config)
        };
        let Some((tally, published_to)) = text else {
            return Ok(Some(scanned));
        };
        // Columns the filters need are in the projection, since the filters
        // stay above the scan; if they cannot be planned here anyway, the
        // search is not measured rather than measured as matching nothing.
        let matches = conjunction(filters.iter().cloned()).and_then(|predicate| {
            let df_schema = DFSchema::try_from(scanned.schema()).ok()?;
            state.create_physical_expr(predicate, &df_schema).ok()
        });
        let measured = TextScanExec::new(
            scanned,
            matches,
            tally,
            published_to,
            started,
            limit.is_some(),
        )?;
        Ok(Some(Arc::new(measured)))
    }
}

/// A hot window answer past its charge, from `ingesters` when known.
fn hot_exhausted(ingesters: &[String]) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::ResourcesExhausted(format!(
        "hot window answers past the memory pool {}",
        ingesters.join(", ")
    ))
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

/// [`coerce_to_schema`] of each of `batches`, the columns each one casts
/// charged to `charge` before the batch is kept; a refused charge fails
/// with no more than that batch made.
fn coerce_charged(
    batches: &[arrow::record_batch::RecordBatch],
    schema: &arrow::datatypes::SchemaRef,
    charge: Option<&crate::hot::Charge>,
) -> DfResult<Vec<arrow::record_batch::RecordBatch>> {
    if let Some(charge) = charge {
        if !charge(std::mem::size_of_val(batches)) {
            return Err(hot_exhausted(&[]));
        }
    }
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        let coerced = coerce_to_schema(batch, schema)?;
        if let Some(charge) = charge {
            let cast: Vec<arrow::array::ArrayRef> = coerced
                .columns()
                .iter()
                .zip(batch.columns())
                .filter(|(made, given)| made.data_type() != given.data_type())
                .map(|(made, _)| Arc::clone(made))
                .collect();
            let made = fs_ingest::hot::batch_bytes(coerced.num_columns())
                + fs_ingest::hot::held_bytes_of(&cast);
            if !cast.is_empty() && !charge(made) {
                return Err(hot_exhausted(&[]));
            }
        }
        out.push(coerced);
    }
    Ok(out)
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

#[cfg(test)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use std::sync::Mutex;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;

    /// What `array` keeps alive, measured: it and its children at their
    /// size with their `Arc`, and each distinct allocation of their buffers
    /// at its capacity.
    fn kept_by(array: &arrow::array::ArrayRef) -> usize {
        let mut seen = std::collections::HashSet::new();
        let mut bytes = 0;
        let mut pending = vec![Arc::clone(array)];
        while let Some(array) = pending.pop() {
            bytes += std::mem::size_of_val(array.as_ref()) + 2 * std::mem::size_of::<usize>();
            let data = array.to_data();
            let nulls = data.nulls().map(|n| n.inner().inner().clone());
            for buffer in data.buffers().iter().chain(nulls.iter()) {
                if seen.insert(buffer.data_ptr().as_ptr().addr()) {
                    bytes += buffer.capacity();
                }
            }
            pending.extend(
                data.child_data()
                    .iter()
                    .cloned()
                    .map(arrow::array::make_array),
            );
        }
        bytes
    }

    #[test]
    fn each_coerced_batch_is_charged_before_it_is_kept() {
        let hydrated = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("n", DataType::Int64, false),
        ]));
        let declared = Arc::new(Schema::new(vec![
            Field::new(
                "name",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("n", DataType::Int64, false),
        ]));
        let batch = |offset: i64| {
            let names: Vec<String> = (0..4096)
                .map(|i| format!("name-{}", (i + offset) % 3000))
                .collect();
            RecordBatch::try_new(
                Arc::clone(&hydrated),
                vec![
                    Arc::new(StringArray::from(names)),
                    Arc::new(Int64Array::from_iter_values(0..4096)),
                ],
            )
            .unwrap()
        };
        let batches = [batch(0), batch(7)];
        let charges = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&charges);
        let charge: crate::hot::Charge = Arc::new(move |bytes| {
            recorded.lock().unwrap().push(bytes);
            true
        });
        let coerced = coerce_charged(&batches, &declared, Some(&charge)).unwrap();
        let charges = charges.lock().unwrap().clone();
        assert_eq!(charges.len(), 3, "the list, then one charge a batch made");
        assert!(charges[0] >= coerced.capacity() * std::mem::size_of::<RecordBatch>());
        for (made, charged) in coerced.iter().zip(&charges[1..]) {
            let kept = std::mem::size_of::<RecordBatch>()
                + made.num_columns() * std::mem::size_of::<arrow::array::ArrayRef>()
                + kept_by(made.column(0));
            assert!(*charged >= kept, "charged {charged} for {kept}");
        }
        let refuse: crate::hot::Charge = Arc::new(|_| false);
        assert!(matches!(
            coerce_charged(&batches, &declared, Some(&refuse)),
            Err(datafusion::error::DataFusionError::ResourcesExhausted(_))
        ));
    }
}
