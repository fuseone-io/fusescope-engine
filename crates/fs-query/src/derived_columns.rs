//! The spans table's derived columns over its stored ones.
//!
//! The table shows `http_url`, `http_host`, `external_http_method` and
//! `external_http_url` after the stored columns. A scan that needs one
//! reads [`derived::INPUTS`] in their place, from the files and the hot
//! window alike, and [`DeriveExec`] computes the columns above the union.
//! Filters on them are never pushed to a scan: the table declares them
//! unsupported, so they run above. Each batch's derived arrays are charged
//! to the memory pool before they are made, and the charge travels with
//! their buffers, released when the last of them is dropped.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Column, Result as DfResult};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use fs_schema::spans::derived::{self, Derived};
use futures_util::StreamExt;

/// The name derived arrays are charged under.
pub const CONSUMER: &str = "derived span columns";

/// `stored` followed by the derived columns.
pub fn table_schema(stored: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<_> = stored.fields().iter().cloned().collect();
    fields.extend(Derived::ALL.iter().map(|d| Arc::new(d.field())));
    Arc::new(Schema::new_with_metadata(fields, stored.metadata().clone()))
}

/// True when `filter` reads a derived column.
pub fn reads_derived(filter: &Expr) -> bool {
    filter
        .column_refs()
        .iter()
        .any(|c: &&Column| Derived::of(&c.name).is_some())
}

/// What a scan of the table reads and shows: the stored columns it reads,
/// and for each shown column, where it comes from.
#[derive(Debug)]
pub struct Split {
    /// Indices into the stored schema, in the order the stored scan yields them.
    pub stored: Vec<usize>,
    shown: Vec<Shown>,
    wanted: Vec<Derived>,
}

#[derive(Debug, Clone, Copy)]
enum Shown {
    Stored(usize),
    Derived(usize),
}

impl Split {
    /// The split of `projection`, indices into [`table_schema`] of `stored`;
    /// `None` when it shows no derived column, so the stored scan serves it.
    ///
    /// # Errors
    /// An index past the table's columns.
    pub fn of(stored: &SchemaRef, projection: Option<&Vec<usize>>) -> DfResult<Option<Self>> {
        let width = stored.fields().len();
        let all: Vec<usize> = (0..width + Derived::ALL.len()).collect();
        let projection = projection.unwrap_or(&all);
        if projection.iter().all(|i| *i < width) {
            return Ok(None);
        }
        let mut split = Self {
            stored: Vec::new(),
            shown: Vec::with_capacity(projection.len()),
            wanted: Vec::new(),
        };
        for &index in projection {
            let shown = match index.checked_sub(width) {
                Some(d) => {
                    let Some(column) = Derived::ALL.get(d) else {
                        return datafusion::common::internal_err!(
                            "column {index} past the spans table"
                        );
                    };
                    split.wanted.push(*column);
                    Shown::Derived(split.wanted.len() - 1)
                }
                None => Shown::Stored(split.read(index)),
            };
            split.shown.push(shown);
        }
        for input in derived::INPUTS {
            if let Ok(index) = stored.index_of(input) {
                split.read(index);
            }
        }
        Ok(Some(split))
    }

    /// The position of stored column `index` in the stored scan, reading it.
    fn read(&mut self, index: usize) -> usize {
        if let Some(at) = self.stored.iter().position(|i| *i == index) {
            return at;
        }
        self.stored.push(index);
        self.stored.len() - 1
    }
}

/// Computes the derived columns of a [`Split`] above its stored scan.
#[derive(Debug)]
pub struct DeriveExec {
    input: Arc<dyn ExecutionPlan>,
    split: Arc<Split>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl DeriveExec {
    /// `input`, the stored scan of `split`, showing `schema`.
    pub fn new(input: Arc<dyn ExecutionPlan>, split: Split, schema: SchemaRef) -> Self {
        Self::over(input, Arc::new(split), schema)
    }

    fn over(input: Arc<dyn ExecutionPlan>, split: Arc<Split>, schema: SchemaRef) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Self {
            input,
            split,
            schema,
            properties,
        }
    }
}

impl DisplayAs for DeriveExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.split.wanted.iter().map(|d| d.name()).collect();
        write!(f, "DeriveExec: {}", names.join(", "))
    }
}

impl ExecutionPlan for DeriveExec {
    fn name(&self) -> &'static str {
        "DeriveExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DfResult<TreeNodeRecursion>,
    ) -> DfResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let [input] = <[_; 1]>::try_from(children)
            .map_err(|_| datafusion::common::DataFusionError::Internal("one child".into()))?;
        Ok(Arc::new(Self::over(
            input,
            Arc::clone(&self.split),
            Arc::clone(&self.schema),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let inner = self.input.execute(partition, Arc::clone(&context))?;
        let mut reservation = MemoryConsumer::new(CONSUMER).register(context.memory_pool());
        let split = Arc::clone(&self.split);
        let schema = Arc::clone(&self.schema);
        let stream = inner.map(move |batch| derive(&batch?, &split, &schema, &mut reservation));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}

/// `batch` with `split`'s derived columns, charged to `reservation` before
/// they are made and held by their buffers after.
fn derive(
    batch: &RecordBatch,
    split: &Split,
    schema: &SchemaRef,
    reservation: &mut MemoryReservation,
) -> DfResult<RecordBatch> {
    let derivation = derived::derive(batch, &split.wanted)
        .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
    reservation.try_grow(derivation.bytes())?;
    let made = derivation.into_arrays();
    crate::claim::claim(&made, reservation.take());
    let columns = split
        .shown
        .iter()
        .map(|shown| {
            let column = match *shown {
                Shown::Stored(at) => batch.columns().get(at),
                Shown::Derived(at) => made.get(at),
            };
            column.map(Arc::clone).ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(format!("{shown:?} past the batch"))
            })
        })
        .collect::<DfResult<Vec<ArrayRef>>>()?;
    let options = arrow::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns,
        &options,
    )?)
}
