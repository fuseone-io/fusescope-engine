//! A body search over log files: what the text index lets the scan skip,
//! and how many files it kept had a matching row.
//!
//! An eligible scan reads each file with a hidden partition column holding
//! the file's position in the scan. [`TextScanExec`] evaluates the query's
//! filters on every batch to note the files with a matching row, drops the
//! column and, once every partition reached its end, publishes the files
//! and row groups after the catalog, after the index and matched, the
//! precision and the time since the scan's planning began, index lookup
//! included, all together. A scan cut short, by a limit, a cancellation or
//! an error, or one whose filters could not be planned to tell a match,
//! publishes nothing.

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Field, FieldRef, SchemaRef, UInt32Type};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result as DfResult, ScalarValue};
use datafusion::datasource::physical_plan::parquet::ParquetAccessPlan;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream,
};
use datafusion_datasource::PartitionedFile;
use fs_catalog::FileRecord;
use futures_util::{Stream, StreamExt};

use crate::file_index::{FileSelection, Selection};
use crate::querier::QueryStats;

/// Name of the hidden partition column holding a file's position.
pub const FILE_COLUMN: &str = "__fs_file";

/// The hidden partition column.
pub fn file_field() -> FieldRef {
    Arc::new(Field::new(FILE_COLUMN, DataType::UInt32, false))
}

/// Counts of one eligible search, before its scan runs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TextTally {
    /// Files the catalog kept.
    pub files_catalog: u64,
    /// Files the index kept.
    pub files_index: u64,
    /// Row groups of the files whose geometry the index checked.
    pub row_groups_catalog: u64,
    /// Of those, the row groups the index kept.
    pub row_groups_index: u64,
}

/// The files to scan for `selections`, one per file of `files` in order:
/// a skipped file is left out, a file of some row groups reads only them.
/// Every file kept carries its position as the hidden column's value.
pub fn apply(
    files: &[&FileRecord],
    selections: &[FileSelection],
) -> (Vec<PartitionedFile>, TextTally) {
    let mut tally = TextTally {
        files_catalog: files.len() as u64,
        ..TextTally::default()
    };
    let mut kept = Vec::with_capacity(files.len());
    for (file, answer) in files.iter().zip(selections) {
        let count = answer.row_groups.map_or(0, |n| n as u64);
        tally.row_groups_catalog += count;
        let size = u64::try_from(file.size_bytes).unwrap_or(0);
        let position = ScalarValue::UInt32(u32::try_from(kept.len()).ok());
        let partitioned = PartitionedFile::new(file.path.clone(), size);
        let partitioned = match &answer.selection {
            Selection::Skip => continue,
            Selection::All => {
                tally.row_groups_index += count;
                partitioned
            }
            Selection::RowGroups(groups) => {
                tally.row_groups_index += groups.len() as u64;
                let mut plan = ParquetAccessPlan::new_none(answer.row_groups.unwrap_or(0));
                for group in groups {
                    plan.scan(*group);
                }
                partitioned.with_extension(plan)
            }
        };
        let mut partitioned = partitioned;
        partitioned.partition_values = vec![position];
        kept.push(partitioned);
    }
    tally.files_index = kept.len() as u64;
    (kept, tally)
}

/// What the partitions of one scan share until the last one ends.
struct Shared {
    tally: TextTally,
    stats: Arc<QueryStats>,
    started: Instant,
    /// False once a partition failed or the scan was limited.
    publishable: AtomicBool,
    running: AtomicUsize,
    matched: Mutex<HashSet<u32>>,
}

impl Shared {
    fn finish_partition(&self) {
        if self.running.fetch_sub(1, Ordering::AcqRel) == 1
            && self.publishable.swap(false, Ordering::AcqRel)
        {
            let matched = self.matched.lock().map_or(0, |m| m.len() as u64);
            publish(&self.stats, self.tally, matched, self.started.elapsed());
        }
    }
}

fn publish(stats: &QueryStats, tally: TextTally, matched: u64, took: Duration) {
    let add =
        |counter: &std::sync::atomic::AtomicU64, n: u64| counter.fetch_add(n, Ordering::Relaxed);
    add(&stats.text_searches, 1);
    add(&stats.text_files_catalog, tally.files_catalog);
    add(&stats.text_files_index, tally.files_index);
    add(&stats.text_files_matched, matched);
    add(&stats.text_row_groups_catalog, tally.row_groups_catalog);
    add(&stats.text_row_groups_index, tally.row_groups_index);
    // Nothing kept is nothing read in vain.
    let precision = if tally.files_index == 0 {
        1.0
    } else {
        precision_of(matched, tally.files_index)
    };
    stats.text_precision.observe(precision);
    stats.text_latency.0.observe(took);
}

#[allow(clippy::cast_precision_loss)]
fn precision_of(matched: u64, kept: u64) -> f64 {
    matched as f64 / kept as f64
}

/// Scans `input`, whose last column is [`FILE_COLUMN`], notes the files
/// where `predicate` holds and drops the column.
#[derive(Debug)]
pub struct TextScanExec {
    input: Arc<dyn ExecutionPlan>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    shared: Arc<SharedHandle>,
}

/// `Shared` behind a `Debug` the plan display can print.
struct SharedHandle(Shared);

impl std::fmt::Debug for SharedHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TextScan")
            .field("tally", &self.0.tally)
            .finish_non_exhaustive()
    }
}

impl TextScanExec {
    /// Wraps `input` for a search whose planning began at `started`.
    /// `predicate`, over `input`'s schema, decides a match; without one, as
    /// for a `limited` scan, nothing is published.
    ///
    /// # Errors
    /// An input whose last column is not [`FILE_COLUMN`].
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        tally: TextTally,
        stats: Arc<QueryStats>,
        started: Instant,
        limited: bool,
    ) -> DfResult<Self> {
        let full = input.schema();
        let Some(last) = full.fields().len().checked_sub(1) else {
            return datafusion::common::internal_err!("a text scan without its file column");
        };
        if full.field(last).name() != FILE_COLUMN {
            return datafusion::common::internal_err!("a text scan without its file column");
        }
        let schema = Arc::new(full.project(&(0..last).collect::<Vec<_>>())?);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        let partitions = input.output_partitioning().partition_count();
        let shared = Shared {
            tally,
            stats,
            started,
            publishable: AtomicBool::new(!limited && predicate.is_some()),
            running: AtomicUsize::new(partitions),
            matched: Mutex::new(HashSet::new()),
        };
        Ok(Self {
            input,
            predicate,
            schema,
            properties,
            shared: Arc::new(SharedHandle(shared)),
        })
    }
}

impl DisplayAs for TextScanExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TextScanExec")
    }
}

impl ExecutionPlan for TextScanExec {
    fn name(&self) -> &'static str {
        "TextScanExec"
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
        let shared = &self.shared.0;
        Ok(Arc::new(Self::new(
            input,
            self.predicate.clone(),
            shared.tally,
            Arc::clone(&shared.stats),
            shared.started,
            !shared.publishable.load(Ordering::Acquire),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let inner = self.input.execute(partition, context)?;
        Ok(Box::pin(Measured {
            inner,
            schema: Arc::clone(&self.schema),
            predicate: self.predicate.clone(),
            shared: Arc::clone(&self.shared),
            ended: false,
        }))
    }
}

struct Measured {
    inner: SendableRecordBatchStream,
    schema: SchemaRef,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    shared: Arc<SharedHandle>,
    ended: bool,
}

impl Measured {
    fn measure(&self, batch: &RecordBatch) -> DfResult<RecordBatch> {
        let last = batch.num_columns() - 1;
        if let Some(predicate) = &self.predicate {
            let holds = predicate.evaluate(batch)?.into_array(batch.num_rows())?;
            let holds = holds.as_boolean();
            let files = batch.column(last).as_primitive::<UInt32Type>();
            let mut seen: Vec<u32> = (0..batch.num_rows())
                .filter(|&row| holds.is_valid(row) && holds.value(row))
                .map(|row| files.value(row))
                .collect();
            seen.sort_unstable();
            seen.dedup();
            if !seen.is_empty() {
                if let Ok(mut matched) = self.shared.0.matched.lock() {
                    matched.extend(seen);
                }
            }
        }
        Ok(batch.project(&(0..last).collect::<Vec<_>>())?)
    }
}

impl Stream for Measured {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let polled = self.inner.poll_next_unpin(cx);
        let Poll::Ready(next) = polled else {
            return Poll::Pending;
        };
        match next {
            Some(Ok(batch)) => {
                let out = self.measure(&batch);
                if out.is_err() {
                    self.shared.0.publishable.store(false, Ordering::Release);
                }
                Poll::Ready(Some(out))
            }
            Some(Err(e)) => {
                self.shared.0.publishable.store(false, Ordering::Release);
                Poll::Ready(Some(Err(e)))
            }
            None => {
                if !self.ended {
                    self.ended = true;
                    self.shared.0.finish_partition();
                }
                Poll::Ready(None)
            }
        }
    }
}

impl RecordBatchStream for Measured {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use arrow::array::{StringArray, UInt32Array};
    use arrow::datatypes::Schema;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use futures_util::TryStreamExt;

    use super::*;

    fn batch(bodies: &[&str], file: u32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("body", DataType::Utf8, false),
            Field::new(FILE_COLUMN, DataType::UInt32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(bodies.to_vec())),
                Arc::new(UInt32Array::from(vec![file; bodies.len()])),
            ],
        )
        .unwrap()
    }

    /// Two partitions, one file each; the predicate matches `body = 'hit'`.
    fn exec(stats: &Arc<QueryStats>, limited: bool) -> TextScanExec {
        let parts = vec![vec![batch(&["hit", "miss"], 0)], vec![batch(&["miss"], 1)]];
        let input = MemorySourceConfig::try_new_exec(&parts, batch(&[], 0).schema(), None).unwrap();
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("body", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Utf8(Some("hit".into())))),
        ));
        let tally = TextTally {
            files_catalog: 3,
            files_index: 2,
            row_groups_catalog: 6,
            row_groups_index: 2,
        };
        TextScanExec::new(
            input,
            Some(predicate),
            tally,
            Arc::clone(stats),
            Instant::now(),
            limited,
        )
        .unwrap()
    }

    async fn drain(exec: &TextScanExec, partition: usize) -> Vec<RecordBatch> {
        let ctx = Arc::new(TaskContext::default());
        exec.execute(partition, ctx)
            .unwrap()
            .try_collect()
            .await
            .unwrap()
    }

    fn published(stats: &QueryStats) -> [u64; 4] {
        [
            stats.text_searches.load(Ordering::Relaxed),
            stats.text_files_index.load(Ordering::Relaxed),
            stats.text_files_matched.load(Ordering::Relaxed),
            stats.text_precision.0.count(),
        ]
    }

    #[tokio::test]
    async fn every_partition_at_its_end_publishes_once_and_the_file_column_is_dropped() {
        let stats = Arc::new(QueryStats::default());
        let exec = exec(&stats, false);
        let first = drain(&exec, 0).await;
        assert_eq!(first[0].num_columns(), 1, "the file column is dropped");
        assert_eq!(
            published(&stats),
            [0, 0, 0, 0],
            "one partition is still running"
        );
        drain(&exec, 1).await;
        assert_eq!(published(&stats), [1, 2, 1, 1]);
        assert_eq!(stats.text_row_groups_catalog.load(Ordering::Relaxed), 6);
        assert_eq!(stats.text_latency.0.count(), 1);
    }

    #[tokio::test]
    async fn a_partition_dropped_before_its_end_or_a_limited_scan_publishes_nothing() {
        let stats = Arc::new(QueryStats::default());
        let cut = exec(&stats, false);
        drain(&cut, 1).await;
        let ctx = Arc::new(TaskContext::default());
        let mut stream = cut.execute(0, ctx).unwrap();
        stream.try_next().await.unwrap();
        drop(stream);
        assert_eq!(published(&stats), [0, 0, 0, 0]);

        let limited = exec(&stats, true);
        drain(&limited, 0).await;
        drain(&limited, 1).await;
        assert_eq!(published(&stats), [0, 0, 0, 0]);
    }

    #[tokio::test]
    async fn a_scan_that_cannot_tell_a_match_publishes_nothing() {
        let stats = Arc::new(QueryStats::default());
        let parts = vec![vec![batch(&["hit"], 0)]];
        let input = MemorySourceConfig::try_new_exec(&parts, batch(&[], 0).schema(), None).unwrap();
        let exec = TextScanExec::new(
            input,
            None,
            TextTally::default(),
            Arc::clone(&stats),
            Instant::now(),
            false,
        )
        .unwrap();
        drain(&exec, 0).await;
        assert_eq!(published(&stats), [0, 0, 0, 0], "not a precision of zero");
    }

    #[tokio::test]
    async fn a_perfect_precision_lands_in_a_finite_bucket() {
        let stats = Arc::new(QueryStats::default());
        let exec = exec(&stats, false);
        drain(&exec, 0).await;
        drain(&exec, 1).await;
        stats.text_precision.observe(1.0);
        let h = &stats.text_precision.0;
        let finite = h.cumulative()[h.bounds().len() - 1];
        assert_eq!(h.bounds().last(), Some(&1.0));
        assert_eq!(finite, h.count(), "no precision falls in +Inf");
    }

    #[test]
    fn a_skipped_file_is_left_out_and_row_groups_read_only_theirs() {
        use fs_catalog::{FileId, FileState, Level, Signal, TenantId, TimeRange};
        let now = chrono::Utc::now();
        let record = |n: u128| FileRecord {
            id: FileId(uuid::Uuid::from_u128(n)),
            origin: FileId(uuid::Uuid::from_u128(n)),
            tenant_id: TenantId(uuid::Uuid::nil()),
            signal: Signal::Logs,
            level: Level::L0,
            path: format!("t/x/logs/l0/{n}.parquet"),
            size_bytes: 10,
            row_count: 1,
            range: TimeRange {
                start: now,
                end: now,
            },
            services: vec![],
            summary: None,
            state: FileState::Visible,
            created_at: now,
            content_sha256: None,
            usage_accounted: false,
            quarantined_at: None,
        };
        let files = [record(1), record(2), record(3)];
        let refs: Vec<&FileRecord> = files.iter().collect();
        let selections = [
            FileSelection {
                file: files[0].id,
                selection: Selection::Skip,
                row_groups: Some(4),
            },
            FileSelection {
                file: files[1].id,
                selection: Selection::RowGroups(vec![1, 3].into()),
                row_groups: Some(4),
            },
            FileSelection {
                file: files[2].id,
                selection: Selection::All,
                row_groups: None,
            },
        ];
        let (kept, tally) = apply(&refs, &selections);
        assert_eq!(
            tally,
            TextTally {
                files_catalog: 3,
                files_index: 2,
                row_groups_catalog: 8,
                row_groups_index: 2
            }
        );
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].partition_values, [ScalarValue::UInt32(Some(0))]);
        assert_eq!(kept[1].partition_values, [ScalarValue::UInt32(Some(1))]);
        let plan = kept[0].extension::<ParquetAccessPlan>().unwrap();
        assert_eq!(plan.row_group_indexes(), [1, 3]);
        assert!(kept[1].extension::<ParquetAccessPlan>().is_none());
    }
}
