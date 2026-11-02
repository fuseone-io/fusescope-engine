//! The set a metric pass writes: its samples and series files, written a
//! row group at a time into fixed buffers, and the postings of its series
//! file. What each holds is measured after every row group and admitted
//! before it, against the pass's budget.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, FixedSizeBinaryBuilder, Float64Builder, Int64Builder, StringDictionaryBuilder,
    UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{Int32Type, SchemaRef};
use arrow::record_batch::RecordBatch;
use chrono::{TimeZone, Utc};
use fs_catalog::{FileId, FileSetId, Level, NewFile, Signal, TenantId, TimeRange};
use fs_index::postings::{Abandoned, Binding, PostingsBuilder};
use fs_schema::metrics::{
    SERIES_SCHEMA_VERSION, SOURCE_ORDER_BYTES, SOURCE_ORDER_WIDTH, SeriesRow, samples_schema_v1,
    series_batch, series_schema_v2,
};
use object_store::PutPayload;
use object_store::path::Path;
use parquet::arrow::ArrowWriter;

use super::budget::{Budget, Part};
use super::merge::{Key, Sample};
use crate::CompactError;
use crate::output::{
    Chunks, FOOTER_COPIES, FOOTER_FIXED_BYTES, MAX_CHUNK_BYTES, MIN_CHUNK_BYTES, RowGroupCost,
    index_bytes,
};

/// Bytes a sample holds in a row group being gathered: its columns, its
/// keys in the two dictionaries and its source order.
const SAMPLE_BYTES: usize = 4 + 4 + 8 + 8 + 8 + 4 + SOURCE_ORDER_BYTES;

/// One file of the set being written.
struct File {
    id: FileId,
    path: Path,
    writer: ArrowWriter<Chunks>,
    cost: RowGroupCost,
    indexed: usize,
    index_bytes: usize,
    rows: usize,
}

impl File {
    fn new(
        schema: &SchemaRef,
        properties: parquet::file::properties::WriterProperties,
        path: Path,
        id: FileId,
        share: usize,
    ) -> Result<Self, CompactError> {
        let cost = RowGroupCost::new(schema, &properties)?;
        let chunk = (share / 64).clamp(MIN_CHUNK_BYTES, MAX_CHUNK_BYTES);
        Ok(Self {
            id,
            path,
            writer: ArrowWriter::try_new(Chunks::new(chunk), Arc::clone(schema), Some(properties))?,
            cost,
            indexed: 0,
            index_bytes: 0,
            rows: 0,
        })
    }

    /// What it holds: its buffers, the encoder and the footer it will
    /// build, with the copies closing makes.
    fn memory(&self) -> usize {
        self.writer.inner().held()
            + self.writer.memory_size()
            + FOOTER_COPIES * (self.index_bytes + FOOTER_FIXED_BYTES)
    }

    /// What writing a row group of `rows` rows and `payload` bytes holds
    /// besides it: its encoding and its indexes.
    fn growth(&self, rows: usize, payload: usize) -> usize {
        payload
            .saturating_mul(2)
            .saturating_add(self.cost.of(rows, payload))
    }

    fn write(&mut self, group: &RecordBatch) -> Result<(), CompactError> {
        self.writer.write(group)?;
        self.writer.flush()?;
        self.rows += group.num_rows();
        let groups = self.writer.flushed_row_groups();
        self.index_bytes = self
            .index_bytes
            .saturating_add(index_bytes(groups.get(self.indexed..).unwrap_or_default()));
        self.indexed = groups.len();
        Ok(())
    }
}

/// The samples of the row group being gathered.
#[derive(Default)]
struct Gathered {
    names: StringDictionaryBuilder<Int32Type>,
    temporalities: StringDictionaryBuilder<Int32Type>,
    fingerprints: UInt64Builder,
    times: Int64Builder,
    values: Float64Builder,
    flags: UInt32Builder,
    orders: Option<FixedSizeBinaryBuilder>,
    rows: usize,
    bytes: usize,
}

impl Gathered {
    fn push(&mut self, key: &Key, sample: &Sample) -> Result<(), CompactError> {
        self.names.append(&key.name)?;
        self.temporalities.append(sample.temporality.as_str())?;
        self.fingerprints.append_value(key.fingerprint);
        self.times.append_value(sample.unix_milli);
        self.values.append_value(sample.value);
        self.flags.append_value(sample.flags);
        self.orders
            .get_or_insert_with(|| FixedSizeBinaryBuilder::new(SOURCE_ORDER_WIDTH))
            .append_value(sample.order)?;
        self.rows += 1;
        self.bytes += SAMPLE_BYTES;
        Ok(())
    }

    fn finish(&mut self) -> Result<RecordBatch, CompactError> {
        let mut orders = self
            .orders
            .take()
            .unwrap_or_else(|| FixedSizeBinaryBuilder::new(SOURCE_ORDER_WIDTH));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.names.finish()),
            Arc::new(self.temporalities.finish()),
            Arc::new(self.fingerprints.finish()),
            Arc::new(self.times.finish()),
            Arc::new(self.values.finish()),
            Arc::new(self.flags.finish()),
            Arc::new(orders.finish()),
        ];
        self.rows = 0;
        self.bytes = 0;
        Ok(RecordBatch::try_new(samples_schema_v1(), columns)?)
    }
}

/// One set of a pass's outputs being written.
pub(crate) struct SetWriter {
    tenant: TenantId,
    level: Level,
    set: FileSetId,
    samples: File,
    series: File,
    gathered: Gathered,
    /// Series rows of the row group being gathered, and their bytes.
    pending: Vec<SeriesRow>,
    pending_bytes: usize,
    /// Bytes of the series rows of the group the pass holds.
    group_bytes: usize,
    postings: Option<PostingsBuilder>,
    row_group_rows: usize,
    /// Least and greatest sample time, and series hour.
    times: (i64, i64),
    hours: (i64, i64),
    /// Keys of more than one identity written.
    pub(crate) collisions: u64,
}

/// A set finished: its catalog rows, its two files' bytes to upload in
/// order, and its postings, when it has them.
pub(crate) type Finished = (Closed, [(Path, PutPayload); 2], Option<(String, Vec<u8>)>);

/// A set written and uploaded.
pub(crate) struct Closed {
    pub(crate) set: FileSetId,
    pub(crate) files: [NewFile; 2],
    pub(crate) bytes: u64,
}

impl SetWriter {
    /// A new set of `level` for `tenant` under `start`, its hour or day.
    pub(crate) fn new(
        root: &Path,
        (tenant, level, start): (TenantId, Level, chrono::DateTime<Utc>),
        row_group_rows: usize,
        budget: &Budget,
    ) -> Result<Self, CompactError> {
        let properties = |rows| {
            if level == Level::L2 {
                fs_schema::parquet::metric_l2_writer_properties(rows)
            } else {
                fs_schema::parquet::metric_writer_properties(rows)
            }
        };
        let path = |signal, id| {
            if level == Level::L2 {
                crate::merge::l2_path(root, tenant, signal, start, id)
            } else {
                crate::merge::l1_path(root, tenant, signal, start, id)
            }
        };
        let (samples_id, series_id) = (FileId::new(), FileId::new());
        Ok(Self {
            tenant,
            level,
            set: FileSetId::new(),
            samples: File::new(
                &samples_schema_v1(),
                properties(row_group_rows)?,
                path(Signal::MetricSamples, samples_id),
                samples_id,
                budget.share(Part::Samples),
            )?,
            series: File::new(
                &series_schema_v2(),
                properties(row_group_rows)?,
                path(Signal::MetricSeries, series_id),
                series_id,
                budget.share(Part::Series),
            )?,
            gathered: Gathered::default(),
            pending: Vec::new(),
            pending_bytes: 0,
            group_bytes: 0,
            postings: Some(PostingsBuilder::new(budget.share(Part::Postings))),
            row_group_rows: row_group_rows.max(1),
            times: (i64::MAX, i64::MIN),
            hours: (i64::MAX, i64::MIN),
            collisions: 0,
        })
    }

    /// Rows written or gathered.
    pub(crate) fn is_empty(&self) -> bool {
        self.samples.rows == 0 && self.gathered.rows == 0
    }

    /// Bytes encoded so far, both files.
    pub(crate) fn bytes_written(&self) -> usize {
        self.samples.writer.bytes_written() + self.series.writer.bytes_written()
    }

    /// Whether the files have room for one more row group each, the
    /// largest a group may write.
    pub(crate) fn has_room(&self, budget: &Budget) -> bool {
        let samples = self.samples.memory()
            + self
                .samples
                .growth(self.row_group_rows, self.row_group_rows * SAMPLE_BYTES);
        let series = self.series.memory()
            + self
                .series
                .growth(self.row_group_rows, budget.share(Part::Gather) / 2);
        samples <= budget.share(Part::Samples) && series <= budget.share(Part::Series)
    }

    /// Writes the series rows of a group, `collided` when they are more
    /// than one identity, into the set.
    ///
    /// # Errors
    /// A row past the gather budget, or an Arrow or Parquet failure.
    pub(crate) fn add_series(
        &mut self,
        rows: &[SeriesRow],
        collided: bool,
        budget: &mut Budget,
    ) -> Result<(), CompactError> {
        if self.group_bytes == 0 {
            self.group_bytes = rows.iter().map(SeriesRow::bytes).sum();
            budget.take(Part::Gather, self.group_bytes)?;
        }
        for row in rows {
            let bytes = row.bytes();
            if !self.pending.is_empty()
                && (self.pending.len() >= self.row_group_rows || !budget.fits(Part::Gather, bytes))
            {
                self.flush_series(budget)?;
            }
            budget.take(Part::Gather, bytes)?;
            let ordinal = u64::try_from(self.series.rows + self.pending.len()).unwrap_or(u64::MAX);
            if let Some(postings) = self.postings.as_mut() {
                let labels = row.labels.iter().map(|(k, v)| (&**k, &**v));
                if postings.add(ordinal, labels) == Err(Abandoned) {
                    self.postings = None;
                }
            }
            budget.set(
                Part::Postings,
                self.postings.as_ref().map_or(0, PostingsBuilder::held),
            );
            self.hours = (
                self.hours.0.min(row.unix_milli),
                self.hours.1.max(row.unix_milli),
            );
            self.pending_bytes += bytes;
            self.pending.push(row.clone());
        }
        self.collisions += u64::from(collided);
        Ok(())
    }

    /// The group's series rows are no longer held.
    pub(crate) fn end_group(&mut self, budget: &mut Budget) {
        self.group_bytes = 0;
        self.charge_gather(budget);
    }

    fn charge_gather(&self, budget: &mut Budget) {
        budget.set(
            Part::Gather,
            self.gathered.bytes + self.pending_bytes + self.group_bytes,
        );
    }

    /// Writes `sample` of `key` into the set.
    ///
    /// # Errors
    /// The row group past its budget, or an Arrow or Parquet failure.
    pub(crate) fn add_sample(
        &mut self,
        key: &Key,
        sample: &Sample,
        budget: &mut Budget,
    ) -> Result<(), CompactError> {
        if self.gathered.rows >= self.row_group_rows || !budget.fits(Part::Gather, SAMPLE_BYTES) {
            self.flush_samples(budget)?;
        }
        budget.take(Part::Gather, SAMPLE_BYTES + key.name.len())?;
        self.gathered.push(key, sample)?;
        self.gathered.bytes += key.name.len();
        self.times = (
            self.times.0.min(sample.unix_milli),
            self.times.1.max(sample.unix_milli),
        );
        Ok(())
    }

    fn flush_samples(&mut self, budget: &mut Budget) -> Result<(), CompactError> {
        if self.gathered.rows == 0 {
            return Ok(());
        }
        let growth = self.samples.growth(self.gathered.rows, self.gathered.bytes);
        budget.take(Part::Samples, growth)?;
        let batch = self.gathered.finish()?;
        self.samples.write(&batch)?;
        drop(batch);
        budget.set(Part::Samples, self.samples.memory());
        self.charge_gather(budget);
        Ok(())
    }

    fn flush_series(&mut self, budget: &mut Budget) -> Result<(), CompactError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let growth = self.series.growth(self.pending.len(), self.pending_bytes);
        budget.take(Part::Series, growth)?;
        let batch = series_batch(&self.pending, self.pending.len())?;
        self.series.write(&batch)?;
        drop(batch);
        self.pending.clear();
        self.pending_bytes = 0;
        budget.set(Part::Series, self.series.memory());
        self.charge_gather(budget);
        Ok(())
    }

    /// Finishes both files and the postings, for the caller to upload in
    /// that order, with their catalog rows; the parts they held freed.
    ///
    /// # Errors
    /// The postings past their budget once encoded, or a Parquet failure.
    pub(crate) fn finish(mut self, budget: &mut Budget) -> Result<Finished, CompactError> {
        self.flush_samples(budget)?;
        self.flush_series(budget)?;
        let (tenant, level) = (self.tenant, self.level);
        let file = |f: File,
                    signal,
                    range: (i64, i64)|
         -> Result<(NewFile, PutPayload, [u8; 32]), CompactError> {
            let (bytes, size, sha) = f.writer.into_inner()?.finish();
            let at = |ms: i64| Utc.timestamp_millis_opt(ms).single().unwrap_or_default();
            Ok((
                NewFile {
                    id: f.id,
                    origin: f.id,
                    tenant_id: tenant,
                    signal,
                    level,
                    path: f.path.to_string(),
                    size_bytes: i64::try_from(size).unwrap_or(i64::MAX),
                    row_count: i64::try_from(f.rows).unwrap_or(i64::MAX),
                    range: TimeRange {
                        start: at(range.0),
                        end: at(range.1),
                    },
                    services: Vec::new(),
                    summary: None,
                    content_sha256: Some(sha),
                    usage_accounted: true,
                },
                bytes,
                sha,
            ))
        };
        let (samples_path, series_path) = (self.samples.path.clone(), self.series.path.clone());
        let series_rows = self.series.rows;
        let (samples, samples_bytes, _) = file(self.samples, Signal::MetricSamples, self.times)?;
        let (series, series_bytes, series_sha) =
            file(self.series, Signal::MetricSeries, self.hours)?;
        let postings = match (self.postings, u32::try_from(series_rows)) {
            (Some(builder), Ok(row_count)) => {
                let binding = Binding {
                    path: series.path.clone(),
                    sha256: series_sha,
                    schema_version: SERIES_SCHEMA_VERSION,
                    row_count,
                };
                let encoded = builder.encoded_len(&binding);
                if budget.fits(Part::Postings, encoded) {
                    budget.take(Part::Postings, encoded)?;
                    let path = fs_index::postings::sidecar_path(&series.path)
                        .ok_or(CompactError::Merge("a series path without postings"))?;
                    Some((path, builder.finish(&binding)?))
                } else {
                    None
                }
            }
            _ => None,
        };
        budget.set(Part::Samples, 0);
        budget.set(Part::Series, 0);
        budget.set(Part::Gather, 0);
        let bytes = u64::try_from(samples.size_bytes + series.size_bytes).unwrap_or(0);
        Ok((
            Closed {
                set: self.set,
                files: [samples, series],
                bytes,
            },
            [(samples_path, samples_bytes), (series_path, series_bytes)],
            postings,
        ))
    }
}
