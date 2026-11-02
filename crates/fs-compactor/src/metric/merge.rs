//! The merge of a metric pass: the samples and series files of its sets,
//! read in step by their common key, `(metric_name, fingerprint, hour)`.
//!
//! A group is one key: its series rows, merged as a request merges them,
//! two identities of one key both kept, and its samples, one a fingerprint
//! and millisecond, the largest source order kept of a duplicate. A sample
//! kept is copied out of the batch it was read in, so an input holds only
//! the batch being read.

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{
    Array, AsArray, FixedSizeBinaryArray, Float64Array, Int64Array, StringArray, UInt32Array,
    UInt64Array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Float64Type, Int64Type, UInt32Type, UInt64Type};
use arrow::record_batch::RecordBatch;
use fs_schema::metrics::{
    SOURCE_ORDER_BYTES, SOURCE_ORDER_WIDTH, SeriesRow, Temporality, col, series_rows,
    series_schema_v1, series_schema_v2,
};

use crate::CompactError;
use crate::read;

const MILLIS_PER_HOUR: i64 = 3_600_000;

/// A group's key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Key {
    pub(crate) name: Box<str>,
    pub(crate) fingerprint: u64,
    pub(crate) hour: i64,
}

/// A sample kept, its name and fingerprint its group's.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Sample {
    pub(crate) temporality: Temporality,
    pub(crate) unix_milli: i64,
    pub(crate) value: f64,
    pub(crate) flags: u32,
    pub(crate) order: [u8; SOURCE_ORDER_BYTES],
}

/// The columns of a batch of samples the merge reads.
struct Samples {
    names: StringArray,
    temporalities: StringArray,
    fingerprints: UInt64Array,
    times: Int64Array,
    values: Float64Array,
    flags: UInt32Array,
    orders: FixedSizeBinaryArray,
}

impl Samples {
    fn of(batch: &RecordBatch) -> Result<Self, CompactError> {
        let column = |name: &str| -> Result<&Arc<dyn Array>, CompactError> {
            batch
                .column_by_name(name)
                .ok_or(CompactError::Merge("a samples batch without its columns"))
        };
        let other = || CompactError::Merge("a samples column of another type");
        let text = |name: &str| -> Result<StringArray, CompactError> {
            Ok(cast(column(name)?, &DataType::Utf8)?
                .as_string::<i32>()
                .clone())
        };
        Ok(Self {
            names: text(col::METRIC_NAME)?,
            temporalities: text(col::TEMPORALITY)?,
            fingerprints: column(col::FINGERPRINT)?
                .as_primitive_opt::<UInt64Type>()
                .ok_or_else(other)?
                .clone(),
            times: column(col::UNIX_MILLI)?
                .as_primitive_opt::<Int64Type>()
                .ok_or_else(other)?
                .clone(),
            values: column(col::VALUE)?
                .as_primitive_opt::<Float64Type>()
                .ok_or_else(other)?
                .clone(),
            flags: column(col::FLAGS)?
                .as_primitive_opt::<UInt32Type>()
                .ok_or_else(other)?
                .clone(),
            orders: column(col::SOURCE_ORDER)?
                .as_fixed_size_binary_opt()
                .filter(|o| o.value_length() == SOURCE_ORDER_WIDTH)
                .ok_or_else(other)?
                .clone(),
        })
    }

    fn sample(&self, row: usize) -> Result<Sample, CompactError> {
        Ok(Sample {
            temporality: Temporality::parse(self.temporalities.value(row))
                .ok_or(CompactError::Merge("a sample of an unknown temporality"))?,
            unix_milli: self.times.value(row),
            value: self.values.value(row),
            flags: self.flags.value(row),
            order: self
                .orders
                .value(row)
                .try_into()
                .map_err(|_| CompactError::Merge("a source order of another width"))?,
        })
    }
}

/// A sample's sort key: name, fingerprint, time and source order.
type SampleKey<'a> = (&'a str, u64, i64, &'a [u8]);

/// One samples file being read.
struct SampleCursor {
    input: read::Input,
    current: Option<Samples>,
    row: usize,
    /// Its rows are not accounted yet.
    uncounted: bool,
}

impl SampleCursor {
    fn key(&self) -> Option<SampleKey<'_>> {
        let s = self.current.as_ref()?;
        Some((
            s.names.value(self.row),
            s.fingerprints.value(self.row),
            s.times.value(self.row),
            s.orders.value(self.row),
        ))
    }
}

/// One series file being read, a row decoded at a time.
struct SeriesCursor {
    input: read::Input,
    batch: Option<RecordBatch>,
    row: usize,
    head: Option<SeriesRow>,
}

/// The inputs of a pass.
pub(crate) struct Merge {
    samples: Vec<SampleCursor>,
    series: Vec<SeriesCursor>,
    /// The duplicate being resolved: the kept so far.
    pending: Option<Sample>,
    /// Samples of not accounted inputs consumed in the group so far,
    /// duplicates included, and their least and greatest time.
    pub(crate) uncounted: u64,
    pub(crate) uncounted_times: (i64, i64),
}

impl Merge {
    /// The merge of `inputs`, `(samples, series, uncounted)` of each set.
    ///
    /// # Errors
    /// A store or Parquet failure reading the first batches.
    pub(crate) async fn open(
        inputs: Vec<(read::Input, read::Input, bool)>,
    ) -> Result<Self, CompactError> {
        let mut merge = Self {
            samples: Vec::with_capacity(inputs.len()),
            series: Vec::with_capacity(inputs.len()),
            pending: None,
            uncounted: 0,
            uncounted_times: (i64::MAX, i64::MIN),
        };
        for (samples, series, uncounted) in inputs {
            merge.samples.push(SampleCursor {
                input: samples,
                current: None,
                row: 0,
                uncounted,
            });
            merge.series.push(SeriesCursor {
                input: series,
                batch: None,
                row: 0,
                head: None,
            });
        }
        for i in 0..merge.samples.len() {
            merge.load_samples(i).await?;
            merge.load_series(i).await?;
        }
        Ok(merge)
    }

    /// The key of the next group, `None` once every input is read.
    ///
    /// # Errors
    /// Samples whose series no input holds: a set that is not one.
    pub(crate) fn next_key(&self) -> Result<Option<Key>, CompactError> {
        let series = self
            .series_min()
            .and_then(|i| self.series.get(i)?.head.as_ref());
        let samples = self
            .samples_min()
            .and_then(|i| self.samples.get(i)?.key())
            .map(|(name, fp, ms, _)| (name, fp, hour_of(ms)));
        match (series, samples) {
            (None, None) => Ok(None),
            (Some(s), sample) => {
                let key = (s.name.as_ref(), s.fingerprint, s.unix_milli);
                if sample.is_some_and(|k| k < key) {
                    return Err(CompactError::Merge("samples without their series"));
                }
                Ok(Some(Key {
                    name: s.name.clone(),
                    fingerprint: s.fingerprint,
                    hour: s.unix_milli,
                }))
            }
            (None, Some(_)) => Err(CompactError::Merge("samples without their series")),
        }
    }

    /// Starts counting a new group's samples of not accounted inputs.
    pub(crate) fn start_group(&mut self) {
        self.uncounted = 0;
        self.uncounted_times = (i64::MAX, i64::MIN);
    }

    /// The series rows of `key`, merged as a request merges them, in
    /// order, and whether they hold more than one identity.
    ///
    /// # Errors
    /// A store, Parquet or schema failure reading on.
    pub(crate) async fn take_series(
        &mut self,
        key: &Key,
    ) -> Result<(Vec<SeriesRow>, bool), CompactError> {
        let mut kept: Vec<SeriesRow> = Vec::new();
        while let Some(i) = self.series_min() {
            let Some(cursor) = self.series.get_mut(i) else {
                break;
            };
            let Some(head) = cursor.head.as_ref() else {
                break;
            };
            if (head.name.as_ref(), head.fingerprint, head.unix_milli)
                != (key.name.as_ref(), key.fingerprint, key.hour)
            {
                break;
            }
            let mut row = cursor
                .head
                .take()
                .ok_or(CompactError::Merge("a head lost"))?;
            self.load_series(i).await?;
            if !kept.iter_mut().any(|k| k.absorb(&mut row).is_some()) {
                kept.push(row);
            }
        }
        kept.sort_by(SeriesRow::order);
        let collided = kept.len() > 1;
        Ok((kept, collided))
    }

    /// The next sample of `key` kept, of one fingerprint and millisecond
    /// the one of the largest source order; `None` once the group has no
    /// sample left.
    ///
    /// # Errors
    /// A store or Parquet failure reading on, or a sample unreadable.
    pub(crate) async fn next_sample(&mut self, key: &Key) -> Result<Option<Sample>, CompactError> {
        loop {
            let next = self.samples_min().and_then(|i| {
                let cursor = self.samples.get(i)?;
                let (name, fp, ms, _) = cursor.key()?;
                ((name, fp, hour_of(ms)) == (key.name.as_ref(), key.fingerprint, key.hour))
                    .then_some((i, ms))
            });
            let Some((i, ms)) = next else {
                return Ok(self.pending.take());
            };
            if let Some(pending) = self.pending {
                if pending.unix_milli != ms {
                    self.pending = None;
                    return Ok(Some(pending));
                }
            }
            let cursor = self
                .samples
                .get(i)
                .ok_or(CompactError::Merge("a cursor lost"))?;
            let sample = cursor
                .current
                .as_ref()
                .ok_or(CompactError::Merge("a cursor without a batch"))?
                .sample(cursor.row)?;
            if cursor.uncounted {
                self.uncounted += 1;
                let (first, last) = self.uncounted_times;
                self.uncounted_times = (first.min(ms), last.max(ms));
            }
            self.pending = Some(sample);
            self.advance_samples(i).await?;
        }
    }

    /// The samples cursor whose key is least, `None` once all are read.
    fn samples_min(&self) -> Option<usize> {
        let mut best: Option<(usize, SampleKey<'_>)> = None;
        for (i, cursor) in self.samples.iter().enumerate() {
            let Some(key) = cursor.key() else {
                continue;
            };
            let less = best.as_ref().is_none_or(|(_, b)| {
                (key.0.as_bytes(), key.1, key.2, key.3).cmp(&(b.0.as_bytes(), b.1, b.2, b.3))
                    == Ordering::Less
            });
            if less {
                best = Some((i, key));
            }
        }
        best.map(|(i, _)| i)
    }

    /// The series cursor whose head is least.
    fn series_min(&self) -> Option<usize> {
        let mut best: Option<(usize, &SeriesRow)> = None;
        for (i, cursor) in self.series.iter().enumerate() {
            let Some(head) = cursor.head.as_ref() else {
                continue;
            };
            if best.is_none_or(|(_, b)| head.order(b) == Ordering::Less) {
                best = Some((i, head));
            }
        }
        best.map(|(i, _)| i)
    }

    async fn advance_samples(&mut self, i: usize) -> Result<(), CompactError> {
        let Some(cursor) = self.samples.get_mut(i) else {
            return Ok(());
        };
        cursor.row += 1;
        if cursor
            .current
            .as_ref()
            .is_some_and(|s| cursor.row < s.names.len())
        {
            return Ok(());
        }
        self.load_samples(i).await
    }

    async fn load_samples(&mut self, i: usize) -> Result<(), CompactError> {
        let Some(cursor) = self.samples.get_mut(i) else {
            return Ok(());
        };
        cursor.current = None;
        while let Some((batch, _)) = cursor.input.next().await? {
            if batch.num_rows() > 0 {
                cursor.current = Some(Samples::of(&batch)?);
                cursor.row = 0;
                break;
            }
        }
        Ok(())
    }

    async fn load_series(&mut self, i: usize) -> Result<(), CompactError> {
        let Some(cursor) = self.series.get_mut(i) else {
            return Ok(());
        };
        loop {
            if let Some(batch) = cursor.batch.as_ref() {
                if cursor.row < batch.num_rows() {
                    let mut rows = series_rows(&batch.slice(cursor.row, 1))?;
                    cursor.row += 1;
                    cursor.head = rows.pop();
                    return Ok(());
                }
            }
            let Some((batch, _)) = cursor.input.next().await? else {
                cursor.batch = None;
                cursor.head = None;
                return Ok(());
            };
            let schema = if batch.num_columns() == series_schema_v2().fields().len() {
                series_schema_v2()
            } else {
                series_schema_v1()
            };
            cursor.batch = Some(batch.with_schema(schema)?);
            cursor.row = 0;
        }
    }
}

/// The start of the hour of `unix_milli`.
pub(crate) fn hour_of(unix_milli: i64) -> i64 {
    unix_milli - unix_milli.rem_euclid(MILLIS_PER_HOUR)
}
