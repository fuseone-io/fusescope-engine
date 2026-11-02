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
    SOURCE_ORDER_BYTES, SOURCE_ORDER_WIDTH, SeriesRow, Temporality, col, series_row_sizes,
    series_rows, series_schema_v1, series_schema_v2,
};

use super::budget::{Budget, Part};
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

/// One series file being read, a row decoded at a time, each admitted
/// before it is.
struct SeriesCursor {
    input: read::Input,
    /// Its series schema version, read off its footer.
    version: u32,
    /// What each row of the batch decodes to, found from its arrays.
    sizes: Vec<usize>,
    /// What the head holds.
    head_bytes: usize,
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
    /// What the series rows of the key being merged hold.
    group_bytes: usize,
}

impl Merge {
    /// The merge of `inputs`, `(samples, (series, its version), uncounted)`
    /// of each set.
    ///
    /// # Errors
    /// A store or Parquet failure reading the first batches.
    pub(crate) async fn open(
        inputs: Vec<(read::Input, (read::Input, u32), bool)>,
        budget: &mut Budget,
    ) -> Result<Self, CompactError> {
        let mut merge = Self {
            samples: Vec::with_capacity(inputs.len()),
            series: Vec::with_capacity(inputs.len()),
            pending: None,
            uncounted: 0,
            uncounted_times: (i64::MAX, i64::MIN),
            group_bytes: 0,
        };
        for (samples, (series, version), uncounted) in inputs {
            merge.samples.push(SampleCursor {
                input: samples,
                current: None,
                row: 0,
                uncounted,
            });
            merge.series.push(SeriesCursor {
                input: series,
                version,
                sizes: Vec::new(),
                head_bytes: 0,
                batch: None,
                row: 0,
                head: None,
            });
        }
        for i in 0..merge.samples.len() {
            merge.load_samples(i).await?;
            merge.load_series(i, budget).await?;
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

    /// The key's series rows are no longer held.
    pub(crate) fn end_group(&mut self, budget: &mut Budget) {
        budget.give(Part::Inputs, std::mem::take(&mut self.group_bytes));
    }

    /// The series rows of `key`, merged as a request merges them, in
    /// order, and whether they hold more than one identity.
    ///
    /// # Errors
    /// A store, Parquet or schema failure reading on.
    pub(crate) async fn take_series(
        &mut self,
        key: &Key,
        budget: &mut Budget,
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
            let bytes = std::mem::take(&mut cursor.head_bytes);
            self.load_series(i, budget).await?;
            // The row kept takes the description it prefers and grows by
            // what the row dropped shrinks by: what is held moves, only what
            // is dropped now is freed.
            if let Some(grown) = kept.iter_mut().find_map(|k| k.absorb(&mut row)) {
                self.group_bytes = self.group_bytes.saturating_add_signed(grown);
                budget.give(Part::Inputs, bytes.saturating_add_signed(-grown));
                drop(row);
            } else {
                self.group_bytes += bytes;
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

    /// Decodes the next row of input `i` as its head, what it holds
    /// admitted first, and what sizing its batch holds too.
    async fn load_series(&mut self, i: usize, budget: &mut Budget) -> Result<(), CompactError> {
        let Some(cursor) = self.series.get_mut(i) else {
            return Ok(());
        };
        loop {
            if let Some(batch) = cursor.batch.as_ref() {
                if cursor.row < batch.num_rows() {
                    let bytes = cursor.sizes.get(cursor.row).copied().unwrap_or(0);
                    budget.take(Part::Inputs, bytes)?;
                    let mut rows = series_rows(&batch.slice(cursor.row, 1))?;
                    cursor.row += 1;
                    cursor.head = rows.pop();
                    cursor.head_bytes = bytes;
                    return Ok(());
                }
            }
            budget.give(
                Part::Inputs,
                cursor.sizes.capacity() * std::mem::size_of::<usize>(),
            );
            cursor.sizes = Vec::new();
            let Some((batch, _)) = cursor.input.next().await? else {
                cursor.batch = None;
                cursor.head = None;
                return Ok(());
            };
            let schema = if cursor.version == 2 {
                series_schema_v2()
            } else {
                series_schema_v1()
            };
            let batch = batch.with_schema(schema)?;
            budget.take(
                Part::Inputs,
                batch.num_rows() * std::mem::size_of::<usize>(),
            )?;
            cursor.sizes = series_row_sizes(&batch)?;
            cursor.batch = Some(batch);
            cursor.row = 0;
        }
    }
}

/// The start of the hour of `unix_milli`.
pub(crate) fn hour_of(unix_milli: i64) -> i64 {
    unix_milli - unix_milli.rem_euclid(MILLIS_PER_HOUR)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use fs_schema::limits::Limits;
    use fs_schema::metrics::{MetricLimits, Source, convert, preflight, series_batch};
    use object_store::local::LocalFileSystem;
    use object_store::{ObjectStore, ObjectStoreExt};
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::metrics::v1::metric::Data;
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    };

    use super::*;
    use crate::metric::budget::MIN_METRIC_PASS_BYTES;

    fn request(hosts: &[&str]) -> ExportMetricsServiceRequest {
        described(hosts, "")
    }

    fn described(hosts: &[&str], description: &str) -> ExportMetricsServiceRequest {
        let wide = "w".repeat(1024);
        let data_points = hosts
            .iter()
            .map(|host| NumberDataPoint {
                attributes: vec![KeyValue {
                    key: "host".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(format!("{host}{wide}"))),
                    }),
                    ..KeyValue::default()
                }],
                time_unix_nano: 1_792_677_600_000_000_000,
                value: Some(NumberValue::AsDouble(1.0)),
                ..NumberDataPoint::default()
            })
            .collect();
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "cpu".into(),
                        description: description.into(),
                        data: Some(Data::Gauge(Gauge { data_points })),
                        ..Metric::default()
                    }],
                    ..ScopeMetrics::default()
                }],
                ..ResourceMetrics::default()
            }],
        }
    }

    async fn put(store: &Arc<dyn ObjectStore>, path: &str, batch: &RecordBatch) -> i64 {
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(Vec::new(), batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        let bytes = writer.into_inner().unwrap();
        let size = i64::try_from(bytes.len()).unwrap();
        store
            .put(&object_store::path::Path::from(path), bytes.into())
            .await
            .unwrap();
        size
    }

    /// The row each input decodes is admitted before it is, and the rows
    /// of the key being merged stay held until the group ends.
    #[tokio::test]
    async fn the_rows_inputs_decode_and_the_key_holds_are_admitted() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let request = request(&["a", "b"]);
        let (limits, metrics) = (Limits::default(), MetricLimits::default());
        let checked = preflight(&request, &limits, &metrics).unwrap();
        let source = Source {
            instance: [1; 16],
            segment: 1,
            offset: 1,
        };
        let rows = convert(&request, &limits, &metrics, &checked, source).unwrap();
        let series = series_batch(&rows.series, rows.series.len()).unwrap();
        let sizes = series_row_sizes(&series).unwrap();
        let (first, second) = (*sizes.first().unwrap(), *sizes.get(1).unwrap());
        let samples_size = put(&store, "s.parquet", &rows.samples).await;
        let series_size = put(&store, "r.parquet", &series).await;
        let (samples, _) = read::Input::bounded(
            Arc::clone(&store),
            ("s.parquet", samples_size),
            &fs_schema::metrics::samples_schema_v1(),
            &[],
        )
        .await
        .unwrap();
        let (series_input, layout) = read::Input::bounded(
            Arc::clone(&store),
            ("r.parquet", series_size),
            &series_schema_v2(),
            &[],
        )
        .await
        .unwrap();
        let series_input = series_input.with_chunks(layout.plan(usize::MAX, 1024));
        let samples = samples.with_chunks(
            read::Input::bounded(
                Arc::clone(&store),
                ("s.parquet", samples_size),
                &fs_schema::metrics::samples_schema_v1(),
                &[],
            )
            .await
            .unwrap()
            .1
            .plan(usize::MAX, 1024),
        );
        let mut budget = Budget::new(MIN_METRIC_PASS_BYTES);
        let mut merge = Merge::open(vec![(samples, (series_input, 2), true)], &mut budget)
            .await
            .unwrap();
        let sizing = 2 * std::mem::size_of::<usize>();
        assert_eq!(budget.held(Part::Inputs), first + sizing, "the first head");
        let key = merge.next_key().unwrap().unwrap();
        let (kept, _) = merge.take_series(&key, &mut budget).await.unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(
            budget.held(Part::Inputs),
            first + second + sizing,
            "the key's row and the next head"
        );
        merge.end_group(&mut budget);
        assert_eq!(budget.held(Part::Inputs), second + sizing);
        assert!(budget.peak() >= first + second);
    }

    /// One set of `request`, its two files written under `name`, opened.
    async fn set(
        store: &Arc<dyn ObjectStore>,
        name: &str,
        request: &ExportMetricsServiceRequest,
    ) -> (read::Input, (read::Input, u32), bool) {
        let (limits, metrics) = (Limits::default(), MetricLimits::default());
        let checked = preflight(request, &limits, &metrics).unwrap();
        let source = Source {
            instance: [1; 16],
            segment: 1,
            offset: 1,
        };
        let rows = convert(request, &limits, &metrics, &checked, source).unwrap();
        let series = series_batch(&rows.series, rows.series.len()).unwrap();
        let open = |path: String, size: i64, schema| {
            let store = Arc::clone(store);
            async move {
                let (input, layout) = read::Input::bounded(store, (&path, size), &schema, &[])
                    .await
                    .unwrap();
                input.with_chunks(layout.plan(usize::MAX, 1024))
            }
        };
        let (samples_path, series_path) =
            (format!("{name}.s.parquet"), format!("{name}.r.parquet"));
        let samples_size = put(store, &samples_path, &rows.samples).await;
        let series_size = put(store, &series_path, &series).await;
        (
            open(
                samples_path,
                samples_size,
                fs_schema::metrics::samples_schema_v1(),
            )
            .await,
            (open(series_path, series_size, series_schema_v2()).await, 2),
            true,
        )
    }

    /// Of one series in two inputs, the row kept takes the longer
    /// description of the one dropped: what the key holds is the row
    /// kept, as it is after.
    #[tokio::test]
    async fn the_key_holds_its_row_as_it_grows_by_a_description() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let inputs = vec![
            set(&store, "short", &described(&["a"], "")).await,
            set(&store, "long", &described(&["a"], &"d".repeat(4096))).await,
        ];
        let mut budget = Budget::new(MIN_METRIC_PASS_BYTES);
        let mut merge = Merge::open(inputs, &mut budget).await.unwrap();
        let key = merge.next_key().unwrap().unwrap();
        let (kept, _) = merge.take_series(&key, &mut budget).await.unwrap();
        let [row] = kept.as_slice() else {
            panic!("one row: {kept:?}");
        };
        assert_eq!(row.description.len(), 4096, "the longer description");
        assert_eq!(budget.held(Part::Inputs), row.bytes());
        merge.end_group(&mut budget);
        assert_eq!(budget.held(Part::Inputs), 0);
    }
}
