//! What a request converts into: a batch of samples in sort order and
//! the rows of its series, and the series batch a file of them holds.

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int64Builder, MapBuilder,
    StringBuilder, StringDictionaryBuilder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Int32Type};
use arrow::record_batch::RecordBatch;

use super::expand::{Emit, Preflight, Sink, Stop, hour_of};
use super::schema::{samples_schema_v1, series_schema_v1};
use super::{
    MetricType, Rejection, Rejections, SOURCE_ORDER_BYTES, SOURCE_ORDER_WIDTH, Source, Temporality,
};
use crate::SchemaError;
use crate::columns::map_values_field;
use crate::limits::DropStats;

/// Bytes a column of a batch holds besides its values: the buffer
/// rounding of each of its buffers.
const COLUMN_OVERHEAD: usize = 256;

/// Bytes of a sample's columns in a batch: name and temporality keys,
/// fingerprint, time, value, flags and source order.
const SAMPLE_COLUMN_BYTES: usize = 4 + 4 + 8 + 8 + 8 + 4 + SOURCE_ORDER_BYTES;

/// Bytes a dictionary builder holds per distinct value besides its text:
/// its offset and its slot in the hash table.
const DICTIONARY_ENTRY_BYTES: usize = 32;

/// One series of a file: its identity, its hour and its labels.
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesRow {
    /// The series' fingerprint, version 1.
    pub fingerprint: u64,
    /// Start of its hour, in Unix milliseconds.
    pub unix_milli: i64,
    /// Its name.
    pub name: Box<str>,
    /// Its type.
    pub kind: MetricType,
    /// Its temporality.
    pub temporality: Temporality,
    /// Whether it only grows.
    pub monotonic: bool,
    /// Its unit.
    pub unit: Box<str>,
    /// Its description: of differing ones, the longest, then the least in
    /// byte order.
    pub description: Box<str>,
    /// Its labels in key byte order.
    pub labels: Box<[(Box<str>, Box<str>)]>,
}

impl SeriesRow {
    /// Bytes it holds besides its own size: its texts and labels.
    pub fn heap_bytes(&self) -> usize {
        self.name.len()
            + self.unit.len()
            + self.description.len()
            + self.labels.len() * std::mem::size_of::<(Box<str>, Box<str>)>()
            + self
                .labels
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
    }

    /// Bytes it holds, itself included.
    pub fn bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.heap_bytes()
    }

    /// The order series rows are kept in: name, fingerprint, hour. Rows
    /// that compare equal are one series in one hour.
    pub fn order(&self, other: &Self) -> Ordering {
        self.name
            .as_bytes()
            .cmp(other.name.as_bytes())
            .then(self.fingerprint.cmp(&other.fingerprint))
            .then(self.unix_milli.cmp(&other.unix_milli))
    }

    /// Takes from `other`, a row of the same series and hour, its
    /// description when it is preferred, `other` keeping this one's.
    /// Returns how many bytes this row grew by, negative when it shrank.
    pub fn absorb(&mut self, other: &mut Self) -> isize {
        if preferred(&other.description, &self.description) {
            let signed = |n: usize| isize::try_from(n).unwrap_or(isize::MAX);
            let grown = signed(other.description.len()) - signed(self.description.len());
            std::mem::swap(&mut self.description, &mut other.description);
            grown
        } else {
            0
        }
    }
}

/// Sorts `rows` in place by name, fingerprint and hour, and keeps one row
/// of each fingerprint and hour, the description resolved as
/// [`SeriesRow::description`] says. Allocates nothing.
pub fn dedupe_series(rows: &mut Vec<SeriesRow>) {
    rows.sort_unstable_by(SeriesRow::order);
    rows.dedup_by(|later, kept| {
        if later.fingerprint != kept.fingerprint || later.unix_milli != kept.unix_milli {
            return false;
        }
        if preferred(&later.description, &kept.description) {
            std::mem::swap(&mut later.description, &mut kept.description);
        }
        true
    });
}

/// Whether description `a` wins over `b`.
fn preferred(a: &str, b: &str) -> bool {
    a.len() > b.len() || (a.len() == b.len() && a.as_bytes() < b.as_bytes())
}

/// The batch of `rows`, `n` of them, in their order, with
/// [`series_schema_v1`].
///
/// # Errors
/// An Arrow failure.
pub fn series_batch<'a>(
    rows: impl IntoIterator<Item = &'a SeriesRow>,
    n: usize,
) -> Result<RecordBatch, SchemaError> {
    let mut name = StringDictionaryBuilder::<Int32Type>::new();
    let mut temporality = StringDictionaryBuilder::<Int32Type>::new();
    let mut kind = StringDictionaryBuilder::<Int32Type>::new();
    let mut monotonic = BooleanBuilder::with_capacity(n);
    let mut fingerprint = UInt64Builder::with_capacity(n);
    let mut unix_milli = Int64Builder::with_capacity(n);
    let mut labels = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new())
        .with_values_field(map_values_field(DataType::Utf8));
    let mut description = StringBuilder::new();
    let mut unit = StringBuilder::new();
    for row in rows {
        name.append(&row.name)?;
        temporality.append(row.temporality.as_str())?;
        kind.append(row.kind.as_str())?;
        monotonic.append_value(row.monotonic);
        fingerprint.append_value(row.fingerprint);
        unix_milli.append_value(row.unix_milli);
        for (k, v) in &row.labels {
            labels.keys().append_value(k);
            labels.values().append_value(v);
        }
        labels.append(true)?;
        description.append_value(&row.description);
        unit.append_value(&row.unit);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(name.finish()),
        Arc::new(temporality.finish()),
        Arc::new(kind.finish()),
        Arc::new(monotonic.finish()),
        Arc::new(fingerprint.finish()),
        Arc::new(unix_milli.finish()),
        Arc::new(labels.finish()),
        Arc::new(description.finish()),
        Arc::new(unit.finish()),
    ];
    Ok(RecordBatch::try_new(series_schema_v1(), columns)?)
}

/// `samples` with each source order taken from `source`, its ordinal
/// kept: what a request converted before its WAL position was known has
/// once it is.
///
/// # Errors
/// A batch without a source order column of the samples schema.
pub fn with_source(samples: &RecordBatch, source: Source) -> Result<RecordBatch, SchemaError> {
    use arrow::array::{Array, AsArray};
    let index = samples
        .schema()
        .index_of(super::schema::col::SOURCE_ORDER)?;
    let old = samples
        .column(index)
        .as_fixed_size_binary_opt()
        .filter(|a| a.value_length() == SOURCE_ORDER_WIDTH)
        .ok_or(SchemaError::Layout(
            "metrics: a source order of another width",
        ))?;
    let mut order = FixedSizeBinaryBuilder::with_capacity(old.len(), SOURCE_ORDER_WIDTH);
    for row in 0..old.len() {
        let ordinal = old
            .value(row)
            .get(SOURCE_ORDER_BYTES - 4..)
            .and_then(|b| <[u8; 4]>::try_from(b).ok())
            .map_or(0, u32::from_be_bytes);
        order.append_value(source.order(ordinal))?;
    }
    let mut columns = samples.columns().to_vec();
    if let Some(column) = columns.get_mut(index) {
        *column = Arc::new(order.finish());
    }
    Ok(RecordBatch::try_new(samples.schema(), columns)?)
}

/// A request's samples, sorted by name, fingerprint, time and source
/// order, and its series, deduplicated as [`dedupe_series`] does.
#[derive(Debug)]
pub struct MetricRows {
    /// The samples, with [`super::samples_schema_v1`].
    pub samples: RecordBatch,
    /// The series of those samples.
    pub series: Vec<SeriesRow>,
    /// Points refused, by reason.
    pub rejected: Rejections,
    /// Attributes dropped or truncated.
    pub stats: DropStats,
}

/// A sample before its batch: its name is an index into the names.
struct Sample {
    name: u32,
    temporality: Temporality,
    fingerprint: u64,
    unix_milli: i64,
    value: f64,
    flags: u32,
    ordinal: u32,
}

/// Builds a request's rows into capacities its preflight counted.
pub(crate) struct Builder {
    samples: Vec<Sample>,
    names: Vec<Box<str>>,
    /// Names of the current metric's series, indices into `names`.
    metric_names: Vec<u32>,
    series: Vec<SeriesRow>,
}

impl Builder {
    pub(crate) fn with_capacity(checked: &Preflight) -> Self {
        Self {
            samples: Vec::with_capacity(checked.samples),
            names: Vec::with_capacity(checked.series),
            metric_names: Vec::with_capacity(8),
            series: Vec::with_capacity(checked.series),
        }
    }

    fn name(&mut self, name: &str) -> Result<u32, Stop> {
        if let Some(i) = self
            .metric_names
            .iter()
            .find(|i| self.names.get(**i as usize).is_some_and(|n| &**n == name))
        {
            return Ok(*i);
        }
        if self.names.len() == self.names.capacity() {
            return Err(past_preflight());
        }
        let i = u32::try_from(self.names.len()).map_err(|_| past_preflight())?;
        self.names.push(name.into());
        self.metric_names.push(i);
        Ok(i)
    }

    pub(crate) fn finish(
        mut self,
        source: Source,
        rejected: Rejections,
        stats: DropStats,
    ) -> Result<MetricRows, SchemaError> {
        let names = &self.names;
        let text = |i: u32| names.get(i as usize).map_or(&b""[..], |n| n.as_bytes());
        self.samples.sort_unstable_by(|a, b| {
            text(a.name)
                .cmp(text(b.name))
                .then(a.fingerprint.cmp(&b.fingerprint))
                .then(a.unix_milli.cmp(&b.unix_milli))
                .then(a.ordinal.cmp(&b.ordinal))
        });
        let n = self.samples.len();
        let name_bytes = self.names.iter().map(|n| n.len()).sum();
        let mut name =
            StringDictionaryBuilder::<Int32Type>::with_capacity(n, self.names.len(), name_bytes);
        let mut temporality = StringDictionaryBuilder::<Int32Type>::with_capacity(n, 3, 32);
        let mut fingerprint = UInt64Builder::with_capacity(n);
        let mut unix_milli = Int64Builder::with_capacity(n);
        let mut value = Float64Builder::with_capacity(n);
        let mut flags = UInt32Builder::with_capacity(n);
        let mut order = FixedSizeBinaryBuilder::with_capacity(n, SOURCE_ORDER_WIDTH);
        for s in &self.samples {
            name.append(std::str::from_utf8(text(s.name)).unwrap_or_default())?;
            temporality.append(s.temporality.as_str())?;
            fingerprint.append_value(s.fingerprint);
            unix_milli.append_value(s.unix_milli);
            value.append_value(s.value);
            flags.append_value(s.flags);
            order.append_value(source.order(s.ordinal))?;
        }
        drop(std::mem::take(&mut self.samples));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(name.finish()),
            Arc::new(temporality.finish()),
            Arc::new(fingerprint.finish()),
            Arc::new(unix_milli.finish()),
            Arc::new(value.finish()),
            Arc::new(flags.finish()),
            Arc::new(order.finish()),
        ];
        let samples = RecordBatch::try_new(samples_schema_v1(), columns)?;
        dedupe_series(&mut self.series);
        Ok(MetricRows {
            samples,
            series: self.series,
            rejected,
            stats,
        })
    }
}

fn past_preflight() -> Stop {
    Stop::Schema(SchemaError::Layout(
        "metrics: conversion past its preflight",
    ))
}

impl Sink for Builder {
    fn reject(&mut self, _reason: Rejection, _points: usize) {}

    fn metric(&mut self) {
        self.metric_names.clear();
    }

    fn emit(&mut self, emit: &Emit<'_>) -> Result<(), Stop> {
        if self.samples.len() == self.samples.capacity()
            || self.series.len() == self.series.capacity()
        {
            return Err(past_preflight());
        }
        let ordinal = u32::try_from(self.samples.len()).map_err(|_| past_preflight())?;
        let name = self.name(emit.key.name)?;
        self.samples.push(Sample {
            name,
            temporality: emit.key.temporality,
            fingerprint: emit.fingerprint,
            unix_milli: emit.unix_milli,
            value: emit.value,
            flags: emit.flags,
            ordinal,
        });
        self.series.push(SeriesRow {
            fingerprint: emit.fingerprint,
            unix_milli: hour_of(emit.unix_milli),
            name: emit.key.name.into(),
            kind: emit.key.kind,
            temporality: emit.key.temporality,
            monotonic: emit.key.monotonic,
            unit: emit.key.unit.into(),
            description: emit.description.into(),
            labels: emit
                .labels
                .iter()
                .map(|(k, v)| (Box::<str>::from(*k), Box::<str>::from(*v)))
                .collect(),
        });
        Ok(())
    }
}

/// What the rows of `p` hold once converted: the batch of samples and the
/// series rows.
pub(crate) fn rows_bound(p: &Preflight) -> usize {
    let series = p
        .series
        .saturating_mul(std::mem::size_of::<SeriesRow>())
        .saturating_add(
            p.labels
                .saturating_mul(std::mem::size_of::<(Box<str>, Box<str>)>()),
        )
        .saturating_add(p.label_bytes)
        .saturating_add(p.text_bytes);
    let batch = p
        .samples
        .saturating_mul(SAMPLE_COLUMN_BYTES)
        .saturating_add(p.name_bytes)
        .saturating_add(7 * COLUMN_OVERHEAD);
    series.saturating_add(batch)
}

/// What converting `p` holds besides its rows, and only while it runs:
/// the samples before their batch, their names and the traversal's
/// scratch.
pub(crate) fn transient_bound(p: &Preflight) -> usize {
    let samples = p.samples.saturating_mul(std::mem::size_of::<Sample>());
    let names = p
        .series
        .saturating_mul(std::mem::size_of::<Box<str>>() + DICTIONARY_ENTRY_BYTES)
        .saturating_add(p.name_bytes);
    samples.saturating_add(names).saturating_add(p.scratch)
}

/// The most bytes [`super::convert`] holds at once for `p`.
pub(crate) fn bytes_bound(p: &Preflight) -> usize {
    rows_bound(p).saturating_add(transient_bound(p))
}
