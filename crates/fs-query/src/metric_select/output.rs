//! The answer of a select: one row a labelset, in labelset order, the
//! series of one labelset merged in the order given, the first winning a
//! time they share, as the Go backend's `sortAndMerge` does.

use std::sync::{Arc, OnceLock};

use arrow::array::{
    ArrayRef, Float64Array, Int64Array, ListArray, MapArray, StringArray, StringBuilder,
    StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::execution::memory_pool::MemoryReservation;

use super::samples::Samples;
use crate::QueryError;

/// Bytes of one kept sample: its time and value.
const SAMPLE_BYTES: usize = 16;

/// One series of the answer: its labels, sorted by key, none empty, and
/// its samples in time order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Series {
    pub(crate) labels: Box<[(Box<str>, Box<str>)]>,
    pub(crate) samples: Samples,
}

/// The schema of a select's answer.
pub fn select_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("labels", DataType::Map(entries_field(), true), false),
                Field::new("timestamps", DataType::List(item(DataType::Int64)), false),
                Field::new("values", DataType::List(item(DataType::Float64)), false),
            ]))
        })
        .clone()
}

fn entries_field() -> Arc<Field> {
    Arc::new(Field::new(
        "entries",
        DataType::Struct(entry_fields()),
        false,
    ))
}

fn entry_fields() -> Fields {
    Fields::from(vec![
        Field::new("keys", DataType::Utf8, false),
        Field::new("values", DataType::Utf8, false),
    ])
}

fn item(data_type: DataType) -> Arc<Field> {
    Arc::new(Field::new("item", data_type, false))
}

/// `series`, given in name and fingerprint order, sorted by labelset and
/// merged, those without samples left out; at most `max_rows` samples are
/// ever built, the merged vector reserved before it is.
///
/// # Errors
/// More samples than `max_rows`, or a refused reservation.
pub(crate) fn fuse(
    mut series: Vec<Series>,
    max_rows: usize,
    reservation: &mut MemoryReservation,
) -> Result<Vec<Series>, QueryError> {
    series.retain(|s| !s.samples.is_empty());
    reservation.try_grow(series.len() / 2 * std::mem::size_of::<Series>())?;
    series.sort_by(|a, b| a.labels.cmp(&b.labels));
    reservation.shrink(series.len() / 2 * std::mem::size_of::<Series>());
    let mut built = 0usize;
    let mut out: Vec<Series> = Vec::with_capacity(series.len());
    for next in series {
        match out.last_mut() {
            Some(last) if last.labels == next.labels => {
                let bound = last.samples.len() + next.samples.len();
                reservation.try_grow(bound * SAMPLE_BYTES)?;
                built -= last.samples.len();
                let merged = merge(&last.samples, &next.samples, &mut built, max_rows)?;
                let freed = last.samples.capacity() + next.samples.capacity();
                last.samples = merged;
                reservation.shrink(freed * SAMPLE_BYTES);
            }
            _ => {
                built += next.samples.len();
                if built > max_rows {
                    return Err(QueryError::TooManyRows(max_rows));
                }
                out.push(next);
            }
        }
    }
    Ok(out)
}

/// `a` and `b` in time order, `a`'s sample kept at a time both hold,
/// each sample counted into `built` as it is.
fn merge(
    a: &Samples,
    b: &Samples,
    built: &mut usize,
    max_rows: usize,
) -> Result<Samples, QueryError> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut first, mut second) = (a.iter().peekable(), b.iter().peekable());
    loop {
        let next = match (first.peek(), second.peek()) {
            (Some(left), Some(right)) if left.0 < right.0 => first.next(),
            (Some(left), Some(right)) if left.0 > right.0 => second.next(),
            (Some(_), Some(_)) => {
                second.next();
                first.next()
            }
            (Some(_), None) => first.next(),
            (None, Some(_)) => second.next(),
            (None, None) => break,
        };
        if let Some(sample) = next {
            *built += 1;
            if *built > max_rows {
                return Err(QueryError::TooManyRows(max_rows));
            }
            out.push(*sample);
        }
    }
    Ok(out)
}

/// The answer's one batch, its arrays reserved before they are built.
///
/// # Errors
/// More labels or samples than an array's offsets count, a refused
/// reservation, or arrays Arrow refuses.
pub(crate) fn batch(
    series: &[Series],
    reservation: &mut MemoryReservation,
) -> Result<RecordBatch, QueryError> {
    let labels: usize = series.iter().map(|s| s.labels.len()).sum();
    let text: usize = series
        .iter()
        .flat_map(|s| s.labels.iter())
        .map(|(k, v)| k.len() + v.len())
        .sum();
    let samples: usize = series.iter().map(|s| s.samples.len()).sum();
    let too_many = || QueryError::TooManyRows(usize::try_from(i32::MAX).unwrap_or(usize::MAX));
    if i32::try_from(text).is_err()
        || i32::try_from(labels).is_err()
        || i32::try_from(samples).is_err()
    {
        return Err(too_many());
    }
    reservation.try_grow(text + labels * 8 + samples * SAMPLE_BYTES + series.len() * 12)?;
    let (mut keys, mut values) = (
        StringBuilder::with_capacity(labels, text),
        StringBuilder::with_capacity(labels, text),
    );
    for (key, value) in series.iter().flat_map(|s| s.labels.iter()) {
        keys.append_value(key);
        values.append_value(value);
    }
    let (keys, values) = (keys.finish(), values.finish());
    let entries = StructArray::try_new(
        entry_fields(),
        vec![Arc::new(keys) as ArrayRef, Arc::new(values)],
        None,
    )?;
    let map = MapArray::try_new(
        entries_field(),
        OffsetBuffer::from_lengths(series.iter().map(|s| s.labels.len())),
        entries,
        None,
        true,
    )?;
    let lengths = || series.iter().map(|s| s.samples.len());
    let flat = || series.iter().flat_map(|s| s.samples.iter());
    let times = ListArray::try_new(
        item(DataType::Int64),
        OffsetBuffer::from_lengths(lengths()),
        Arc::new(Int64Array::from_iter_values(flat().map(|(t, _)| *t))),
        None,
    )?;
    let values = ListArray::try_new(
        item(DataType::Float64),
        OffsetBuffer::from_lengths(lengths()),
        Arc::new(Float64Array::from_iter_values(flat().map(|(_, v)| *v))),
        None,
    )?;
    Ok(RecordBatch::try_new(
        select_schema(),
        vec![Arc::new(map), Arc::new(times), Arc::new(values)],
    )?)
}

/// One batch of one non-null text column named `name`, of `values`.
///
/// # Errors
/// More text than its offsets count, or a refused reservation.
pub(crate) fn text_batch(
    name: &str,
    values: &[Box<str>],
    reservation: &mut MemoryReservation,
) -> Result<RecordBatch, QueryError> {
    let text: usize = values.iter().map(|v| v.len()).sum();
    if i32::try_from(text).is_err() {
        return Err(QueryError::TooManyRows(values.len()));
    }
    reservation.try_grow(text + values.len() * 4)?;
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Utf8, false)]));
    let column = StringArray::from_iter_values(values.iter().map(|v| &**v));
    Ok(RecordBatch::try_new(schema, vec![Arc::new(column)])?)
}
