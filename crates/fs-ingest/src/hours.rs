//! Calendar hours of span rows: an L0 file never holds rows of two.

use std::collections::{BTreeSet, HashMap};

use arrow::array::{AsArray, PrimitiveArray, UInt32Array};
use arrow::compute::take_record_batch;
use arrow::datatypes::TimestampNanosecondType;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, TimeZone, Utc};
use fs_schema::spans::col;

use crate::IngestError;

const HOUR_NANOS: i64 = 3_600_000_000_000;

/// Start of the UTC hour holding a timestamp in nanoseconds since the epoch.
pub fn hour_of(nanos: i64) -> DateTime<Utc> {
    Utc.timestamp_nanos(hour_floor(nanos))
}

/// Every UTC hour with a row in `batches`, in order.
///
/// # Errors
/// A batch without the span timestamp column.
pub fn hours_in(batches: &[RecordBatch]) -> Result<BTreeSet<DateTime<Utc>>, IngestError> {
    let mut starts = BTreeSet::new();
    for batch in batches {
        let ts = timestamps(batch)?;
        let (Some(min), Some(max)) = (arrow::compute::min(ts), arrow::compute::max(ts)) else {
            continue;
        };
        if hour_floor(min) == hour_floor(max) {
            starts.insert(hour_floor(min));
        } else {
            starts.extend(ts.values().iter().map(|t| hour_floor(*t)));
        }
    }
    Ok(starts.into_iter().map(|s| Utc.timestamp_nanos(s)).collect())
}

/// The rows of one batch split by hour.
#[derive(Debug)]
pub struct Partition {
    /// One batch per hour asked for, in the same order, empty when none.
    pub by_hour: Vec<RecordBatch>,
    /// The rows of every other hour, and rows without a timestamp.
    pub rest: RecordBatch,
}

/// Splits `batch` by hour in one pass over its timestamps: the rows of each
/// of `hours` and the rest. A row is placed by the hour its timestamp
/// starts in, so the last representable timestamp has an hour too.
///
/// # Errors
/// A batch without the span timestamp column, more rows than can be
/// indexed, or an Arrow failure.
pub fn partition(batch: &RecordBatch, hours: &[DateTime<Utc>]) -> Result<Partition, IngestError> {
    let wanted: HashMap<i64, usize> = hours
        .iter()
        .enumerate()
        .map(|(i, hour)| hour_nanos(*hour).map(|start| (start, i)))
        .collect::<Result<_, _>>()?;
    let ts = timestamps(batch)?;
    let mut by_hour: Vec<Vec<u32>> = hours.iter().map(|_| Vec::new()).collect();
    let mut rest = Vec::new();
    for (row, t) in ts.iter().enumerate() {
        let row = u32::try_from(row)
            .map_err(|_| IngestError::Encode("too many rows in one batch to partition".into()))?;
        match t.and_then(|t| wanted.get(&hour_floor(t))) {
            Some(i) => {
                if let Some(rows) = by_hour.get_mut(*i) {
                    rows.push(row);
                }
            }
            None => rest.push(row),
        }
    }
    let take = |rows: Vec<u32>| take_record_batch(batch, &UInt32Array::from(rows));
    Ok(Partition {
        by_hour: by_hour.into_iter().map(take).collect::<Result<_, _>>()?,
        rest: take(rest)?,
    })
}

fn hour_floor(nanos: i64) -> i64 {
    nanos.div_euclid(HOUR_NANOS) * HOUR_NANOS
}

fn hour_nanos(hour: DateTime<Utc>) -> Result<i64, IngestError> {
    hour.timestamp_nanos_opt()
        .ok_or_else(|| IngestError::Encode(format!("hour {hour} is outside the nanosecond range")))
}

fn timestamps(
    batch: &RecordBatch,
) -> Result<&PrimitiveArray<TimestampNanosecondType>, IngestError> {
    let index = batch.schema().index_of(col::TIMESTAMP)?;
    batch
        .column(index)
        .as_primitive_opt::<TimestampNanosecondType>()
        .ok_or_else(|| IngestError::Encode("span timestamps are not nanoseconds".into()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::TimestampNanosecondArray;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use super::*;

    /// 2026-10-22T14:00:00Z.
    const H14: i64 = 1_792_677_600_000_000_000;

    fn batch(timestamps: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            col::TIMESTAMP,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));
        let ts = TimestampNanosecondArray::from(timestamps.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(ts)]).unwrap()
    }

    #[test]
    fn an_hour_starts_on_the_hour_and_ends_before_the_next() {
        assert_eq!(hour_of(H14).timestamp_nanos_opt(), Some(H14));
        assert_eq!(hour_of(H14 + HOUR_NANOS - 1), hour_of(H14));
        assert_ne!(hour_of(H14 + HOUR_NANOS), hour_of(H14));
        assert_eq!(
            hour_of(-1),
            Utc.timestamp_nanos(-HOUR_NANOS),
            "before the epoch rounds down"
        );
    }

    /// Rows go to the hour they start in, the rest apart; the last
    /// representable timestamp, which the converter clamps to, has an hour
    /// like any other instead of falling off an open range.
    #[test]
    fn rows_are_partitioned_by_their_hour_in_one_pass() {
        let rows = batch(&[
            H14 + 5,
            H14 + HOUR_NANOS + 5,
            H14 + 7,
            H14 + 3 * HOUR_NANOS,
            i64::MAX,
        ]);
        let hours: Vec<_> = hours_in(std::slice::from_ref(&rows))
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(
            hours,
            [
                hour_of(H14),
                hour_of(H14 + HOUR_NANOS),
                hour_of(H14 + 3 * HOUR_NANOS),
                hour_of(i64::MAX)
            ]
        );
        let split = partition(&rows, &[hour_of(H14), hour_of(H14 + HOUR_NANOS)]).unwrap();
        let counts: Vec<_> = split.by_hour.iter().map(RecordBatch::num_rows).collect();
        assert_eq!(counts, [2, 1]);
        assert_eq!(split.rest.num_rows(), 2);
        let last = partition(&rows, &[hour_of(i64::MAX)]).unwrap();
        assert_eq!(last.by_hour.first().map(RecordBatch::num_rows), Some(1));
        assert_eq!(last.rest.num_rows(), 4);
        let none = partition(&rows, &[]).unwrap();
        assert_eq!(none.by_hour.len(), 0);
        assert_eq!(none.rest.num_rows(), 5);
        assert_eq!(
            hours_in(&[batch(&[H14, H14 + 1])]).unwrap().len(),
            1,
            "one hour needs no scan"
        );
        assert_eq!(hours_in(&[batch(&[])]).unwrap().len(), 0);
    }
}
