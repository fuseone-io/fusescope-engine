//! Calendar hours of span rows: an L0 file never holds rows of two.

use std::collections::BTreeSet;

use arrow::array::{AsArray, BooleanArray, PrimitiveArray};
use arrow::compute::filter_record_batch;
use arrow::datatypes::TimestampNanosecondType;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, TimeZone, Utc};
use fs_schema::spans::col;

use crate::IngestError;

const HOUR_NANOS: i64 = 3_600_000_000_000;

/// Start of the UTC hour holding a timestamp in nanoseconds since the epoch.
pub fn hour_of(nanos: i64) -> DateTime<Utc> {
    Utc.timestamp_nanos(nanos.div_euclid(HOUR_NANOS) * HOUR_NANOS)
}

/// Every UTC hour with a row in `batches`, in order.
///
/// # Errors
/// A batch without the span timestamp column.
pub fn hours_in(batches: &[RecordBatch]) -> Result<BTreeSet<DateTime<Utc>>, IngestError> {
    let mut hours = BTreeSet::new();
    for batch in batches {
        let ts = timestamps(batch)?;
        let (Some(min), Some(max)) = (arrow::compute::min(ts), arrow::compute::max(ts)) else {
            continue;
        };
        if hour_of(min) == hour_of(max) {
            hours.insert(hour_of(min));
        } else {
            hours.extend(ts.values().iter().map(|t| hour_of(*t)));
        }
    }
    Ok(hours)
}

/// The rows of `batch` whose timestamp falls in `hour`.
///
/// # Errors
/// A batch without the span timestamp column, or an Arrow failure.
pub fn rows_in_hour(batch: &RecordBatch, hour: DateTime<Utc>) -> Result<RecordBatch, IngestError> {
    let start = hour_nanos(hour)?;
    let end = start.saturating_add(HOUR_NANOS);
    let ts = timestamps(batch)?;
    let mask: BooleanArray = ts
        .iter()
        .map(|t| t.map(|t| t >= start && t < end))
        .collect();
    Ok(filter_record_batch(batch, &mask)?)
}

/// The rows of `batch` whose timestamp falls in none of `hours`.
///
/// # Errors
/// A batch without the span timestamp column, or an Arrow failure.
pub fn rows_outside(
    batch: &RecordBatch,
    hours: &[DateTime<Utc>],
) -> Result<RecordBatch, IngestError> {
    let starts = hours
        .iter()
        .map(|h| hour_nanos(*h))
        .collect::<Result<Vec<_>, _>>()?;
    let ts = timestamps(batch)?;
    let mask: BooleanArray = ts
        .iter()
        .map(|t| {
            t.map(|t| {
                !starts
                    .iter()
                    .any(|s| t >= *s && t < s.saturating_add(HOUR_NANOS))
            })
        })
        .collect();
    Ok(filter_record_batch(batch, &mask)?)
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

    #[test]
    fn rows_are_split_by_their_hour_and_the_rest_kept_apart() {
        let rows = batch(&[H14 + 5, H14 + HOUR_NANOS + 5, H14 + 7, H14 + 3 * HOUR_NANOS]);
        let hours: Vec<_> = hours_in(std::slice::from_ref(&rows))
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(
            hours,
            [
                hour_of(H14),
                hour_of(H14 + HOUR_NANOS),
                hour_of(H14 + 3 * HOUR_NANOS)
            ]
        );
        assert_eq!(rows_in_hour(&rows, hour_of(H14)).unwrap().num_rows(), 2);
        assert_eq!(
            rows_in_hour(&rows, hour_of(H14 + HOUR_NANOS))
                .unwrap()
                .num_rows(),
            1
        );
        assert_eq!(
            rows_in_hour(&rows, hour_of(H14 + 2 * HOUR_NANOS))
                .unwrap()
                .num_rows(),
            0
        );
        let outside = rows_outside(&rows, &[hour_of(H14), hour_of(H14 + HOUR_NANOS)]).unwrap();
        assert_eq!(outside.num_rows(), 1);
        assert_eq!(
            hours_in(&[batch(&[H14, H14 + 1])]).unwrap().len(),
            1,
            "one hour needs no scan"
        );
        assert!(hours_in(&[batch(&[])]).unwrap().is_empty());
    }
}
