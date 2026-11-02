//! The samples of resolved series: read through the samples table with
//! their source order, sorted by name, fingerprint, time and source order
//! descending, so the first row of a time is the one that wins and a
//! series' samples arrive in order; reduced to the last of each step
//! bucket when asked.

use std::collections::HashMap;

use arrow::array::AsArray;
use arrow::datatypes::{DataType, Float64Type, Int64Type, UInt32Type, UInt64Type};
use datafusion::dataframe::DataFrame;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::logical_expr::{col, lit};
use fs_schema::metrics::col as c;
use futures_util::StreamExt;

use super::request::{Buckets, Window};
use crate::QueryError;

/// Prometheus' stale marker.
pub const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// Most values a filter lists before it is left to the rows read.
const IN_LIST_MAX: usize = 1024;

/// Bytes of one kept sample: its time and value.
const SAMPLE_BYTES: usize = 16;

/// The samples of one series, in time order.
pub(crate) type Samples = Vec<(i64, f64)>;

/// The series asked for, by name and fingerprint, to their position.
pub(crate) type Positions = HashMap<Box<str>, HashMap<u64, usize>>;

/// Reads into `out`, one vector a position of `positions`, the samples of
/// `window` that `frame`, the samples table with its source order,
/// holds, at most `max_rows` of them after dedupe and reduction, each
/// vector's growth reserved before it grows.
///
/// # Errors
/// Planning or execution failure, a refused reservation, or more
/// samples than `max_rows`.
pub(crate) async fn read(
    frame: DataFrame,
    positions: &Positions,
    window: Window,
    buckets: Option<Buckets>,
    max_rows: usize,
    out: &mut [Samples],
    reservation: &mut MemoryReservation,
) -> Result<usize, QueryError> {
    let mut frame = frame.filter(
        col(c::UNIX_MILLI)
            .gt_eq(lit(window.start))
            .and(col(c::UNIX_MILLI).lt_eq(lit(window.end))),
    )?;
    if positions.len() <= IN_LIST_MAX {
        let names = positions.keys().map(|n| lit(&**n)).collect();
        frame = frame.filter(col(c::METRIC_NAME).in_list(names, false))?;
    }
    let fingerprints: usize = positions.values().map(HashMap::len).sum();
    if fingerprints <= IN_LIST_MAX {
        let list = positions
            .values()
            .flat_map(HashMap::keys)
            .map(|fp| lit(*fp))
            .collect();
        frame = frame.filter(col(c::FINGERPRINT).in_list(list, false))?;
    }
    let frame = frame
        .select_columns(&[
            c::METRIC_NAME,
            c::FINGERPRINT,
            c::UNIX_MILLI,
            c::VALUE,
            c::FLAGS,
            c::SOURCE_ORDER,
        ])?
        .sort(vec![
            col(c::METRIC_NAME).sort(true, false),
            col(c::FINGERPRINT).sort(true, false),
            col(c::UNIX_MILLI).sort(true, false),
            col(c::SOURCE_ORDER).sort(false, false),
        ])?;
    let mut stream = frame.execute_stream().await?;
    let mut reader = Reader {
        positions,
        buckets,
        max_rows,
        kept: 0,
        last: None,
        bucket: None,
    };
    while let Some(batch) = stream.next().await {
        reader.batch(&batch?, out, reservation)?;
    }
    Ok(reader.kept)
}

struct Reader<'a> {
    positions: &'a Positions,
    buckets: Option<Buckets>,
    max_rows: usize,
    kept: usize,
    /// The series and time of the last row read.
    last: Option<(usize, i64)>,
    /// The series and bucket of the last sample kept.
    bucket: Option<(usize, i64)>,
}

impl Reader<'_> {
    fn batch(
        &mut self,
        batch: &arrow::record_batch::RecordBatch,
        out: &mut [Samples],
        reservation: &mut MemoryReservation,
    ) -> Result<(), QueryError> {
        let names = arrow::compute::cast(batch.column(0), &DataType::Utf8)?;
        let names = names.as_string::<i32>();
        let fingerprints = batch.column(1).as_primitive::<UInt64Type>();
        let times = batch.column(2).as_primitive::<Int64Type>();
        let values = batch.column(3).as_primitive::<Float64Type>();
        let flags = batch.column(4).as_primitive::<UInt32Type>();
        let mut series: Option<(&str, Option<&HashMap<u64, usize>>)> = None;
        for row in 0..batch.num_rows() {
            let name = names.value(row);
            let of_name = match series {
                Some((last, of_name)) if last == name => of_name,
                _ => {
                    let of_name = self.positions.get(name);
                    series = Some((name, of_name));
                    of_name
                }
            };
            let Some(&at) = of_name.and_then(|f| f.get(&fingerprints.value(row))) else {
                continue;
            };
            let time = times.value(row);
            if self.last.replace((at, time)) == Some((at, time)) {
                continue;
            }
            let value = if flags.value(row) & 1 == 1 {
                f64::from_bits(STALE_NAN_BITS)
            } else {
                values.value(row)
            };
            let Some(samples) = out.get_mut(at) else {
                continue;
            };
            if let Some(buckets) = self.buckets {
                let bucket = buckets
                    .of(time)
                    .ok_or_else(|| QueryError::InvalidAction("a sample past its bucket".into()))?;
                if self.bucket.replace((at, bucket)) == Some((at, bucket)) {
                    if let Some(last) = samples.last_mut() {
                        *last = (time, value);
                    }
                    continue;
                }
            }
            self.kept += 1;
            if self.kept > self.max_rows {
                return Err(QueryError::TooManyRows(self.max_rows));
            }
            if samples.len() == samples.capacity() {
                let more = samples.capacity().max(4);
                reservation.try_grow(more * SAMPLE_BYTES)?;
                samples.reserve_exact(more);
            }
            samples.push((time, value));
        }
        Ok(())
    }
}
