//! Writes the two files of a metric flush: the samples, sorted and
//! deduplicated, then their series.
//!
//! Every input batch is sorted by a permutation of its rows, by `(name,
//! fingerprint, time, source order)`, and the inputs merge k ways in that
//! order; of a run of one fingerprint and millisecond only the last, the
//! largest source order, is kept, so a file written from chunks and one
//! written from the same rows replayed one request at a time are the same
//! bytes. Row groups are gathered, written and flushed one at a time. The
//! series file is written from the series of the same rows, already one a
//! fingerprint and hour in order.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::Arc;

use arrow::array::{Array, AsArray, FixedSizeBinaryArray};
use arrow::datatypes::{Int32Type, Int64Type, UInt64Type};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::{DateTime, Duration, TimeZone, Utc};
use fs_catalog::{FileId, Level, MetricRole, NewFile, TenantId, TimeRange};
use fs_schema::metrics::{
    SOURCE_ORDER_BYTES, col, samples_schema_v1, series_schema_v1, series_schema_v2,
};
use object_store::path::Path;
use parquet::arrow::ArrowWriter;

use crate::IngestError;
use crate::buffer::HotBatch;
use crate::flush::{Prepared, check_layout};
use crate::record::{FileLayout, RowSignal};

/// `{root}/t/{tenant}/metrics/{samples|series}/l0/dt=YYYY-MM-DD/hr=HH/{id}.parquet`
pub fn metric_path(
    root: &Path,
    tenant: TenantId,
    role: MetricRole,
    hour: DateTime<Utc>,
    id: FileId,
) -> Path {
    let relative = format!(
        "t/{}/{}/l0/dt={}/hr={:02}/{}.parquet",
        tenant.0,
        role.signal().path_name(),
        hour.format("%Y-%m-%d"),
        hour.format("%H"),
        id.0
    );
    if root.as_ref().is_empty() {
        Path::from(relative)
    } else {
        Path::from(format!("{root}/{relative}"))
    }
}

/// The samples file `id` of `tenant`'s rows of `hour` in `batches`.
///
/// CPU bound: callers run it on a blocking thread.
///
/// # Errors
/// Arrow or Parquet failures, inputs that are not samples, or a layout
/// this engine does not write.
pub fn prepare_samples(
    tenant: TenantId,
    root: &Path,
    id: FileId,
    hour: DateTime<Utc>,
    batches: &[RecordBatch],
    layout: FileLayout,
) -> Result<Prepared, IngestError> {
    check_layout(&layout, RowSignal::Metrics)?;
    let schema = samples_schema_v1();
    let sort_started = std::time::Instant::now();
    let inputs: Vec<&RecordBatch> = batches.iter().filter(|b| b.num_rows() > 0).collect();
    let keys = Keys::new(&inputs)?;
    let concat = sort_started.elapsed();
    let path = metric_path(root, tenant, MetricRole::Samples, hour, id);
    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(
        &mut out,
        Arc::clone(&schema),
        Some(fs_schema::parquet::metric_writer_properties(
            layout.row_group_rows,
        )?),
    )?;
    let mut merge = std::time::Duration::ZERO;
    let mut encode = std::time::Duration::ZERO;
    let mut writer_peak = 0;
    let mut rows = Vec::new();
    let (mut ts_min, mut ts_max) = (i64::MAX, i64::MIN);
    let owned = batches_of(&inputs);
    let mut heads = keys.heads();
    let mut pending = None;
    loop {
        let merge_started = std::time::Instant::now();
        let picks = keys.next_group(&mut heads, &mut pending, layout.row_group_rows);
        if picks.is_empty() {
            break;
        }
        let group = fs_schema::parquet::gather(&schema, &owned, &picks)?;
        merge += merge_started.elapsed();
        let times = group
            .column(schema.index_of(col::UNIX_MILLI)?)
            .as_primitive::<Int64Type>();
        ts_min = ts_min.min(arrow::compute::min(times).unwrap_or(ts_min));
        ts_max = ts_max.max(arrow::compute::max(times).unwrap_or(ts_max));
        let encode_started = std::time::Instant::now();
        writer.write(&group)?;
        writer_peak = writer_peak.max(writer.memory_size());
        writer.flush()?;
        encode += encode_started.elapsed();
        rows.push(HotBatch {
            batch: group,
            traces: None,
        });
    }
    let encode_started = std::time::Instant::now();
    writer.close()?;
    encode += encode_started.elapsed();
    let row_count: usize = rows.iter().map(|r| r.batch.num_rows()).sum();
    if row_count == 0 {
        (ts_min, ts_max) = (0, 0);
    }
    let range = TimeRange {
        start: Utc.timestamp_millis_opt(ts_min).single().unwrap_or(hour),
        end: Utc.timestamp_millis_opt(ts_max).single().unwrap_or(hour),
    };
    Ok(prepared(
        (tenant, id, MetricRole::Samples, path),
        Bytes::from(out),
        rows,
        range,
        (concat, merge, encode, writer_peak),
    ))
}

/// The series file `id` of `tenant`'s rows of `hour`: `series`, rows of
/// the series schema in order, written in row groups of the layout's size.
///
/// # Errors
/// Parquet failures, or a layout this engine does not write.
pub fn prepare_series(
    tenant: TenantId,
    root: &Path,
    id: FileId,
    hour: DateTime<Utc>,
    series: &RecordBatch,
    layout: FileLayout,
) -> Result<Prepared, IngestError> {
    check_layout(&layout, RowSignal::Metrics)?;
    if series.schema().fields() != series_schema_v2().fields() {
        return Err(IngestError::Encode("series rows of another schema".into()));
    }
    let (schema, series) = if layout.version == FileLayout::METRIC_VERSION {
        (series_schema_v2(), series.clone())
    } else {
        let schema = series_schema_v1();
        let columns = series
            .columns()
            .get(..schema.fields().len())
            .unwrap_or_default()
            .to_vec();
        (Arc::clone(&schema), RecordBatch::try_new(schema, columns)?)
    };
    let series = &series;
    let path = metric_path(root, tenant, MetricRole::Series, hour, id);
    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(
        &mut out,
        Arc::clone(&schema),
        Some(fs_schema::parquet::metric_writer_properties(
            layout.row_group_rows,
        )?),
    )?;
    let encode_started = std::time::Instant::now();
    let mut writer_peak = 0;
    let mut rows = Vec::new();
    let step = layout.row_group_rows.max(1);
    let mut start = 0;
    while start < series.num_rows() {
        let group = series.slice(start, step.min(series.num_rows() - start));
        writer.write(&group)?;
        writer_peak = writer_peak.max(writer.memory_size());
        writer.flush()?;
        start += group.num_rows();
        rows.push(HotBatch {
            batch: group,
            traces: None,
        });
    }
    writer.close()?;
    let encode = encode_started.elapsed();
    let range = TimeRange {
        start: hour,
        end: hour + Duration::hours(1) - Duration::milliseconds(1),
    };
    Ok(prepared(
        (tenant, id, MetricRole::Series, path),
        Bytes::from(out),
        rows,
        range,
        (
            std::time::Duration::ZERO,
            std::time::Duration::ZERO,
            encode,
            writer_peak,
        ),
    ))
}

fn prepared(
    (tenant, id, role, path): (TenantId, FileId, MetricRole, Path),
    bytes: Bytes,
    rows: Vec<HotBatch>,
    range: TimeRange,
    (concat, sort, encode, writer_peak): (
        std::time::Duration,
        std::time::Duration,
        std::time::Duration,
        usize,
    ),
) -> Prepared {
    let row_count: usize = rows.iter().map(|r| r.batch.num_rows()).sum();
    let content_sha256 = crate::sha256::digest(&bytes);
    let file = NewFile {
        id,
        origin: id,
        tenant_id: tenant,
        signal: role.signal(),
        level: Level::L0,
        path: path.to_string(),
        size_bytes: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
        row_count: i64::try_from(row_count).unwrap_or(i64::MAX),
        range,
        services: Vec::new(),
        summary: None,
        content_sha256: Some(content_sha256),
        usage_accounted: false,
    };
    Prepared {
        file,
        bytes,
        index: None,
        rows,
        concat,
        sort,
        encode,
        writer_peak,
    }
}

fn batches_of(inputs: &[&RecordBatch]) -> Vec<RecordBatch> {
    inputs.iter().map(|b| (*b).clone()).collect()
}

/// The sort keys of every input, read in place, and each input's rows in
/// key order.
struct Keys<'a> {
    /// Per input, per dictionary key of its names: the name's rank among
    /// every name of the file.
    ranks: Vec<Vec<u32>>,
    names: Vec<&'a arrow::array::PrimitiveArray<Int32Type>>,
    fingerprints: Vec<&'a [u64]>,
    times: Vec<&'a [i64]>,
    orders: Vec<&'a FixedSizeBinaryArray>,
    /// Per input, its rows in key order.
    sorted: Vec<Vec<u32>>,
}

/// One row waiting at the head of its input: name rank, fingerprint,
/// time, source order, input, position in the input's order.
type Head = Reverse<(u32, u64, i64, [u8; SOURCE_ORDER_BYTES], usize, usize)>;

impl<'a> Keys<'a> {
    fn new(inputs: &[&'a RecordBatch]) -> Result<Self, IngestError> {
        let mut all = BTreeMap::new();
        let mut columns = Vec::with_capacity(inputs.len());
        for batch in inputs {
            let schema = batch.schema();
            let column = |name: &str| -> Result<&'a Arc<dyn Array>, IngestError> {
                Ok(batch.column(schema.index_of(name)?))
            };
            let names = column(col::METRIC_NAME)?
                .as_dictionary_opt::<Int32Type>()
                .ok_or_else(|| IngestError::Encode("sample names are not a dictionary".into()))?;
            let values = names.values().as_string::<i32>();
            for value in values.iter().flatten() {
                all.entry(value.to_owned()).or_insert(0u32);
            }
            let fingerprints = column(col::FINGERPRINT)?.as_primitive::<UInt64Type>();
            let times = column(col::UNIX_MILLI)?.as_primitive::<Int64Type>();
            let orders = column(col::SOURCE_ORDER)?
                .as_fixed_size_binary_opt()
                .filter(|a| usize::try_from(a.value_length()).ok() == Some(SOURCE_ORDER_BYTES))
                .ok_or_else(|| IngestError::Encode("a source order of another width".into()))?;
            columns.push((names, fingerprints.values(), times.values(), orders));
        }
        for (rank, slot) in all.values_mut().enumerate() {
            *slot = u32::try_from(rank).unwrap_or(u32::MAX);
        }
        let mut keys = Self {
            ranks: Vec::with_capacity(inputs.len()),
            names: Vec::with_capacity(inputs.len()),
            fingerprints: Vec::with_capacity(inputs.len()),
            times: Vec::with_capacity(inputs.len()),
            orders: Vec::with_capacity(inputs.len()),
            sorted: Vec::with_capacity(inputs.len()),
        };
        for (names, fingerprints, times, orders) in columns {
            let values = names.values().as_string::<i32>();
            keys.ranks.push(
                values
                    .iter()
                    .map(|v| v.and_then(|v| all.get(v)).copied().unwrap_or(0))
                    .collect(),
            );
            keys.names.push(names.keys());
            keys.fingerprints.push(fingerprints);
            keys.times.push(times);
            keys.orders.push(orders);
        }
        for input in 0..inputs.len() {
            let n = u32::try_from(keys.fingerprints.get(input).map_or(0, |f| f.len()))
                .map_err(|_| IngestError::Encode("too many samples in one batch".into()))?;
            let mut order: Vec<u32> = (0..n).collect();
            order.sort_unstable_by_key(|row| keys.key(input, *row as usize));
            keys.sorted.push(order);
        }
        Ok(keys)
    }

    fn key(&self, input: usize, row: usize) -> Option<(u32, u64, i64, [u8; SOURCE_ORDER_BYTES])> {
        let name = self.names.get(input)?.values().get(row)?;
        let rank = *self.ranks.get(input)?.get(usize::try_from(*name).ok()?)?;
        let fingerprint = *self.fingerprints.get(input)?.get(row)?;
        let time = *self.times.get(input)?.get(row)?;
        let order =
            <[u8; SOURCE_ORDER_BYTES]>::try_from(self.orders.get(input)?.value(row)).ok()?;
        Some((rank, fingerprint, time, order))
    }

    fn head(&self, input: usize, at: usize) -> Option<Head> {
        let row = *self.sorted.get(input)?.get(at)? as usize;
        let (rank, fingerprint, time, order) = self.key(input, row)?;
        Some(Reverse((rank, fingerprint, time, order, input, at)))
    }

    fn heads(&self) -> BinaryHeap<Head> {
        (0..self.sorted.len())
            .filter_map(|i| self.head(i, 0))
            .collect()
    }

    /// The next `limit` rows in merged order, as `(input, row)`, each the
    /// last of its name, fingerprint and millisecond. `pending` carries the
    /// row a run may still replace across groups.
    fn next_group(
        &self,
        heads: &mut BinaryHeap<Head>,
        pending: &mut Option<Pending>,
        limit: usize,
    ) -> Vec<(usize, usize)> {
        let mut out = Vec::new();
        while out.len() < limit.max(1) {
            let Some(Reverse((rank, fingerprint, time, _, input, at))) = heads.pop() else {
                out.extend(pending.take().map(|p| (p.input, p.row)));
                break;
            };
            if let Some(next) = self.head(input, at + 1) {
                heads.push(next);
            }
            let row = self
                .sorted
                .get(input)
                .and_then(|s| s.get(at))
                .map_or(0, |r| *r as usize);
            let next = Pending {
                key: (rank, fingerprint, time),
                input,
                row,
            };
            match pending {
                Some(p) if p.key == next.key => *pending = Some(next),
                _ => out.extend(pending.replace(next).map(|p| (p.input, p.row))),
            }
        }
        out
    }
}

/// The row a run of one name, fingerprint and millisecond keeps so far.
#[derive(Debug, Clone, Copy)]
struct Pending {
    key: (u32, u64, i64),
    input: usize,
    row: usize,
}

#[cfg(test)]
mod tests;
