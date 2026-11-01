//! `usage_1h`: rows and usage logical bytes per signal, service and hour.
//!
//! The usage logical bytes of a row, version 1, are the payload of its
//! fields after the ingest limits: a primitive by its width, a boolean as
//! one byte, a string or a binary by its bytes, a fixed-size binary by its
//! size, a dictionary value by the value it names, a list, a map or a
//! struct by what it holds, recursively, a null as nothing. They leave out
//! compression, offsets, validity, dictionaries and Parquet overhead, and
//! the columns derived from others ([`derived_columns`]). Every sum is
//! checked: an overflow is an error, never a saturated count.
//!
//! A contribution is the counts of one pass for one hour, written as one
//! file of format v1; the table sums every contribution.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use arrow::array::{
    Array, ArrayRef, AsArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt8Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit, TimestampNanosecondType};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::SchemaError;

/// Version of the usage logical bytes.
pub const BYTES_VERSION: u32 = 1;
/// Version of the contribution format.
pub const SCHEMA_VERSION: u32 = 1;
/// `fusescope.signal` of a contribution.
pub const SIGNAL: &str = "usage_1h";
/// SQL table name.
pub const TABLE: &str = "usage_1h";

/// Column names of a contribution.
pub mod columns {
    /// Signal code of the rows counted: 1 spans, 2 logs.
    pub const SIGNAL: &str = "signal";
    /// Service of the rows, null for rows without one.
    pub const SERVICE_NAME: &str = "service_name";
    /// Start of the UTC hour of the rows' timestamps.
    pub const HOUR: &str = "hour";
    /// Rows counted.
    pub const ROWS: &str = "rows";
    /// Their usage logical bytes.
    pub const BYTES: &str = "bytes";
}

const HOUR_NANOS: i64 = 3_600_000_000_000;

/// The rows a contribution counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UsageSignal {
    /// Span rows, signal 1.
    Spans,
    /// Log rows, signal 2.
    Logs,
}

impl UsageSignal {
    /// The catalog code the `signal` column carries.
    pub fn code(self) -> u8 {
        match self {
            Self::Spans => 1,
            Self::Logs => 2,
        }
    }
}

/// Columns of `signal`'s rows that are derived from others or copies of
/// them, left out of the logical bytes: the time bucket, the resource
/// fingerprint, enum strings, `has_error` and the promoted attributes.
pub fn derived_columns(signal: UsageSignal) -> Vec<&'static str> {
    let mut out = vec!["ts_bucket_start", "resource_fingerprint"];
    match signal {
        UsageSignal::Spans => {
            out.extend([
                "kind_string",
                "status_code_string",
                "is_remote",
                "has_error",
            ]);
            out.extend(crate::spans::PROMOTED.iter().map(|p| p.column));
        }
        UsageSignal::Logs => {
            out.extend(crate::logs::LOG_PROMOTED.iter().map(|p| p.column));
        }
    }
    out
}

/// The usage logical bytes of every row of `batch`, rows of `signal`.
///
/// # Errors
/// A column of a type the version does not size, or an overflow.
pub fn row_bytes(batch: &RecordBatch, signal: UsageSignal) -> Result<Vec<u64>, SchemaError> {
    let skip = derived_columns(signal);
    let mut rows = vec![0u64; batch.num_rows()];
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if skip.contains(&field.name().as_str()) {
            continue;
        }
        for (row, size) in rows.iter_mut().zip(element_bytes(column)?) {
            *row = row.checked_add(size).ok_or(OVERFLOW)?;
        }
    }
    Ok(rows)
}

const OVERFLOW: SchemaError = SchemaError::Layout("usage logical bytes overflow");

/// The logical bytes of each element of `array`; nulls are nothing.
fn element_bytes(array: &ArrayRef) -> Result<Vec<u64>, SchemaError> {
    let valid = |i: usize, size: u64| if array.is_valid(i) { size } else { 0 };
    let len = array.len();
    let out = match array.data_type() {
        DataType::Boolean => (0..len).map(|i| valid(i, 1)).collect(),
        DataType::FixedSizeBinary(n) => {
            let n = u64::try_from(*n).map_err(|_| OVERFLOW)?;
            (0..len).map(|i| valid(i, n)).collect()
        }
        DataType::Utf8 => lengths(array.as_string::<i32>().iter().map(|v| v.map(str::len))),
        DataType::LargeUtf8 => lengths(array.as_string::<i64>().iter().map(|v| v.map(str::len))),
        DataType::Utf8View => lengths(array.as_string_view().iter().map(|v| v.map(str::len))),
        DataType::Binary => lengths(array.as_binary::<i32>().iter().map(|v| v.map(<[u8]>::len))),
        DataType::LargeBinary => {
            lengths(array.as_binary::<i64>().iter().map(|v| v.map(<[u8]>::len)))
        }
        DataType::Dictionary(_, _) => {
            let dictionary = array.as_any_dictionary();
            let values = element_bytes(dictionary.values())?;
            let keys = dictionary.normalized_keys();
            (0..len)
                .map(|i| {
                    let size = keys
                        .get(i)
                        .and_then(|k| values.get(*k))
                        .copied()
                        .unwrap_or(0);
                    valid(i, size)
                })
                .collect()
        }
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let child = element_bytes(list.values())?;
            per_row(&child, list.value_offsets(), array)?
        }
        DataType::Map(_, _) => {
            let map = array.as_map();
            let entries: ArrayRef = Arc::new(map.entries().clone());
            let child = element_bytes(&entries)?;
            per_row(&child, map.value_offsets(), array)?
        }
        DataType::Struct(_) => {
            let fields = array.as_struct();
            let mut sums = vec![0u64; len];
            for column in fields.columns() {
                for (sum, size) in sums.iter_mut().zip(element_bytes(column)?) {
                    *sum = sum.checked_add(size).ok_or(OVERFLOW)?;
                }
            }
            (0..len)
                .map(|i| valid(i, sums.get(i).copied().unwrap_or(0)))
                .collect()
        }
        other => match other.primitive_width() {
            Some(width) => {
                let width = u64::try_from(width).map_err(|_| OVERFLOW)?;
                (0..len).map(|i| valid(i, width)).collect()
            }
            None => return Err(SchemaError::Layout("a type usage bytes v1 does not size")),
        },
    };
    Ok(out)
}

fn lengths(values: impl Iterator<Item = Option<usize>>) -> Vec<u64> {
    values.map(|v| v.map_or(0, |len| len as u64)).collect()
}

/// Sums `child` over each row's `offsets` range; a null row is nothing.
fn per_row<O: arrow::array::OffsetSizeTrait>(
    child: &[u64],
    offsets: &[O],
    array: &ArrayRef,
) -> Result<Vec<u64>, SchemaError> {
    let mut out = Vec::with_capacity(array.len());
    for (i, window) in offsets.windows(2).enumerate() {
        let (start, end) = match window {
            [start, end] => (start.as_usize(), end.as_usize()),
            _ => (0, 0),
        };
        let mut sum = 0u64;
        if array.is_valid(i) {
            for size in child.get(start..end).unwrap_or_default() {
                sum = sum.checked_add(*size).ok_or(OVERFLOW)?;
            }
        }
        out.push(sum);
    }
    Ok(out)
}

/// Rows and logical bytes of one signal, per hour and service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageCounts {
    signal: UsageSignal,
    entries: BTreeMap<(i64, Option<String>), (u64, u64)>,
    /// What the entries hold, by [`UsageCounts::entry_bytes`].
    bytes: usize,
}

/// Bytes an entry holds besides its service name: its tree node, key and
/// counts.
const ENTRY_OVERHEAD: usize = 96;

impl UsageCounts {
    /// No row counted yet.
    pub fn new(signal: UsageSignal) -> Self {
        Self {
            signal,
            entries: BTreeMap::new(),
            bytes: 0,
        }
    }

    /// What one entry for `service` holds.
    pub fn entry_bytes(service: &str) -> usize {
        ENTRY_OVERHEAD + service.len()
    }

    /// What the counts hold, by [`Self::entry_bytes`].
    pub fn estimated_bytes(&self) -> usize {
        self.bytes
    }

    /// The signal counted.
    pub fn signal(&self) -> UsageSignal {
        self.signal
    }

    /// Distinct `(hour, service)` pairs counted.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when nothing was counted.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Counts every row of `batch`, by the hour of its `timestamp` and its
    /// `service_name`.
    ///
    /// # Errors
    /// A batch without those columns, a type the version does not size, or
    /// an overflow.
    pub fn absorb(&mut self, batch: &RecordBatch) -> Result<(), SchemaError> {
        let mut never = Vec::new();
        self.absorb_within(batch, usize::MAX, &mut never)
    }

    /// [`Self::absorb`] within `budget` bytes: before a new pair would take
    /// the counts past it, the counts so far are moved to `full` and
    /// counting goes on from none, so every row is counted once across
    /// `full` and `self`, each within the budget, or holding one pair.
    ///
    /// # Errors
    /// As [`Self::absorb`].
    pub fn absorb_within(
        &mut self,
        batch: &RecordBatch,
        budget: usize,
        full: &mut Vec<UsageCounts>,
    ) -> Result<(), SchemaError> {
        let bytes = row_bytes(batch, self.signal)?;
        let ts = batch
            .column_by_name("timestamp")
            .ok_or(SchemaError::Layout("usage: no timestamp column"))?
            .as_primitive_opt::<TimestampNanosecondType>()
            .ok_or(SchemaError::Layout("usage: timestamp is not nanoseconds"))?;
        let services = arrow::compute::cast(
            batch
                .column_by_name("service_name")
                .ok_or(SchemaError::Layout("usage: no service_name column"))?,
            &DataType::Utf8,
        )?;
        let services = services.as_string::<i32>();
        for (row, size) in bytes.into_iter().enumerate() {
            let hour = ts.value(row).div_euclid(HOUR_NANOS) * HOUR_NANOS;
            let name = services.is_valid(row).then(|| services.value(row));
            let key = (hour, name.map(str::to_owned));
            if !self.entries.contains_key(&key) {
                let cost = Self::entry_bytes(name.unwrap_or_default());
                if !self.entries.is_empty() && self.bytes.saturating_add(cost) > budget {
                    full.push(std::mem::replace(self, Self::new(self.signal)));
                }
                self.bytes = self.bytes.saturating_add(cost);
            }
            let entry = self.entries.entry(key).or_insert((0, 0));
            entry.0 = entry.0.checked_add(1).ok_or(OVERFLOW)?;
            entry.1 = entry.1.checked_add(size).ok_or(OVERFLOW)?;
        }
        Ok(())
    }

    /// Adds `other`, of the same signal.
    ///
    /// # Errors
    /// Another signal, or an overflow.
    pub fn merge(&mut self, other: Self) -> Result<(), SchemaError> {
        if other.signal != self.signal {
            return Err(SchemaError::Layout("usage counts of another signal"));
        }
        for (key, (rows, bytes)) in other.entries {
            if !self.entries.contains_key(&key) {
                self.bytes = self
                    .bytes
                    .saturating_add(Self::entry_bytes(key.1.as_deref().unwrap_or_default()));
            }
            let entry = self.entries.entry(key).or_insert((0, 0));
            entry.0 = entry.0.checked_add(rows).ok_or(OVERFLOW)?;
            entry.1 = entry.1.checked_add(bytes).ok_or(OVERFLOW)?;
        }
        Ok(())
    }

    /// The hours counted, as nanoseconds since the epoch, in order.
    pub fn hours(&self) -> Vec<i64> {
        let mut hours: Vec<i64> = self.entries.keys().map(|(hour, _)| *hour).collect();
        hours.dedup();
        hours
    }

    /// `(service, rows, bytes)` of `hour`, services in order.
    pub fn of_hour(&self, hour: i64) -> impl Iterator<Item = (Option<&str>, u64, u64)> {
        self.entries
            .range((hour, None)..(hour.saturating_add(1), None))
            .map(|((_, service), (rows, bytes))| (service.as_deref(), *rows, *bytes))
    }

    /// The contribution of `hour` as a batch of format v1.
    ///
    /// # Errors
    /// Arrow failure.
    pub fn hour_batch(&self, hour: i64) -> Result<RecordBatch, SchemaError> {
        let rows: Vec<_> = self.of_hour(hour).collect();
        let code = self.signal.code();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt8Array::from(vec![code; rows.len()])),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampNanosecondArray::from(vec![hour; rows.len()]).with_timezone("UTC")),
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ];
        Ok(RecordBatch::try_new(schema(), columns)?)
    }
}

/// Exact physical layout of format v1.
pub fn schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let metadata = [
                ("fusescope.signal".to_owned(), SIGNAL.to_owned()),
                (
                    "fusescope.schema_version".to_owned(),
                    SCHEMA_VERSION.to_string(),
                ),
                (
                    "fusescope.usage_bytes_version".to_owned(),
                    BYTES_VERSION.to_string(),
                ),
            ];
            Arc::new(Schema::new_with_metadata(
                vec![
                    Field::new(columns::SIGNAL, DataType::UInt8, false),
                    Field::new(columns::SERVICE_NAME, DataType::Utf8, true),
                    Field::new(
                        columns::HOUR,
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                        false,
                    ),
                    Field::new(columns::ROWS, DataType::UInt64, false),
                    Field::new(columns::BYTES, DataType::UInt64, false),
                ],
                metadata.into_iter().collect(),
            ))
        })
        .clone()
}

/// Refuses another signal, version, physical type, order or nullability.
///
/// # Errors
/// The schema is not exactly format v1 with usage bytes v1.
pub fn check_schema(found: &Schema) -> Result<(), SchemaError> {
    let expected = schema();
    if found.metadata() != expected.metadata() || found.fields() != expected.fields() {
        return Err(SchemaError::Layout("usage_1h format v1"));
    }
    Ok(())
}

/// One row of a contribution: signal code, service, hour in nanoseconds,
/// rows and bytes.
pub type UsageRow = (u8, Option<String>, i64, u64, u64);

/// The columns [`RowOrder`] reads.
pub const ROW_KEY_COLUMNS: &[&str] = &[columns::SIGNAL, columns::SERVICE_NAME, columns::HOUR];

/// Refuses rows a sum would misread: a signal other than spans or logs,
/// an hour not on the hour, or keys out of the order [`encode_hour`]
/// writes them, strictly ascending by `(signal, hour, service)` with no
/// service first, which is how a `(signal, service, hour)` listed twice
/// shows. Batch by batch, holding one key, so a file is checked in the
/// memory of its largest batch.
#[derive(Debug, Default)]
pub struct RowOrder {
    last: Option<(u8, i64, Option<String>)>,
}

impl RowOrder {
    /// Checks the next `batch` of the file, with at least
    /// [`ROW_KEY_COLUMNS`].
    ///
    /// # Errors
    /// A row as above, or a batch without those columns.
    pub fn check(&mut self, batch: &RecordBatch) -> Result<(), SchemaError> {
        let column = |name: &str| {
            batch.column_by_name(name).ok_or(SchemaError::Layout(
                "usage_1h rows without their key columns",
            ))
        };
        let signal = column(columns::SIGNAL)?
            .as_primitive_opt::<arrow::datatypes::UInt8Type>()
            .ok_or(SchemaError::Layout("usage_1h signal"))?;
        let service = column(columns::SERVICE_NAME)?
            .as_string_opt::<i32>()
            .ok_or(SchemaError::Layout("usage_1h service_name"))?;
        let hour = column(columns::HOUR)?
            .as_primitive_opt::<TimestampNanosecondType>()
            .ok_or(SchemaError::Layout("usage_1h hour"))?;
        for i in 0..batch.num_rows() {
            let key = (
                signal.value(i),
                hour.value(i),
                service.is_valid(i).then(|| service.value(i)),
            );
            if !matches!(key.0, 1 | 2) {
                return Err(SchemaError::Layout(
                    "usage_1h row of a signal other than spans or logs",
                ));
            }
            if key.1.rem_euclid(HOUR_NANOS) != 0 {
                return Err(SchemaError::Layout(
                    "usage_1h row of an hour not on the hour",
                ));
            }
            match &mut self.last {
                Some((s, h, name)) if (*s, *h, name.as_deref()) >= key => {
                    return Err(SchemaError::Layout(
                        "usage_1h keys out of order or listed twice in one file",
                    ));
                }
                Some((s, h, name)) => {
                    (*s, *h) = (key.0, key.1);
                    match (name.as_mut(), key.2) {
                        (Some(buffer), Some(value)) => {
                            buffer.clear();
                            buffer.push_str(value);
                        }
                        (_, value) => *name = value.map(str::to_owned),
                    }
                }
                None => self.last = Some((key.0, key.1, key.2.map(str::to_owned))),
            }
        }
        Ok(())
    }
}

/// [`RowOrder`] over the whole file.
///
/// # Errors
/// As [`RowOrder::check`].
pub fn check_rows(batches: &[RecordBatch]) -> Result<(), SchemaError> {
    let mut order = RowOrder::default();
    batches.iter().try_for_each(|batch| order.check(batch))
}

/// Encodes the contribution of `hour`.
///
/// # Errors
/// Arrow or Parquet failure.
pub fn encode_hour(counts: &UsageCounts, hour: i64) -> Result<Vec<u8>, SchemaError> {
    let batch = counts.hour_batch(hour)?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(
            crate::parquet::ZSTD_LEVEL,
        )?))
        .build();
    let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), Some(properties))?;
    writer.write(&batch)?;
    Ok(writer.into_inner()?)
}

/// Decodes and validates a contribution into `(signal, service, hour, rows, bytes)` rows.
///
/// # Errors
/// It is not a `usage_1h` format-v1 file, or [`check_rows`] refuses it.
pub fn decode(bytes: bytes::Bytes) -> Result<Vec<UsageRow>, SchemaError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    check_schema(builder.schema())?;
    let batches = builder.build()?.collect::<Result<Vec<_>, _>>()?;
    check_rows(&batches)?;
    let mut out = Vec::new();
    for batch in batches {
        let signal = batch
            .column(0)
            .as_primitive::<arrow::datatypes::UInt8Type>();
        let service = batch.column(1).as_string::<i32>();
        let hour = batch.column(2).as_primitive::<TimestampNanosecondType>();
        let rows = batch
            .column(3)
            .as_primitive::<arrow::datatypes::UInt64Type>();
        let bytes = batch
            .column(4)
            .as_primitive::<arrow::datatypes::UInt64Type>();
        for i in 0..batch.num_rows() {
            out.push((
                signal.value(i),
                service.is_valid(i).then(|| service.value(i).to_owned()),
                hour.value(i),
                rows.value(i),
                bytes.value(i),
            ));
        }
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)]
mod tests;
