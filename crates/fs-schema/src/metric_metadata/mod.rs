//! `metric_metadata`: per metric and label, each value and type it was
//! received as, with the first and last time a sample of it was reported
//! (`MAPPING.md`, "Metrics").
//!
//! A contribution, format v1, holds what one pass saw: one row a
//! `(metric_name, attr_name, attr_type, attr_datatype, attr_string_value)`,
//! strictly ascending by those as bytes, with `first_reported_unix_milli`
//! and `last_reported_unix_milli`. A label is listed under the level it won
//! from and once for each type its value was received as; `__name__` and a
//! label of legacy origin are not listed. The table aggregates every
//! contribution by the key, the least first and the greatest last.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::SchemaError;
use crate::metrics::{Datatype, LabelLevel, NAME, SeriesRow};

/// Version of the contribution format.
pub const SCHEMA_VERSION: u32 = 1;
/// `fusescope.signal` of a contribution.
pub const SIGNAL: &str = "metric_metadata";
/// SQL table name, as the Go backend reads it.
pub const TABLE: &str = "metadata";

/// Column names.
pub mod columns {
    /// The series' name.
    pub const METRIC_NAME: &str = "metric_name";
    /// The label's key.
    pub const ATTR_NAME: &str = "attr_name";
    /// `resource`, `scope` or `point`.
    pub const ATTR_TYPE: &str = "attr_type";
    /// The OTLP type the value was received as, as pdata names it.
    pub const ATTR_DATATYPE: &str = "attr_datatype";
    /// The value, as the label holds it.
    pub const ATTR_STRING_VALUE: &str = "attr_string_value";
    /// The earliest sample of it, in Unix milliseconds.
    pub const FIRST_REPORTED: &str = "first_reported_unix_milli";
    /// The latest sample of it, in Unix milliseconds.
    pub const LAST_REPORTED: &str = "last_reported_unix_milli";
}

/// Bytes an entry holds besides its texts: its tree node, key and times.
const ENTRY_OVERHEAD: usize = 160;

const OTHER: SchemaError = SchemaError::Layout("metric_metadata format v1");

/// What one pass saw, by key.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetadataCounts {
    /// By key, encoded as [`encode_key`] does, in the order of the fields.
    entries: BTreeMap<Box<[u8]>, (i64, i64)>,
    bytes: usize,
    /// The key being looked up, reused, its capacity counted in `bytes`.
    scratch: Vec<u8>,
}

impl MetadataCounts {
    /// What one entry of these texts holds, of the longest level and type.
    pub fn entry_bytes(metric: &str, attr: &str, value: &str) -> usize {
        ENTRY_OVERHEAD + key_len([metric, attr, "resource", "Double", value].map(str::as_bytes))
    }

    /// What the entries and the key being looked up hold.
    pub fn estimated_bytes(&self) -> usize {
        self.bytes
    }

    /// The least first and greatest last time listed, `None` without an
    /// entry.
    pub fn times(&self) -> Option<(i64, i64)> {
        self.entries
            .values()
            .copied()
            .reduce(|a, b| (a.0.min(b.0), a.1.max(b.1)))
    }

    /// Entries held.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True without an entry.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Lists the labels of `row`, whose samples span `first` to `last`, from
    /// its `from`th entry on, a label and type each, within `budget` bytes,
    /// every allocation admitted before it is made. Before a new entry, or
    /// the key it is looked up by, would take the counts past it, listing
    /// stops and the entry to resume at is returned. `None` once the row is
    /// listed.
    ///
    /// # Errors
    /// An entry that does not fit the budget even in empty counts.
    pub fn add_series(
        &mut self,
        row: &SeriesRow,
        (first, last): (i64, i64),
        from: usize,
        budget: usize,
    ) -> Result<Option<usize>, SchemaError> {
        let past = || SchemaError::Layout("metric_metadata: an entry past its budget alone");
        let entries = row
            .labels
            .iter()
            .zip(row.label_meta.iter())
            .filter(|((key, _), _)| &**key != NAME)
            .filter_map(|(label, meta)| Some((label, meta.level()?, *meta)))
            .flat_map(|(label, level, meta)| meta.datatypes().map(move |d| (label, level, d)));
        for (at, ((key, value), level, datatype)) in entries.enumerate().skip(from) {
            let fields =
                [&*row.name, key, level.as_str(), datatype.as_str(), value].map(str::as_bytes);
            let needed = key_len(fields);
            let grown = needed.saturating_sub(self.scratch.capacity());
            if grown > 0 && self.bytes.saturating_add(grown) > budget {
                if self.is_empty() {
                    return Err(past());
                }
                return Ok(Some(at));
            }
            let before = self.scratch.capacity();
            encode_key(&mut self.scratch, fields);
            self.bytes = self.bytes.saturating_add(self.scratch.capacity() - before);
            if let Some(times) = self.entries.get_mut(self.scratch.as_slice()) {
                times.0 = times.0.min(first);
                times.1 = times.1.max(last);
                continue;
            }
            let cost = ENTRY_OVERHEAD + needed;
            if self.bytes.saturating_add(cost) > budget {
                if self.is_empty() {
                    return Err(past());
                }
                return Ok(Some(at));
            }
            self.bytes = self.bytes.saturating_add(cost);
            self.entries
                .insert(self.scratch.as_slice().into(), (first, last));
        }
        Ok(None)
    }

    /// The counts so far, `self` starting from none.
    #[must_use]
    pub fn take(&mut self) -> Self {
        std::mem::take(self)
    }

    /// The contribution as a batch of format v1, in key order.
    ///
    /// # Errors
    /// Arrow failure.
    pub fn batch(&self) -> Result<RecordBatch, SchemaError> {
        let keys: Vec<[String; 5]> = self.entries.keys().map(|k| decode_key(k)).collect();
        let text = |i: usize| -> ArrayRef {
            Arc::new(StringArray::from_iter_values(
                keys.iter().map(|k| k.get(i).map_or("", String::as_str)),
            ))
        };
        let times = |f: fn(&(i64, i64)) -> i64| -> ArrayRef {
            Arc::new(Int64Array::from_iter_values(self.entries.values().map(f)))
        };
        let columns = vec![
            text(0),
            text(1),
            text(2),
            text(3),
            text(4),
            times(|t| t.0),
            times(|t| t.1),
        ];
        Ok(RecordBatch::try_new(schema(), columns)?)
    }
}

/// Bytes [`encode_key`] writes for `fields`.
fn key_len(fields: [&[u8]; 5]) -> usize {
    fields
        .iter()
        .map(|f| f.len() + f.iter().fold(0, |n, b| n + usize::from(*b == 0)) + 2)
        .sum()
}

/// Writes `fields` into `out` so keys compare as bytes as their fields
/// compare in turn: a zero byte as `0x00 0xff`, each field ended by
/// `0x00 0x00`.
fn encode_key(out: &mut Vec<u8>, fields: [&[u8]; 5]) {
    out.clear();
    out.reserve_exact(key_len(fields));
    for field in fields {
        for byte in field {
            out.push(*byte);
            if *byte == 0 {
                out.push(0xff);
            }
        }
        out.extend_from_slice(&[0, 0]);
    }
}

/// The fields of a key [`encode_key`] wrote.
fn decode_key(key: &[u8]) -> [String; 5] {
    let mut fields: [Vec<u8>; 5] = Default::default();
    let mut at = 0;
    let mut bytes = key.iter().copied().peekable();
    while let Some(byte) = bytes.next() {
        if byte == 0 {
            if bytes.next() == Some(0) {
                at += 1;
                continue;
            }
            if let Some(field) = fields.get_mut(at) {
                field.push(0);
            }
            continue;
        }
        if let Some(field) = fields.get_mut(at) {
            field.push(byte);
        }
    }
    fields.map(|f| String::from_utf8(f).unwrap_or_default())
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
            ];
            let text = |name| Field::new(name, DataType::Utf8, false);
            let time = |name| Field::new(name, DataType::Int64, false);
            Arc::new(Schema::new_with_metadata(
                vec![
                    text(columns::METRIC_NAME),
                    text(columns::ATTR_NAME),
                    text(columns::ATTR_TYPE),
                    text(columns::ATTR_DATATYPE),
                    text(columns::ATTR_STRING_VALUE),
                    time(columns::FIRST_REPORTED),
                    time(columns::LAST_REPORTED),
                ],
                metadata.into_iter().collect(),
            ))
        })
        .clone()
}

/// Refuses another signal, version, physical type, order or nullability.
///
/// # Errors
/// The schema is not exactly format v1.
pub fn check_schema(found: &Schema) -> Result<(), SchemaError> {
    let expected = schema();
    if found.metadata() != expected.metadata() || found.fields() != expected.fields() {
        return Err(OTHER);
    }
    Ok(())
}

/// Refuses rows the table would misread: a level or type it does not
/// name, a first time past the last, or keys not strictly ascending as
/// bytes. Batch by batch, holding one key.
#[derive(Debug, Default)]
pub struct RowOrder {
    last: Option<Vec<Vec<u8>>>,
}

impl RowOrder {
    /// Checks the next `batch` of the file, of the columns and types of
    /// format v1, whatever metadata its schema carries.
    ///
    /// # Errors
    /// A row as above, or a batch of other columns.
    pub fn check(&mut self, batch: &RecordBatch) -> Result<(), SchemaError> {
        if batch.schema().fields() != schema().fields() {
            return Err(OTHER);
        }
        let text: Vec<&StringArray> = (0..5)
            .map(|i| batch.column(i).as_string_opt::<i32>().ok_or(OTHER))
            .collect::<Result<_, _>>()?;
        let time = |i: usize| batch.column(i).as_primitive_opt::<Int64Type>().ok_or(OTHER);
        let (first, last) = (time(5)?, time(6)?);
        let levels =
            [LabelLevel::Resource, LabelLevel::Scope, LabelLevel::Point].map(LabelLevel::as_str);
        let types = Datatype::ALL.map(Datatype::as_str);
        for row in 0..batch.num_rows() {
            let key: Vec<&[u8]> = text.iter().map(|c| c.value(row).as_bytes()).collect();
            let (level, datatype) = (
                text.get(2).map_or("", |c| c.value(row)),
                text.get(3).map_or("", |c| c.value(row)),
            );
            if !levels.contains(&level) || !types.contains(&datatype) {
                return Err(SchemaError::Layout(
                    "metric_metadata row of an unknown level or type",
                ));
            }
            if first.value(row) > last.value(row) {
                return Err(SchemaError::Layout(
                    "metric_metadata row first reported after its last",
                ));
            }
            if self
                .last
                .as_ref()
                .is_some_and(|before| before.iter().map(Vec::as_slice).ge(key.iter().copied()))
            {
                return Err(SchemaError::Layout(
                    "metric_metadata keys out of order or listed twice in one file",
                ));
            }
            self.last = Some(key.iter().map(|k| k.to_vec()).collect());
        }
        Ok(())
    }
}

/// Encodes a contribution.
///
/// # Errors
/// Arrow or Parquet failure.
pub fn encode(counts: &MetadataCounts) -> Result<Vec<u8>, SchemaError> {
    let batch = counts.batch()?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(
            crate::parquet::ZSTD_LEVEL,
        )?))
        .build();
    let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), Some(properties))?;
    writer.write(&batch)?;
    Ok(writer.into_inner()?)
}

/// One row: metric, attribute, level, type, value, first and last.
pub type MetadataRow = (String, String, String, String, String, i64, i64);

/// Decodes and validates a contribution.
///
/// # Errors
/// It is not a `metric_metadata` file of format v1, or [`RowOrder`]
/// refuses it.
pub fn decode(bytes: bytes::Bytes) -> Result<Vec<MetadataRow>, SchemaError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    check_schema(builder.schema())?;
    let schema = Arc::clone(builder.schema());
    let mut order = RowOrder::default();
    let mut out = Vec::new();
    for batch in builder.build()? {
        let batch = batch?.with_schema(Arc::clone(&schema))?;
        order.check(&batch)?;
        let text = |i: usize| batch.column(i).as_string::<i32>();
        let time = |i: usize| batch.column(i).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            out.push((
                text(0).value(row).to_owned(),
                text(1).value(row).to_owned(),
                text(2).value(row).to_owned(),
                text(3).value(row).to_owned(),
                text(4).value(row).to_owned(),
                time(5).value(row),
                time(6).value(row),
            ));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
