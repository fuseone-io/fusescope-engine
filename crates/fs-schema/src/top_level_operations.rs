//! Per-compaction-pass top-level operations.
//!
//! Format v1 has one row per `(service_name, name)` seen in the pass on a
//! span that has no parent or whose parent is remote (`is_remote = "true"`),
//! with `last_seen`, the latest such span start. Files are partial
//! contributions: queries combine every visible file with `max(last_seen)`.

use std::collections::BTreeMap;
use std::mem::size_of;
use std::sync::{Arc, OnceLock};

use arrow::array::{Array, AsArray, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::SchemaError;
use crate::spans::col;
pub use crate::trace_summary::LimitExceeded;

/// Version this engine writes and reads.
pub const SCHEMA_VERSION: u32 = 1;
/// Value of the `fusescope.signal` metadata.
pub const SIGNAL: &str = "top_level_operations";
/// Name in a query session.
pub const TABLE: &str = "top_level_operations";
/// `is_remote` of a span whose parent context is known to be remote.
pub const REMOTE_PARENT: &str = "true";
const BTREE_ENTRY_OVERHEAD: usize = 4 * size_of::<usize>();
const OPERATION_OUTPUT_FIXED_BYTES: usize = 2 * size_of::<i32>() + size_of::<i64>();

/// One operation's map entry, B-tree node slack and fixed Arrow output
/// storage, before the UTF-8 bytes of its service and name.
pub const OPERATION_ENTRY_BYTES: usize = size_of::<(Arc<str>, Arc<str>)>()
    + size_of::<i64>()
    + BTREE_ENTRY_OVERHEAD
    + OPERATION_OUTPUT_FIXED_BYTES;

/// Column names.
pub mod columns {
    /// Service of the span.
    pub const SERVICE_NAME: &str = "service_name";
    /// Span name.
    pub const NAME: &str = "name";
    /// Latest start of a top-level span of the operation in the pass.
    pub const LAST_SEEN: &str = "last_seen";
}

/// Operations built from one compaction pass. Crossing either cap clears
/// the partial map and permanently marks the contribution skipped.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TopLevelOperations {
    rows: BTreeMap<(Arc<str>, Arc<str>), i64>,
    range: Option<(i64, i64)>,
    estimated_bytes: usize,
    exceeded: Option<LimitExceeded>,
}

impl TopLevelOperations {
    /// Adds the top-level spans of a batch while distinct operations and
    /// conservatively charged resident bytes stay within their caps. A span
    /// without a service never counts. Once a cap is crossed, it stays empty.
    ///
    /// # Errors
    /// A required span column is missing or has another physical type.
    pub fn absorb(
        &mut self,
        batch: &RecordBatch,
        max_operations: usize,
        max_bytes: usize,
    ) -> Result<(), SchemaError> {
        if self.exceeded.is_some() {
            return Ok(());
        }
        let schema = batch.schema();
        let parents = batch.column(schema.index_of(col::PARENT_SPAN_ID)?);
        let timestamps = batch
            .column(schema.index_of(col::TIMESTAMP)?)
            .as_primitive_opt::<TimestampNanosecondType>()
            .ok_or(SchemaError::Layout("timestamp"))?;
        let text = |name: &str| -> Result<Arc<dyn Array>, SchemaError> {
            Ok(arrow::compute::cast(
                batch.column(schema.index_of(name)?),
                &DataType::Utf8,
            )?)
        };
        let remotes = text(col::IS_REMOTE)?;
        let remotes = remotes.as_string::<i32>();
        let services = text(col::SERVICE_NAME)?;
        let services = services.as_string::<i32>();
        let names = text(col::NAME)?;
        let names = names.as_string::<i32>();
        for row in 0..batch.num_rows() {
            let top_level = parents.is_null(row)
                || (!remotes.is_null(row) && remotes.value(row) == REMOTE_PARENT);
            if !top_level || services.is_null(row) || services.value(row).is_empty() {
                continue;
            }
            let service = services.value(row);
            let name = if names.is_null(row) {
                ""
            } else {
                names.value(row)
            };
            let start = timestamps.value(row);
            if !self.add(service, name, start, max_operations, max_bytes) {
                return Ok(());
            }
        }
        Ok(())
    }

    fn add(
        &mut self,
        service: &str,
        name: &str,
        start: i64,
        max_operations: usize,
        max_bytes: usize,
    ) -> bool {
        let key = (Arc::<str>::from(service), Arc::<str>::from(name));
        if let Some(last_seen) = self.rows.get_mut(&key) {
            *last_seen = (*last_seen).max(start);
        } else {
            let charge = OPERATION_ENTRY_BYTES
                .saturating_add(service.len())
                .saturating_add(name.len());
            let next_bytes = self.estimated_bytes.saturating_add(charge);
            if self.rows.len() >= max_operations {
                self.skip(LimitExceeded {
                    limit: "top_level_operations_max_operations",
                    cap: max_operations,
                    observed: self.rows.len().saturating_add(1),
                    estimated_bytes: next_bytes,
                });
                return false;
            }
            if next_bytes > max_bytes {
                self.skip(LimitExceeded {
                    limit: "top_level_operations_max_bytes",
                    cap: max_bytes,
                    observed: next_bytes,
                    estimated_bytes: next_bytes,
                });
                return false;
            }
            self.estimated_bytes = next_bytes;
            self.rows.insert(key, start);
        }
        self.widen(start);
        true
    }

    fn widen(&mut self, start: i64) {
        self.range = Some(match self.range {
            None => (start, start),
            Some((low, high)) => (low.min(start), high.max(start)),
        });
    }

    /// True when a cap was crossed and no partial file may be emitted.
    pub fn skipped(&self) -> bool {
        self.exceeded.is_some()
    }

    /// The first bound crossed by this contribution.
    pub fn limit_exceeded(&self) -> Option<LimitExceeded> {
        self.exceeded
    }

    /// Number of operation rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// True when no contribution can be written.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Conservative bytes charged before every allocation retained by the pass.
    pub fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    /// `(service_name, name, last_seen)` in service then name order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str, i64)> {
        self.rows
            .iter()
            .map(|((service, name), last_seen)| (service.as_ref(), name.as_ref(), *last_seen))
    }

    /// Earliest and latest start of the spans counted, the file's catalog
    /// range: it expires with the pass's L1 files.
    pub fn range(&self) -> Option<(i64, i64)> {
        self.range
    }

    /// Merges another partial contribution, used when a file has several
    /// row groups.
    pub fn merge(&mut self, other: Self) {
        if let Some(exceeded) = self.exceeded.or(other.exceeded) {
            self.skip(exceeded);
            return;
        }
        for ((service, name), last_seen) in other.rows {
            self.rows
                .entry((service, name))
                .and_modify(|known| *known = (*known).max(last_seen))
                .or_insert(last_seen);
        }
        if let Some((low, high)) = other.range {
            self.widen(low);
            self.widen(high);
        }
        self.estimated_bytes = self.rows.keys().fold(0, |bytes, (service, name)| {
            bytes
                .saturating_add(OPERATION_ENTRY_BYTES)
                .saturating_add(service.len())
                .saturating_add(name.len())
        });
    }

    fn skip(&mut self, exceeded: LimitExceeded) {
        self.rows.clear();
        self.range = None;
        self.estimated_bytes = 0;
        self.exceeded = Some(exceeded);
    }

    /// Encodes the rows as one Arrow batch.
    ///
    /// # Errors
    /// Arrow refuses an inconsistent array.
    pub fn to_batch(&self) -> Result<RecordBatch, SchemaError> {
        let services: StringArray = self.iter().map(|(service, _, _)| Some(service)).collect();
        let names: StringArray = self.iter().map(|(_, name, _)| Some(name)).collect();
        let seen: Vec<i64> = self.iter().map(|(_, _, last_seen)| last_seen).collect();
        Ok(RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(services),
                Arc::new(names),
                Arc::new(TimestampNanosecondArray::from(seen).with_timezone("UTC")),
            ],
        )?)
    }

    fn from_batch(batch: &RecordBatch) -> Result<Self, SchemaError> {
        let schema = batch.schema();
        let column = |name: &str| -> Result<&arrow::array::ArrayRef, SchemaError> {
            Ok(batch.column(schema.index_of(name)?))
        };
        let services = column(columns::SERVICE_NAME)?
            .as_string_opt::<i32>()
            .ok_or(SchemaError::Layout("top-level operations service_name"))?;
        let names = column(columns::NAME)?
            .as_string_opt::<i32>()
            .ok_or(SchemaError::Layout("top-level operations name"))?;
        let seen = column(columns::LAST_SEEN)?
            .as_primitive_opt::<TimestampNanosecondType>()
            .ok_or(SchemaError::Layout("top-level operations last_seen"))?;
        let mut out = Self::default();
        for row in 0..batch.num_rows() {
            let last_seen = seen.value(row);
            out.rows
                .entry((Arc::from(services.value(row)), Arc::from(names.value(row))))
                .and_modify(|known| *known = (*known).max(last_seen))
                .or_insert(last_seen);
            out.widen(last_seen);
        }
        out.estimated_bytes = out.rows.keys().fold(0, |bytes, (service, name)| {
            bytes
                .saturating_add(OPERATION_ENTRY_BYTES)
                .saturating_add(service.len())
                .saturating_add(name.len())
        });
        Ok(out)
    }
}

/// Exact physical layout of format v1.
pub fn schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let timestamp = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
            let metadata = [
                ("fusescope.signal".to_owned(), SIGNAL.to_owned()),
                (
                    "fusescope.schema_version".to_owned(),
                    SCHEMA_VERSION.to_string(),
                ),
            ];
            Arc::new(Schema::new_with_metadata(
                vec![
                    Field::new(columns::SERVICE_NAME, DataType::Utf8, false),
                    Field::new(columns::NAME, DataType::Utf8, false),
                    Field::new(columns::LAST_SEEN, timestamp, false),
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
    let metadata = found.metadata();
    let version = SCHEMA_VERSION.to_string();
    let valid = metadata.get("fusescope.signal").map(String::as_str) == Some(SIGNAL)
        && metadata.get("fusescope.schema_version").map(String::as_str) == Some(version.as_str())
        && found.fields() == expected.fields();
    if !valid {
        return Err(SchemaError::Layout("top_level_operations format v1"));
    }
    Ok(())
}

/// Encodes one pass's contribution as Parquet.
///
/// # Errors
/// Arrow or Parquet failure.
pub fn encode(rows: &TopLevelOperations) -> Result<Vec<u8>, SchemaError> {
    let batch = rows.to_batch()?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(
            crate::parquet::ZSTD_LEVEL,
        )?))
        .build();
    let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), Some(properties))?;
    writer.write(&batch)?;
    Ok(writer.into_inner()?)
}

/// Decodes and validates a Parquet contribution.
///
/// # Errors
/// It is not a `top_level_operations` format-v1 file.
pub fn decode(bytes: bytes::Bytes) -> Result<TopLevelOperations, SchemaError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    check_schema(builder.schema())?;
    let mut out = TopLevelOperations::default();
    for batch in builder.build()? {
        out.merge(TopLevelOperations::from_batch(&batch?)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
