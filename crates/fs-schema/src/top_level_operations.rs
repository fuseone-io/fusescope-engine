//! Per-compaction-pass top-level operations.
//!
//! Format v1 has one row per `(service_name, name)` seen in the pass on a
//! span of a named service that has no parent, or whose parent, found in
//! the same pass by `(trace_id, parent_span_id)`, belongs to another
//! service, an unnamed one included; kind and `is_remote` play no part.
//! `last_seen` is the latest such span start. Files are partial
//! contributions: queries combine every visible file with `max(last_seen)`.
//! Files written before the parent rule hold roots and spans whose parent
//! was remote, and are read as written.
//!
//! A pass keeps every span's service until [`TopLevelOperations::finish`],
//! so a child met before its parent is resolved at the end; that index is
//! charged to the contribution's own byte cap.

use std::collections::{BTreeMap, HashMap, HashSet};
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
const BTREE_ENTRY_OVERHEAD: usize = 4 * size_of::<usize>();
/// One indexed span, with hash table slack for growth.
const INDEX_ENTRY_BYTES: usize = 2 * (size_of::<(SpanKey, Arc<str>)>() + 1);
/// One child waiting for its parent, with vector slack for growth.
const PENDING_ENTRY_BYTES: usize = 2 * size_of::<Pending>();
/// One interned name besides its UTF-8 bytes: the `Arc` header and a set slot.
const INTERNED_NAME_BYTES: usize = 2 * size_of::<usize>() + 2 * size_of::<Arc<str>>();
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
    /// The service of every span of the pass, `""` for an unnamed one.
    parents: HashMap<SpanKey, Arc<str>>,
    pending: Vec<Pending>,
    names: HashSet<Arc<str>>,
    max_operations: usize,
    max_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SpanKey {
    trace: [u8; 16],
    span: [u8; 8],
}

/// A child of a named service whose parent the pass had not met yet.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Pending {
    parent: SpanKey,
    service: Arc<str>,
    name: Arc<str>,
    start: i64,
}

impl TopLevelOperations {
    /// Adds a batch of the pass: its roots and the children whose parent
    /// is already known count now, the others wait for
    /// [`TopLevelOperations::finish`]. A span without a service never
    /// counts. Once a cap is crossed, it stays empty.
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
        self.max_operations = if self.max_operations == 0 {
            max_operations
        } else {
            self.max_operations.min(max_operations)
        };
        self.max_bytes = if self.max_bytes == 0 {
            max_bytes
        } else {
            self.max_bytes.min(max_bytes)
        };
        let schema = batch.schema();
        let id = |name: &'static str| {
            batch
                .column(schema.index_of(name)?)
                .as_fixed_size_binary_opt()
                .ok_or(SchemaError::Layout(name))
        };
        let traces = id(col::TRACE_ID)?;
        let spans = id(col::SPAN_ID)?;
        let parents = id(col::PARENT_SPAN_ID)?;
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
        let services = text(col::SERVICE_NAME)?;
        let services = services.as_string::<i32>();
        let names = text(col::NAME)?;
        let names = names.as_string::<i32>();
        for row in 0..batch.num_rows() {
            let trace: [u8; 16] = traces
                .value(row)
                .try_into()
                .map_err(|_| SchemaError::Layout("trace_id"))?;
            let span: [u8; 8] = spans
                .value(row)
                .try_into()
                .map_err(|_| SchemaError::Layout("span_id"))?;
            let service = if services.is_null(row) {
                ""
            } else {
                services.value(row)
            };
            if !self.index(SpanKey { trace, span }, service) {
                return Ok(());
            }
            if service.is_empty() {
                continue;
            }
            let name = if names.is_null(row) {
                ""
            } else {
                names.value(row)
            };
            let start = timestamps.value(row);
            let counted = if parents.is_null(row) {
                self.add(service, name, start)
            } else {
                let parent = SpanKey {
                    trace,
                    span: parents
                        .value(row)
                        .try_into()
                        .map_err(|_| SchemaError::Layout("parent_span_id"))?,
                };
                match self.parents.get(&parent) {
                    Some(known) if known.as_ref() != service => self.add(service, name, start),
                    Some(_) => true,
                    None => self.wait(parent, service, name, start),
                }
            };
            if !counted {
                return Ok(());
            }
        }
        Ok(())
    }

    /// The most absorbing `batch` can add to the estimate: each row an
    /// indexed span, a waiting child and a new operation, its service and
    /// name interned anew.
    ///
    /// # Errors
    /// A required span column is missing or has another physical type.
    pub fn growth_bound(batch: &RecordBatch) -> Result<usize, SchemaError> {
        let schema = batch.schema();
        let text = |name: &str| -> Result<usize, SchemaError> {
            let column =
                arrow::compute::cast(batch.column(schema.index_of(name)?), &DataType::Utf8)?;
            Ok(column.as_string::<i32>().value_data().len())
        };
        let per_row = INDEX_ENTRY_BYTES
            + PENDING_ENTRY_BYTES
            + OPERATION_ENTRY_BYTES
            + 2 * INTERNED_NAME_BYTES;
        let text = text(col::SERVICE_NAME)?.saturating_add(text(col::NAME)?);
        Ok(batch
            .num_rows()
            .saturating_mul(per_row)
            .saturating_add(text.saturating_mul(2)))
    }

    /// Resolves the children that met their parent after them, then
    /// releases the pass's index, leaving only the operations charged.
    pub fn finish(&mut self) {
        for child in std::mem::take(&mut self.pending) {
            if self.exceeded.is_some() {
                break;
            }
            if self
                .parents
                .get(&child.parent)
                .is_some_and(|known| *known != child.service)
            {
                self.add(&child.service, &child.name, child.start);
            }
        }
        self.parents = HashMap::new();
        self.names = HashSet::new();
        self.estimated_bytes = self.rows_bytes();
    }

    /// Records `key`'s service, charged before it is kept.
    fn index(&mut self, key: SpanKey, service: &str) -> bool {
        if self.parents.contains_key(&key) {
            return true;
        }
        let mut charge = INDEX_ENTRY_BYTES;
        if !self.names.contains(service) {
            charge = charge
                .saturating_add(service.len())
                .saturating_add(INTERNED_NAME_BYTES);
        }
        if !self.charge(charge) {
            return false;
        }
        let service = self.intern(service);
        self.parents.insert(key, service);
        true
    }

    /// Keeps a child for [`TopLevelOperations::finish`], charged before it is kept.
    fn wait(&mut self, parent: SpanKey, service: &str, name: &str, start: i64) -> bool {
        let mut charge = PENDING_ENTRY_BYTES;
        if !self.names.contains(name) {
            charge = charge
                .saturating_add(name.len())
                .saturating_add(INTERNED_NAME_BYTES);
        }
        if !self.charge(charge) {
            return false;
        }
        let pending = Pending {
            parent,
            service: self.intern(service),
            name: self.intern(name),
            start,
        };
        self.pending.push(pending);
        true
    }

    fn intern(&mut self, name: &str) -> Arc<str> {
        if let Some(known) = self.names.get(name) {
            return Arc::clone(known);
        }
        let interned: Arc<str> = Arc::from(name);
        self.names.insert(Arc::clone(&interned));
        interned
    }

    /// Adds `bytes` to the estimate, or skips the contribution past its cap.
    fn charge(&mut self, bytes: usize) -> bool {
        let next_bytes = self.estimated_bytes.saturating_add(bytes);
        if next_bytes > self.max_bytes {
            self.skip(LimitExceeded {
                limit: "top_level_operations_max_bytes",
                cap: self.max_bytes,
                observed: next_bytes,
                estimated_bytes: next_bytes,
            });
            return false;
        }
        self.estimated_bytes = next_bytes;
        true
    }

    fn rows_bytes(&self) -> usize {
        self.rows.keys().fold(0, |bytes, (service, name)| {
            bytes
                .saturating_add(OPERATION_ENTRY_BYTES)
                .saturating_add(service.len())
                .saturating_add(name.len())
        })
    }

    fn add(&mut self, service: &str, name: &str, start: i64) -> bool {
        let key = (self.intern(service), self.intern(name));
        if let Some(last_seen) = self.rows.get_mut(&key) {
            *last_seen = (*last_seen).max(start);
        } else {
            if self.rows.len() >= self.max_operations {
                let next_bytes = self
                    .estimated_bytes
                    .saturating_add(OPERATION_ENTRY_BYTES)
                    .saturating_add(service.len())
                    .saturating_add(name.len());
                self.skip(LimitExceeded {
                    limit: "top_level_operations_max_operations",
                    cap: self.max_operations,
                    observed: self.rows.len().saturating_add(1),
                    estimated_bytes: next_bytes,
                });
                return false;
            }
            let charge = OPERATION_ENTRY_BYTES
                .saturating_add(service.len())
                .saturating_add(name.len());
            if !self.charge(charge) {
                return false;
            }
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
        self.estimated_bytes = self.rows_bytes();
    }

    fn skip(&mut self, exceeded: LimitExceeded) {
        self.rows.clear();
        self.range = None;
        self.estimated_bytes = 0;
        self.parents = HashMap::new();
        self.pending = Vec::new();
        self.names = HashSet::new();
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
        out.estimated_bytes = out.rows_bytes();
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
