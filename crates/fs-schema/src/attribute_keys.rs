//! The `attribute_keys` derived table: every attribute key seen in a
//! tenant's spans, where it lives, its type, how many spans carry it,
//! when it was first and last seen and which hour's budget admitted it.
//! The compactor builds it from the rows it merges; `metadata.keys` and
//! SQL read it.
//!
//! Format v2: one Parquet file per tenant under `derived/attribute_keys`,
//! columns `key` Utf8, `source` Utf8 (`resource`, `span`, `scope`), `type`
//! Utf8 (`string`, `number`, `bool`), `count` UInt64 (spans carrying the
//! key), `first_seen`, `last_seen` and `admitted_hour` Timestamp(ns, UTC),
//! the last the start of the hour whose `max_keys_hour` admitted the key,
//! rows sorted by (source, type, key). v1 lacked `admitted_hour`; a v1
//! file reads with the hour of `first_seen` in its place. The schema
//! metadata names the signal and the version, and each version has one
//! physical layout: v1 the six columns, v2 the seven, every column
//! required, `admitted_hour` always the start of an hour. Every reader
//! checks signal, version and layout with [`check_schema`] and refuses
//! anything else, so a file is never read as a version it is not.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, AsArray, StringArray, TimestampNanosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::SchemaError;
use crate::spans::col;

/// Version of the table layout this engine writes.
pub const SCHEMA_VERSION: u32 = 2;

/// Versions this engine reads.
pub const READABLE_VERSIONS: [u32; 2] = [1, 2];

/// Value of the `fusescope.signal` schema metadata.
pub const SIGNAL: &str = "attribute_keys";

/// Nanoseconds in an hour.
const HOUR_NS: i64 = 3_600_000_000_000;

/// Where a key lives on a span.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KeySource {
    /// Resource attribute.
    Resource,
    /// Span attribute.
    Span,
    /// Instrumentation scope attribute.
    Scope,
}

impl KeySource {
    /// Name as stored.
    pub fn name(self) -> &'static str {
        match self {
            KeySource::Resource => "resource",
            KeySource::Span => "span",
            KeySource::Scope => "scope",
        }
    }

    fn parse(name: &str) -> Option<Self> {
        match name {
            "resource" => Some(KeySource::Resource),
            "span" => Some(KeySource::Span),
            "scope" => Some(KeySource::Scope),
            _ => None,
        }
    }
}

/// Type of a key's values, as the span schema stores them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KeyType {
    /// `attributes_string`, `resources_string`, `scope_string`.
    String,
    /// `attributes_number`.
    Number,
    /// `attributes_bool`.
    Bool,
}

impl KeyType {
    /// Name as stored.
    pub fn name(self) -> &'static str {
        match self {
            KeyType::String => "string",
            KeyType::Number => "number",
            KeyType::Bool => "bool",
        }
    }

    fn parse(name: &str) -> Option<Self> {
        match name {
            "string" => Some(KeyType::String),
            "number" => Some(KeyType::Number),
            "bool" => Some(KeyType::Bool),
            _ => None,
        }
    }
}

/// What is known about one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeySeen {
    /// Spans carrying the key.
    pub count: u64,
    /// Earliest `timestamp` of a span carrying it, nanoseconds.
    pub first_seen: i64,
    /// Latest `timestamp` of a span carrying it, nanoseconds.
    pub last_seen: i64,
    /// Start of the hour whose budget admitted the key, nanoseconds.
    /// Fixed once admitted: late data of an earlier hour widens the count
    /// and the sightings, never the admission, so an hour's budget is
    /// never charged after the fact.
    pub admitted_hour: i64,
}

impl KeySeen {
    fn widen(&mut self, other: KeySeen) {
        self.count = self.count.saturating_add(other.count);
        self.first_seen = self.first_seen.min(other.first_seen);
        self.last_seen = self.last_seen.max(other.last_seen);
    }
}

/// Keys of one tenant, in table order. Keys seen since the stats were
/// built, decoded or settled count as new, against the cap [`absorb`] is
/// given.
///
/// [`absorb`]: KeyStats::absorb
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KeyStats {
    keys: BTreeMap<(KeySource, KeyType), BTreeMap<String, KeySeen>>,
    new_keys: usize,
    dropped: u64,
}

/// The map columns of the span schema and what their keys are.
const MAP_COLUMNS: [(&str, KeySource, KeyType); 5] = [
    (col::RESOURCES_STRING, KeySource::Resource, KeyType::String),
    (col::ATTRIBUTES_STRING, KeySource::Span, KeyType::String),
    (col::ATTRIBUTES_NUMBER, KeySource::Span, KeyType::Number),
    (col::ATTRIBUTES_BOOL, KeySource::Span, KeyType::Bool),
    (col::SCOPE_STRING, KeySource::Scope, KeyType::String),
];

/// The span columns [`KeyStats::absorb`] reads: a batch projected to
/// these is enough.
pub const READ_COLUMNS: [&str; 6] = [
    col::TIMESTAMP,
    col::RESOURCES_STRING,
    col::ATTRIBUTES_STRING,
    col::ATTRIBUTES_NUMBER,
    col::ATTRIBUTES_BOOL,
    col::SCOPE_STRING,
];

/// Who decides whether a key not seen before is counted, and on which
/// hour.
enum Admission<'a> {
    /// The budget of one hour: so many keys, then entries are dropped.
    Budget { hour: i64, max_new_keys: usize },
    /// A table built before: its keys, on the hours that admitted them;
    /// any other key is left out as it was then.
    Known(&'a KeyStats),
}

impl Admission<'_> {
    fn admit(
        &self,
        source: KeySource,
        kind: KeyType,
        key: &str,
        new_keys: &mut usize,
        dropped: &mut u64,
    ) -> Option<i64> {
        match self {
            Admission::Budget { hour, max_new_keys } => {
                if *new_keys < *max_new_keys {
                    *new_keys += 1;
                    Some(*hour)
                } else {
                    *dropped += 1;
                    None
                }
            }
            Admission::Known(known) => known
                .keys
                .get(&(source, kind))
                .and_then(|keys| keys.get(key))
                .map(|seen| seen.admitted_hour),
        }
    }
}

impl KeyStats {
    /// Counts the keys of every span in `batch`, a batch of the span
    /// schema, once per span however often a span repeats a key. A key
    /// not seen before is admitted on `hour`'s budget while fewer than
    /// `max_new_keys` were admitted since the stats were built, decoded
    /// or settled; beyond that its entries are dropped and counted,
    /// without allocating for them. Which keys are admitted follows batch
    /// order, so it is the same for the same rows.
    ///
    /// # Errors
    /// A batch without the timestamp or one of the map columns, or with
    /// one of another type.
    pub fn absorb(
        &mut self,
        batch: &RecordBatch,
        hour: i64,
        max_new_keys: usize,
    ) -> Result<(), SchemaError> {
        if hour.rem_euclid(HOUR_NS) != 0 {
            return Err(SchemaError::Layout("hour is not the start of an hour"));
        }
        self.absorb_with(batch, &Admission::Budget { hour, max_new_keys })
    }

    /// Counts the keys of every span in `batch` that `known` lists, each
    /// on the hour that admitted it there, as a table rebuilt from the
    /// files that remain after some were deleted; a key `known` does not
    /// list is left out, as it was when the files were compacted, and is
    /// neither counted as new nor as dropped.
    ///
    /// # Errors
    /// As [`absorb`](Self::absorb).
    pub fn absorb_known(
        &mut self,
        batch: &RecordBatch,
        known: &KeyStats,
    ) -> Result<(), SchemaError> {
        self.absorb_with(batch, &Admission::Known(known))
    }

    fn absorb_with(
        &mut self,
        batch: &RecordBatch,
        admission: &Admission<'_>,
    ) -> Result<(), SchemaError> {
        let Self {
            keys,
            new_keys,
            dropped,
        } = self;
        let schema = batch.schema();
        let timestamps = batch
            .column(schema.index_of(col::TIMESTAMP)?)
            .as_primitive_opt::<TimestampNanosecondType>()
            .ok_or(SchemaError::Layout("timestamp"))?;
        let mut in_row: HashSet<&str> = HashSet::new();
        for (name, source, kind) in MAP_COLUMNS {
            let map = batch
                .column(schema.index_of(name)?)
                .as_map_opt()
                .ok_or(SchemaError::Layout("attribute map"))?;
            let map_keys = map
                .keys()
                .as_string_opt::<i32>()
                .ok_or(SchemaError::Layout("attribute map keys"))?;
            let offsets = map.value_offsets();
            let seen_here = keys.entry((source, kind)).or_default();
            for row in 0..map.len() {
                let (Some(start), Some(end)) = (offsets.get(row), offsets.get(row + 1)) else {
                    break;
                };
                let at = timestamps.value(row);
                in_row.clear();
                for entry in *start..*end {
                    let entry =
                        usize::try_from(entry).map_err(|_| SchemaError::Layout("offset"))?;
                    let key = map_keys.value(entry);
                    if !in_row.insert(key) {
                        continue;
                    }
                    let sighting = |admitted_hour| KeySeen {
                        count: 1,
                        first_seen: at,
                        last_seen: at,
                        admitted_hour,
                    };
                    if let Some(known) = seen_here.get_mut(key) {
                        known.widen(sighting(known.admitted_hour));
                    } else if let Some(hour) = admission.admit(source, kind, key, new_keys, dropped)
                    {
                        seen_here.insert(key.to_owned(), sighting(hour));
                    }
                }
            }
        }
        Ok(())
    }

    /// Adds everything `other` knows; its keys count as new here.
    pub fn merge(&mut self, other: KeyStats) {
        for ((source, kind), keys) in other.keys {
            let known = self.keys.entry((source, kind)).or_default();
            for (key, seen) in keys {
                if let Some(known) = known.get_mut(&key) {
                    known.widen(seen);
                } else {
                    known.insert(key, seen);
                    self.new_keys += 1;
                }
            }
        }
        self.dropped = self.dropped.saturating_add(other.dropped);
    }

    /// Distinct (source, key, type) rows.
    pub fn len(&self) -> usize {
        self.keys.values().map(BTreeMap::len).sum()
    }

    /// True when nothing was seen.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Keys admitted since the stats were built, decoded or settled.
    pub fn new_keys(&self) -> usize {
        self.new_keys
    }

    /// Keys admitted on the budget of `hour`, the start of an hour in
    /// nanoseconds.
    pub fn admitted_in(&self, hour: i64) -> usize {
        self.iter()
            .filter(|(_, _, _, seen)| seen.admitted_hour == hour)
            .count()
    }

    /// Entries of keys beyond the cap that were not counted.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// The same stats with nothing counted as new, as a table read back.
    #[must_use]
    pub fn settled(mut self) -> Self {
        self.new_keys = 0;
        self
    }

    /// Rows in table order.
    pub fn iter(&self) -> impl Iterator<Item = (KeySource, &str, KeyType, KeySeen)> {
        self.keys.iter().flat_map(|((source, kind), keys)| {
            keys.iter()
                .map(move |(key, seen)| (*source, key.as_str(), *kind, *seen))
        })
    }

    /// Earliest first and latest last seen over every key, nanoseconds.
    pub fn range(&self) -> Option<(i64, i64)> {
        self.iter().fold(None, |range, (_, _, _, seen)| {
            Some(match range {
                None => (seen.first_seen, seen.last_seen),
                Some((first, last)) => (first.min(seen.first_seen), last.max(seen.last_seen)),
            })
        })
    }

    /// The table as one batch of [`attribute_keys_schema_v2`].
    ///
    /// # Errors
    /// Arrow refusing the arrays, which cannot happen for a consistent map.
    pub fn to_batch(&self) -> Result<RecordBatch, SchemaError> {
        let rows = self.len();
        let mut keys = Vec::with_capacity(rows);
        let mut sources = Vec::with_capacity(rows);
        let mut kinds = Vec::with_capacity(rows);
        let mut counts = Vec::with_capacity(rows);
        let mut firsts = Vec::with_capacity(rows);
        let mut lasts = Vec::with_capacity(rows);
        let mut admitted = Vec::with_capacity(rows);
        for (source, key, kind, seen) in self.iter() {
            keys.push(key);
            sources.push(source.name());
            kinds.push(kind.name());
            counts.push(seen.count);
            firsts.push(seen.first_seen);
            lasts.push(seen.last_seen);
            admitted.push(seen.admitted_hour);
        }
        Ok(RecordBatch::try_new(
            attribute_keys_schema_v2(),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(sources)),
                Arc::new(StringArray::from(kinds)),
                Arc::new(UInt64Array::from(counts)),
                Arc::new(TimestampNanosecondArray::from(firsts).with_timezone("UTC")),
                Arc::new(TimestampNanosecondArray::from(lasts).with_timezone("UTC")),
                Arc::new(TimestampNanosecondArray::from(admitted).with_timezone("UTC")),
            ],
        )?)
    }

    /// Reads a batch of the table at `version`, exactly as that version
    /// lays it out; nothing read counts as new.
    ///
    /// # Errors
    /// A batch missing a column of its version, carrying one it does not
    /// have, with a null `admitted_hour` or one that is not the start of
    /// an hour, or with a source or type the version does not know.
    pub fn from_batch(batch: &RecordBatch, version: u32) -> Result<Self, SchemaError> {
        let schema = batch.schema();
        let column = |name: &str| -> Result<&arrow::array::ArrayRef, SchemaError> {
            Ok(batch.column(schema.index_of(name)?))
        };
        let keys = column(col_names::KEY)?
            .as_string_opt::<i32>()
            .ok_or(SchemaError::Layout("key"))?;
        let sources = column(col_names::SOURCE)?
            .as_string_opt::<i32>()
            .ok_or(SchemaError::Layout("source"))?;
        let kinds = column(col_names::TYPE)?
            .as_string_opt::<i32>()
            .ok_or(SchemaError::Layout("type"))?;
        let counts = column(col_names::COUNT)?
            .as_primitive_opt::<arrow::datatypes::UInt64Type>()
            .ok_or(SchemaError::Layout("count"))?;
        let firsts = column(col_names::FIRST_SEEN)?
            .as_primitive_opt::<TimestampNanosecondType>()
            .ok_or(SchemaError::Layout("first_seen"))?;
        let lasts = column(col_names::LAST_SEEN)?
            .as_primitive_opt::<TimestampNanosecondType>()
            .ok_or(SchemaError::Layout("last_seen"))?;
        let admitted = match (version, schema.index_of(col_names::ADMITTED_HOUR).ok()) {
            (1, None) => None,
            (1, Some(_)) => return Err(SchemaError::Layout("admitted_hour in a v1 table")),
            (2, None) => return Err(SchemaError::Layout("admitted_hour missing from a v2 table")),
            (2, Some(index)) => {
                let admitted = batch
                    .column(index)
                    .as_primitive_opt::<TimestampNanosecondType>()
                    .ok_or(SchemaError::Layout("admitted_hour"))?;
                if admitted.null_count() > 0 {
                    return Err(SchemaError::Layout("null admitted_hour in a v2 table"));
                }
                if admitted.values().iter().any(|a| a.rem_euclid(HOUR_NS) != 0) {
                    return Err(SchemaError::Layout(
                        "admitted_hour that is not the start of an hour",
                    ));
                }
                Some(admitted)
            }
            _ => return Err(SchemaError::Layout("attribute_keys schema version")),
        };
        let mut out = KeyStats::default();
        for row in 0..batch.num_rows() {
            let source =
                KeySource::parse(sources.value(row)).ok_or(SchemaError::Layout("source"))?;
            let kind = KeyType::parse(kinds.value(row)).ok_or(SchemaError::Layout("type"))?;
            let first_seen = firsts.value(row);
            let seen = KeySeen {
                count: counts.value(row),
                first_seen,
                last_seen: lasts.value(row),
                admitted_hour: admitted.map_or_else(|| hour_of(first_seen), |a| a.value(row)),
            };
            out.keys
                .entry((source, kind))
                .or_default()
                .entry(keys.value(row).to_owned())
                .and_modify(|known| known.widen(seen))
                .or_insert(seen);
        }
        Ok(out)
    }
}

/// Start of the hour holding `ns`, nanoseconds.
fn hour_of(ns: i64) -> i64 {
    ns.div_euclid(HOUR_NS) * HOUR_NS
}

/// Column names of the table.
pub mod col_names {
    /// The attribute key.
    pub const KEY: &str = "key";
    /// Where it lives: `resource`, `span` or `scope`.
    pub const SOURCE: &str = "source";
    /// Its type: `string`, `number` or `bool`.
    pub const TYPE: &str = "type";
    /// Spans carrying it.
    pub const COUNT: &str = "count";
    /// Earliest span carrying it.
    pub const FIRST_SEEN: &str = "first_seen";
    /// Latest span carrying it.
    pub const LAST_SEEN: &str = "last_seen";
    /// Start of the hour whose budget admitted it; absent in v1.
    pub const ADMITTED_HOUR: &str = "admitted_hour";
}

/// Name of the table in a query session.
pub const TABLE: &str = "attribute_keys";

/// Schema of the table, version 2, as written: `admitted_hour` is never
/// null.
pub fn attribute_keys_schema_v2() -> SchemaRef {
    with_metadata(SCHEMA_VERSION, false)
}

/// The table as a query session reads it: v2, with `admitted_hour`
/// nullable so a v1 file, which lacks it, reads with it null. A logical
/// schema only: a file laid out like it is refused by [`check_schema`].
pub fn table_schema() -> SchemaRef {
    with_metadata(SCHEMA_VERSION, true)
}

/// The columns of `version`, in order, as (name, type).
fn layout(version: u32) -> Vec<(&'static str, DataType)> {
    let timestamp = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    let mut columns = vec![
        (col_names::KEY, DataType::Utf8),
        (col_names::SOURCE, DataType::Utf8),
        (col_names::TYPE, DataType::Utf8),
        (col_names::COUNT, DataType::UInt64),
        (col_names::FIRST_SEEN, timestamp.clone()),
        (col_names::LAST_SEEN, timestamp.clone()),
    ];
    if version >= 2 {
        columns.push((col_names::ADMITTED_HOUR, timestamp));
    }
    columns
}

fn with_metadata(version: u32, admission_nullable: bool) -> SchemaRef {
    let metadata = [
        ("fusescope.signal".to_owned(), SIGNAL.to_owned()),
        ("fusescope.schema_version".to_owned(), version.to_string()),
    ];
    let fields = layout(version)
        .into_iter()
        .map(|(name, data_type)| {
            let nullable = name == col_names::ADMITTED_HOUR && admission_nullable;
            Field::new(name, data_type, nullable)
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(
        fields,
        metadata.into_iter().collect(),
    ))
}

/// Refuses a file schema that is not this table at a version this engine
/// reads, laid out exactly as that version is, every column required;
/// returns the version.
///
/// # Errors
/// Another signal, a version this engine does not read, no version, or
/// columns other than the version's, by name, order, type or nullability.
pub fn check_schema(schema: &Schema) -> Result<u32, SchemaError> {
    let metadata = schema.metadata();
    if metadata.get("fusescope.signal").map(String::as_str) != Some(SIGNAL) {
        return Err(SchemaError::Layout("attribute_keys signal"));
    }
    let version = metadata
        .get("fusescope.schema_version")
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|v| READABLE_VERSIONS.contains(v))
        .ok_or(SchemaError::Layout("attribute_keys schema version"))?;
    let expected = layout(version);
    let same = schema.fields().len() == expected.len()
        && schema
            .fields()
            .iter()
            .zip(&expected)
            .all(|(field, (name, data_type))| {
                field.name() == name && field.data_type() == data_type && !field.is_nullable()
            });
    if !same {
        return Err(SchemaError::Layout(
            "attribute_keys columns for the version",
        ));
    }
    Ok(version)
}

/// Encodes `stats` as one Parquet file of the table.
///
/// # Errors
/// Parquet or Arrow failure.
pub fn encode(stats: &KeyStats) -> Result<Vec<u8>, SchemaError> {
    let batch = stats.to_batch()?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(
            crate::parquet::ZSTD_LEVEL,
        )?))
        .build();
    let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), Some(properties))?;
    writer.write(&batch)?;
    Ok(writer.into_inner()?)
}

/// Reads a Parquet file of the table, refusing another signal or a
/// version this engine does not read.
///
/// # Errors
/// Not a Parquet file of this table at a readable version.
pub fn decode(bytes: bytes::Bytes) -> Result<KeyStats, SchemaError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    let version = check_schema(builder.schema())?;
    let mut out = KeyStats::default();
    for batch in builder.build()? {
        out.merge(KeyStats::from_batch(&batch?, version)?);
    }
    Ok(out.settled())
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    use super::*;
    use crate::limits::Limits;
    use crate::spans::SpanBatchBuilder;

    /// 2026-10-22T14:00:00Z
    const H14: i64 = 1_792_677_600_000_000_000;
    const H15: i64 = H14 + HOUR_NS;

    fn kv(key: &str, value: Value) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue { value: Some(value) }),
            ..KeyValue::default()
        }
    }

    fn string(s: &str) -> Value {
        Value::StringValue(s.into())
    }

    fn batch_with(spans: Vec<Span>) -> RecordBatch {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![
                        kv("service.name", string("api")),
                        kv("host.name", string("h1")),
                    ],
                    ..Resource::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "lib".into(),
                        attributes: vec![kv("lib.flavour", string("x"))],
                        ..InstrumentationScope::default()
                    }),
                    spans,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let mut builder = SpanBatchBuilder::new(Limits::default());
        builder.append_request(&request).unwrap();
        builder.finish().unwrap()
    }

    fn span(start: i64, attributes: Vec<KeyValue>) -> Span {
        let start = u64::try_from(start).unwrap();
        Span {
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            name: "op".into(),
            start_time_unix_nano: start,
            end_time_unix_nano: start + 1,
            attributes,
            ..Span::default()
        }
    }

    fn seen(count: u64, first: i64, last: i64) -> KeySeen {
        KeySeen {
            count,
            first_seen: first,
            last_seen: last,
            admitted_hour: H14,
        }
    }

    #[test]
    fn keys_are_counted_once_per_span_by_source_and_type_with_their_sightings() {
        let batch = batch_with(vec![
            span(
                H14 + 1_000,
                vec![
                    kv("http.method", string("GET")),
                    kv("http.method", string("GET again")),
                    kv("http.status", Value::IntValue(200)),
                    kv("retry", Value::BoolValue(true)),
                ],
            ),
            span(H14 + 3_000, vec![kv("http.method", string("POST"))]),
        ]);
        let mut stats = KeyStats::default();
        stats.absorb(&batch, H14, usize::MAX).unwrap();
        let rows: Vec<_> = stats.iter().collect();
        let (a, b) = (H14 + 1_000, H14 + 3_000);
        assert_eq!(
            rows,
            vec![
                (
                    KeySource::Resource,
                    "host.name",
                    KeyType::String,
                    seen(2, a, b)
                ),
                (
                    KeySource::Resource,
                    "service.name",
                    KeyType::String,
                    seen(2, a, b)
                ),
                (
                    KeySource::Span,
                    "http.method",
                    KeyType::String,
                    seen(2, a, b)
                ),
                (
                    KeySource::Span,
                    "http.status",
                    KeyType::Number,
                    seen(1, a, a)
                ),
                (KeySource::Span, "retry", KeyType::Bool, seen(1, a, a)),
                (
                    KeySource::Scope,
                    "lib.flavour",
                    KeyType::String,
                    seen(2, a, b)
                ),
            ],
            "a key repeated on one span counts once"
        );
        assert_eq!(stats.range(), Some((a, b)));
        assert_eq!((stats.new_keys(), stats.dropped()), (6, 0));
        assert_eq!(stats.admitted_in(H14), 6);
        assert_eq!(stats.admitted_in(H15), 0);
    }

    #[test]
    fn keys_beyond_the_cap_are_dropped_in_batch_order_and_known_keys_still_count() {
        let batch = batch_with(vec![span(
            H14 + 1_000,
            vec![kv("a", string("1")), kv("b", string("2"))],
        )]);
        let mut stats = KeyStats::default();
        stats.absorb(&batch, H14, 3).unwrap();
        let kept: Vec<&str> = stats.iter().map(|(_, key, _, _)| key).collect();
        assert_eq!(
            kept,
            ["host.name", "service.name", "a"],
            "the resource keys come first, then the first span key"
        );
        assert_eq!(stats.dropped(), 2, "b and the scope key");

        // Decoded keys are not new: a cap of one admits one more key on
        // the next hour's budget and keeps counting the known ones.
        let table = decode(bytes::Bytes::from(encode(&stats).unwrap())).unwrap();
        assert_eq!(table.new_keys(), 0);
        assert_eq!(table.admitted_in(H14), 3, "the budget survives the file");
        let mut table = table;
        table
            .absorb(
                &batch_with(vec![span(
                    H15 + 2_000,
                    vec![kv("c", string("3")), kv("a", string("4"))],
                )]),
                H15,
                1,
            )
            .unwrap();
        let rows: Vec<_> = table
            .iter()
            .map(|(_, key, _, seen)| (key, seen.count, seen.admitted_hour))
            .collect();
        assert_eq!(
            rows,
            [
                ("host.name", 2, H14),
                ("service.name", 2, H14),
                ("a", 2, H14),
                ("c", 1, H15)
            ],
            "c admitted on the new hour, the scope key dropped again"
        );
        assert_eq!((table.new_keys(), table.dropped()), (1, 1));
        assert_eq!((table.admitted_in(H14), table.admitted_in(H15)), (3, 1));
    }

    #[test]
    fn a_merge_sums_counts_and_widens_the_sightings_and_the_file_round_trips() {
        let mut first = KeyStats::default();
        first
            .absorb(
                &batch_with(vec![span(
                    H14 + 5_000,
                    vec![kv("http.method", string("GET"))],
                )]),
                H14,
                usize::MAX,
            )
            .unwrap();
        let mut second = KeyStats::default();
        second
            .absorb(
                &batch_with(vec![span(
                    H14 + 2_000,
                    vec![kv("http.method", string("PUT")), kv("db.name", string("x"))],
                )]),
                H14,
                usize::MAX,
            )
            .unwrap();
        first.merge(second);
        let method = first
            .iter()
            .find(|(_, key, _, _)| *key == "http.method")
            .unwrap()
            .3;
        assert_eq!(method.count, 2);
        assert_eq!(
            (method.first_seen, method.last_seen),
            (H14 + 2_000, H14 + 5_000)
        );
        assert_eq!(first.len(), 5);

        let bytes = encode(&first).unwrap();
        let back = decode(bytes::Bytes::from(bytes)).unwrap();
        let rows: Vec<_> = back.iter().collect();
        assert_eq!(rows, first.iter().collect::<Vec<_>>());
        assert_eq!(
            back.to_batch().unwrap().schema().metadata()["fusescope.schema_version"],
            "2"
        );
    }

    #[test]
    fn a_v1_file_reads_with_the_hour_of_first_seen_as_its_admission() {
        let mut stats = KeyStats::default();
        stats
            .absorb(
                &batch_with(vec![span(H15 + 7, vec![kv("a", string("1"))])]),
                H14,
                usize::MAX,
            )
            .unwrap();
        let v2 = stats.to_batch().unwrap();
        let v1_fields: Vec<_> = v2.schema().fields().iter().take(6).cloned().collect();
        let mut metadata = v2.schema().metadata().clone();
        metadata.insert("fusescope.schema_version".into(), "1".into());
        let schema = Arc::new(Schema::new_with_metadata(v1_fields, metadata));
        let v1 = RecordBatch::try_new(Arc::clone(&schema), v2.columns().get(..6).unwrap().to_vec())
            .unwrap();
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
        writer.write(&v1).unwrap();
        let read = decode(bytes::Bytes::from(writer.into_inner().unwrap())).unwrap();
        assert_eq!(read.len(), 4);
        assert!(
            read.iter().all(|(_, _, _, seen)| seen.admitted_hour == H15),
            "admitted on the hour of first_seen, not the hour the stats were built"
        );
    }

    #[test]
    fn another_version_or_signal_is_refused() {
        let batch = KeyStats::default().to_batch().unwrap();
        for (key, value) in [
            ("fusescope.schema_version", "3"),
            ("fusescope.signal", "spans"),
        ] {
            let mut metadata = batch.schema().metadata().clone();
            metadata.insert(key.into(), value.into());
            let schema = Arc::new(Schema::new_with_metadata(
                batch.schema().fields().clone(),
                metadata,
            ));
            assert!(matches!(check_schema(&schema), Err(SchemaError::Layout(_))));
            let foreign =
                RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec()).unwrap();
            let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
            writer.write(&foreign).unwrap();
            let bytes = writer.into_inner().unwrap();
            let err = decode(bytes::Bytes::from(bytes)).unwrap_err();
            assert!(matches!(err, SchemaError::Layout(_)), "{err}");
        }
        assert_eq!(check_schema(&attribute_keys_schema_v2()).unwrap(), 2);
        assert!(
            matches!(check_schema(&table_schema()), Err(SchemaError::Layout(_))),
            "the logical schema of a session is not a physical layout"
        );
        assert!(
            matches!(
                KeyStats::from_batch(&batch_with(Vec::new()), 2).unwrap_err(),
                SchemaError::Arrow(_)
            ),
            "a span batch is not the table"
        );
    }

    /// A file of `version` with the first `fields` columns of v2 and, when
    /// given, `admission` in place of the seventh, as a nullable column
    /// when `nullable`.
    fn file_of(
        version: &str,
        fields: usize,
        admission: Option<(Vec<Option<i64>>, bool)>,
    ) -> Vec<u8> {
        let mut stats = KeyStats::default();
        stats
            .absorb(
                &batch_with(vec![span(H14 + 7, vec![kv("a", string("1"))])]),
                H14,
                usize::MAX,
            )
            .unwrap();
        let v2 = stats.to_batch().unwrap();
        let mut columns = v2.columns().get(..fields).unwrap().to_vec();
        let mut metadata = v2.schema().metadata().clone();
        metadata.insert("fusescope.schema_version".into(), version.into());
        let mut fields_kept: Vec<_> = v2.schema().fields().iter().take(fields).cloned().collect();
        if let Some((admission, nullable)) = admission {
            columns.pop();
            fields_kept.pop();
            fields_kept.push(Arc::new(Field::new(
                col_names::ADMITTED_HOUR,
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                nullable,
            )));
            columns.push(Arc::new(
                TimestampNanosecondArray::from(admission).with_timezone("UTC"),
            ));
        }
        let schema = Arc::new(Schema::new_with_metadata(fields_kept, metadata));
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.into_inner().unwrap()
    }

    #[test]
    fn each_version_reads_only_its_own_layout() {
        assert_eq!(
            decode(bytes::Bytes::from(file_of("1", 6, None)))
                .unwrap()
                .len(),
            4,
            "v1: six columns"
        );
        assert_eq!(
            decode(bytes::Bytes::from(file_of("2", 7, None)))
                .unwrap()
                .len(),
            4,
            "v2: seven columns"
        );
        let aligned = vec![Some(H14); 4];
        for (what, bytes) in [
            ("v1 with admitted_hour", file_of("1", 7, None)),
            ("v2 without admitted_hour", file_of("2", 6, None)),
            (
                "v2 with a nullable admitted_hour",
                file_of("2", 7, Some((aligned.clone(), true))),
            ),
            (
                "v2 with a null admitted_hour",
                file_of(
                    "2",
                    7,
                    Some((vec![Some(H14), None, Some(H14), Some(H14)], true)),
                ),
            ),
        ] {
            let bytes = bytes::Bytes::from(bytes);
            let footer = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
            assert!(
                matches!(check_schema(footer.schema()), Err(SchemaError::Layout(_))),
                "{what}: the footer alone tells"
            );
            let err = decode(bytes).unwrap_err();
            assert!(matches!(err, SchemaError::Layout(_)), "{what}: {err}");
        }

        // A required admitted_hour that is not the start of an hour passes
        // the footer, as it must, and is refused by the reader.
        let misaligned = bytes::Bytes::from(file_of(
            "2",
            7,
            Some((vec![Some(H14), Some(H14 + 1), Some(H14), Some(H14)], false)),
        ));
        let footer = ParquetRecordBatchReaderBuilder::try_new(misaligned.clone()).unwrap();
        assert_eq!(check_schema(footer.schema()).unwrap(), 2);
        let err = decode(misaligned).unwrap_err();
        assert!(matches!(err, SchemaError::Layout(_)), "{err}");
        let aligned = decode(bytes::Bytes::from(file_of("2", 7, Some((aligned, false))))).unwrap();
        assert_eq!(
            (aligned.admitted_in(H14), aligned.new_keys()),
            (4, 0),
            "the hour is kept as written and nothing read counts as new"
        );
    }

    #[test]
    fn a_rebuild_counts_only_the_keys_the_table_before_knows_on_their_hours() {
        let mut before = KeyStats::default();
        before
            .absorb(
                &batch_with(vec![span(
                    H14 + 1_000,
                    vec![kv("http.method", string("GET"))],
                )]),
                H14,
                usize::MAX,
            )
            .unwrap();
        let before = before.settled();
        let remaining = batch_with(vec![
            span(
                H15 + 5_000,
                vec![
                    kv("http.method", string("POST")),
                    kv("db.system", string("pg")),
                ],
            ),
            span(H15 + 9_000, vec![kv("http.method", string("GET"))]),
        ]);
        let schema = remaining.schema();
        let projected = remaining
            .project(&READ_COLUMNS.map(|c| schema.index_of(c).unwrap()))
            .unwrap();
        let mut rebuilt = KeyStats::default();
        rebuilt.absorb_known(&projected, &before).unwrap();
        let rows: Vec<_> = rebuilt
            .iter()
            .map(|(_, key, _, seen)| {
                (
                    key,
                    seen.count,
                    seen.first_seen,
                    seen.last_seen,
                    seen.admitted_hour,
                )
            })
            .collect();
        assert_eq!(
            rows,
            [
                ("host.name", 2, H15 + 5_000, H15 + 9_000, H14),
                ("service.name", 2, H15 + 5_000, H15 + 9_000, H14),
                ("http.method", 2, H15 + 5_000, H15 + 9_000, H14),
                ("lib.flavour", 2, H15 + 5_000, H15 + 9_000, H14),
            ],
            "sightings from the rows that remain, admissions from the table before; db.system is left out"
        );
        assert_eq!(rebuilt.new_keys(), 0);
        assert_eq!(rebuilt.dropped(), 0);
    }

    #[test]
    fn an_hour_that_is_not_an_hour_start_is_refused_by_absorb() {
        let mut stats = KeyStats::default();
        let err = stats
            .absorb(&batch_with(Vec::new()), H14 + 1, usize::MAX)
            .unwrap_err();
        assert!(matches!(err, SchemaError::Layout(_)), "{err}");
    }

    #[test]
    fn a_known_key_keeps_the_hour_that_admitted_it_when_late_data_arrives() {
        // Hour 15 is compacted first and admits one key.
        let mut stats = KeyStats::default();
        stats
            .absorb(
                &batch_with(vec![span(H15 + 1, vec![kv("a", string("1"))])]),
                H15,
                1,
            )
            .unwrap();
        let admitted: Vec<_> = stats
            .iter()
            .map(|(_, key, _, seen)| (key, seen.admitted_hour))
            .collect();
        assert_eq!(admitted, [("service.name", H15)]);

        // Late data of hour 14, in a later pass, carries the same key and
        // a new one: the known key stays charged to 15, the new one takes
        // 14's budget.
        let mut stats = stats.settled();
        stats
            .absorb(
                &batch_with(vec![span(H14 + 1, vec![kv("a", string("2"))])]),
                H14,
                1,
            )
            .unwrap();
        let rows: Vec<_> = stats
            .iter()
            .map(|(_, key, _, seen)| (key, seen.count, seen.first_seen, seen.admitted_hour))
            .collect();
        assert_eq!(
            rows,
            [
                ("host.name", 1, H14 + 1, H14),
                ("service.name", 2, H14 + 1, H15),
            ],
            "sightings widen, the admission does not"
        );
        assert_eq!((stats.admitted_in(H14), stats.admitted_in(H15)), (1, 1));

        // Hour 14 is full: more late data counts known keys and admits none.
        let before = stats.clone();
        let mut stats = stats.settled();
        stats
            .absorb(
                &batch_with(vec![span(H14 + 2, vec![kv("b", string("3"))])]),
                H14,
                0,
            )
            .unwrap();
        assert_eq!(stats.len(), before.len());
        assert_eq!((stats.admitted_in(H14), stats.admitted_in(H15)), (1, 1));
        assert_eq!(stats.dropped(), before.dropped() + 2, "b and the scope key");
        let service = stats
            .iter()
            .find(|(_, key, _, _)| *key == "service.name")
            .unwrap()
            .3;
        assert_eq!((service.count, service.admitted_hour), (3, H15));
    }
}
