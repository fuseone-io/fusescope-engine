//! The `attribute_keys` derived table: every attribute key seen in a
//! tenant's spans, where it lives, its type, how many spans carry it and
//! when it was first and last seen. The compactor builds it from the rows
//! it merges; `metadata.keys` and SQL read it.
//!
//! Format v1: one Parquet file per tenant under `derived/attribute_keys`,
//! columns `key` Utf8, `source` Utf8 (`resource`, `span`, `scope`), `type`
//! Utf8 (`string`, `number`, `bool`), `count` UInt64 (spans carrying the
//! key), `first_seen` and `last_seen` Timestamp(ns, UTC), rows sorted by
//! (source, type, key). The schema metadata names the signal and the
//! version; every reader checks both with [`check_schema`] and refuses
//! anything else.

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

/// Version of the table layout.
pub const SCHEMA_VERSION: u32 = 1;

/// Value of the `fusescope.signal` schema metadata.
pub const SIGNAL: &str = "attribute_keys";

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
}

impl KeySeen {
    fn widen(&mut self, other: KeySeen) {
        self.count = self.count.saturating_add(other.count);
        self.first_seen = self.first_seen.min(other.first_seen);
        self.last_seen = self.last_seen.max(other.last_seen);
    }
}

/// Keys of one tenant, in table order. Keys seen since the stats were
/// built or decoded count as new, against the cap [`absorb`] is given.
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

impl KeyStats {
    /// Counts the keys of every span in `batch`, a batch of the span
    /// schema, once per span however often a span repeats a key. A key
    /// not seen before is admitted while fewer than `max_new_keys` were
    /// admitted since the stats were built; beyond that its entries are
    /// dropped and counted, without allocating for them. Which keys are
    /// admitted follows batch order, so it is the same for the same rows.
    ///
    /// # Errors
    /// A batch without the timestamp or one of the map columns, or with
    /// one of another type.
    pub fn absorb(&mut self, batch: &RecordBatch, max_new_keys: usize) -> Result<(), SchemaError> {
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
            let keys = map
                .keys()
                .as_string_opt::<i32>()
                .ok_or(SchemaError::Layout("attribute map keys"))?;
            let offsets = map.value_offsets();
            let known = self.keys.entry((source, kind)).or_default();
            for row in 0..map.len() {
                let (Some(start), Some(end)) = (offsets.get(row), offsets.get(row + 1)) else {
                    break;
                };
                let at = timestamps.value(row);
                in_row.clear();
                for entry in *start..*end {
                    let entry =
                        usize::try_from(entry).map_err(|_| SchemaError::Layout("offset"))?;
                    let key = keys.value(entry);
                    if !in_row.insert(key) {
                        continue;
                    }
                    let seen = KeySeen {
                        count: 1,
                        first_seen: at,
                        last_seen: at,
                    };
                    if let Some(known) = known.get_mut(key) {
                        known.widen(seen);
                    } else if self.new_keys < max_new_keys {
                        known.insert(key.to_owned(), seen);
                        self.new_keys += 1;
                    } else {
                        self.dropped += 1;
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

    /// The same stats with nothing counted as new, as a table read back.
    #[must_use]
    pub fn settled(mut self) -> Self {
        self.new_keys = 0;
        self
    }

    /// Entries of keys beyond the cap that were not counted.
    pub fn dropped(&self) -> u64 {
        self.dropped
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

    /// The table as one batch of [`attribute_keys_schema_v1`].
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
        for (source, key, kind, seen) in self.iter() {
            keys.push(key);
            sources.push(source.name());
            kinds.push(kind.name());
            counts.push(seen.count);
            firsts.push(seen.first_seen);
            lasts.push(seen.last_seen);
        }
        Ok(RecordBatch::try_new(
            attribute_keys_schema_v1(),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(sources)),
                Arc::new(StringArray::from(kinds)),
                Arc::new(UInt64Array::from(counts)),
                Arc::new(TimestampNanosecondArray::from(firsts).with_timezone("UTC")),
                Arc::new(TimestampNanosecondArray::from(lasts).with_timezone("UTC")),
            ],
        )?)
    }

    /// Reads a batch of the table; nothing read counts as new.
    ///
    /// # Errors
    /// A batch missing a column or with a source or type the version does
    /// not know.
    pub fn from_batch(batch: &RecordBatch) -> Result<Self, SchemaError> {
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
        let mut out = KeyStats::default();
        for row in 0..batch.num_rows() {
            let source =
                KeySource::parse(sources.value(row)).ok_or(SchemaError::Layout("source"))?;
            let kind = KeyType::parse(kinds.value(row)).ok_or(SchemaError::Layout("type"))?;
            let seen = KeySeen {
                count: counts.value(row),
                first_seen: firsts.value(row),
                last_seen: lasts.value(row),
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
}

/// Name of the table in a query session.
pub const TABLE: &str = "attribute_keys";

/// Schema of the table, version 1.
pub fn attribute_keys_schema_v1() -> SchemaRef {
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
            Field::new(col_names::KEY, DataType::Utf8, false),
            Field::new(col_names::SOURCE, DataType::Utf8, false),
            Field::new(col_names::TYPE, DataType::Utf8, false),
            Field::new(col_names::COUNT, DataType::UInt64, false),
            Field::new(col_names::FIRST_SEEN, timestamp.clone(), false),
            Field::new(col_names::LAST_SEEN, timestamp, false),
        ],
        metadata.into_iter().collect(),
    ))
}

/// Refuses a file schema that is not this table at this version.
///
/// # Errors
/// Another signal, another version, or no version at all.
pub fn check_schema(schema: &Schema) -> Result<(), SchemaError> {
    let metadata = schema.metadata();
    if metadata.get("fusescope.signal").map(String::as_str) != Some(SIGNAL) {
        return Err(SchemaError::Layout("attribute_keys signal"));
    }
    let version = metadata
        .get("fusescope.schema_version")
        .and_then(|v| v.parse::<u32>().ok());
    if version != Some(SCHEMA_VERSION) {
        return Err(SchemaError::Layout("attribute_keys schema version"));
    }
    Ok(())
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

/// Reads a Parquet file of the table, refusing another signal or version.
///
/// # Errors
/// Not a Parquet file of this table at this version.
pub fn decode(bytes: bytes::Bytes) -> Result<KeyStats, SchemaError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    check_schema(builder.schema())?;
    let mut out = KeyStats::default();
    for batch in builder.build()? {
        out.merge(KeyStats::from_batch(&batch?)?);
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

    fn span(start: u64, attributes: Vec<KeyValue>) -> Span {
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
        }
    }

    #[test]
    fn keys_are_counted_once_per_span_by_source_and_type_with_their_sightings() {
        let batch = batch_with(vec![
            span(
                1_000,
                vec![
                    kv("http.method", string("GET")),
                    kv("http.method", string("GET again")),
                    kv("http.status", Value::IntValue(200)),
                    kv("retry", Value::BoolValue(true)),
                ],
            ),
            span(3_000, vec![kv("http.method", string("POST"))]),
        ]);
        let mut stats = KeyStats::default();
        stats.absorb(&batch, usize::MAX).unwrap();
        let rows: Vec<_> = stats.iter().collect();
        assert_eq!(
            rows,
            vec![
                (
                    KeySource::Resource,
                    "host.name",
                    KeyType::String,
                    seen(2, 1_000, 3_000)
                ),
                (
                    KeySource::Resource,
                    "service.name",
                    KeyType::String,
                    seen(2, 1_000, 3_000)
                ),
                (
                    KeySource::Span,
                    "http.method",
                    KeyType::String,
                    seen(2, 1_000, 3_000)
                ),
                (
                    KeySource::Span,
                    "http.status",
                    KeyType::Number,
                    seen(1, 1_000, 1_000)
                ),
                (
                    KeySource::Span,
                    "retry",
                    KeyType::Bool,
                    seen(1, 1_000, 1_000)
                ),
                (
                    KeySource::Scope,
                    "lib.flavour",
                    KeyType::String,
                    seen(2, 1_000, 3_000)
                ),
            ],
            "a key repeated on one span counts once"
        );
        assert_eq!(stats.range(), Some((1_000, 3_000)));
        assert_eq!((stats.new_keys(), stats.dropped()), (6, 0));
    }

    #[test]
    fn keys_beyond_the_cap_are_dropped_in_batch_order_and_known_keys_still_count() {
        let batch = batch_with(vec![span(
            1_000,
            vec![kv("a", string("1")), kv("b", string("2"))],
        )]);
        let mut stats = KeyStats::default();
        stats.absorb(&batch, 3).unwrap();
        let kept: Vec<&str> = stats.iter().map(|(_, key, _, _)| key).collect();
        assert_eq!(
            kept,
            ["host.name", "service.name", "a"],
            "the resource keys come first, then the first span key"
        );
        assert_eq!(stats.dropped(), 2, "b and the scope key");

        // Decoded keys are not new: a cap of one admits one more key and
        // keeps counting the known ones.
        let table = decode(bytes::Bytes::from(encode(&stats).unwrap())).unwrap();
        assert_eq!(table.new_keys(), 0);
        let mut table = table;
        table
            .absorb(
                &batch_with(vec![span(
                    2_000,
                    vec![kv("c", string("3")), kv("a", string("4"))],
                )]),
                1,
            )
            .unwrap();
        let rows: Vec<_> = table
            .iter()
            .map(|(_, key, _, seen)| (key, seen.count))
            .collect();
        assert_eq!(
            rows,
            [("host.name", 2), ("service.name", 2), ("a", 2), ("c", 1)],
            "c admitted, the scope key dropped again"
        );
        assert_eq!((table.new_keys(), table.dropped()), (1, 1));
    }

    #[test]
    fn a_merge_sums_counts_and_widens_the_sightings_and_the_file_round_trips() {
        let mut first = KeyStats::default();
        first
            .absorb(
                &batch_with(vec![span(5_000, vec![kv("http.method", string("GET"))])]),
                usize::MAX,
            )
            .unwrap();
        let mut second = KeyStats::default();
        second
            .absorb(
                &batch_with(vec![span(
                    2_000,
                    vec![kv("http.method", string("PUT")), kv("db.name", string("x"))],
                )]),
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
        assert_eq!((method.first_seen, method.last_seen), (2_000, 5_000));
        assert_eq!(first.len(), 5);

        let bytes = encode(&first).unwrap();
        let back = decode(bytes::Bytes::from(bytes)).unwrap();
        let rows: Vec<_> = back.iter().collect();
        assert_eq!(rows, first.iter().collect::<Vec<_>>());
        assert_eq!(
            back.to_batch().unwrap().schema().metadata()["fusescope.schema_version"],
            "1"
        );
    }

    #[test]
    fn another_version_or_signal_is_refused() {
        let batch = KeyStats::default().to_batch().unwrap();
        for (key, value) in [
            ("fusescope.schema_version", "2"),
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
        assert!(check_schema(&attribute_keys_schema_v1()).is_ok());
        assert!(
            matches!(
                KeyStats::from_batch(&batch_with(Vec::new())).unwrap_err(),
                SchemaError::Arrow(_)
            ),
            "a span batch is not the table"
        );
    }
}
