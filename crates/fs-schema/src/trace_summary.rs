//! Per-compaction-pass trace summaries.
//!
//! Format v1 has one row per trace in the pass: `trace_id`, `start`, `end`,
//! `num_spans`, sorted distinct `services`, and `has_error`. Files are partial
//! contributions: queries combine every visible file for the hour.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, OnceLock};

use arrow::array::{
    Array, AsArray, BooleanArray, FixedSizeBinaryBuilder, ListBuilder, StringBuilder,
    TimestampNanosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::SchemaError;
use crate::spans::{TRACE_ID_BYTES, col};

/// Version this engine writes and reads.
pub const SCHEMA_VERSION: u32 = 1;
/// Value of the `fusescope.signal` metadata.
pub const SIGNAL: &str = "trace_summary";
/// Name in a query session.
pub const TABLE: &str = "trace_summary";
/// Conservative inline and tree-node charge for one trace before services.
pub const TRACE_ENTRY_BYTES: usize = 512;
/// Fixed charge for one service reference retained and encoded by one trace.
pub const TRACE_SERVICE_BYTES: usize = 64;
/// Charge above UTF-8 bytes for one service interned by a pass.
pub const INTERNED_SERVICE_BYTES: usize = 96;

/// Column names.
pub mod columns {
    /// Trace identity.
    pub const TRACE_ID: &str = "trace_id";
    /// Earliest span start.
    pub const START: &str = "start";
    /// Latest span end.
    pub const END: &str = "end";
    /// Spans in this pass.
    pub const NUM_SPANS: &str = "num_spans";
    /// Sorted distinct services in this pass.
    pub const SERVICES: &str = "services";
    /// Whether a span in this pass has error status.
    pub const HAS_ERROR: &str = "has_error";
}

/// One trace's contribution from a compaction pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceSummary {
    /// Earliest start in nanoseconds.
    pub start: i64,
    /// Latest end in nanoseconds.
    pub end: i64,
    /// Spans seen.
    pub num_spans: u64,
    /// Sorted distinct non-empty service names.
    pub services: Vec<Arc<str>>,
    /// Whether any span has error status.
    pub has_error: bool,
}

impl TraceSummary {
    fn merge(&mut self, other: Self) {
        self.start = self.start.min(other.start);
        self.end = self.end.max(other.end);
        self.num_spans = self.num_spans.saturating_add(other.num_spans);
        for service in other.services {
            if let Err(at) = self
                .services
                .binary_search_by(|known| known.as_ref().cmp(service.as_ref()))
            {
                self.services.insert(at, service);
            }
        }
        self.has_error |= other.has_error;
    }
}

/// Summaries built from one compaction pass. Crossing either cap clears the
/// partial map and permanently marks the contribution skipped.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TraceSummaries {
    rows: BTreeMap<[u8; TRACE_ID_BYTES], TraceSummary>,
    services: HashSet<Arc<str>>,
    estimated_bytes: usize,
    skipped: bool,
}

impl TraceSummaries {
    /// Adds a span batch while distinct traces and conservatively charged
    /// resident bytes stay within their caps. Once crossed, it stays empty.
    ///
    /// # Errors
    /// A required span column is missing or has another physical type.
    pub fn absorb(
        &mut self,
        batch: &RecordBatch,
        max_traces: usize,
        max_bytes: usize,
    ) -> Result<(), SchemaError> {
        if self.skipped {
            return Ok(());
        }
        let schema = batch.schema();
        let trace_ids = batch
            .column(schema.index_of(col::TRACE_ID)?)
            .as_fixed_size_binary();
        let timestamps = batch
            .column(schema.index_of(col::TIMESTAMP)?)
            .as_primitive_opt::<TimestampNanosecondType>()
            .ok_or(SchemaError::Layout("timestamp"))?;
        let durations = batch
            .column(schema.index_of(col::DURATION_NANO)?)
            .as_primitive_opt::<arrow::datatypes::UInt64Type>()
            .ok_or(SchemaError::Layout("duration_nano"))?;
        let errors = batch
            .column(schema.index_of(col::HAS_ERROR)?)
            .as_boolean_opt()
            .ok_or(SchemaError::Layout("has_error"))?;
        let services = arrow::compute::cast(
            batch.column(schema.index_of(col::SERVICE_NAME)?),
            &DataType::Utf8,
        )?;
        let services = services.as_string::<i32>();
        for row in 0..batch.num_rows() {
            let id: [u8; TRACE_ID_BYTES] = trace_ids
                .value(row)
                .try_into()
                .map_err(|_| SchemaError::Layout("trace_id"))?;
            let new_trace = !self.rows.contains_key(&id);
            let start = timestamps.value(row);
            let duration = i64::try_from(durations.value(row)).unwrap_or(i64::MAX);
            let end = start.saturating_add(duration);
            let service = (!services.is_null(row))
                .then(|| services.value(row))
                .filter(|value| !value.is_empty());
            let new_link = service.is_some_and(|service| {
                self.rows.get(&id).is_none_or(|known| {
                    known
                        .services
                        .binary_search_by(|value| value.as_ref().cmp(service))
                        .is_err()
                })
            });
            let new_service = service.is_some_and(|service| !self.services.contains(service));
            let charge =
                usize::from(new_trace)
                    .saturating_mul(TRACE_ENTRY_BYTES)
                    .saturating_add(usize::from(new_link).saturating_mul(
                        TRACE_SERVICE_BYTES.saturating_add(
                            service.map_or(0, |value| value.len().saturating_mul(2)),
                        ),
                    ))
                    .saturating_add(service.filter(|_| new_service).map_or(0, |value| {
                        value.len().saturating_add(INTERNED_SERVICE_BYTES)
                    }));
            let Some(next_bytes) = self.estimated_bytes.checked_add(charge) else {
                self.skip();
                return Ok(());
            };
            if (new_trace && self.rows.len() >= max_traces) || next_bytes > max_bytes {
                self.skip();
                return Ok(());
            }
            self.estimated_bytes = next_bytes;
            let service = if new_link {
                service.map(|value| {
                    if let Some(known) = self.services.get(value) {
                        Arc::clone(known)
                    } else {
                        let value: Arc<str> = Arc::from(value);
                        self.services.insert(Arc::clone(&value));
                        value
                    }
                })
            } else {
                None
            };
            let entry = self.rows.entry(id).or_insert_with(|| TraceSummary {
                start,
                end,
                num_spans: 0,
                services: Vec::new(),
                has_error: false,
            });
            entry.start = entry.start.min(start);
            entry.end = entry.end.max(end);
            entry.num_spans = entry.num_spans.saturating_add(1);
            if let Some(service) = service {
                if let Err(at) = entry
                    .services
                    .binary_search_by(|known| known.as_ref().cmp(service.as_ref()))
                {
                    entry.services.insert(at, service);
                }
            }
            entry.has_error |= errors.value(row);
        }
        Ok(())
    }

    /// True when a cap was crossed and no partial file may be emitted.
    pub fn skipped(&self) -> bool {
        self.skipped
    }

    /// Number of trace rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Conservative bytes charged before every allocation retained by the pass.
    pub fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    /// True when no contribution can be written.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Rows in trace-id order.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8; TRACE_ID_BYTES], &TraceSummary)> {
        self.rows.iter()
    }

    /// Earliest and latest span starts. Compaction inputs are hour-partitioned,
    /// so this range expires with their L1 files even when a span has a bad or
    /// exceptionally distant end timestamp.
    pub fn range(&self) -> Option<(i64, i64)> {
        self.rows.values().fold(None, |range, row| {
            Some(match range {
                None => (row.start, row.start),
                Some((start, end)) => (start.min(row.start), end.max(row.start)),
            })
        })
    }

    /// Merges another partial contribution, used when a file has several
    /// row groups.
    pub fn merge(&mut self, other: Self) {
        if self.skipped || other.skipped {
            self.skip();
            return;
        }
        for (id, summary) in other.rows {
            self.rows
                .entry(id)
                .and_modify(|known| known.merge(summary.clone()))
                .or_insert(summary);
        }
        self.recount();
    }

    fn skip(&mut self) {
        self.rows.clear();
        self.services.clear();
        self.estimated_bytes = 0;
        self.skipped = true;
    }

    fn recount(&mut self) {
        self.services.clear();
        self.estimated_bytes = self.rows.len().saturating_mul(TRACE_ENTRY_BYTES);
        for row in self.rows.values_mut() {
            for service in &mut row.services {
                if let Some(known) = self.services.get(service.as_ref()) {
                    *service = Arc::clone(known);
                } else {
                    self.services.insert(Arc::clone(service));
                }
            }
            self.estimated_bytes = self
                .estimated_bytes
                .saturating_add(row.services.iter().fold(0usize, |bytes, service| {
                    bytes.saturating_add(
                        TRACE_SERVICE_BYTES.saturating_add(service.len().saturating_mul(2)),
                    )
                }));
        }
        self.estimated_bytes = self.estimated_bytes.saturating_add(
            self.services
                .iter()
                .map(|service| service.len().saturating_add(INTERNED_SERVICE_BYTES))
                .sum::<usize>(),
        );
    }

    /// Encodes the rows as one Arrow batch.
    ///
    /// # Errors
    /// Arrow refuses an inconsistent array.
    pub fn to_batch(&self) -> Result<RecordBatch, SchemaError> {
        let width = i32::try_from(TRACE_ID_BYTES)
            .map_err(|_| SchemaError::Layout("trace summary trace_id width"))?;
        let mut ids = FixedSizeBinaryBuilder::new(width);
        let mut starts = Vec::with_capacity(self.len());
        let mut ends = Vec::with_capacity(self.len());
        let mut counts = Vec::with_capacity(self.len());
        let mut services = ListBuilder::new(StringBuilder::new()).with_field(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            false,
        )));
        let mut errors = Vec::with_capacity(self.len());
        for (id, row) in &self.rows {
            ids.append_value(id)?;
            starts.push(row.start);
            ends.push(row.end);
            counts.push(row.num_spans);
            for service in &row.services {
                services.values().append_value(service);
            }
            services.append(true);
            errors.push(row.has_error);
        }
        Ok(RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(ids.finish()),
                Arc::new(TimestampNanosecondArray::from(starts).with_timezone("UTC")),
                Arc::new(TimestampNanosecondArray::from(ends).with_timezone("UTC")),
                Arc::new(UInt64Array::from(counts)),
                Arc::new(services.finish()),
                Arc::new(BooleanArray::from(errors)),
            ],
        )?)
    }

    fn from_batch(batch: &RecordBatch) -> Result<Self, SchemaError> {
        let schema = batch.schema();
        let column = |name: &str| -> Result<&arrow::array::ArrayRef, SchemaError> {
            Ok(batch.column(schema.index_of(name)?))
        };
        let ids = column(columns::TRACE_ID)?.as_fixed_size_binary();
        let starts = column(columns::START)?
            .as_primitive_opt::<TimestampNanosecondType>()
            .ok_or(SchemaError::Layout("trace summary start"))?;
        let ends = column(columns::END)?
            .as_primitive_opt::<TimestampNanosecondType>()
            .ok_or(SchemaError::Layout("trace summary end"))?;
        let counts = column(columns::NUM_SPANS)?
            .as_primitive_opt::<arrow::datatypes::UInt64Type>()
            .ok_or(SchemaError::Layout("trace summary num_spans"))?;
        let service_lists = column(columns::SERVICES)?
            .as_list_opt::<i32>()
            .ok_or(SchemaError::Layout("trace summary services"))?;
        let service_values = service_lists
            .values()
            .as_string_opt::<i32>()
            .ok_or(SchemaError::Layout("trace summary service"))?;
        let errors = column(columns::HAS_ERROR)?
            .as_boolean_opt()
            .ok_or(SchemaError::Layout("trace summary has_error"))?;
        let offsets = service_lists.value_offsets();
        let mut out = Self::default();
        for row in 0..batch.num_rows() {
            let id: [u8; TRACE_ID_BYTES] = ids
                .value(row)
                .try_into()
                .map_err(|_| SchemaError::Layout("trace summary trace_id"))?;
            let (Some(start), Some(end)) = (offsets.get(row), offsets.get(row + 1)) else {
                return Err(SchemaError::Layout("trace summary services offsets"));
            };
            let mut names = Vec::new();
            for index in *start..*end {
                let index = usize::try_from(index)
                    .map_err(|_| SchemaError::Layout("trace summary service offset"))?;
                let name: Arc<str> = Arc::from(service_values.value(index));
                if names
                    .last()
                    .is_none_or(|last: &Arc<str>| last.as_ref() != name.as_ref())
                {
                    names.push(name);
                }
            }
            let summary = TraceSummary {
                start: starts.value(row),
                end: ends.value(row),
                num_spans: counts.value(row),
                services: names,
                has_error: errors.value(row),
            };
            out.rows
                .entry(id)
                .and_modify(|known| known.merge(summary.clone()))
                .or_insert(summary);
        }
        out.recount();
        Ok(out)
    }
}

/// Exact physical layout of format v1.
pub fn schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let timestamp = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
            let services = DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)));
            let metadata = [
                ("fusescope.signal".to_owned(), SIGNAL.to_owned()),
                (
                    "fusescope.schema_version".to_owned(),
                    SCHEMA_VERSION.to_string(),
                ),
            ];
            Arc::new(Schema::new_with_metadata(
                vec![
                    Field::new(columns::TRACE_ID, DataType::FixedSizeBinary(16), false),
                    Field::new(columns::START, timestamp.clone(), false),
                    Field::new(columns::END, timestamp, false),
                    Field::new(columns::NUM_SPANS, DataType::UInt64, false),
                    Field::new(columns::SERVICES, services, false),
                    Field::new(columns::HAS_ERROR, DataType::Boolean, false),
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
        return Err(SchemaError::Layout("trace_summary format v1"));
    }
    Ok(())
}

/// Encodes one pass's contribution as Parquet.
///
/// # Errors
/// Arrow or Parquet failure.
pub fn encode(rows: &TraceSummaries) -> Result<Vec<u8>, SchemaError> {
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
/// It is not a `trace_summary` format-v1 file.
pub fn decode(bytes: bytes::Bytes) -> Result<TraceSummaries, SchemaError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    check_schema(builder.schema())?;
    let mut out = TraceSummaries::default();
    for batch in builder.build()? {
        out.merge(TraceSummaries::from_batch(&batch?)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};

    use super::*;
    use crate::limits::Limits;
    use crate::spans::SpanBatchBuilder;

    fn resource(service: &str, spans: Vec<Span>) -> ResourceSpans {
        ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(service.into())),
                    }),
                    ..KeyValue::default()
                }],
                ..Resource::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans,
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }
    }

    fn span(trace: u8, span: u8, start: u64, duration: u64, error: bool) -> Span {
        Span {
            trace_id: vec![trace; 16],
            span_id: vec![span; 8],
            name: "op".into(),
            start_time_unix_nano: start,
            end_time_unix_nano: start + duration,
            status: error.then_some(Status {
                code: 2,
                ..Status::default()
            }),
            ..Span::default()
        }
    }

    fn batch() -> RecordBatch {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![
                resource(
                    "api",
                    vec![span(1, 1, 100, 20, false), span(2, 2, 500, 5, false)],
                ),
                resource("worker", vec![span(1, 3, 90, 50, true)]),
            ],
        };
        let mut builder = SpanBatchBuilder::new(Limits::default());
        builder.append_request(&request).unwrap();
        builder.finish().unwrap()
    }

    #[test]
    fn summaries_match_the_span_aggregation_and_round_trip() {
        let mut rows = TraceSummaries::default();
        rows.absorb(&batch(), 10, usize::MAX).unwrap();
        let first = rows.iter().next().unwrap();
        assert_eq!(*first.0, [1; 16]);
        assert_eq!((first.1.start, first.1.end), (90, 140));
        assert_eq!(first.1.num_spans, 2);
        assert_eq!(
            first
                .1
                .services
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<_>>(),
            ["api", "worker"]
        );
        assert!(first.1.has_error);
        assert_eq!(rows.range(), Some((90, 500)));

        let bytes = encode(&rows).unwrap();
        assert_eq!(decode(bytes.into()).unwrap(), rows);
    }

    #[test]
    fn committed_format_v1_fixture_remains_readable() {
        let mut expected = TraceSummaries::default();
        expected.absorb(&batch(), 10, usize::MAX).unwrap();
        let bytes = bytes::Bytes::from_static(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/trace_summary_v1.parquet"
        )));
        assert_eq!(decode(bytes).unwrap(), expected);
    }

    #[test]
    fn crossing_the_trace_cap_emits_no_partial_table() {
        let mut rows = TraceSummaries::default();
        rows.absorb(&batch(), 1, usize::MAX).unwrap();
        assert!(rows.skipped());
        assert_eq!(rows.len(), 0);
        rows.absorb(&batch(), usize::MAX, usize::MAX).unwrap();
        assert_eq!(rows.len(), 0, "a skipped pass never resumes partially");
    }

    #[test]
    fn byte_cap_is_charged_before_the_first_trace_is_retained() {
        let mut rows = TraceSummaries::default();
        rows.absorb(&batch(), usize::MAX, TRACE_ENTRY_BYTES - 1)
            .unwrap();
        assert!(rows.skipped());
        assert_eq!(rows.estimated_bytes(), 0);
    }

    #[test]
    fn another_version_or_nullable_column_is_refused() {
        let expected = schema();
        let mut metadata = expected.metadata().clone();
        metadata.insert("fusescope.schema_version".into(), "2".into());
        let future = Schema::new_with_metadata(expected.fields().clone(), metadata);
        assert!(check_schema(&future).is_err());
        let mut fields = expected.fields().to_vec();
        *fields.first_mut().unwrap() = Arc::new(Field::new(
            columns::TRACE_ID,
            DataType::FixedSizeBinary(16),
            true,
        ));
        let nullable = Schema::new_with_metadata(fields, expected.metadata().clone());
        assert!(check_schema(&nullable).is_err());
    }
}
