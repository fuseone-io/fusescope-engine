//! Converts OTLP `ResourceLogs` into Arrow record batches with [`log_schema_v1`].

use arrow::array::{
    ArrayBuilder, ArrayRef, FixedSizeBinaryBuilder, StringBuilder, TimestampNanosecondBuilder,
    UInt8Builder, UInt32Builder, UInt64Builder,
};
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs};

use super::log_id;
use super::schema::{LOG_PROMOTED, log_schema_v1};
use crate::SchemaError;
use crate::attrs::{TypedAttributes, render_any, split_strings, split_typed};
use crate::bucket::ts_bucket_start;
use crate::columns::{
    BoolMapBuilder, DictBuilder, NumberMapBuilder, StringMapBuilder, append_bool_map,
    append_number_map, append_string_map, bool_map, compact, number_map, string_map, timestamp,
};
use crate::fingerprint::resource_fingerprint;
use crate::limits::{DropStats, Limits, truncate_utf8};
use crate::spans::PromotedScope;

/// OTLP `LogRecordFlags`: the W3C trace flags in the low byte.
const TRACE_FLAGS_MASK: u32 = 0xff;

/// Accumulates log records and produces one [`RecordBatch`] per
/// [`LogBatchBuilder::finish`].
///
/// The builder is reusable: `finish` resets it. Limits are applied on the way
/// in and counted in [`LogBatchBuilder::stats`].
pub struct LogBatchBuilder {
    limits: Limits,
    stats: DropStats,
    rows: usize,
    ts_bucket_start: UInt64Builder,
    timestamp: TimestampNanosecondBuilder,
    observed_timestamp: TimestampNanosecondBuilder,
    id: FixedSizeBinaryBuilder,
    trace_id: FixedSizeBinaryBuilder,
    span_id: FixedSizeBinaryBuilder,
    trace_flags: UInt32Builder,
    severity_text: DictBuilder,
    severity_number: UInt8Builder,
    body: StringBuilder,
    resource_fingerprint: UInt64Builder,
    resources_string: StringMapBuilder,
    attributes_string: StringMapBuilder,
    attributes_number: NumberMapBuilder,
    attributes_bool: BoolMapBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_string: StringMapBuilder,
    promoted: Vec<DictBuilder>,
}

/// Resource level values shared by every record under one `ResourceLogs`.
struct ResourceCtx {
    fingerprint: u64,
    attributes: Vec<(String, String)>,
}

/// Scope level values shared by every record under one `ScopeLogs`.
struct ScopeCtx {
    name: String,
    version: String,
    attributes: Vec<(String, String)>,
}

/// Where a record's id comes from: the request's origin and the record's
/// position in it, rejected records included.
struct Origin<'a> {
    seed: &'a [u8],
    ordinal: u64,
}

impl Default for LogBatchBuilder {
    fn default() -> Self {
        Self::new(Limits::default())
    }
}

impl LogBatchBuilder {
    /// Creates an empty builder applying `limits`.
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            stats: DropStats::default(),
            rows: 0,
            ts_bucket_start: UInt64Builder::new(),
            timestamp: timestamp(),
            observed_timestamp: timestamp(),
            id: FixedSizeBinaryBuilder::new(16),
            trace_id: FixedSizeBinaryBuilder::new(16),
            span_id: FixedSizeBinaryBuilder::new(8),
            trace_flags: UInt32Builder::new(),
            severity_text: DictBuilder::new(),
            severity_number: UInt8Builder::new(),
            body: StringBuilder::new(),
            resource_fingerprint: UInt64Builder::new(),
            resources_string: string_map(),
            attributes_string: string_map(),
            attributes_number: number_map(),
            attributes_bool: bool_map(),
            scope_name: StringBuilder::new(),
            scope_version: StringBuilder::new(),
            scope_string: string_map(),
            promoted: LOG_PROMOTED.iter().map(|_| DictBuilder::new()).collect(),
        }
    }

    /// Records accumulated since the last `finish`.
    pub fn len(&self) -> usize {
        self.rows
    }

    /// True when no record is pending.
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Drop counters since the builder was created.
    pub fn stats(&self) -> DropStats {
        self.stats
    }

    /// Appends every record of an OTLP export request. `seed` names where the
    /// request comes from, for example its WAL position; the same request
    /// under the same seed gets the same ids.
    ///
    /// # Errors
    /// Returns an error when an Arrow builder fails, which leaves the builder in
    /// an undefined state; callers should discard it.
    pub fn append_request(
        &mut self,
        request: &ExportLogsServiceRequest,
        seed: &[u8],
    ) -> Result<(), SchemaError> {
        let mut origin = Origin { seed, ordinal: 0 };
        for rl in &request.resource_logs {
            self.append_resource_logs(rl, &mut origin)?;
        }
        Ok(())
    }

    fn append_resource_logs(
        &mut self,
        rl: &ResourceLogs,
        origin: &mut Origin<'_>,
    ) -> Result<(), SchemaError> {
        let attributes = rl.resource.as_ref().map_or_else(Vec::new, |r| {
            split_strings(&r.attributes, &self.limits, &mut self.stats)
        });
        let fingerprint =
            resource_fingerprint(attributes.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        let resource = ResourceCtx {
            fingerprint,
            attributes,
        };
        for sl in &rl.scope_logs {
            let scope = self.scope_ctx(sl.scope.as_ref());
            for record in &sl.log_records {
                let ordinal = origin.ordinal;
                origin.ordinal += 1;
                let Some(time) = event_time(record) else {
                    self.stats.logs_rejected += 1;
                    continue;
                };
                let id = log_id(time, origin.seed, ordinal);
                self.append_record(&resource, &scope, record, time, &id)?;
            }
        }
        Ok(())
    }

    fn scope_ctx(&mut self, scope: Option<&InstrumentationScope>) -> ScopeCtx {
        scope.map_or_else(
            || ScopeCtx {
                name: String::new(),
                version: String::new(),
                attributes: Vec::new(),
            },
            |s| ScopeCtx {
                name: s.name.clone(),
                version: s.version.clone(),
                attributes: split_strings(&s.attributes, &self.limits, &mut self.stats),
            },
        )
    }

    fn append_record(
        &mut self,
        resource: &ResourceCtx,
        scope: &ScopeCtx,
        record: &LogRecord,
        time: u64,
        id: &[u8],
    ) -> Result<(), SchemaError> {
        let typed = split_typed(&record.attributes, &self.limits, &mut self.stats);
        let observed = match record.observed_time_unix_nano {
            0 => time,
            t => t,
        };

        self.ts_bucket_start.append_value(ts_bucket_start(time));
        self.timestamp.append_value(nanos(time));
        self.observed_timestamp.append_value(nanos(observed));
        self.id.append_value(id)?;
        append_valid_id(&mut self.trace_id, 16, &record.trace_id)?;
        append_valid_id(&mut self.span_id, 8, &record.span_id)?;
        self.trace_flags
            .append_value(record.flags & TRACE_FLAGS_MASK);
        self.severity_text.append(&record.severity_text)?;
        self.severity_number
            .append_value(u8::try_from(record.severity_number).unwrap_or(0));
        let body = self.body_text(record);
        self.body.append_value(body);

        self.resource_fingerprint.append_value(resource.fingerprint);
        append_string_map(&mut self.resources_string, &resource.attributes)?;
        append_string_map(&mut self.attributes_string, &typed.strings)?;
        append_number_map(&mut self.attributes_number, &typed.numbers)?;
        append_bool_map(&mut self.attributes_bool, &typed.bools)?;
        self.scope_name.append_value(&scope.name);
        self.scope_version.append_value(&scope.version);
        append_string_map(&mut self.scope_string, &scope.attributes)?;
        self.append_promoted(resource, &typed)?;

        self.rows += 1;
        Ok(())
    }

    fn body_text(&mut self, record: &LogRecord) -> String {
        let rendered = record
            .body
            .as_ref()
            .and_then(|b| b.value.as_ref())
            .map(render_any)
            .unwrap_or_default();
        let cut = truncate_utf8(&rendered, self.limits.max_body_bytes);
        if cut.len() < rendered.len() {
            self.stats.bodies_truncated += 1;
            return cut.to_owned();
        }
        rendered
    }

    fn append_promoted(
        &mut self,
        resource: &ResourceCtx,
        typed: &TypedAttributes,
    ) -> Result<(), SchemaError> {
        for (spec, builder) in LOG_PROMOTED.iter().zip(self.promoted.iter_mut()) {
            let value = spec.keys.iter().find_map(|k| match spec.scope {
                PromotedScope::Resource => resource
                    .attributes
                    .iter()
                    .find(|(key, _)| key == k)
                    .map(|(_, v)| v.as_str()),
                PromotedScope::Attribute => typed.string(k),
            });
            match value {
                Some(v) => {
                    builder.append(v)?;
                }
                None => builder.append_null(),
            }
        }
        Ok(())
    }

    /// Produces the batch for everything appended so far and resets the builder.
    ///
    /// # Errors
    /// Returns an error when the columns do not fit the schema, which indicates a
    /// bug in this crate rather than bad input.
    pub fn finish(&mut self) -> Result<RecordBatch, SchemaError> {
        let mut columns: Vec<ArrayRef> = vec![
            ArrayBuilder::finish(&mut self.ts_bucket_start),
            ArrayBuilder::finish(&mut self.timestamp),
            ArrayBuilder::finish(&mut self.observed_timestamp),
            ArrayBuilder::finish(&mut self.id),
            ArrayBuilder::finish(&mut self.trace_id),
            ArrayBuilder::finish(&mut self.span_id),
            ArrayBuilder::finish(&mut self.trace_flags),
            ArrayBuilder::finish(&mut self.severity_text),
            ArrayBuilder::finish(&mut self.severity_number),
            ArrayBuilder::finish(&mut self.body),
            ArrayBuilder::finish(&mut self.resource_fingerprint),
            ArrayBuilder::finish(&mut self.resources_string),
            ArrayBuilder::finish(&mut self.attributes_string),
            ArrayBuilder::finish(&mut self.attributes_number),
            ArrayBuilder::finish(&mut self.attributes_bool),
            ArrayBuilder::finish(&mut self.scope_name),
            ArrayBuilder::finish(&mut self.scope_version),
            ArrayBuilder::finish(&mut self.scope_string),
        ];
        columns.extend(self.promoted.iter_mut().map(ArrayBuilder::finish));
        self.rows = 0;
        let batch = RecordBatch::try_new(log_schema_v1(), columns)?;
        compact(&batch)
    }
}

/// The record's time: the event time, else the observed time; `None` when
/// both are unknown.
fn event_time(record: &LogRecord) -> Option<u64> {
    [record.time_unix_nano, record.observed_time_unix_nano]
        .into_iter()
        .find(|t| *t != 0)
}

fn nanos(unix_nano: u64) -> i64 {
    i64::try_from(unix_nano).unwrap_or(i64::MAX)
}

/// Appends `id`, or null when it is absent or invalid: OTLP treats an id of the
/// wrong length or of all zeroes as no id.
fn append_valid_id(
    builder: &mut FixedSizeBinaryBuilder,
    width: usize,
    id: &[u8],
) -> Result<(), SchemaError> {
    if id.len() == width && id.iter().any(|b| *b != 0) {
        builder.append_value(id)?;
    } else {
        builder.append_null();
    }
    Ok(())
}
