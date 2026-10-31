//! Converts OTLP `ResourceSpans` into Arrow record batches with [`span_schema_v1`].

use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, Int8Builder, Int16Builder,
    ListBuilder, StringBuilder, StructBuilder, TimestampNanosecondBuilder, UInt32Builder,
    UInt64Builder,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link, SpanKind};
use opentelemetry_proto::tonic::trace::v1::status::StatusCode;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

use super::schema::{PROMOTED, PromotedScope, event_fields, link_fields, span_schema_v1};
use crate::SchemaError;
use crate::attrs::{TypedAttributes, split_strings, split_typed};
use crate::bucket::ts_bucket_start;
use crate::columns::{
    BoolMapBuilder, DictBuilder, NumberMapBuilder, StringMapBuilder, append_bool_map,
    append_number_map, append_string_map, bool_map, compact, list_item_field, number_map,
    string_map, timestamp,
};
use crate::fingerprint::resource_fingerprint;
use crate::limits::{DropStats, Limits};

/// OTLP `SpanFlags`: bit set when the parent context's `is_remote` is known.
const FLAG_HAS_IS_REMOTE: u32 = 0x100;
/// OTLP `SpanFlags`: bit set when the parent context is remote.
const FLAG_IS_REMOTE: u32 = 0x200;

/// Accumulates spans and produces one [`RecordBatch`] per [`SpanBatchBuilder::finish`].
///
/// The builder is reusable: `finish` resets it. Limits are applied on the way
/// in and counted in [`SpanBatchBuilder::stats`].
pub struct SpanBatchBuilder {
    limits: Limits,
    stats: DropStats,
    rows: usize,
    ts_bucket_start: UInt64Builder,
    timestamp: TimestampNanosecondBuilder,
    trace_id: FixedSizeBinaryBuilder,
    span_id: FixedSizeBinaryBuilder,
    parent_span_id: FixedSizeBinaryBuilder,
    trace_state: StringBuilder,
    flags: UInt32Builder,
    name: DictBuilder,
    kind: Int8Builder,
    kind_string: DictBuilder,
    duration_nano: UInt64Builder,
    status_code: Int16Builder,
    status_code_string: DictBuilder,
    status_message: StringBuilder,
    has_error: BooleanBuilder,
    is_remote: DictBuilder,
    resource_fingerprint: UInt64Builder,
    resources_string: StringMapBuilder,
    attributes_string: StringMapBuilder,
    attributes_number: NumberMapBuilder,
    attributes_bool: BoolMapBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_string: StringMapBuilder,
    events: ListBuilder<StructBuilder>,
    links: ListBuilder<StructBuilder>,
    promoted: Vec<DictBuilder>,
}

/// Resource level values shared by every span under one `ResourceSpans`.
struct ResourceCtx {
    fingerprint: u64,
    attributes: Vec<(String, String)>,
}

/// Scope level values shared by every span under one `ScopeSpans`.
struct ScopeCtx {
    name: String,
    version: String,
    attributes: Vec<(String, String)>,
}

fn event_list() -> ListBuilder<StructBuilder> {
    let fields = event_fields();
    let builders: Vec<Box<dyn ArrayBuilder>> = vec![
        Box::new(timestamp()),
        Box::new(StringBuilder::new()),
        Box::new(string_map()),
    ];
    ListBuilder::new(StructBuilder::new(fields.clone(), builders))
        .with_field(list_item_field(DataType::Struct(fields)))
}

fn link_list() -> ListBuilder<StructBuilder> {
    let fields = link_fields();
    let builders: Vec<Box<dyn ArrayBuilder>> = vec![
        Box::new(FixedSizeBinaryBuilder::new(16)),
        Box::new(FixedSizeBinaryBuilder::new(8)),
        Box::new(StringBuilder::new()),
        Box::new(string_map()),
    ];
    ListBuilder::new(StructBuilder::new(fields.clone(), builders))
        .with_field(list_item_field(DataType::Struct(fields)))
}

impl Default for SpanBatchBuilder {
    fn default() -> Self {
        Self::new(Limits::default())
    }
}

impl SpanBatchBuilder {
    /// Creates an empty builder applying `limits`.
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            stats: DropStats::default(),
            rows: 0,
            ts_bucket_start: UInt64Builder::new(),
            timestamp: timestamp(),
            trace_id: FixedSizeBinaryBuilder::new(16),
            span_id: FixedSizeBinaryBuilder::new(8),
            parent_span_id: FixedSizeBinaryBuilder::new(8),
            trace_state: StringBuilder::new(),
            flags: UInt32Builder::new(),
            name: DictBuilder::new(),
            kind: Int8Builder::new(),
            kind_string: DictBuilder::new(),
            duration_nano: UInt64Builder::new(),
            status_code: Int16Builder::new(),
            status_code_string: DictBuilder::new(),
            status_message: StringBuilder::new(),
            has_error: BooleanBuilder::new(),
            is_remote: DictBuilder::new(),
            resource_fingerprint: UInt64Builder::new(),
            resources_string: string_map(),
            attributes_string: string_map(),
            attributes_number: number_map(),
            attributes_bool: bool_map(),
            scope_name: StringBuilder::new(),
            scope_version: StringBuilder::new(),
            scope_string: string_map(),
            events: event_list(),
            links: link_list(),
            promoted: PROMOTED.iter().map(|_| DictBuilder::new()).collect(),
        }
    }

    /// Spans accumulated since the last `finish`.
    pub fn len(&self) -> usize {
        self.rows
    }

    /// True when no span is pending.
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Drop counters since the builder was created.
    pub fn stats(&self) -> DropStats {
        self.stats
    }

    /// Appends every span of an OTLP export request.
    ///
    /// # Errors
    /// Returns an error when an Arrow builder fails, which leaves the builder in
    /// an undefined state; callers should discard it.
    pub fn append_request(
        &mut self,
        request: &ExportTraceServiceRequest,
    ) -> Result<(), SchemaError> {
        for rs in &request.resource_spans {
            self.append_resource_spans(rs)?;
        }
        Ok(())
    }

    /// Appends every span under one resource.
    ///
    /// # Errors
    /// See [`SpanBatchBuilder::append_request`].
    pub fn append_resource_spans(&mut self, rs: &ResourceSpans) -> Result<(), SchemaError> {
        let attributes = rs.resource.as_ref().map_or_else(Vec::new, |r| {
            split_strings(&r.attributes, &self.limits, &mut self.stats)
        });
        let fingerprint =
            resource_fingerprint(attributes.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        let resource = ResourceCtx {
            fingerprint,
            attributes,
        };
        for ss in &rs.scope_spans {
            self.append_scope_spans(&resource, ss)?;
        }
        Ok(())
    }

    fn append_scope_spans(
        &mut self,
        resource: &ResourceCtx,
        ss: &ScopeSpans,
    ) -> Result<(), SchemaError> {
        let scope = self.scope_ctx(ss.scope.as_ref());
        for span in &ss.spans {
            if span.trace_id.len() != 16
                || span.span_id.len() != 8
                || !matches!(span.parent_span_id.len(), 0 | 8)
            {
                self.stats.spans_rejected += 1;
                continue;
            }
            self.append_span(resource, &scope, span)?;
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

    fn append_span(
        &mut self,
        resource: &ResourceCtx,
        scope: &ScopeCtx,
        span: &Span,
    ) -> Result<(), SchemaError> {
        let typed = split_typed(&span.attributes, &self.limits, &mut self.stats);

        self.ts_bucket_start
            .append_value(ts_bucket_start(span.start_time_unix_nano));
        self.timestamp
            .append_value(i64::try_from(span.start_time_unix_nano).unwrap_or(i64::MAX));
        self.trace_id.append_value(&span.trace_id)?;
        self.span_id.append_value(&span.span_id)?;
        if span.parent_span_id.is_empty() {
            self.parent_span_id.append_null();
        } else {
            self.parent_span_id.append_value(&span.parent_span_id)?;
        }
        self.trace_state.append_value(&span.trace_state);
        self.flags.append_value(span.flags);
        self.name.append(&span.name)?;

        let kind = SpanKind::try_from(span.kind).unwrap_or(SpanKind::Unspecified);
        self.kind.append_value(i8::try_from(span.kind).unwrap_or(0));
        self.kind_string.append(kind_name(kind))?;
        self.duration_nano.append_value(
            span.end_time_unix_nano
                .saturating_sub(span.start_time_unix_nano),
        );

        let (code, message) = span
            .status
            .as_ref()
            .map_or((0, ""), |s| (s.code, s.message.as_str()));
        let status = StatusCode::try_from(code).unwrap_or(StatusCode::Unset);
        self.status_code
            .append_value(i16::try_from(code).unwrap_or(0));
        self.status_code_string.append(status_name(status))?;
        self.status_message.append_value(message);
        self.has_error.append_value(status == StatusCode::Error);
        self.is_remote.append(is_remote(span.flags))?;

        self.resource_fingerprint.append_value(resource.fingerprint);
        append_string_map(&mut self.resources_string, &resource.attributes)?;
        append_string_map(&mut self.attributes_string, &typed.strings)?;
        append_number_map(&mut self.attributes_number, &typed.numbers)?;
        append_bool_map(&mut self.attributes_bool, &typed.bools)?;
        self.scope_name.append_value(&scope.name);
        self.scope_version.append_value(&scope.version);
        append_string_map(&mut self.scope_string, &scope.attributes)?;

        self.append_events(&span.events)?;
        self.append_links(&span.links)?;
        self.append_promoted(resource, &typed)?;

        self.rows += 1;
        Ok(())
    }

    fn append_events(&mut self, events: &[Event]) -> Result<(), SchemaError> {
        let kept = events.len().min(self.limits.max_events);
        self.stats.events_dropped += (events.len() - kept) as u64;
        for event in events.iter().take(kept) {
            let attributes = split_strings(&event.attributes, &self.limits, &mut self.stats);
            let item = self.events.values();
            item.field_builder::<TimestampNanosecondBuilder>(0)
                .ok_or(SchemaError::Layout("events.timestamp"))?
                .append_value(i64::try_from(event.time_unix_nano).unwrap_or(i64::MAX));
            item.field_builder::<StringBuilder>(1)
                .ok_or(SchemaError::Layout("events.name"))?
                .append_value(&event.name);
            let map = item
                .field_builder::<StringMapBuilder>(2)
                .ok_or(SchemaError::Layout("events.attributes"))?;
            append_string_map(map, &attributes)?;
            item.append(true);
        }
        self.events.append(true);
        Ok(())
    }

    fn append_links(&mut self, links: &[Link]) -> Result<(), SchemaError> {
        let mut kept = 0usize;
        for link in links {
            if kept >= self.limits.max_links || link.trace_id.len() != 16 || link.span_id.len() != 8
            {
                self.stats.links_dropped += 1;
                continue;
            }
            kept += 1;
            let attributes = split_strings(&link.attributes, &self.limits, &mut self.stats);
            let item = self.links.values();
            item.field_builder::<FixedSizeBinaryBuilder>(0)
                .ok_or(SchemaError::Layout("links.trace_id"))?
                .append_value(&link.trace_id)?;
            item.field_builder::<FixedSizeBinaryBuilder>(1)
                .ok_or(SchemaError::Layout("links.span_id"))?
                .append_value(&link.span_id)?;
            item.field_builder::<StringBuilder>(2)
                .ok_or(SchemaError::Layout("links.trace_state"))?
                .append_value(&link.trace_state);
            let map = item
                .field_builder::<StringMapBuilder>(3)
                .ok_or(SchemaError::Layout("links.attributes"))?;
            append_string_map(map, &attributes)?;
            item.append(true);
        }
        self.links.append(true);
        Ok(())
    }

    fn append_promoted(
        &mut self,
        resource: &ResourceCtx,
        typed: &TypedAttributes,
    ) -> Result<(), SchemaError> {
        for (spec, builder) in PROMOTED.iter().zip(self.promoted.iter_mut()) {
            let value = match spec.scope {
                PromotedScope::Resource => spec
                    .keys
                    .iter()
                    .find_map(|k| resource.attributes.iter().find(|(key, _)| key == k))
                    .map(|(_, v)| v.clone()),
                PromotedScope::Attribute => {
                    spec.keys.iter().find_map(|k| promoted_from_typed(typed, k))
                }
            };
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
            ArrayBuilder::finish(&mut self.trace_id),
            ArrayBuilder::finish(&mut self.span_id),
            ArrayBuilder::finish(&mut self.parent_span_id),
            ArrayBuilder::finish(&mut self.trace_state),
            ArrayBuilder::finish(&mut self.flags),
            ArrayBuilder::finish(&mut self.name),
            ArrayBuilder::finish(&mut self.kind),
            ArrayBuilder::finish(&mut self.kind_string),
            ArrayBuilder::finish(&mut self.duration_nano),
            ArrayBuilder::finish(&mut self.status_code),
            ArrayBuilder::finish(&mut self.status_code_string),
            ArrayBuilder::finish(&mut self.status_message),
            ArrayBuilder::finish(&mut self.has_error),
            ArrayBuilder::finish(&mut self.is_remote),
            ArrayBuilder::finish(&mut self.resource_fingerprint),
            ArrayBuilder::finish(&mut self.resources_string),
            ArrayBuilder::finish(&mut self.attributes_string),
            ArrayBuilder::finish(&mut self.attributes_number),
            ArrayBuilder::finish(&mut self.attributes_bool),
            ArrayBuilder::finish(&mut self.scope_name),
            ArrayBuilder::finish(&mut self.scope_version),
            ArrayBuilder::finish(&mut self.scope_string),
            ArrayBuilder::finish(&mut self.events),
            ArrayBuilder::finish(&mut self.links),
        ];
        columns.extend(self.promoted.iter_mut().map(ArrayBuilder::finish));
        self.rows = 0;
        let batch = RecordBatch::try_new(span_schema_v1(), columns)?;
        compact(&batch)
    }
}

/// A promoted attribute value: the string form, or an integral number rendered
/// without a fraction (status codes arrive as integers).
fn promoted_from_typed(typed: &TypedAttributes, key: &str) -> Option<String> {
    if let Some(s) = typed.string(key) {
        return Some(s.to_owned());
    }
    let n = typed.number(key)?;
    if n.fract() == 0.0 && n.abs() < 1e15 {
        // Integral and within the exactly representable range; the cast is exact.
        #[allow(clippy::cast_possible_truncation)]
        return Some((n as i64).to_string());
    }
    Some(n.to_string())
}

fn kind_name(kind: SpanKind) -> &'static str {
    match kind {
        SpanKind::Unspecified => "Unspecified",
        SpanKind::Internal => "Internal",
        SpanKind::Server => "Server",
        SpanKind::Client => "Client",
        SpanKind::Producer => "Producer",
        SpanKind::Consumer => "Consumer",
    }
}

fn status_name(status: StatusCode) -> &'static str {
    match status {
        StatusCode::Unset => "Unset",
        StatusCode::Ok => "Ok",
        StatusCode::Error => "Error",
    }
}

fn is_remote(flags: u32) -> &'static str {
    if flags & FLAG_HAS_IS_REMOTE == 0 {
        ""
    } else if flags & FLAG_IS_REMOTE == 0 {
        "false"
    } else {
        "true"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_remote_decodes_flags() {
        assert_eq!(is_remote(0), "");
        assert_eq!(is_remote(FLAG_HAS_IS_REMOTE), "false");
        assert_eq!(is_remote(FLAG_HAS_IS_REMOTE | FLAG_IS_REMOTE), "true");
        assert_eq!(is_remote(FLAG_IS_REMOTE), "");
    }

    #[test]
    fn promoted_numbers_render_as_integers() {
        let typed = TypedAttributes {
            strings: vec![],
            numbers: vec![
                ("http.response.status_code".into(), 503.0),
                ("ratio".into(), 0.5),
            ],
            bools: vec![],
        };
        assert_eq!(
            promoted_from_typed(&typed, "http.response.status_code").as_deref(),
            Some("503")
        );
        assert_eq!(promoted_from_typed(&typed, "ratio").as_deref(), Some("0.5"));
        assert_eq!(promoted_from_typed(&typed, "missing"), None);
    }

    #[test]
    fn finished_batches_are_compact() {
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
        let spans = (0..12u8)
            .map(|i| Span {
                trace_id: vec![1; 16],
                span_id: vec![i + 1; 8],
                name: "op".into(),
                start_time_unix_nano: 1_000 + u64::from(i),
                end_time_unix_nano: 2_000 + u64::from(i),
                ..Span::default()
            })
            .collect();
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let mut b = SpanBatchBuilder::default();
        b.append_request(&request).unwrap();
        let batch = b.finish().unwrap();
        assert_eq!(batch.num_rows(), 12);
        let bytes = batch.get_array_memory_size();
        assert!(
            bytes < 16 * 1024,
            "12 bare spans should not need {bytes} bytes"
        );
    }

    #[test]
    fn empty_builder_finishes_to_empty_batch() {
        let mut b = SpanBatchBuilder::default();
        let batch = b.finish().unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), span_schema_v1());
    }
}
