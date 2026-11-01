use std::fmt::Write as _;

use fs_schema::limits::Limits;
use fs_schema::spans::{SpanBatchBuilder, span_schema_v1};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

use super::*;
use crate::sizes::row_sizes;

/// 2026-10-30T00:00:00Z.
const T0: u64 = 1_793_318_400_000_000_000;

impl Chunks {
    fn capacity(&self) -> usize {
        self.done.iter().map(Bytes::len).sum::<usize>() + self.current.capacity()
    }
}

/// `spans` spans, each its own service and trace, with an attribute of
/// `value_bytes` bytes of hex noise.
fn spans(spans: usize, value_bytes: usize) -> RecordBatch {
    let resource_spans = (0..spans)
        .map(|i| {
            let mut state = (i as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let mut value = String::new();
            while value.len() < value_bytes {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                write!(value, "{state:016x}").unwrap();
            }
            let kv = |key: &str, value: String| KeyValue {
                key: key.into(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(value)),
                }),
                ..KeyValue::default()
            };
            ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", format!("service-{i:05}"))],
                    ..Resource::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: (i as u128 + 1).to_be_bytes().to_vec(),
                        span_id: vec![1; 8],
                        name: "op".into(),
                        start_time_unix_nano: T0 + i as u64,
                        end_time_unix_nano: T0 + i as u64 + 1,
                        attributes: vec![kv("payload", value)],
                        ..Span::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }
        })
        .collect();
    let mut builder = SpanBatchBuilder::new(Limits::default());
    builder
        .append_request(&ExportTraceServiceRequest { resource_spans })
        .unwrap();
    builder.finish().unwrap()
}

fn config(output_bytes: usize) -> MergeConfig {
    MergeConfig {
        level: Level::L2,
        row_group_rows: 8192,
        read_batch_rows: 8192,
        target_bytes: usize::MAX,
        output_bytes,
        max_trace_index_bytes: 1 << 20,
        max_services_per_file: 100_000,
        max_new_keys: 0,
        trace_summary_max_traces: 0,
        trace_summary_max_bytes: 0,
        service_graph_max_spans: 0,
        text_index: fs_index::TextIndexLimits::DEFAULT,
        service_graph_max_bytes: 0,
        usage_max_bytes: 1 << 20,
    }
}

#[test]
fn a_buffer_is_charged_whole_before_it_is_allocated() {
    let mut chunks = Chunks::new(MIN_CHUNK_BYTES);
    for n in [1, MIN_CHUNK_BYTES - 2, 1, 1, 1, 3 * MIN_CHUNK_BYTES] {
        let held = chunks.held();
        std::io::Write::write_all(&mut chunks, &vec![7u8; n]).unwrap();
        assert!(
            chunks.capacity() <= held + n,
            "{} allocated, {held} charged before {n} bytes",
            chunks.capacity()
        );
        assert!(chunks.capacity() <= chunks.held());
    }
}

/// Small row groups cross the output's buffers again and again: no write
/// takes the buffers, the page indexes and the footer past what the
/// output charged before it for itself and the row group, and what it
/// charges before closing covers the footer closing writes.
#[test]
fn an_output_holds_no_more_than_it_charges_through_its_footer() {
    let schema = span_schema_v1();
    let batch = spans(400, 1024);
    let sizes = row_sizes(&batch).unwrap();
    let mut output = OutputBuilder::new(
        &schema,
        Signal::Traces,
        FileId::new(),
        Path::from("t/x/traces/l2/dt=2026-10-30/out.parquet"),
        &config(8 << 20),
    )
    .unwrap();
    let mut crossed = 0;
    for start in (0..batch.num_rows()).step_by(5) {
        let group = batch.slice(start, 5);
        let payload: usize = sizes.get(start..start + 5).unwrap().iter().sum();
        let before = output.memory();
        let charged =
            before + 3 * (2 * payload) + output.row_group_overhead(group.num_rows(), payload);
        let buffers = output.writer.inner().done.len();
        output.write(&group).unwrap();
        crossed += output.writer.inner().done.len() - buffers;
        let held = output.writer.inner().capacity() + output.writer.memory_size();
        assert!(held <= charged, "{held} held, {charged} charged");
        assert!(held <= output.memory());
    }
    assert!(crossed >= 2, "{crossed} buffers filled");
    let charged = output.memory();
    let written = output.writer.inner().len;
    let indexes = output.index_bytes;
    let chunk = output.writer.inner().size;
    let finished = output.finish(TenantId(uuid::Uuid::nil())).unwrap();
    let size = finished.bytes.content_length();
    assert!(
        size - written <= indexes + FOOTER_FIXED_BYTES,
        "a footer of {} bytes, {indexes} of indexes counted",
        size - written
    );
    assert!(size.div_ceil(chunk) * chunk <= charged);
}
