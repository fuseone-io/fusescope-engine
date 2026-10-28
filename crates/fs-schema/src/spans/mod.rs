//! Span schema and the OTLP spans to Arrow converter.

mod builder;
mod schema;

pub use builder::SpanBatchBuilder;
pub use schema::{
    PROMOTED, Promoted, PromotedScope, SCHEMA_VERSION, col, event_struct_type, link_struct_type,
    span_schema_v1,
};

/// Bytes of a trace id.
pub const TRACE_ID_BYTES: usize = 16;

/// The distinct trace ids of a span batch, sorted, or `None` when it has no
/// `trace_id` column of the declared type.
pub fn trace_ids(batch: &arrow::record_batch::RecordBatch) -> Option<Vec<[u8; TRACE_ID_BYTES]>> {
    use arrow::array::AsArray;
    let index = batch.schema().index_of(col::TRACE_ID).ok()?;
    let ids = batch.column(index).as_fixed_size_binary_opt()?;
    let mut set: Vec<[u8; TRACE_ID_BYTES]> = ids
        .iter()
        .flatten()
        .filter_map(|v| <[u8; TRACE_ID_BYTES]>::try_from(v).ok())
        .collect();
    set.sort_unstable();
    set.dedup();
    Some(set)
}
