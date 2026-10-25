//! Span schema and the OTLP spans to Arrow converter.

mod builder;
mod schema;

pub use builder::SpanBatchBuilder;
pub use schema::{
    PROMOTED, Promoted, PromotedScope, SCHEMA_VERSION, col, event_struct_type, link_struct_type,
    span_schema_v1,
};
