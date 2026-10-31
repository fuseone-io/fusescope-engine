//! Arrow schema for logs, version 1.
//!
//! Logical names follow the Go backend (ADR 0007).

use std::sync::{Arc, OnceLock};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::columns::{dict_utf8, map_type, timestamp_ns};
use crate::spans::{Promoted, PromotedScope};

/// Version of the log schema produced by this crate.
pub const SCHEMA_VERSION: u32 = 1;

/// Column names. Shared by the builder, the query layer and the Go dialect.
pub mod col {
    pub use crate::spans::col::{
        ATTRIBUTES_BOOL, ATTRIBUTES_NUMBER, ATTRIBUTES_STRING, RESOURCE_FINGERPRINT,
        RESOURCES_STRING, SCOPE_NAME, SCOPE_STRING, SCOPE_VERSION, SERVICE_NAME, SPAN_ID,
        TIMESTAMP, TRACE_ID, TS_BUCKET_START,
    };
    /// When the record was observed by the collection system.
    pub const OBSERVED_TIMESTAMP: &str = "observed_timestamp";
    /// 16 byte ULID.
    pub const ID: &str = "id";
    /// W3C trace flags.
    pub const TRACE_FLAGS: &str = "trace_flags";
    /// Severity as given by the source.
    pub const SEVERITY_TEXT: &str = "severity_text";
    /// OTLP severity number.
    pub const SEVERITY_NUMBER: &str = "severity_number";
    /// Body rendered as text.
    pub const BODY: &str = "body";
}

/// Resource attributes promoted to native columns, with the span rules.
pub const LOG_PROMOTED: &[Promoted] = &[
    Promoted {
        column: "service_name",
        scope: PromotedScope::Resource,
        keys: &["service.name"],
    },
    Promoted {
        column: "deployment_environment",
        scope: PromotedScope::Resource,
        keys: &["deployment.environment.name", "deployment.environment"],
    },
];

/// The log schema, version 1. Built once and shared.
pub fn log_schema_v1() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA.get_or_init(|| Arc::new(build_schema())).clone()
}

fn build_schema() -> Schema {
    let mut fields = vec![
        Field::new(col::TS_BUCKET_START, DataType::UInt64, false),
        Field::new(col::TIMESTAMP, timestamp_ns(), false),
        Field::new(col::OBSERVED_TIMESTAMP, timestamp_ns(), false),
        Field::new(col::ID, DataType::FixedSizeBinary(16), false),
        Field::new(col::TRACE_ID, DataType::FixedSizeBinary(16), true),
        Field::new(col::SPAN_ID, DataType::FixedSizeBinary(8), true),
        Field::new(col::TRACE_FLAGS, DataType::UInt32, false),
        Field::new(col::SEVERITY_TEXT, dict_utf8(), false),
        Field::new(col::SEVERITY_NUMBER, DataType::UInt8, false),
        Field::new(col::BODY, DataType::Utf8, false),
        Field::new(col::RESOURCE_FINGERPRINT, DataType::UInt64, false),
        Field::new(col::RESOURCES_STRING, map_type(DataType::Utf8), false),
        Field::new(col::ATTRIBUTES_STRING, map_type(DataType::Utf8), false),
        Field::new(col::ATTRIBUTES_NUMBER, map_type(DataType::Float64), false),
        Field::new(col::ATTRIBUTES_BOOL, map_type(DataType::Boolean), false),
        Field::new(col::SCOPE_NAME, DataType::Utf8, false),
        Field::new(col::SCOPE_VERSION, DataType::Utf8, false),
        Field::new(col::SCOPE_STRING, map_type(DataType::Utf8), false),
    ];
    fields.extend(
        LOG_PROMOTED
            .iter()
            .map(|p| Field::new(p.column, dict_utf8(), true)),
    );
    let metadata = [
        ("fusescope.signal".to_owned(), "logs".to_owned()),
        (
            "fusescope.schema_version".to_owned(),
            SCHEMA_VERSION.to_string(),
        ),
    ];
    Schema::new_with_metadata(fields, metadata.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn promoted_columns_are_resource_only_and_shared_with_spans() {
        for p in LOG_PROMOTED {
            assert_eq!(p.scope, PromotedScope::Resource, "{}", p.column);
            assert!(crate::spans::PROMOTED.contains(p), "{}", p.column);
        }
    }
}
