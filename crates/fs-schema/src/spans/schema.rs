//! Arrow schema for spans, version 1.
//!
//! Logical names follow the Go backend (ADR 0007). Physical types are chosen
//! for Parquet. Nested types are declared with the exact field names and
//! nullability the arrow builders produce, so a batch from
//! [`super::SpanBatchBuilder`] always matches [`span_schema_v1`].

use std::sync::{Arc, OnceLock};

use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit};

/// Version of the span schema produced by this crate.
pub const SCHEMA_VERSION: u32 = 1;

/// Column names. Shared by the builder, the query layer and the Go dialect.
pub mod col {
    /// 30 minute partition bucket, seconds.
    pub const TS_BUCKET_START: &str = "ts_bucket_start";
    /// Span start.
    pub const TIMESTAMP: &str = "timestamp";
    /// 16 byte trace id.
    pub const TRACE_ID: &str = "trace_id";
    /// 8 byte span id.
    pub const SPAN_ID: &str = "span_id";
    /// 8 byte parent span id, null for root spans.
    pub const PARENT_SPAN_ID: &str = "parent_span_id";
    /// W3C tracestate.
    pub const TRACE_STATE: &str = "trace_state";
    /// OTLP span flags.
    pub const FLAGS: &str = "flags";
    /// Span name.
    pub const NAME: &str = "name";
    /// OTLP span kind enum.
    pub const KIND: &str = "kind";
    /// Span kind as text.
    pub const KIND_STRING: &str = "kind_string";
    /// Duration in nanoseconds.
    pub const DURATION_NANO: &str = "duration_nano";
    /// OTLP status code enum.
    pub const STATUS_CODE: &str = "status_code";
    /// Status code as text.
    pub const STATUS_CODE_STRING: &str = "status_code_string";
    /// Status message.
    pub const STATUS_MESSAGE: &str = "status_message";
    /// True when status is ERROR.
    pub const HAS_ERROR: &str = "has_error";
    /// Parent context is remote: "true", "false" or "" when unknown.
    pub const IS_REMOTE: &str = "is_remote";
    /// Hash of the resource attributes.
    pub const RESOURCE_FINGERPRINT: &str = "resource_fingerprint";
    /// Resource attributes, all as text.
    pub const RESOURCES_STRING: &str = "resources_string";
    /// String span attributes.
    pub const ATTRIBUTES_STRING: &str = "attributes_string";
    /// Numeric span attributes.
    pub const ATTRIBUTES_NUMBER: &str = "attributes_number";
    /// Boolean span attributes.
    pub const ATTRIBUTES_BOOL: &str = "attributes_bool";
    /// Instrumentation scope name.
    pub const SCOPE_NAME: &str = "scope_name";
    /// Instrumentation scope version.
    pub const SCOPE_VERSION: &str = "scope_version";
    /// Instrumentation scope attributes, all as text.
    pub const SCOPE_STRING: &str = "scope_string";
    /// Span events.
    pub const EVENTS: &str = "events";
    /// Span links.
    pub const LINKS: &str = "links";
}

/// Where a promoted attribute is read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromotedScope {
    /// Resource attributes.
    Resource,
    /// Span attributes.
    Attribute,
}

/// A native column fed from an attribute, for pruning and fast filters.
///
/// `keys` are tried in order; the first present wins. This is how current and
/// legacy semantic convention names map to one column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Promoted {
    /// Physical column name.
    pub column: &'static str,
    /// Attribute set the value comes from.
    pub scope: PromotedScope,
    /// Candidate attribute keys, highest priority first.
    pub keys: &'static [&'static str],
}

/// Attributes promoted from day one because the Go backend and the UI depend on them.
pub const PROMOTED: &[Promoted] = &[
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
    Promoted {
        column: "http_route",
        scope: PromotedScope::Attribute,
        keys: &["http.route"],
    },
    Promoted {
        column: "http_method",
        scope: PromotedScope::Attribute,
        keys: &["http.request.method", "http.method"],
    },
    Promoted {
        column: "response_status_code",
        scope: PromotedScope::Attribute,
        keys: &[
            "http.response.status_code",
            "http.status_code",
            "rpc.grpc.status_code",
        ],
    },
    Promoted {
        column: "db_system",
        scope: PromotedScope::Attribute,
        keys: &["db.system.name", "db.system"],
    },
    Promoted {
        column: "db_name",
        scope: PromotedScope::Attribute,
        keys: &["db.namespace", "db.name"],
    },
    Promoted {
        column: "db_operation",
        scope: PromotedScope::Attribute,
        keys: &["db.operation.name", "db.operation"],
    },
    Promoted {
        column: "rpc_system",
        scope: PromotedScope::Attribute,
        keys: &["rpc.system"],
    },
    Promoted {
        column: "rpc_service",
        scope: PromotedScope::Attribute,
        keys: &["rpc.service"],
    },
    Promoted {
        column: "rpc_method",
        scope: PromotedScope::Attribute,
        keys: &["rpc.method"],
    },
    Promoted {
        column: "messaging_system",
        scope: PromotedScope::Attribute,
        keys: &["messaging.system"],
    },
    Promoted {
        column: "messaging_operation",
        scope: PromotedScope::Attribute,
        keys: &[
            "messaging.operation.type",
            "messaging.operation.name",
            "messaging.operation",
        ],
    },
    Promoted {
        column: "peer_service",
        scope: PromotedScope::Attribute,
        keys: &["peer.service"],
    },
];

/// Dictionary encoded string, for low cardinality columns.
pub(crate) fn dict_utf8() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

/// Nanosecond UTC timestamp.
pub(crate) fn timestamp_ns() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
}

/// Map with non-null keys and non-null values of `value`, as the arrow
/// `MapBuilder` produces with its default field names.
pub(crate) fn map_type(value: DataType) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(map_entry_fields(value)),
            false,
        )),
        false,
    )
}

/// The `keys`/`values` pair inside a map entry.
pub(crate) fn map_entry_fields(value: DataType) -> Fields {
    Fields::from(vec![
        Field::new("keys", DataType::Utf8, false),
        Field::new("values", value, false),
    ])
}

/// Value field passed to `MapBuilder::with_values_field` so values are non-null.
pub(crate) fn map_values_field(value: DataType) -> FieldRef {
    Arc::new(Field::new("values", value, false))
}

/// Fields of one span event.
pub(crate) fn event_fields() -> Fields {
    Fields::from(vec![
        Field::new("timestamp", timestamp_ns(), false),
        Field::new("name", DataType::Utf8, false),
        Field::new("attributes", map_type(DataType::Utf8), false),
    ])
}

/// Struct type of one span event.
pub fn event_struct_type() -> DataType {
    DataType::Struct(event_fields())
}

/// Fields of one span link.
pub(crate) fn link_fields() -> Fields {
    Fields::from(vec![
        Field::new("trace_id", DataType::FixedSizeBinary(16), false),
        Field::new("span_id", DataType::FixedSizeBinary(8), false),
        Field::new("trace_state", DataType::Utf8, false),
        Field::new("attributes", map_type(DataType::Utf8), false),
    ])
}

/// Struct type of one span link.
pub fn link_struct_type() -> DataType {
    DataType::Struct(link_fields())
}

/// Non-null list of non-null `item`s, as `ListBuilder::with_field` produces.
pub(crate) fn list_type(item: DataType) -> DataType {
    DataType::List(list_item_field(item))
}

/// Item field for list builders.
pub(crate) fn list_item_field(item: DataType) -> FieldRef {
    Arc::new(Field::new("item", item, false))
}

/// The span schema, version 1. Built once and shared.
pub fn span_schema_v1() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA.get_or_init(|| Arc::new(build_schema())).clone()
}

fn build_schema() -> Schema {
    let mut fields = vec![
        Field::new(col::TS_BUCKET_START, DataType::UInt64, false),
        Field::new(col::TIMESTAMP, timestamp_ns(), false),
        Field::new(col::TRACE_ID, DataType::FixedSizeBinary(16), false),
        Field::new(col::SPAN_ID, DataType::FixedSizeBinary(8), false),
        Field::new(col::PARENT_SPAN_ID, DataType::FixedSizeBinary(8), true),
        Field::new(col::TRACE_STATE, DataType::Utf8, false),
        Field::new(col::FLAGS, DataType::UInt32, false),
        Field::new(col::NAME, dict_utf8(), false),
        Field::new(col::KIND, DataType::Int8, false),
        Field::new(col::KIND_STRING, dict_utf8(), false),
        Field::new(col::DURATION_NANO, DataType::UInt64, false),
        Field::new(col::STATUS_CODE, DataType::Int16, false),
        Field::new(col::STATUS_CODE_STRING, dict_utf8(), false),
        Field::new(col::STATUS_MESSAGE, DataType::Utf8, false),
        Field::new(col::HAS_ERROR, DataType::Boolean, false),
        Field::new(col::IS_REMOTE, dict_utf8(), false),
        Field::new(col::RESOURCE_FINGERPRINT, DataType::UInt64, false),
        Field::new(col::RESOURCES_STRING, map_type(DataType::Utf8), false),
        Field::new(col::ATTRIBUTES_STRING, map_type(DataType::Utf8), false),
        Field::new(col::ATTRIBUTES_NUMBER, map_type(DataType::Float64), false),
        Field::new(col::ATTRIBUTES_BOOL, map_type(DataType::Boolean), false),
        Field::new(col::SCOPE_NAME, DataType::Utf8, false),
        Field::new(col::SCOPE_VERSION, DataType::Utf8, false),
        Field::new(col::SCOPE_STRING, map_type(DataType::Utf8), false),
        Field::new(col::EVENTS, list_type(event_struct_type()), false),
        Field::new(col::LINKS, list_type(link_struct_type()), false),
    ];
    fields.extend(
        PROMOTED
            .iter()
            .map(|p| Field::new(p.column, dict_utf8(), true)),
    );
    let metadata = [
        ("fusescope.signal".to_owned(), "spans".to_owned()),
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
    fn schema_has_fixed_columns_then_promoted() {
        let schema = span_schema_v1();
        assert_eq!(schema.fields().len(), 26 + PROMOTED.len());
        assert_eq!(schema.field(0).name(), col::TS_BUCKET_START);
        assert_eq!(schema.field(26).name(), "service_name");
        assert_eq!(schema.metadata()["fusescope.schema_version"], "1");
    }

    #[test]
    fn promoted_columns_are_unique_and_snake_case() {
        let mut seen = std::collections::HashSet::new();
        for p in PROMOTED {
            assert!(
                seen.insert(p.column),
                "duplicate promoted column {}",
                p.column
            );
            assert!(
                p.column.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "{}",
                p.column
            );
            assert_ne!(p.keys.len(), 0, "{} has no source keys", p.column);
        }
    }

    #[test]
    fn schema_is_shared() {
        assert!(Arc::ptr_eq(&span_schema_v1(), &span_schema_v1()));
    }
}
