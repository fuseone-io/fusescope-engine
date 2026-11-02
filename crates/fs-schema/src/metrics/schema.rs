//! Arrow schemas of metric samples, version 1, and series, versions 1
//! and 2.
//!
//! Logical names follow the Go backend (ADR 0007); `_source_order` is the
//! engine's own and no table shows it.

use std::sync::{Arc, OnceLock};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use super::SOURCE_ORDER_WIDTH;
use crate::columns::{dict_utf8, list_type, map_type};

/// Version of the samples schema produced by this crate.
pub const SAMPLES_SCHEMA_VERSION: u32 = 1;

/// Version of the series schema produced by this crate.
pub const SERIES_SCHEMA_VERSION: u32 = 2;

/// Column names.
pub mod col {
    /// The series' name.
    pub const METRIC_NAME: &str = "metric_name";
    /// `Delta`, `Cumulative` or `Unspecified`.
    pub const TEMPORALITY: &str = "temporality";
    /// The series' fingerprint.
    pub const FINGERPRINT: &str = "fingerprint";
    /// A sample's time, or a series row's hour, in Unix milliseconds.
    pub const UNIX_MILLI: &str = "unix_milli";
    /// A sample's value.
    pub const VALUE: &str = "value";
    /// OTLP data point flags; bit 0 marks no recorded value.
    pub const FLAGS: &str = "flags";
    /// Where a sample came from, the larger winning a duplicate.
    pub const SOURCE_ORDER: &str = "_source_order";
    /// `Gauge`, `Sum`, `Histogram` or `Summary`.
    pub const TYPE: &str = "type";
    /// Whether a series only grows.
    pub const IS_MONOTONIC: &str = "is_monotonic";
    /// A series' labels.
    pub const LABELS: &str = "labels";
    /// A series' description.
    pub const DESCRIPTION: &str = "description";
    /// A series' unit.
    pub const UNIT: &str = "unit";
    /// The origin of each label of a series, version 2.
    pub const LABEL_METADATA: &str = "label_metadata";
}

/// The samples schema, version 1, sorted by name, fingerprint, time and
/// source order.
pub fn samples_schema_v1() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let fields = vec![
                Field::new(col::METRIC_NAME, dict_utf8(), false),
                Field::new(col::TEMPORALITY, dict_utf8(), false),
                Field::new(col::FINGERPRINT, DataType::UInt64, false),
                Field::new(col::UNIX_MILLI, DataType::Int64, false),
                Field::new(col::VALUE, DataType::Float64, false),
                Field::new(col::FLAGS, DataType::UInt32, false),
                Field::new(
                    col::SOURCE_ORDER,
                    DataType::FixedSizeBinary(SOURCE_ORDER_WIDTH),
                    false,
                ),
            ];
            Arc::new(with_metadata(
                fields,
                "metric_samples",
                SAMPLES_SCHEMA_VERSION,
            ))
        })
        .clone()
}

/// The series schema, version 1: one row a series and hour, sorted as
/// [`super::SeriesRow::order`] keeps them.
pub fn series_schema_v1() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| Arc::new(with_metadata(series_fields(), "metric_series", 1)))
        .clone()
}

/// The series schema, version 2: version 1 and `label_metadata`, one
/// [`super::LabelMeta`] a label, in the order of `labels`.
pub fn series_schema_v2() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let mut fields = series_fields();
            fields.push(Field::new(
                col::LABEL_METADATA,
                list_type(DataType::UInt16),
                false,
            ));
            Arc::new(with_metadata(fields, "metric_series", 2))
        })
        .clone()
}

fn series_fields() -> Vec<Field> {
    vec![
        Field::new(col::METRIC_NAME, dict_utf8(), false),
        Field::new(col::TEMPORALITY, dict_utf8(), false),
        Field::new(col::TYPE, dict_utf8(), false),
        Field::new(col::IS_MONOTONIC, DataType::Boolean, false),
        Field::new(col::FINGERPRINT, DataType::UInt64, false),
        Field::new(col::UNIX_MILLI, DataType::Int64, false),
        Field::new(col::LABELS, map_type(DataType::Utf8), false),
        Field::new(col::DESCRIPTION, DataType::Utf8, false),
        Field::new(col::UNIT, DataType::Utf8, false),
    ]
}

fn with_metadata(fields: Vec<Field>, signal: &str, version: u32) -> Schema {
    let metadata = [
        ("fusescope.signal".to_owned(), signal.to_owned()),
        ("fusescope.schema_version".to_owned(), version.to_string()),
    ];
    Schema::new_with_metadata(fields, metadata.into_iter().collect())
}
