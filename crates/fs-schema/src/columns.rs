//! Arrow column types and builders shared by every signal's schema.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, AsArray, BooleanBuilder, Float64Builder, MapBuilder, StringBuilder,
    StringDictionaryBuilder, TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Int32Type, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::SchemaError;

pub(crate) type StringMapBuilder = MapBuilder<StringBuilder, StringBuilder>;
pub(crate) type NumberMapBuilder = MapBuilder<StringBuilder, Float64Builder>;
pub(crate) type BoolMapBuilder = MapBuilder<StringBuilder, BooleanBuilder>;
pub(crate) type DictBuilder = StringDictionaryBuilder<Int32Type>;

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

/// Non-null list of non-null `item`s, as `ListBuilder::with_field` produces.
pub(crate) fn list_type(item: DataType) -> DataType {
    DataType::List(list_item_field(item))
}

/// Item field for list builders.
pub(crate) fn list_item_field(item: DataType) -> FieldRef {
    Arc::new(Field::new("item", item, false))
}

pub(crate) fn string_map() -> StringMapBuilder {
    MapBuilder::new(None, StringBuilder::new(), StringBuilder::new())
        .with_values_field(map_values_field(DataType::Utf8))
}

pub(crate) fn number_map() -> NumberMapBuilder {
    MapBuilder::new(None, StringBuilder::new(), Float64Builder::new())
        .with_values_field(map_values_field(DataType::Float64))
}

pub(crate) fn bool_map() -> BoolMapBuilder {
    MapBuilder::new(None, StringBuilder::new(), BooleanBuilder::new())
        .with_values_field(map_values_field(DataType::Boolean))
}

pub(crate) fn timestamp() -> TimestampNanosecondBuilder {
    TimestampNanosecondBuilder::new().with_timezone("UTC")
}

/// Copies a batch into exactly sized buffers.
///
/// Builders over-allocate, and a batch that sits in a buffer for up to a flush
/// interval would otherwise account for tens of kilobytes per span, tripping
/// byte thresholds early. `concat` with a single input returns it unchanged, so
/// an empty slice is added to force the copy. Dictionary columns are rebuilt
/// through their value type, because concatenating dictionaries allocates a
/// values buffer with builder-sized capacity.
pub(crate) fn compact(batch: &RecordBatch) -> Result<RecordBatch, SchemaError> {
    let columns = batch
        .columns()
        .iter()
        .map(|column| match column.data_type() {
            DataType::Dictionary(_, _) => {
                let dict = column.as_dictionary::<Int32Type>();
                let keys = exact_copy(dict.keys())?;
                let values = exact_copy(dict.values().as_ref())?;
                let keys = keys.as_primitive::<Int32Type>().clone();
                let rebuilt = arrow::array::DictionaryArray::<Int32Type>::try_new(keys, values)?;
                Ok(Arc::new(rebuilt) as ArrayRef)
            }
            _ => exact_copy(column.as_ref()),
        })
        .collect::<Result<Vec<ArrayRef>, arrow::error::ArrowError>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

/// `concat` with a single input returns it unchanged; an empty slice forces a
/// copy into buffers sized to the data.
fn exact_copy(array: &dyn arrow::array::Array) -> Result<ArrayRef, arrow::error::ArrowError> {
    arrow::compute::concat(&[&array.slice(0, 0), array])
}

pub(crate) fn append_string_map(
    map: &mut StringMapBuilder,
    entries: &[(String, String)],
) -> Result<(), SchemaError> {
    for (k, v) in entries {
        map.keys().append_value(k);
        map.values().append_value(v);
    }
    map.append(true)?;
    Ok(())
}

pub(crate) fn append_number_map(
    map: &mut NumberMapBuilder,
    entries: &[(String, f64)],
) -> Result<(), SchemaError> {
    for (k, v) in entries {
        map.keys().append_value(k);
        map.values().append_value(*v);
    }
    map.append(true)?;
    Ok(())
}

pub(crate) fn append_bool_map(
    map: &mut BoolMapBuilder,
    entries: &[(String, bool)],
) -> Result<(), SchemaError> {
    for (k, v) in entries {
        map.keys().append_value(k);
        map.values().append_value(*v);
    }
    map.append(true)?;
    Ok(())
}
