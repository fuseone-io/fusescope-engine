//! Series rows read back from a batch of a series file, version 1 or 2.

use arrow::array::{Array, AsArray, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, UInt16Type};
use arrow::record_batch::RecordBatch;

use super::label_meta::LabelMeta;
use super::rows::SeriesRow;
use super::schema::col;
use super::{MetricType, Temporality};
use crate::SchemaError;

const OTHER: SchemaError = SchemaError::Layout("metrics: not a batch of series this engine reads");

pub(crate) fn series_rows(batch: &RecordBatch) -> Result<Vec<SeriesRow>, SchemaError> {
    let schema = batch.schema();
    let metadata = schema.metadata();
    if metadata.get("fusescope.signal").map(String::as_str) != Some("metric_series") {
        return Err(OTHER);
    }
    let version = match metadata.get("fusescope.schema_version").map(String::as_str) {
        Some("1") => 1,
        Some("2") => 2,
        _ => return Err(OTHER),
    };
    let text = |name: &str| -> Result<StringArray, SchemaError> {
        let column = batch.column(schema.index_of(name)?);
        Ok(cast(column, &DataType::Utf8)?.as_string::<i32>().clone())
    };
    let names = text(col::METRIC_NAME)?;
    let temporality = text(col::TEMPORALITY)?;
    let kinds = text(col::TYPE)?;
    let (descriptions, units) = (text(col::DESCRIPTION)?, text(col::UNIT)?);
    let monotonic = batch
        .column(schema.index_of(col::IS_MONOTONIC)?)
        .as_boolean_opt()
        .ok_or(OTHER)?
        .clone();
    let fingerprint = batch
        .column(schema.index_of(col::FINGERPRINT)?)
        .as_primitive_opt::<arrow::datatypes::UInt64Type>()
        .ok_or(OTHER)?
        .clone();
    let hour = batch
        .column(schema.index_of(col::UNIX_MILLI)?)
        .as_primitive_opt::<arrow::datatypes::Int64Type>()
        .ok_or(OTHER)?
        .clone();
    let labels = batch
        .column(schema.index_of(col::LABELS)?)
        .as_map_opt()
        .ok_or(OTHER)?
        .clone();
    let keys = cast(labels.keys(), &DataType::Utf8)?
        .as_string::<i32>()
        .clone();
    let values = cast(labels.values(), &DataType::Utf8)?
        .as_string::<i32>()
        .clone();
    let origins = if version == 2 {
        Some(
            batch
                .column(schema.index_of(col::LABEL_METADATA)?)
                .as_list_opt::<i32>()
                .ok_or(OTHER)?
                .clone(),
        )
    } else {
        None
    };
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let offsets = labels.value_offsets();
        let bound = |i: usize| {
            offsets
                .get(i)
                .and_then(|o| usize::try_from(*o).ok())
                .ok_or(OTHER)
        };
        let (start, end) = (bound(row)?, bound(row + 1)?);
        let pairs: Box<[(Box<str>, Box<str>)]> = (start..end)
            .map(|i| (keys.value(i).into(), values.value(i).into()))
            .collect();
        let label_meta: Box<[LabelMeta]> = match &origins {
            None => vec![LabelMeta::LEGACY; pairs.len()].into(),
            Some(list) => {
                let item = list.value(row);
                let bits = item.as_primitive_opt::<UInt16Type>().ok_or(OTHER)?;
                if bits.len() != pairs.len() || bits.null_count() > 0 {
                    return Err(SchemaError::Layout(
                        "metrics: a series whose origins do not match its labels",
                    ));
                }
                bits.values()
                    .iter()
                    .map(|b| LabelMeta::decode(*b))
                    .collect::<Result<_, _>>()?
            }
        };
        rows.push(SeriesRow {
            fingerprint: fingerprint.value(row),
            unix_milli: hour.value(row),
            name: names.value(row).into(),
            kind: MetricType::parse(kinds.value(row)).ok_or(OTHER)?,
            temporality: Temporality::parse(temporality.value(row)).ok_or(OTHER)?,
            monotonic: monotonic.value(row),
            unit: units.value(row).into(),
            description: descriptions.value(row).into(),
            labels: pairs,
            label_meta,
        });
    }
    Ok(rows)
}
