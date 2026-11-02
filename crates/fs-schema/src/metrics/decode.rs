//! Series rows read back from a batch of a series file, version 1 or 2.

use arrow::array::{Array, ArrayRef, AsArray, DictionaryArray, StringArray};
use arrow::datatypes::{Int32Type, UInt16Type};
use arrow::record_batch::RecordBatch;

use super::label_meta::LabelMeta;
use super::rows::SeriesRow;
use super::schema::col;
use super::{MetricType, Temporality};
use crate::SchemaError;

const OTHER: SchemaError = SchemaError::Layout("metrics: not a batch of series this engine reads");

/// A column of text read in place, a dictionary or plain, without copying
/// its values.
enum Text<'a> {
    Dictionary(&'a DictionaryArray<Int32Type>, &'a StringArray),
    Plain(&'a StringArray),
}

impl<'a> Text<'a> {
    fn of(column: &'a ArrayRef) -> Result<Self, SchemaError> {
        if let Some(plain) = column.as_string_opt::<i32>() {
            return Ok(Self::Plain(plain));
        }
        let dictionary = column.as_dictionary_opt::<Int32Type>().ok_or(OTHER)?;
        let values = dictionary.values().as_string_opt::<i32>().ok_or(OTHER)?;
        Ok(Self::Dictionary(dictionary, values))
    }

    fn value(&self, row: usize) -> Result<&'a str, SchemaError> {
        match self {
            Self::Plain(plain) => Ok(plain.value(row)),
            Self::Dictionary(dictionary, values) => {
                let key = usize::try_from(dictionary.keys().value(row)).map_err(|_| OTHER)?;
                if key >= values.len() {
                    return Err(OTHER);
                }
                Ok(values.value(key))
            }
        }
    }
}

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
    let column =
        |name: &str| -> Result<&ArrayRef, SchemaError> { Ok(batch.column(schema.index_of(name)?)) };
    let names = Text::of(column(col::METRIC_NAME)?)?;
    let temporality = Text::of(column(col::TEMPORALITY)?)?;
    let kinds = Text::of(column(col::TYPE)?)?;
    let (descriptions, units) = (
        Text::of(column(col::DESCRIPTION)?)?,
        Text::of(column(col::UNIT)?)?,
    );
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
        .ok_or(OTHER)?;
    let (keys, values) = (Text::of(labels.keys())?, Text::of(labels.values())?);
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
            .map(|i| Ok((keys.value(i)?.into(), values.value(i)?.into())))
            .collect::<Result<_, SchemaError>>()?;
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
            name: names.value(row)?.into(),
            kind: MetricType::parse(kinds.value(row)?).ok_or(OTHER)?,
            temporality: Temporality::parse(temporality.value(row)?).ok_or(OTHER)?,
            monotonic: monotonic.value(row),
            unit: units.value(row)?.into(),
            description: descriptions.value(row)?.into(),
            labels: pairs,
            label_meta,
        });
    }
    Ok(rows)
}

/// What [`SeriesRow::bytes`] of each row of `batch`, of the series schema
/// version 1 or 2, comes to, read off its arrays in place: nothing is
/// allocated but the sizes.
pub(crate) fn series_row_sizes(batch: &RecordBatch) -> Result<Vec<usize>, SchemaError> {
    let schema = batch.schema();
    let column =
        |name: &str| -> Result<&ArrayRef, SchemaError> { Ok(batch.column(schema.index_of(name)?)) };
    let names = Text::of(column(col::METRIC_NAME)?)?;
    let (units, descriptions) = (
        Text::of(column(col::UNIT)?)?,
        Text::of(column(col::DESCRIPTION)?)?,
    );
    let labels = column(col::LABELS)?.as_map_opt().ok_or(OTHER)?;
    let (keys, values) = (Text::of(labels.keys())?, Text::of(labels.values())?);
    let offsets = labels.value_offsets();
    let label = std::mem::size_of::<(Box<str>, Box<str>)>() + std::mem::size_of::<LabelMeta>();
    let mut sizes = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let bound = |i: usize| {
            offsets
                .get(i)
                .and_then(|o| usize::try_from(*o).ok())
                .ok_or(OTHER)
        };
        let (start, end) = (bound(row)?, bound(row + 1)?);
        let mut texts = 0;
        for i in start..end {
            texts += keys.value(i)?.len() + values.value(i)?.len();
        }
        sizes.push(
            std::mem::size_of::<SeriesRow>()
                + names.value(row)?.len()
                + units.value(row)?.len()
                + descriptions.value(row)?.len()
                + (end - start) * label
                + texts,
        );
    }
    Ok(sizes)
}
