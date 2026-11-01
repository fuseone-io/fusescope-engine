//! The bytes each row of a batch decodes to, so a row group gathered from
//! picked rows is sized exactly before it is allocated.

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::CompactError;

/// Bytes of each row of `batch`: its values, offsets and validity, nested
/// values included, as a copy of the row holds them.
///
/// # Errors
/// A column type the merge does not read.
pub(crate) fn row_sizes(batch: &RecordBatch) -> Result<Vec<usize>, CompactError> {
    let mut sizes = vec![0usize; batch.num_rows()];
    for column in batch.columns() {
        for (size, value) in sizes.iter_mut().zip(element_sizes(column)?) {
            *size = size.saturating_add(value);
        }
    }
    Ok(sizes)
}

/// Bytes of each element of `array`.
fn element_sizes(array: &ArrayRef) -> Result<Vec<usize>, CompactError> {
    let len = array.len();
    // A validity bit, rounded up to a byte.
    let base = usize::from(array.nulls().is_some());
    let sizes = match array.data_type() {
        DataType::Null => vec![0; len],
        DataType::Boolean => vec![1 + base; len],
        DataType::Utf8 => {
            let a = array.as_string::<i32>();
            (0..len)
                .map(|i| a.value_length(i).unsigned_abs() as usize + 4 + base)
                .collect()
        }
        DataType::LargeUtf8 => {
            let a = array.as_string::<i64>();
            (0..len)
                .map(|i| usize::try_from(a.value_length(i)).unwrap_or(0) + 8 + base)
                .collect()
        }
        DataType::Binary => {
            let a = array.as_binary::<i32>();
            (0..len)
                .map(|i| a.value_length(i).unsigned_abs() as usize + 4 + base)
                .collect()
        }
        DataType::FixedSizeBinary(width) => {
            vec![usize::try_from(*width).unwrap_or(0) + base; len]
        }
        DataType::List(_) => {
            let a = array.as_list::<i32>();
            nested(&element_sizes(a.values())?, a.value_offsets(), 4 + base)
        }
        DataType::LargeList(_) => {
            let a = array.as_list::<i64>();
            nested(&element_sizes(a.values())?, a.value_offsets(), 8 + base)
        }
        DataType::Map(_, _) => {
            let a = array.as_map();
            let entries: ArrayRef = std::sync::Arc::new(a.entries().clone());
            nested(&element_sizes(&entries)?, a.value_offsets(), 4 + base)
        }
        DataType::Struct(_) => {
            let a = array.as_struct();
            let mut sizes = vec![base; len];
            for child in a.columns() {
                for (size, value) in sizes.iter_mut().zip(element_sizes(child)?) {
                    *size = size.saturating_add(value);
                }
            }
            sizes
        }
        DataType::Dictionary(key, _) => {
            let a = array.as_any_dictionary();
            let values = element_sizes(a.values())?;
            let width = key.primitive_width().unwrap_or(8) + base;
            let keys = arrow::compute::cast(a.keys(), &DataType::UInt64)?;
            keys.as_primitive::<arrow::datatypes::UInt64Type>()
                .iter()
                .map(|k| {
                    let value = k
                        .and_then(|k| usize::try_from(k).ok())
                        .and_then(|k| values.get(k));
                    width + value.copied().unwrap_or(0)
                })
                .collect()
        }
        other => match other.primitive_width() {
            Some(width) => vec![width + base; len],
            None => return Err(CompactError::Merge("a column type the merge cannot size")),
        },
    };
    Ok(sizes)
}

/// Each row's `overhead` and the sizes of its children, by `offsets`.
fn nested<O: arrow::array::OffsetSizeTrait>(
    children: &[usize],
    offsets: &[O],
    overhead: usize,
) -> Vec<usize> {
    offsets
        .windows(2)
        .map(|w| match w {
            [start, end] => children
                .get(start.as_usize()..end.as_usize())
                .unwrap_or_default()
                .iter()
                .fold(overhead, |sum, size| sum.saturating_add(*size)),
            _ => overhead,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ListArray, StringArray, UInt64Array};
    use arrow::datatypes::{Field, Schema, UInt64Type};

    use super::*;

    #[test]
    fn a_row_is_sized_by_its_own_values_not_the_batch_average() {
        let text = StringArray::from(vec![Some("a"), None, Some(&*"x".repeat(1000))]);
        let list = ListArray::from_iter_primitive::<UInt64Type, _, _>(vec![
            Some(vec![Some(1)]),
            Some(vec![]),
            Some(vec![Some(1), Some(2), Some(3)]),
        ]);
        let schema = Schema::new(vec![
            Field::new("n", DataType::UInt64, false),
            Field::new("text", DataType::Utf8, true),
            Field::new("list", list.data_type().clone(), true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt64Array::from(vec![1, 2, 3])),
                Arc::new(text),
                Arc::new(list),
            ],
        )
        .unwrap();
        let sizes = row_sizes(&batch).unwrap();
        // u64, then the string with its offset and validity, then the list
        // offset and its u64 values.
        assert_eq!(sizes, vec![8 + 6 + 4 + 8, 8 + 5 + 4, 8 + 1005 + 4 + 24]);
        let sliced = row_sizes(&batch.slice(2, 1)).unwrap();
        assert_eq!(sliced, vec![8 + 1005 + 4 + 24]);
    }
}
