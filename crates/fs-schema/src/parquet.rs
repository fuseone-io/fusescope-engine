//! The engine's Parquet encoding of span batches: the sort every file
//! keeps, the writer settings and the row group bounds, shared by the
//! ingester's flush and the compactor so files from both are the same
//! bytes for the same rows.

use std::sync::Arc;

use arrow::array::{Array, UInt32Array};
use arrow::compute::{SortColumn, interleave, lexsort_to_indices, take_record_batch};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{
    EnabledStatistics, WriterProperties, WriterPropertiesBuilder, WriterVersion,
};
use parquet::schema::types::ColumnPath;

use crate::SchemaError;
use crate::spans::col;

/// Rows per row group when the caller gives no size. Statistics still prune
/// well at this size and a flush of a few hundred thousand rows makes a few
/// groups, so the writer never holds the whole file.
pub const DEFAULT_ROW_GROUP_ROWS: usize = 262_144;

/// Most rows one row group may hold, from configuration or from an intent.
/// The size feeds the bloom filters of three columns, each sized by it and
/// held outside the flush lease, so it is bounded here and nowhere else.
pub const MAX_ROW_GROUP_ROWS: usize = 1_048_576;

/// zstd level at L0 and L1: fast; the compactor recompresses at L2.
pub const ZSTD_LEVEL: i32 = 3;

/// False positive rate of every bloom filter.
const BLOOM_FPP: f64 = 0.01;

/// Refuses a row group size outside `1..=MAX_ROW_GROUP_ROWS`.
///
/// # Errors
/// Zero, or more than [`MAX_ROW_GROUP_ROWS`].
pub fn check_row_group_rows(rows: usize) -> Result<(), SchemaError> {
    if (1..=MAX_ROW_GROUP_ROWS).contains(&rows) {
        Ok(())
    } else {
        Err(SchemaError::RowGroupRows(rows))
    }
}

/// Columns of a span file that carry a bloom filter.
pub const SPAN_BLOOM_COLUMNS: [&str; 3] = [col::TRACE_ID, col::SPAN_ID, col::SERVICE_NAME];

/// Columns of a log file that carry a bloom filter. Severity has a handful
/// of values, which the dictionary and the page statistics already prune.
pub const LOG_BLOOM_COLUMNS: [&str; 2] = [col::TRACE_ID, col::SERVICE_NAME];

/// The engine's writer settings for span row groups of `row_group_rows`.
/// Row groups end on an explicit flush, never inside `write`: the writer's
/// own limit sits one row above any group handed to it, so its memory can
/// be read while it still holds the whole group. Only
/// [`SPAN_BLOOM_COLUMNS`] carry a bloom filter, sized to the group.
///
/// # Errors
/// A zstd level the library refuses.
pub fn writer_properties(row_group_rows: usize) -> Result<WriterProperties, SchemaError> {
    Ok(with_blooms(
        base_properties(row_group_rows)?,
        &SPAN_BLOOM_COLUMNS,
        row_group_rows,
    )
    .build())
}

/// The span writer settings of file layout 3, kept so a retried flush of an
/// intent of that layout reproduces its bytes: the global bloom settings
/// put a filter sized to the group on every column, while the three
/// columns enabled one by one get the library's default rate, sized to the
/// writer's row limit.
///
/// # Errors
/// A zstd level the library refuses.
pub fn writer_properties_layout_v3(row_group_rows: usize) -> Result<WriterProperties, SchemaError> {
    Ok(base_properties(row_group_rows)?
        .set_bloom_filter_max_ndv(u64::try_from(row_group_rows).unwrap_or(u64::MAX))
        .set_column_bloom_filter_enabled(ColumnPath::from(col::TRACE_ID), true)
        .set_column_bloom_filter_enabled(ColumnPath::from(col::SPAN_ID), true)
        .set_column_bloom_filter_enabled(ColumnPath::from(col::SERVICE_NAME), true)
        .set_bloom_filter_fpp(BLOOM_FPP)
        .build())
}

/// [`writer_properties`] for log batches: only [`LOG_BLOOM_COLUMNS`] carry
/// a bloom filter.
///
/// # Errors
/// A zstd level the library refuses.
pub fn log_writer_properties(row_group_rows: usize) -> Result<WriterProperties, SchemaError> {
    Ok(with_blooms(
        base_properties(row_group_rows)?,
        &LOG_BLOOM_COLUMNS,
        row_group_rows,
    )
    .build())
}

fn with_blooms(
    mut builder: WriterPropertiesBuilder,
    columns: &[&str],
    row_group_rows: usize,
) -> WriterPropertiesBuilder {
    let ndv = u64::try_from(row_group_rows).unwrap_or(u64::MAX);
    for column in columns {
        builder = builder
            .set_column_bloom_filter_enabled(ColumnPath::from(*column), true)
            .set_column_bloom_filter_max_ndv(ColumnPath::from(*column), ndv)
            .set_column_bloom_filter_fpp(ColumnPath::from(*column), BLOOM_FPP);
    }
    builder
}

fn base_properties(row_group_rows: usize) -> Result<WriterPropertiesBuilder, SchemaError> {
    Ok(WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(ZSTD_LEVEL)?))
        .set_max_row_group_row_count(Some(row_group_rows.saturating_add(1)))
        .set_statistics_enabled(EnabledStatistics::Page))
}

/// Sorts one batch by `(service_name, timestamp, row)`: a total order, so
/// the result does not depend on the sort algorithm.
///
/// # Errors
/// A missing column, or a batch of more than `u32::MAX` rows.
pub fn sort_batch(batch: &RecordBatch) -> Result<RecordBatch, SchemaError> {
    let schema = batch.schema();
    let service = batch.column(schema.index_of(col::SERVICE_NAME)?);
    let timestamp = batch.column(schema.index_of(col::TIMESTAMP)?);
    let rows = u32::try_from(batch.num_rows()).map_err(|_| SchemaError::Layout("batch rows"))?;
    let position: Arc<dyn Array> = Arc::new(UInt32Array::from_iter_values(0..rows));
    let indices = lexsort_to_indices(
        &[
            SortColumn {
                values: Arc::clone(service),
                options: None,
            },
            SortColumn {
                values: Arc::clone(timestamp),
                options: None,
            },
            SortColumn {
                values: position,
                options: None,
            },
        ],
        None,
    )?;
    Ok(take_record_batch(batch, &indices)?)
}

/// One batch of the rows `indices` name as `(input, row)` across `inputs`,
/// in that order.
///
/// # Errors
/// Inputs whose columns differ from `schema`.
pub fn gather(
    schema: &SchemaRef,
    inputs: &[RecordBatch],
    indices: &[(usize, usize)],
) -> Result<RecordBatch, SchemaError> {
    let columns = (0..schema.fields().len())
        .map(|c| {
            let sources: Vec<&dyn Array> = inputs.iter().map(|b| b.column(c).as_ref()).collect();
            interleave(&sources, indices)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_group_bounds() {
        assert!(check_row_group_rows(1).is_ok());
        assert!(check_row_group_rows(MAX_ROW_GROUP_ROWS).is_ok());
        assert!(check_row_group_rows(0).is_err());
        assert!(check_row_group_rows(MAX_ROW_GROUP_ROWS + 1).is_err());
    }

    fn assert_blooms_only_on(props: &WriterProperties, columns: &[&str]) {
        for column in columns {
            let bloom = props
                .bloom_filter_properties(&ColumnPath::from(*column))
                .unwrap();
            assert_eq!((bloom.ndv(), bloom.fpp()), (1000, BLOOM_FPP), "{column}");
        }
        for column in [
            col::SPAN_ID,
            "body",
            "severity_text",
            col::TIMESTAMP,
            "name",
        ] {
            if !columns.contains(&column) {
                assert!(
                    props
                        .bloom_filter_properties(&ColumnPath::from(column))
                        .is_none(),
                    "{column}"
                );
            }
        }
    }

    #[test]
    fn blooms_are_sized_to_the_group_on_their_columns_alone() {
        assert_blooms_only_on(&writer_properties(1000).unwrap(), &SPAN_BLOOM_COLUMNS);
        assert_blooms_only_on(&log_writer_properties(1000).unwrap(), &LOG_BLOOM_COLUMNS);
    }

    #[test]
    fn layout_v3_keeps_a_bloom_on_every_column() {
        let props = writer_properties_layout_v3(1000).unwrap();
        let other = props
            .bloom_filter_properties(&ColumnPath::from(col::TIMESTAMP))
            .unwrap();
        assert_eq!((other.ndv(), other.fpp()), (1000, BLOOM_FPP));
        let default = parquet::file::properties::BloomFilterProperties::default();
        let trace = props
            .bloom_filter_properties(&ColumnPath::from(col::TRACE_ID))
            .unwrap();
        assert_eq!((trace.ndv(), trace.fpp()), (1001, default.fpp()));
    }

    #[test]
    fn writer_limit_sits_one_row_above_the_group() {
        let props = writer_properties(10).unwrap();
        assert_eq!(props.max_row_group_row_count(), Some(11));
    }
}
