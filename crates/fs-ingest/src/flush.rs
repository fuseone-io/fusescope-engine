//! Writes a tenant buffer as one sorted Parquet L0 file.

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::compute::{SortColumn, concat_batches, lexsort_to_indices, take_record_batch};
use arrow::datatypes::{Int32Type, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use fs_catalog::{FileId, Level, NewFile, Signal, TenantId, TimeRange};
use fs_schema::spans::{col, span_schema_v1};
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;

use crate::IngestError;

/// Rows per row group. One group per million rows keeps footers small while
/// statistics still prune well.
const ROW_GROUP_ROWS: usize = 1_000_000;

/// zstd level for L0: fast, the compactor recompresses at L2.
const ZSTD_LEVEL: i32 = 3;

/// A Parquet file ready to upload plus its catalog entry.
pub struct Prepared {
    /// Catalog row.
    pub file: NewFile,
    /// File bytes.
    pub bytes: Bytes,
}

/// Concatenates, sorts by `(service_name, timestamp)` and encodes `batches`.
///
/// CPU bound: callers run it on a blocking thread.
///
/// # Errors
/// Arrow or Parquet failures.
pub fn prepare(
    tenant: TenantId,
    root: &Path,
    id: FileId,
    batches: &[RecordBatch],
) -> Result<Prepared, IngestError> {
    let schema = span_schema_v1();
    let merged = concat_batches(&schema, batches)?;
    let sorted = sort_spans(&merged)?;

    let ts = sorted
        .column(schema.index_of(col::TIMESTAMP)?)
        .as_primitive::<TimestampNanosecondType>();
    let ts_min = arrow::compute::min(ts).unwrap_or(0);
    let ts_max = arrow::compute::max(ts).unwrap_or(0);
    let services = distinct_services(&sorted)?;

    let start = Utc.timestamp_nanos(ts_min);
    let path = l0_path(root, tenant, start, id);
    let bytes = encode_parquet(&sorted)?;

    let file = NewFile {
        id,
        tenant_id: tenant,
        signal: Signal::Traces,
        level: Level::L0,
        path: path.to_string(),
        size_bytes: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
        row_count: i64::try_from(sorted.num_rows()).unwrap_or(i64::MAX),
        range: TimeRange {
            start,
            end: Utc.timestamp_nanos(ts_max),
        },
        services,
        summary: None,
    };
    Ok(Prepared { file, bytes })
}

/// Uploads a prepared file to its path without overwriting: a retried flush
/// finds the object already there and leaves it. Returns whether this call
/// created it. A store without conditional writes is an error, not a plain
/// put: overwriting is never allowed.
///
/// # Errors
/// Bucket failure other than "already exists", including missing support for
/// conditional writes.
pub async fn upload(store: &dyn ObjectStore, prepared: &Prepared) -> Result<bool, IngestError> {
    let path = Path::from(prepared.file.path.as_str());
    let payload = PutPayload::from_bytes(prepared.bytes.clone());
    let create = PutOptions {
        mode: PutMode::Create,
        ..PutOptions::default()
    };
    match store.put_opts(&path, payload, create).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// `{root}/t/{tenant}/traces/l0/dt=YYYY-MM-DD/hr=HH/{id}.parquet`
pub fn l0_path(root: &Path, tenant: TenantId, start: DateTime<Utc>, id: FileId) -> Path {
    let relative = format!(
        "t/{}/traces/l0/dt={}/hr={:02}/{}.parquet",
        tenant.0,
        start.format("%Y-%m-%d"),
        start.format("%H"),
        id.0
    );
    if root.as_ref().is_empty() {
        Path::from(relative)
    } else {
        Path::from(format!("{root}/{relative}"))
    }
}

/// Sorts a span batch by `(service_name, timestamp)`.
///
/// # Errors
/// Arrow failures or a missing column.
pub fn sort_spans(batch: &RecordBatch) -> Result<RecordBatch, IngestError> {
    let schema = batch.schema();
    let service = batch.column(schema.index_of("service_name")?);
    let timestamp = batch.column(schema.index_of(col::TIMESTAMP)?);
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
        ],
        None,
    )?;
    Ok(take_record_batch(batch, &indices)?)
}

fn distinct_services(batch: &RecordBatch) -> Result<Vec<String>, IngestError> {
    let column = batch.column(batch.schema().index_of("service_name")?);
    let dict = column.as_dictionary::<Int32Type>();
    let values = dict.values().as_string::<i32>();
    let mut seen = std::collections::BTreeSet::new();
    for key in dict.keys().iter().flatten() {
        if let Ok(i) = usize::try_from(key) {
            if values.is_valid(i) {
                seen.insert(values.value(i).to_owned());
            }
        }
    }
    Ok(seen.into_iter().collect())
}

/// Encodes a sorted span batch as a Parquet L0 file with the engine's writer settings.
///
/// # Errors
/// Parquet failures.
pub fn encode_parquet(batch: &RecordBatch) -> Result<Bytes, IngestError> {
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(ZSTD_LEVEL)?))
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_column_bloom_filter_enabled(ColumnPath::from(col::TRACE_ID), true)
        .set_column_bloom_filter_enabled(ColumnPath::from(col::SPAN_ID), true)
        .set_column_bloom_filter_enabled(ColumnPath::from("service_name"), true)
        .set_bloom_filter_fpp(0.01)
        .build();
    let mut out = Vec::with_capacity(batch.get_array_memory_size() / 2);
    let mut writer = ArrowWriter::try_new(&mut out, batch.schema(), Some(props))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l0_paths_partition_by_hour() {
        let tenant = TenantId(uuid::Uuid::nil());
        let id = FileId(uuid::Uuid::nil());
        let start = Utc.with_ymd_and_hms(2026, 10, 24, 14, 17, 3).unwrap();
        assert_eq!(
            l0_path(&Path::from(""), tenant, start, id).to_string(),
            "t/00000000-0000-0000-0000-000000000000/traces/l0/dt=2026-10-24/hr=14/00000000-0000-0000-0000-000000000000.parquet"
        );
        assert!(
            l0_path(&Path::from("engine"), tenant, start, id)
                .to_string()
                .starts_with("engine/t/")
        );
    }
}
