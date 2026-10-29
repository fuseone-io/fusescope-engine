//! The `attribute_keys` table as an object: read whole, written whole.

use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use fs_catalog::{FileId, Level, NewFile, Signal, TenantId, TimeRange};
use fs_schema::attribute_keys::KeyStats;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::CompactError;
use crate::merge::attribute_keys_path;

/// The table at `path`, decoded.
///
/// # Errors
/// A store failure, or bytes that are not a readable table.
pub(crate) async fn read_table(
    store: &dyn ObjectStore,
    path: &str,
) -> Result<KeyStats, CompactError> {
    let bytes = store.get(&Path::from(path)).await?.bytes().await?;
    Ok(fs_schema::attribute_keys::decode(bytes)?)
}

/// `keys` encoded as the table of `tenant` with its catalog row, placed
/// in the partition of `hour`; `None` when there is nothing to list.
///
/// # Errors
/// Encoding failure.
pub(crate) fn table_file(
    root: &Path,
    tenant: TenantId,
    hour: DateTime<Utc>,
    keys: &KeyStats,
) -> Result<Option<(NewFile, Bytes)>, CompactError> {
    let Some((first, last)) = keys.range() else {
        return Ok(None);
    };
    let bytes = Bytes::from(fs_schema::attribute_keys::encode(keys)?);
    let id = FileId::new();
    let path = attribute_keys_path(root, tenant, hour, id);
    let file = NewFile {
        id,
        origin: id,
        tenant_id: tenant,
        signal: Signal::AttributeKeys,
        level: Level::L1,
        path: path.to_string(),
        size_bytes: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
        row_count: i64::try_from(keys.len()).unwrap_or(i64::MAX),
        range: TimeRange {
            start: Utc.timestamp_nanos(first),
            end: Utc.timestamp_nanos(last),
        },
        services: Vec::new(),
        summary: None,
    };
    Ok(Some((file, bytes)))
}
