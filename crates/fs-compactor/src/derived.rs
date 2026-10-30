//! The `attribute_keys` table as an object: read whole, written whole.

use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use fs_catalog::{FileId, Level, NewFile, Signal, TenantId, TimeRange};
use fs_schema::attribute_keys::KeyStats;
use fs_schema::service_graph::ServiceGraph;
use fs_schema::trace_summary::TraceSummaries;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::CompactError;
use crate::merge::{attribute_keys_path, service_graph_path, trace_summary_path};

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
        content_sha256: Some(crate::objects::sha256(&bytes)),
    };
    Ok(Some((file, bytes)))
}

/// One pass's trace summaries with their catalog row.
///
/// # Errors
/// Encoding failure.
pub(crate) fn trace_summary_file(
    root: &Path,
    tenant: TenantId,
    hour: DateTime<Utc>,
    rows: &TraceSummaries,
) -> Result<Option<(NewFile, Bytes)>, CompactError> {
    let Some((first, last)) = rows.range() else {
        return Ok(None);
    };
    let bytes = Bytes::from(fs_schema::trace_summary::encode(rows)?);
    let id = FileId::new();
    let path = trace_summary_path(root, tenant, hour, id);
    let services = rows
        .iter()
        .flat_map(|(_, row)| row.services.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .map(|service| service.to_string())
        .collect();
    Ok(Some((
        NewFile {
            id,
            origin: id,
            tenant_id: tenant,
            signal: Signal::TraceSummary,
            level: Level::L1,
            path: path.to_string(),
            size_bytes: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
            row_count: i64::try_from(rows.len()).unwrap_or(i64::MAX),
            range: TimeRange {
                start: Utc.timestamp_nanos(first),
                end: Utc.timestamp_nanos(last),
            },
            services,
            summary: None,
            content_sha256: Some(crate::objects::sha256(&bytes)),
        },
        bytes,
    )))
}

/// One pass's service graph with its catalog row.
///
/// # Errors
/// Encoding failure.
pub(crate) fn service_graph_file(
    root: &Path,
    tenant: TenantId,
    hour: DateTime<Utc>,
    graph: &ServiceGraph,
) -> Result<Option<(NewFile, Bytes)>, CompactError> {
    let Some((first, last)) = graph.range() else {
        return Ok(None);
    };
    let bytes = Bytes::from(fs_schema::service_graph::encode(graph)?);
    let id = FileId::new();
    let path = service_graph_path(root, tenant, hour, id);
    let services = graph
        .iter()
        .flat_map(|edge| {
            std::iter::once(edge.src.clone()).chain(
                (edge.dst_kind == fs_schema::service_graph::DestinationKind::Service)
                    .then(|| edge.dst.clone()),
            )
        })
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    Ok(Some((
        NewFile {
            id,
            origin: id,
            tenant_id: tenant,
            signal: Signal::ServiceGraph1m,
            level: Level::L1,
            path: path.to_string(),
            size_bytes: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
            row_count: i64::try_from(graph.len()).unwrap_or(i64::MAX),
            range: TimeRange {
                start: Utc.timestamp_nanos(first),
                end: Utc.timestamp_nanos(last),
            },
            services,
            summary: None,
            content_sha256: Some(crate::objects::sha256(&bytes)),
        },
        bytes,
    )))
}
