//! The orphan sweep: objects in the bucket without a file row.
//!
//! A crash between an upload and its registration, or a compaction that
//! did not publish, leaves objects no row points at. The sweep lists the
//! engine's objects, remembers in the catalog the ones without a row, and
//! deletes an object only when it has been seen without a row for longer
//! than the orphan TTL: an upload still waiting for its registration is
//! seen once and registered before the next look. Only span files and
//! their sidecars are considered.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fs_catalog::Catalog;
use futures_util::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tracing::{info, warn};

use crate::CompactError;

/// Counters of the orphan sweep.
#[derive(Debug, Default)]
pub struct OrphanStats {
    /// Sweeps completed.
    pub sweeps: AtomicU64,
    /// Objects listed by completed sweeps.
    pub listed: AtomicU64,
    /// Objects seen without a row, over all sweeps.
    pub seen: AtomicU64,
    /// Objects deleted after a TTL without a row.
    pub deleted: AtomicU64,
    /// Remembered orphans found with a row, or gone, before their TTL.
    pub forgotten: AtomicU64,
}

/// What one sweep did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Swept {
    /// Objects listed.
    pub listed: usize,
    /// Objects without a row, remembered.
    pub seen: usize,
    /// Objects deleted.
    pub deleted: usize,
    /// Remembered orphans forgotten.
    pub forgotten: usize,
}

/// One sweep under `root`: remembers objects without a row, deletes the
/// ones remembered for longer than `ttl`.
///
/// # Errors
/// Catalog or bucket failure; a sweep that fails leaves nothing half done
/// that the next one does not redo.
pub async fn sweep_once(
    catalog: &Catalog,
    store: &dyn ObjectStore,
    root: &Path,
    ttl: Duration,
    stats: &Arc<OrphanStats>,
) -> Result<Swept, CompactError> {
    let prefix = if root.as_ref().is_empty() {
        Path::from("t")
    } else {
        Path::from(format!("{root}/t"))
    };
    let objects: Vec<String> = store
        .list(Some(&prefix))
        .map_ok(|meta| meta.location.to_string())
        .try_filter(|path| std::future::ready(matches!(extension(path), Some("parquet" | "tix"))))
        .try_collect()
        .await?;
    let mut swept = Swept {
        listed: objects.len(),
        ..Swept::default()
    };

    // An object has a row when its path is one, or when it is the sidecar
    // of a path that is one.
    let candidates: Vec<String> = objects
        .iter()
        .flat_map(|path| owners(path))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let registered = catalog.registered_paths(&candidates).await?;
    let present: HashSet<&String> = objects.iter().collect();
    let without_row: Vec<String> = objects
        .iter()
        .filter(|path| !owners(path).iter().any(|o| registered.contains(o)))
        .cloned()
        .collect();
    catalog.orphans_seen(&without_row).await?;
    swept.seen = without_row.len();

    let due = catalog.orphans_due(ttl).await?;
    let mut deleted = Vec::new();
    let mut forgotten = Vec::new();
    for path in due {
        let still_orphan = present.contains(&path) && without_row.contains(&path);
        if !still_orphan {
            forgotten.push(path);
            continue;
        }
        match store.delete(&Path::from(path.as_str())).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => deleted.push(path),
            Err(e) => {
                warn!(role = "compactor", path = %path, error = %e, "orphan delete failed; the next sweep retries");
            }
        }
    }
    catalog.orphans_forget(&deleted).await?;
    catalog.orphans_forget(&forgotten).await?;
    swept.deleted = deleted.len();
    swept.forgotten = forgotten.len();

    stats.sweeps.fetch_add(1, Ordering::Relaxed);
    stats
        .listed
        .fetch_add(swept.listed as u64, Ordering::Relaxed);
    stats.seen.fetch_add(swept.seen as u64, Ordering::Relaxed);
    stats
        .deleted
        .fetch_add(swept.deleted as u64, Ordering::Relaxed);
    stats
        .forgotten
        .fetch_add(swept.forgotten as u64, Ordering::Relaxed);
    if swept.seen > 0 || swept.deleted > 0 {
        info!(
            role = "compactor",
            listed = swept.listed,
            without_row = swept.seen,
            deleted = swept.deleted,
            forgotten = swept.forgotten,
            "orphan sweep"
        );
    }
    Ok(swept)
}

/// The file rows that would own `path`: itself for a span file, the span
/// file of each level for a sidecar.
fn owners(path: &str) -> Vec<String> {
    if extension(path) == Some("tix") {
        fs_index::owners_of_sidecar(path)
    } else {
        vec![path.to_owned()]
    }
}

/// The extension the engine wrote, exactly as written.
fn extension(path: &str) -> Option<&str> {
    path.rsplit_once('.').map(|(_, ext)| ext)
}
