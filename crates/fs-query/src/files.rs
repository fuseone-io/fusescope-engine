//! Catalog-backed, in-memory list of visible files per tenant and signal.
//!
//! Entries refresh on catalog notifications and expire after a short TTL as a
//! safety net, since notifications can be lost across reconnects.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fs_catalog::{Catalog, FileRecord, Signal, TenantId};
use tokio::sync::{RwLock, watch};
use tracing::{debug, warn};

use crate::QueryError;

/// How long a cached list is trusted without a notification.
pub const STALE_AFTER: Duration = Duration::from_secs(5);

struct Entry {
    files: Arc<Vec<FileRecord>>,
    loaded: Instant,
}

/// Shared file cache.
pub struct FileCache {
    catalog: Catalog,
    entries: RwLock<HashMap<(TenantId, Signal), Entry>>,
}

impl FileCache {
    /// Empty cache over `catalog`.
    pub fn new(catalog: Catalog) -> Self {
        Self {
            catalog,
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Visible files of `tenant` and `signal`, oldest first. Loads from the
    /// catalog when missing or stale.
    ///
    /// # Errors
    /// Catalog failure on a load.
    pub async fn files(
        &self,
        tenant: TenantId,
        signal: Signal,
    ) -> Result<Arc<Vec<FileRecord>>, QueryError> {
        let key = (tenant, signal);
        if let Some(entry) = self.entries.read().await.get(&key) {
            if entry.loaded.elapsed() < STALE_AFTER {
                return Ok(Arc::clone(&entry.files));
            }
        }
        let files = Arc::new(self.catalog.all_visible_files(tenant, signal).await?);
        debug!(tenant = %tenant.0, ?signal, files = files.len(), "file list loaded");
        self.entries.write().await.insert(
            key,
            Entry {
                files: Arc::clone(&files),
                loaded: Instant::now(),
            },
        );
        Ok(files)
    }

    /// Forgets the cached list so the next lookup reloads.
    pub async fn invalidate(&self, tenant: TenantId, signal: Signal) {
        self.entries.write().await.remove(&(tenant, signal));
    }

    /// Follows catalog notifications until `shutdown` flips, invalidating
    /// what changed. Reconnects on failure.
    pub async fn follow(&self, mut shutdown: watch::Receiver<bool>) {
        loop {
            let mut watcher = match self.catalog.watch().await {
                Ok(w) => w,
                Err(e) => {
                    warn!(error = %e, "catalog watch failed, retrying");
                    tokio::select! {
                        () = tokio::time::sleep(Duration::from_secs(2)) => continue,
                        _ = shutdown.changed() => return,
                    }
                }
            };
            loop {
                tokio::select! {
                    change = watcher.next() => match change {
                        Ok(change) => self.invalidate(change.tenant_id, change.signal).await,
                        Err(e) => {
                            warn!(error = %e, "catalog watch lost, reconnecting");
                            break;
                        }
                    },
                    _ = shutdown.changed() => return,
                }
            }
        }
    }
}
