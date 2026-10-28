//! The compactor: one active instance per deployment rewrites and purges
//! files. Which instance is active is decided by a lease in the catalog
//! ([`lease`]); the others stand by and take over when it lapses.
//!
//! See DESIGN.md at the repository root for the role of this crate.

use std::sync::Arc;
use std::time::Duration;

use fs_catalog::Catalog;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::sync::watch;
use tracing::{info, warn};

pub mod lease;

pub use lease::Leadership;

/// Crate version, exposed for diagnostics and the `--version` flag.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// What the compactor does and how often.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactorConfig {
    /// How often quarantined files are purged.
    pub purge_interval: Duration,
    /// How long a quarantined file stays readable before deletion.
    pub quarantine_ttl: Duration,
    /// Files purged per pass.
    pub purge_batch: i64,
    /// How long the leader lease lasts without a renewal. Renewed every
    /// third of it.
    pub lease_ttl: Duration,
}

/// Runs the compactor until `shutdown` flips. Work happens only while
/// `holder` holds the lease; the lease is renewed every third of its TTL
/// and released on the way out.
///
/// # Errors
/// Never: catalog and bucket failures are logged and retried next pass.
pub async fn run(
    catalog: Catalog,
    store: Arc<dyn ObjectStore>,
    config: CompactorConfig,
    holder: String,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut leadership = Leadership::new(catalog.clone(), holder, config.lease_ttl);
    let mut renew = tokio::time::interval(leadership.renew_every());
    let mut purge = tokio::time::interval(config.purge_interval.max(Duration::from_secs(1)));
    purge.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = renew.tick() => {
                leadership.refresh().await;
            }
            _ = purge.tick(), if leadership.is_leader() => {
                purge_once(&catalog, store.as_ref(), config.quarantine_ttl, config.purge_batch).await;
            }
            _ = shutdown.changed() => break,
        }
    }
    leadership.release().await;
}

/// One purge pass: deletes the catalog rows of quarantined files past
/// `quarantine_ttl`, then their objects. An object that fails to delete is
/// left for the orphan sweep. Returns how many rows went.
pub async fn purge_once(
    catalog: &Catalog,
    store: &dyn ObjectStore,
    quarantine_ttl: Duration,
    batch: i64,
) -> usize {
    let ttl = chrono::Duration::from_std(quarantine_ttl).unwrap_or(chrono::Duration::MAX);
    let purged = match catalog.purge_quarantined(ttl, batch).await {
        Ok(p) => p,
        Err(e) => {
            warn!(role = "compactor", error = %e, "purge query failed");
            return 0;
        }
    };
    let mut deleted = 0usize;
    for file in &purged {
        match store.delete(&Path::from(file.path.as_str())).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => deleted += 1,
            Err(e) => {
                warn!(role = "compactor", path = %file.path, error = %e, "object delete failed; orphan sweep will retry");
            }
        }
    }
    if !purged.is_empty() {
        info!(
            role = "compactor",
            rows = purged.len(),
            deleted,
            "purged quarantined files"
        );
    }
    purged.len()
}

#[cfg(test)]
mod tests {
    #[test]
    fn version_is_set() {
        assert_ne!(super::VERSION, "");
    }
}
