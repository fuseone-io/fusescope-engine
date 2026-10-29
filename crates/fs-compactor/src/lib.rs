//! The compactor: one active instance per deployment rewrites and purges
//! files. Which instance is active is decided by a lease in the catalog
//! ([`lease`]); the others stand by and take over when it lapses.
//!
//! The lease is renewed on its own loop, never behind the work: work runs
//! on a task that is started when the lease is taken and aborted, and
//! waited for, when it is lost, so a slow pass neither lets the lease
//! lapse nor outlives it. A renewal is bounded by the renewal period, so a
//! stuck one reports the loss before the TTL. Work cut short that way
//! leaves at most objects without rows, which the orphan sweep removes.
//!
//! See DESIGN.md at the repository root for the role of this crate.

use std::sync::Arc;
use std::time::Duration;

use fs_catalog::Catalog;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub mod compact;
mod derived;
pub mod lease;
pub mod merge;
mod objects;
pub mod orphans;
pub mod retention;

pub use compact::{CompactionConfig, Compactor, CompactorStats};
pub use lease::Leadership;
pub use orphans::{OrphanStats, SweepConfig, Sweeper};
pub use retention::{Retention, RetentionConfig, RetentionStats};

/// Why a compaction pass failed.
#[derive(Debug, thiserror::Error)]
pub enum CompactError {
    /// Catalog failure, a refused swap included.
    #[error("catalog: {0}")]
    Catalog(#[from] fs_catalog::CatalogError),
    /// Bucket failure.
    #[error("bucket: {0}")]
    Store(#[from] object_store::Error),
    /// Parquet failure reading an input or writing an output.
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// Arrow failure.
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// The span schema refused a batch or a writer setting.
    #[error("schema: {0}")]
    Schema(#[from] fs_schema::SchemaError),
    /// The sidecar could not be built.
    #[error("trace index: {0}")]
    Index(#[from] fs_index::IndexError),
    /// An output path already held an object: ids are fresh, so a bug.
    #[error("object already exists at {0}")]
    ObjectExists(String),
    /// The merge lost track of a row group. A bug.
    #[error("merge: {0}")]
    Merge(&'static str),
    /// A failure injected by test configuration.
    #[error("injected failure: {0}")]
    Injected(&'static str),
    /// The tenant of the hour being compacted is no longer in the catalog.
    #[error("tenant {} is gone", .0.0)]
    TenantGone(fs_catalog::TenantId),
}

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
    /// How often closed hours are looked at for compaction.
    pub compaction_interval: Duration,
    /// What one compaction pass takes on.
    pub compaction: CompactionConfig,
    /// How often the bucket is swept for objects without a row.
    pub orphan_interval: Duration,
    /// Bounds of one sweep.
    pub sweep: SweepConfig,
    /// How often files beyond their tenant's retention are looked for.
    pub retention_interval: Duration,
    /// Bounds of retention.
    pub retention: RetentionConfig,
}

/// Counters of every kind of work the compactor does.
#[derive(Debug, Default)]
pub struct Stats {
    /// Compaction passes.
    pub compaction: Arc<CompactorStats>,
    /// Orphan sweeps.
    pub orphans: Arc<OrphanStats>,
    /// Retention passes and `attribute_keys` rebuilds.
    pub retention: Arc<RetentionStats>,
}

/// Runs the compactor until `shutdown` flips. Work happens only while this
/// incarnation of `node_id` holds the lease; the lease is renewed every
/// third of its TTL and released on the way out. Outputs go under `root`.
pub async fn run(
    catalog: Catalog,
    store: Arc<dyn ObjectStore>,
    root: Path,
    config: CompactorConfig,
    node_id: &str,
    stats: Arc<Stats>,
    mut shutdown: watch::Receiver<bool>,
) {
    let leadership = Arc::new(Leadership::new(catalog.clone(), node_id, config.lease_ttl));
    let compactor = Arc::new(Compactor::new(
        catalog.clone(),
        Arc::clone(&store),
        root.clone(),
        config.compaction.clone(),
        Arc::clone(&stats.compaction),
    ));
    let retention = Arc::new(Retention::new(
        catalog.clone(),
        Arc::clone(&store),
        root.clone(),
        config.retention.clone(),
        Arc::clone(&stats.retention),
    ));
    let (leader_tx, leader_rx) = watch::channel(false);
    let mut renew = tokio::time::interval(leadership.renew_every());
    renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut work: Option<JoinHandle<()>> = None;
    loop {
        tokio::select! {
            _ = renew.tick() => {
                let held = leadership.refresh().await;
                leader_tx.send_if_modified(|leader| std::mem::replace(leader, held) != held);
                if held {
                    if work.as_ref().is_none_or(JoinHandle::is_finished) {
                        work = Some(tokio::spawn(work_loop(
                            catalog.clone(),
                            Arc::clone(&store),
                            root.clone(),
                            Arc::clone(&compactor),
                            Arc::clone(&retention),
                            Arc::clone(&leadership),
                            Arc::clone(&stats.orphans),
                            config.clone(),
                            leader_rx.clone(),
                        )));
                    }
                } else if let Some(task) = work.take() {
                    stop(task).await;
                }
            }
            _ = shutdown.changed() => break,
        }
    }
    if let Some(task) = work.take() {
        stop(task).await;
    }
    leadership.release().await;
}

/// Aborts the work and waits until it has stopped, so nothing of it runs
/// once the lease is reported lost or released.
async fn stop(task: JoinHandle<()>) {
    task.abort();
    let _ = task.await;
}

/// Most compaction passes one tick runs before yielding to the next tick.
const PASSES_PER_TICK: usize = 64;

/// The work of the leader: purge, compaction, retention and orphan passes
/// at their intervals until the lease is lost, one at a time. Every swap
/// carries the fence of the lease as it is at that moment; no fence, no
/// pass.
#[allow(clippy::too_many_arguments)]
async fn work_loop(
    catalog: Catalog,
    store: Arc<dyn ObjectStore>,
    root: Path,
    compactor: Arc<Compactor>,
    retention: Arc<Retention>,
    leadership: Arc<Leadership>,
    orphan_stats: Arc<OrphanStats>,
    config: CompactorConfig,
    mut leader: watch::Receiver<bool>,
) {
    let mut purge = tokio::time::interval(config.purge_interval.max(Duration::from_secs(1)));
    purge.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut compact = tokio::time::interval(config.compaction_interval.max(Duration::from_secs(1)));
    compact.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut orphans = tokio::time::interval(config.orphan_interval.max(Duration::from_secs(1)));
    orphans.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut expire = tokio::time::interval(config.retention_interval.max(Duration::from_secs(1)));
    expire.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut sweeper = Sweeper::new(
        catalog.clone(),
        Arc::clone(&store),
        root,
        config.sweep.clone(),
        orphan_stats,
    );
    loop {
        tokio::select! {
            _ = orphans.tick() => {
                if let Err(e) = sweeper.sweep_once().await {
                    warn!(role = "compactor", error = %e, "orphan sweep failed");
                }
            }
            _ = purge.tick() => {
                purge_once(
                    &catalog,
                    store.as_ref(),
                    config.quarantine_ttl,
                    config.purge_batch,
                    config.sweep.delete_backoff,
                )
                .await;
            }
            _ = compact.tick() => {
                for _ in 0..PASSES_PER_TICK {
                    let Some(fence) = leadership.fence() else {
                        break;
                    };
                    match compactor.compact_once(&fence).await {
                        Ok(Some(_)) => {}
                        Ok(None) => break,
                        Err(e) => {
                            warn!(role = "compactor", error = %e, "compaction pass failed");
                            break;
                        }
                    }
                }
            }
            _ = expire.tick() => {
                retention_passes(&retention, &leadership).await;
            }
            changed = leader.changed() => {
                if changed.is_err() || !*leader.borrow() {
                    return;
                }
            }
        }
    }
}

/// Expiry passes until nothing is beyond its cut, then rebuilds until no
/// table is stale, each kind bounded per tick; a failure ends its kind
/// for this tick.
async fn retention_passes(retention: &Retention, leadership: &Leadership) {
    retention.forget_failures();
    for _ in 0..PASSES_PER_TICK {
        let Some(fence) = leadership.fence() else {
            return;
        };
        match retention.expire_once(&fence).await {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(e) => {
                warn!(role = "compactor", error = %e, "retention pass failed");
                break;
            }
        }
    }
    for _ in 0..PASSES_PER_TICK {
        let Some(fence) = leadership.fence() else {
            return;
        };
        match retention.rebuild_once(&fence).await {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(e) => {
                warn!(role = "compactor", error = %e, "attribute_keys rebuild failed");
                break;
            }
        }
    }
}

/// One purge pass: deletes the catalog rows of quarantined files past
/// `quarantine_ttl`, each with its paths marked for deletion, then each
/// file's object and its `trace_id` sidecar under their claims, leaving
/// tombstones. An object that fails to delete waits `delete_backoff` and
/// is finished by the orphan sweep. Returns how many rows went.
pub async fn purge_once(
    catalog: &Catalog,
    store: &dyn ObjectStore,
    quarantine_ttl: Duration,
    batch: i64,
    delete_backoff: Duration,
) -> usize {
    let purged = match catalog.purge_quarantined(quarantine_ttl, batch).await {
        Ok(p) => p,
        Err(e) => {
            warn!(role = "compactor", error = %e, "purge query failed");
            return 0;
        }
    };
    let mut deleted = 0usize;
    for file in &purged {
        let mut paths = vec![file.path.clone()];
        paths.extend(fs_index::sidecar_path(&file.path));
        for path in paths {
            if delete_claimed(catalog, store, &path, delete_backoff).await {
                deleted += 1;
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

/// Deletes `path` under its claim and leaves the tombstone; true when the
/// object is gone. A path no longer claimable was finished by a sweeper.
async fn delete_claimed(
    catalog: &Catalog,
    store: &dyn ObjectStore,
    path: &str,
    delete_backoff: Duration,
) -> bool {
    let claim = match catalog.orphan_claim(path).await {
        Ok(Some(claim)) => claim,
        Ok(None) => return false,
        Err(e) => {
            warn!(role = "compactor", path = %path, error = %e, "purge claim failed; orphan sweep will retry");
            return false;
        }
    };
    let outcome = match store.delete(&Path::from(path)).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => claim.deleted().await.map(|()| true),
        Err(e) => {
            warn!(role = "compactor", path = %path, error = %e, "object delete failed; orphan sweep will retry");
            claim.retry_later(delete_backoff).await.map(|()| false)
        }
    };
    outcome.unwrap_or_else(|e| {
        warn!(role = "compactor", path = %path, error = %e, "purge claim could not be settled");
        false
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn version_is_set() {
        assert_ne!(super::VERSION, "");
    }
}
