//! Role runners. Each runs until the shutdown signal flips to `true`.
//!
//! Phase 0: the loops below are the parts of each role that already have a
//! backing crate. Ingest, query and compaction proper arrive with their crates.

use std::sync::Arc;
use std::time::Duration;

use fs_catalog::Catalog;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::ServerError;
use crate::config::Config;

/// Shared handles every role receives.
#[derive(Clone)]
pub struct Context {
    /// Configuration.
    pub config: Arc<Config>,
    /// Catalog.
    pub catalog: Catalog,
    /// Bucket.
    pub store: Arc<dyn ObjectStore>,
    /// Engine prefix inside the bucket.
    pub root: Path,
    /// This process's stable id.
    pub node_id: String,
}

/// Heartbeat period of the ingester. The querier treats 10 s of silence as down.
pub const HEARTBEAT: Duration = Duration::from_secs(2);

/// Ingester: opens the WAL, replays it, and heartbeats to the catalog.
///
/// # Errors
/// WAL or catalog failures.
pub async fn ingester(
    ctx: Context,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServerError> {
    let wal_config = fs_wal::WalConfig {
        dir: ctx.config.wal.dir.clone(),
        segment_max_bytes: ctx.config.wal.segment_max_bytes,
        max_record_bytes: ctx.config.wal.max_record_bytes,
        sync: ctx.config.wal.sync.into(),
    };
    let mut wal = fs_wal::Wal::open(wal_config)?;
    let recovery = wal.recovery().clone();
    let replayed = wal.replay()?.count();
    info!(
        role = "ingester",
        dir = %ctx.config.wal.dir.display(),
        replayed_records = replayed,
        truncated_bytes = recovery.truncated_bytes,
        damage = ?recovery.damage,
        "wal ready"
    );
    let mut tick = tokio::time::interval(HEARTBEAT);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if let Err(e) = ctx.catalog.heartbeat(&ctx.node_id, &ctx.config.server.flight_addr).await {
                    warn!(role = "ingester", error = %e, "heartbeat failed");
                }
            }
            _ = shutdown.changed() => break,
        }
    }
    wal.sync()?;
    info!(role = "ingester", "stopped");
    Ok(())
}

/// Querier: follows catalog change notifications. The in-memory file cache
/// that consumes them lands with `fs-query`.
///
/// # Errors
/// Never in phase 0; connection failures are logged and retried.
pub async fn querier(ctx: Context, mut shutdown: watch::Receiver<bool>) -> Result<(), ServerError> {
    info!(role = "querier", "ready");
    loop {
        let mut watcher = match ctx.catalog.watch().await {
            Ok(w) => w,
            Err(e) => {
                warn!(role = "querier", error = %e, "catalog watch failed, retrying");
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_secs(2)) => continue,
                    _ = shutdown.changed() => break,
                }
            }
        };
        loop {
            tokio::select! {
                change = watcher.next() => match change {
                    Ok(change) => debug!(role = "querier", tenant = %change.tenant_id.0, signal = ?change.signal, "files changed"),
                    Err(e) => {
                        warn!(role = "querier", error = %e, "watch lost, reconnecting");
                        break;
                    }
                },
                _ = shutdown.changed() => {
                    info!(role = "querier", "stopped");
                    return Ok(());
                }
            }
        }
    }
    info!(role = "querier", "stopped");
    Ok(())
}

/// Compactor: purges quarantined files past their TTL, deleting the objects
/// after the rows. Assumes a single compactor until the lease lands.
///
/// # Errors
/// Never in phase 0; failures are logged and the next pass retries.
pub async fn compactor(
    ctx: Context,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServerError> {
    info!(role = "compactor", "ready");
    let mut tick = tokio::time::interval(Duration::from_secs(
        ctx.config.compactor.purge_interval_secs.max(1),
    ));
    loop {
        tokio::select! {
            _ = tick.tick() => purge_once(&ctx).await,
            _ = shutdown.changed() => break,
        }
    }
    info!(role = "compactor", "stopped");
    Ok(())
}

/// One purge pass. Public so tests and an operator command can run it directly.
pub async fn purge_once(ctx: &Context) {
    let ttl = chrono::Duration::seconds(
        i64::try_from(ctx.config.compactor.quarantine_ttl_secs).unwrap_or(i64::MAX),
    );
    let purged = match ctx
        .catalog
        .purge_quarantined(ttl, ctx.config.compactor.purge_batch)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            warn!(role = "compactor", error = %e, "purge query failed");
            return;
        }
    };
    let mut deleted = 0usize;
    for file in &purged {
        match ctx.store.delete(&Path::from(file.path.as_str())).await {
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
}
