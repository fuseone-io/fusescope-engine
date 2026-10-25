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
use tracing::{info, warn};

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

/// Ingester: opens and replays the WAL, serves OTLP, flushes Parquet L0 files
/// and heartbeats to the catalog so queriers find it for hot-window scans.
///
/// # Errors
/// WAL, bind, transport or catalog failures.
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
    let wal = fs_wal::Wal::open(wal_config)?;
    let recovery = wal.recovery().clone();
    info!(
        role = "ingester",
        dir = %ctx.config.wal.dir.display(),
        truncated_bytes = recovery.truncated_bytes,
        damage = ?recovery.damage,
        "wal opened"
    );
    let ingest_config = ctx.config.ingest.to_ingest_config()?;
    let ingester = fs_ingest::Ingester::new(
        ingest_config,
        ctx.catalog.clone(),
        ctx.store.clone(),
        ctx.root.clone(),
        wal,
    )?;

    let heartbeat = {
        let ctx = ctx.clone();
        let mut shutdown = shutdown.clone();
        async move {
            let mut tick = tokio::time::interval(HEARTBEAT);
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        if let Err(e) = ctx.catalog.heartbeat(&ctx.node_id, &ctx.config.ingest.hot_advertise).await {
                            warn!(role = "ingester", error = %e, "heartbeat failed");
                        }
                    }
                    _ = shutdown.changed() => break,
                }
            }
        }
    };
    let (served, ()) = tokio::join!(ingester.run(shutdown.clone(), None, None), heartbeat);
    served?;
    let _ = shutdown.borrow_and_update();
    info!(role = "ingester", "stopped");
    Ok(())
}

/// Querier: serves Arrow Flight SQL on the configured address and keeps the
/// file cache fresh from catalog notifications.
///
/// # Errors
/// Engine construction, bind or transport failures.
pub async fn querier(ctx: Context, shutdown: watch::Receiver<bool>) -> Result<(), ServerError> {
    let listen: std::net::SocketAddr = ctx.config.server.flight_addr.parse().map_err(|e| {
        ServerError::InvalidConfig(format!(
            "server.flight_addr {:?}: {e}",
            ctx.config.server.flight_addr
        ))
    })?;
    let querier = fs_query::Querier::new(
        ctx.catalog.clone(),
        ctx.store.clone(),
        ctx.config.query.spill_dir.clone(),
        ctx.config.query.to_limits(),
    )?;
    let cache = querier.cache();
    let follower = cache.follow(shutdown.clone());
    let service = fs_query::flight::FlightSql::new(querier, ctx.catalog.clone());
    info!(role = "querier", %listen, "ready");
    let (served, ()) = tokio::join!(service.serve(listen, shutdown, None), follower);
    served?;
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
