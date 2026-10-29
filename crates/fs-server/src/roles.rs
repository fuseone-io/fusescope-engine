//! Role runners. Each runs until the shutdown signal flips to `true`.
//!
//! Phase 0: the loops below are the parts of each role that already have a
//! backing crate. Ingest, query and compaction proper arrive with their crates.

use std::sync::Arc;
use std::time::Duration;

use fs_catalog::Catalog;
use object_store::ObjectStore;
use object_store::path::Path;
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
    /// Counters exposed on `/metrics`.
    pub metrics: Arc<crate::metrics::Metrics>,
    /// What the start found under the bucket prefix.
    pub ownership: crate::bucket::Ownership,
    /// What `/readyz` answers from; every role reports into it.
    pub health: Arc<crate::health::Health>,
    /// An ingester configuration in place of the file's, for tests that
    /// need the ingester's own knobs.
    pub ingest_override: Option<fs_ingest::IngestConfig>,
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
    let ingest_config = match ctx.ingest_override.clone() {
        Some(config) => config,
        None => ctx.config.ingest.to_ingest_config()?,
    };
    let ingester = Arc::new(fs_ingest::Ingester::new(
        ingest_config,
        ctx.catalog.clone(),
        ctx.store.clone(),
        ctx.root.clone(),
        wal,
    )?);
    let wal_probe = Arc::clone(&ingester);
    ctx.metrics.register_ingest(
        ingester.shared_stats(),
        Arc::new(move || wal_probe.wal_bytes()),
    );
    ctx.metrics.register_writer(ingester.writer_stats());
    ctx.metrics.register_flush(ingester.flush_stats());

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
    ctx.health.wal(true);
    let (otlp_tx, otlp_rx) = tokio::sync::oneshot::channel();
    let came_up = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let up = tokio::spawn({
        let health = Arc::clone(&ctx.health);
        let came_up = Arc::clone(&came_up);
        async move {
            if otlp_rx.await.is_ok() {
                health.role_up();
                came_up.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
    });
    // The server ending, by the stop or by a failure such as a port it
    // cannot bind, ends the heartbeat with it: the role returns at once
    // instead of beating for a server that is gone.
    let served = tokio::select! {
        served = ingester.run(shutdown.clone(), Some(otlp_tx), None) => served,
        () = async {
            heartbeat.await;
            std::future::pending::<()>().await;
        } => Ok(()),
    };
    up.abort();
    if came_up.load(std::sync::atomic::Ordering::SeqCst) {
        ctx.health.role_down();
    }
    ctx.health.wal(false);
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
    ctx.metrics.register_query(querier.stats());
    let cache = querier.cache();
    let follower = cache.follow(shutdown.clone());
    let service = fs_query::flight::FlightSql::new(querier, ctx.catalog.clone());
    info!(role = "querier", %listen, "ready");
    let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
    let came_up = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let up = tokio::spawn({
        let health = Arc::clone(&ctx.health);
        let came_up = Arc::clone(&came_up);
        async move {
            if bound_rx.await.is_ok() {
                health.role_up();
                came_up.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
    });
    // The server ending ends the follower with it, so a port that cannot
    // be bound returns the error at once.
    let served = tokio::select! {
        served = service.serve(listen, shutdown, Some(bound_tx)) => served,
        () = async {
            follower.await;
            std::future::pending::<()>().await;
        } => Ok(()),
    };
    up.abort();
    if came_up.load(std::sync::atomic::Ordering::SeqCst) {
        ctx.health.role_down();
    }
    served?;
    info!(role = "querier", "stopped");
    Ok(())
}

/// Compactor: takes the compactor lease in the catalog and, while it holds
/// it, purges quarantined files past their TTL. Without the lease it
/// stands by and takes over when the holder lets go or lapses.
///
/// # Errors
/// Never; failures are logged and the next pass retries.
pub async fn compactor(ctx: Context, shutdown: watch::Receiver<bool>) -> Result<(), ServerError> {
    info!(role = "compactor", holder = %ctx.node_id, "ready");
    let stats = Arc::new(fs_compactor::Stats::default());
    ctx.metrics.register_compactor(Arc::clone(&stats));
    ctx.health.role_up();
    fs_compactor::run(
        ctx.catalog.clone(),
        ctx.store.clone(),
        ctx.root.clone(),
        ctx.config
            .compactor
            .to_compactor(ctx.config.ingest.row_group_rows),
        &ctx.node_id,
        stats,
        shutdown,
    )
    .await;
    ctx.health.role_down();
    info!(role = "compactor", "stopped");
    Ok(())
}
