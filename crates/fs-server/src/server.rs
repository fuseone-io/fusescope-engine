//! Bootstrap and role supervision.

use std::sync::Arc;

use fs_catalog::Catalog;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{error, info};

use crate::ServerError;
use crate::bucket;
use crate::config::{Config, Secrets, redact_url};
use crate::role::Role;
use crate::roles::{self, Context};

/// Outcome of [`check`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckReport {
    /// Node id the process would use.
    pub node_id: String,
    /// Bucket probe passed (list plus write and delete of a marker).
    pub bucket_ok: bool,
    /// Catalog answered and, when configured, migrations are applied.
    pub catalog_ok: bool,
    /// Ingesters with a fresh heartbeat.
    pub live_ingesters: usize,
}

/// Connects to the bucket and the catalog, migrating when configured.
///
/// # Errors
/// Invalid configuration, unreachable bucket or catalog, failed migration.
pub async fn bootstrap(
    config: Config,
    secrets: &Secrets,
    node_id: String,
    write_probe: bool,
) -> Result<Context, ServerError> {
    config.validate(secrets)?;
    let url = config
        .catalog_url(secrets)
        .ok_or_else(|| ServerError::InvalidConfig("catalog url missing".into()))?;

    let store = bucket::open(&config.bucket, secrets)?;
    let root = bucket::root(&config.bucket);
    bucket::probe(store.as_ref(), &root, write_probe).await?;
    info!(kind = ?config.bucket.kind, bucket = ?config.bucket.name, endpoint = ?config.bucket.endpoint, prefix = %root, "bucket reachable");

    let catalog = Catalog::connect(url, config.catalog.max_connections).await?;
    if config.catalog.migrate {
        catalog.migrate().await?;
    }
    sqlx::query("select 1")
        .execute(catalog.pool())
        .await
        .map_err(fs_catalog::CatalogError::from)?;
    info!(url = %redact_url(url), migrate = config.catalog.migrate, "catalog reachable");

    Ok(Context {
        config: Arc::new(config),
        catalog,
        store,
        root,
        node_id,
    })
}

/// Validates configuration and connectivity without starting any role.
///
/// # Errors
/// Whatever [`bootstrap`] returns.
pub async fn check(
    config: Config,
    secrets: Secrets,
    node_id: String,
) -> Result<CheckReport, ServerError> {
    let ctx = bootstrap(config, &secrets, node_id.clone(), true).await?;
    let live = ctx
        .catalog
        .live_ingesters(chrono::Duration::seconds(10))
        .await?;
    Ok(CheckReport {
        node_id,
        bucket_ok: true,
        catalog_ok: true,
        live_ingesters: live.len(),
    })
}

/// Runs `role` until `shutdown` flips to `true`, then waits for every role to stop.
///
/// The first role that fails stops the others and its error is returned.
///
/// # Errors
/// Bootstrap failure or the first role failure.
pub async fn run(
    config: Config,
    secrets: Secrets,
    role: Role,
    node_id: String,
    shutdown: watch::Receiver<bool>,
) -> Result<(), ServerError> {
    let write_probe = role != Role::Querier;
    let ctx = bootstrap(config, &secrets, node_id, write_probe).await?;
    info!(version = crate::VERSION, role = %role, node_id = %ctx.node_id, "fusescope-engine starting");

    let (stop_tx, stop_rx) = watch::channel(false);
    let mut forward = shutdown.clone();
    let forwarder = tokio::spawn(async move {
        if forward.changed().await.is_ok() {
            let _ = stop_tx.send(true);
        }
    });

    let mut tasks: JoinSet<(&'static str, Result<(), ServerError>)> = JoinSet::new();
    for r in role.expand() {
        let ctx = ctx.clone();
        let rx = stop_rx.clone();
        match r {
            Role::Ingester => {
                tasks.spawn(async move { ("ingester", roles::ingester(ctx, rx).await) })
            }
            Role::Querier => tasks.spawn(async move { ("querier", roles::querier(ctx, rx).await) }),
            Role::Compactor => {
                tasks.spawn(async move { ("compactor", roles::compactor(ctx, rx).await) })
            }
            Role::All => unreachable!("expand never yields All"),
        };
    }

    let mut first_error: Option<ServerError> = None;
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((_, Ok(()))) => {}
            Ok((name, Err(e))) => {
                error!(role = name, error = %e, "role failed");
                first_error.get_or_insert(e);
                forwarder.abort();
                tasks.abort_all();
            }
            Err(join) => {
                let e = ServerError::RoleStopped {
                    role: "unknown",
                    reason: join.to_string(),
                };
                first_error.get_or_insert(e);
                tasks.abort_all();
            }
        }
    }
    forwarder.abort();
    info!("fusescope-engine stopped");
    first_error.map_or(Ok(()), Err)
}
