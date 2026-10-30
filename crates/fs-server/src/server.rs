//! Bootstrap and role supervision.

use std::sync::Arc;
use std::time::Duration;

use fs_catalog::Catalog;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{error, info, warn};

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
    /// What the start found under the bucket prefix.
    pub ownership: bucket::Ownership,
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
    let tls = if let Some(files) = config.tls.files() {
        let tls = fs_auth::Tls::load(files, config.tls.limits())?;
        info!(cert = %config.tls.cert, public_client_ca = %config.tls.public_client_ca, hot_client_ca = %config.tls.hot_client_ca, "tls loaded; the flight listeners require client certificates");
        Some(tls)
    } else {
        warn!(
            "tls is off: the flight listeners accept plain text; turn [tls] on outside development"
        );
        None
    };
    let jwt = match config.auth.jwt() {
        Some((keys, rules, reload)) => {
            let verifier = fs_auth::JwtVerifier::load(keys, rules, reload)?;
            info!(keys = %config.auth.jwt_keys, issuer = %config.auth.jwt_issuer, "flight sql takes tenant jwts only");
            Some(verifier)
        }
        None => None,
    };
    let url = config
        .catalog_url(secrets)
        .ok_or_else(|| ServerError::InvalidConfig("catalog url missing".into()))?;

    let store = bucket::open(&config.bucket, secrets)?;
    let root = bucket::root(&config.bucket);
    bucket::reachable(store.as_ref(), &root).await?;
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
    let identity = catalog.identity().await?;
    let ownership = bucket::claim(
        store.as_ref(),
        &root,
        identity,
        write_probe,
        config.bucket.adopt_prefix,
    )
    .await?;
    info!(catalog = %identity, prefix = %root, ?ownership, "bucket prefix is this catalog's");
    // Only now may anything be written: the prefix is ours.
    if write_probe {
        bucket::probe_write(store.as_ref(), &root).await?;
    }

    let metrics = Arc::new(crate::metrics::Metrics::default());
    let health = Arc::new(crate::health::Health::default());
    if let Some(tls) = &tls {
        metrics.register_tls(tls.stats());
        health.follow_tls(tls.stats());
    }
    if let Some(jwt) = &jwt {
        metrics.register_jwks(jwt.stats());
        health.follow_jwks(jwt.stats());
    }
    Ok(Context {
        config: Arc::new(config),
        catalog,
        store,
        root,
        node_id,
        metrics,
        ownership,
        health,
        ingest_override: None,
        tls,
        jwt,
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
        ownership: ctx.ownership,
    })
}

/// Administrative actions that need the catalog but no roles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminAction {
    /// Create a tenant with default limits.
    CreateTenant {
        /// Unique name.
        name: String,
    },
    /// Issue a token; the secret is printed once.
    CreateToken {
        /// Tenant id.
        tenant: uuid::Uuid,
        /// Label for the token.
        name: String,
        /// What the token may do.
        scope: fs_catalog::TokenScope,
    },
    /// List a tenant's tokens by id, without secrets.
    ListTokens {
        /// Tenant id.
        tenant: uuid::Uuid,
    },
    /// Revoke a token now.
    RevokeToken {
        /// Its id, as listed.
        id: String,
    },
    /// Issue a successor of a token, the old one working `overlap_secs` more.
    RotateToken {
        /// Its id, as listed.
        id: String,
        /// Seconds both work, at most seven days.
        overlap_secs: u64,
    },
}

/// Runs an administrative action and returns the lines to print.
///
/// # Errors
/// Invalid configuration or catalog failure.
pub async fn admin(
    config: Config,
    secrets: Secrets,
    action: AdminAction,
) -> Result<Vec<String>, ServerError> {
    config.validate(&secrets)?;
    let url = config
        .catalog_url(&secrets)
        .ok_or_else(|| ServerError::InvalidConfig("catalog url missing".into()))?;
    let catalog = Catalog::connect(url, 2).await?;
    if config.catalog.migrate {
        catalog.migrate().await?;
    }
    match action {
        AdminAction::CreateTenant { name } => {
            let tenant = catalog
                .create_tenant(&fs_catalog::NewTenant::with_defaults(name))
                .await?;
            Ok(vec![
                format!("tenant_id={}", tenant.id.0),
                format!("name={}", tenant.name),
            ])
        }
        AdminAction::CreateToken {
            tenant,
            name,
            scope,
        } => {
            let (secret, token) = catalog
                .create_token(fs_catalog::TenantId(tenant), &name, scope)
                .await?;
            crate::tokens::audit("token.create", &token);
            Ok(vec![
                crate::tokens::describe(&token),
                format!("token={secret}"),
                "store the token now; it is not kept in plaintext".to_owned(),
            ])
        }
        AdminAction::ListTokens { tenant } => Ok(catalog
            .tokens(fs_catalog::TenantId(tenant))
            .await?
            .iter()
            .map(crate::tokens::describe)
            .collect()),
        AdminAction::RevokeToken { id } => {
            let token = catalog
                .revoke_token_by_id(&id)
                .await?
                .ok_or_else(|| ServerError::InvalidConfig(format!("no live token with id {id}")))?;
            crate::tokens::audit("token.revoke", &token);
            Ok(vec![crate::tokens::describe(&token)])
        }
        AdminAction::RotateToken { id, overlap_secs } => {
            let (secret, new, old) = catalog
                .rotate_token(&id, Duration::from_secs(overlap_secs))
                .await?
                .ok_or_else(|| ServerError::InvalidConfig(format!("no live token with id {id}")))?;
            crate::tokens::audit("token.rotate.old", &old);
            crate::tokens::audit("token.rotate.new", &new);
            Ok(vec![
                crate::tokens::describe(&old),
                crate::tokens::describe(&new),
                format!("token={secret}"),
                "store the token now; it is not kept in plaintext".to_owned(),
            ])
        }
    }
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
    run_with(config, secrets, role, node_id, shutdown, None).await
}

/// [`run`] with an ingester configuration in place of the file's, for
/// tests that need the ingester's own knobs.
///
/// # Errors
/// See [`run`].
pub async fn run_with(
    config: Config,
    secrets: Secrets,
    role: Role,
    node_id: String,
    shutdown: watch::Receiver<bool>,
    ingest_override: Option<fs_ingest::IngestConfig>,
) -> Result<(), ServerError> {
    let write_probe = role != Role::Querier;
    let mut ctx = bootstrap(config, &secrets, node_id, write_probe).await?;
    ctx.ingest_override = ingest_override;
    info!(version = crate::VERSION, role = %role, node_id = %ctx.node_id, "fusescope-engine starting");
    let roles = role.expand();
    ctx.health
        .expect(roles.len(), roles.contains(&Role::Ingester));

    // The roles stop on `stop`; the HTTP server and the prober outlive them
    // until `http_stop`, so `/readyz` answers 503 for the whole drain.
    let (stop_tx, stop_rx) = watch::channel(false);
    let (http_stop_tx, http_stop_rx) = watch::channel(false);
    let mut forward = shutdown.clone();
    let health = Arc::clone(&ctx.health);
    let forwarder = tokio::spawn(async move {
        if forward.changed().await.is_ok() {
            health.draining();
            let _ = stop_tx.send(true);
        }
    });

    let mut http = start_health(&ctx, http_stop_rx, &forwarder).await?;

    let mut tasks: JoinSet<(&'static str, Result<(), ServerError>)> = JoinSet::new();
    if let Some(tls) = ctx.tls.clone() {
        let rx = stop_rx.clone();
        tasks.spawn(async move {
            tls.run(rx).await;
            ("tls", Ok(()))
        });
    }
    if let Some(jwt) = ctx.jwt.clone() {
        let rx = stop_rx.clone();
        tasks.spawn(async move {
            jwt.run(rx).await;
            ("jwt keys", Ok(()))
        });
    }
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

    let drain = Duration::from_secs(ctx.config.server.drain_timeout_secs);
    let mut first_error = supervise(&mut tasks, &mut http, stop_rx, &forwarder, drain).await;
    forwarder.abort();
    let _ = http_stop_tx.send(true);
    while let Some(joined) = http.join_next().await {
        if let Ok(Err(e)) = joined {
            error!(error = %e, "health server failed");
            first_error.get_or_insert(e);
        }
    }
    info!("fusescope-engine stopped");
    first_error.map_or(Ok(()), Err)
}

/// Starts the health listener, bound before any role starts, and the
/// prober; a port already taken ends the start here.
async fn start_health(
    ctx: &Context,
    http_stop_rx: watch::Receiver<bool>,
    forwarder: &tokio::task::JoinHandle<()>,
) -> Result<JoinSet<Result<(), ServerError>>, ServerError> {
    let mut http: JoinSet<Result<(), ServerError>> = JoinSet::new();
    if !ctx.config.server.metrics_addr.is_empty() {
        let listen: std::net::SocketAddr = ctx.config.server.metrics_addr.parse().map_err(|e| {
            ServerError::InvalidConfig(format!(
                "server.metrics_addr {:?}: {e}",
                ctx.config.server.metrics_addr
            ))
        })?;
        let metrics = Arc::clone(&ctx.metrics);
        let health = Arc::clone(&ctx.health);
        let rx = http_stop_rx.clone();
        let (bound_tx, bound_rx) = tokio::sync::oneshot::channel();
        http.spawn(async move {
            crate::metrics::serve(listen, metrics, health, rx, Some(bound_tx)).await
        });
        // The listener binds before any role starts: a port already taken
        // ends the start here, not after the roles are up and unwatched.
        if bound_rx.await.is_err() {
            forwarder.abort();
            let error = match http.join_next().await {
                Some(Ok(Err(e))) => e,
                Some(Err(join)) => ServerError::RoleStopped {
                    role: "health",
                    reason: join.to_string(),
                },
                _ => ServerError::InvalidConfig("health listener ended before binding".into()),
            };
            return Err(error);
        }
    }
    http.spawn(probe(ctx.clone(), http_stop_rx));
    Ok(http)
}

/// Waits for the roles, watching the health tasks with them, until every
/// role is done: a failure anywhere stops the rest, a stop starts the
/// drain and its deadline abandons what is still held.
async fn supervise(
    tasks: &mut JoinSet<(&'static str, Result<(), ServerError>)>,
    http: &mut JoinSet<Result<(), ServerError>>,
    mut stop_rx: watch::Receiver<bool>,
    forwarder: &tokio::task::JoinHandle<()>,
    drain: Duration,
) -> Option<ServerError> {
    let mut first_error: Option<ServerError> = None;
    let mut draining_since: Option<tokio::time::Instant> = None;
    loop {
        let deadline = draining_since.map(|since| {
            since
                .checked_add(drain)
                .unwrap_or_else(|| since + Duration::from_secs(86_400))
        });
        tokio::select! {
            joined = tasks.join_next() => {
                let Some(joined) = joined else { break };
                match joined {
                    Ok((_, Ok(()))) => {}
                    Ok((name, Err(e))) => {
                        error!(role = name, error = %e, "role failed");
                        first_error.get_or_insert(e);
                        forwarder.abort();
                        tasks.abort_all();
                    }
                    Err(join) if join.is_cancelled() => {}
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
            // The health listener and the prober are watched like a role:
            // a process whose /readyz died must not run on unwatched.
            // Nothing of the health side ends before the stop, so any end
            // here, clean or not, is a failure.
            Some(joined) = http.join_next(), if !http.is_empty() => {
                let failure = match joined {
                    Ok(Ok(())) => Some(ServerError::RoleStopped {
                        role: "health",
                        reason: "ended before the stop".into(),
                    }),
                    Ok(Err(e)) => Some(e),
                    Err(join) if join.is_cancelled() => None,
                    Err(join) => Some(ServerError::RoleStopped {
                        role: "health",
                        reason: join.to_string(),
                    }),
                };
                if let Some(e) = failure {
                    error!(error = %e, "health listener failed; stopping the roles");
                    first_error.get_or_insert(e);
                    forwarder.abort();
                    tasks.abort_all();
                }
            }
            _ = stop_rx.changed(), if draining_since.is_none() => {
                draining_since = Some(tokio::time::Instant::now());
            }
            () = async { tokio::time::sleep_until(deadline.unwrap_or_else(tokio::time::Instant::now)).await }, if deadline.is_some() => {
                error!(after = ?drain, "drain deadline passed; abandoning what the roles still hold");
                first_error.get_or_insert(ServerError::DrainTimeout(drain));
                tasks.abort_all();
                draining_since = None;
            }
        }
    }
    first_error
}

/// Probes the catalog and the bucket every `health_interval_secs`, for
/// `/readyz`, until `stop` flips. Each probe has two seconds.
async fn probe(ctx: Context, mut stop: watch::Receiver<bool>) -> Result<(), ServerError> {
    let every = Duration::from_secs(ctx.config.server.health_interval_secs);
    let mut tick = tokio::time::interval(every);
    let patience = Duration::from_secs(2);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let catalog = tokio::time::timeout(patience, sqlx::query("select 1").execute(ctx.catalog.pool()))
                    .await
                    .is_ok_and(|r| r.is_ok());
                let bucket = tokio::time::timeout(patience, bucket::reachable(ctx.store.as_ref(), &ctx.root))
                    .await
                    .is_ok_and(|r| r.is_ok());
                ctx.health.catalog(catalog);
                ctx.health.bucket(bucket);
            }
            _ = stop.changed() => return Ok(()),
        }
    }
}
