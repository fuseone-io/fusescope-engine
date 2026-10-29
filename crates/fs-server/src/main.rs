//! FuseScope Engine entry point.
//!
//! ```text
//! fusescope-engine --config <file> [--role ingester|querier|compactor|all] [--check]
//! fusescope-engine --config <file> admin tenant create <name>
//! fusescope-engine --config <file> admin token create <tenant-id> <name> [ingest|query]
//! FS_QUERY_TOKEN=... fusescope-engine admin query <addr> <sql>
//! FS_QUERY_TOKEN=... fusescope-engine admin action <addr> <type> [json]
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use fs_server::config::{Config, LogFormat, Secrets};
use fs_server::role::Role;
use fs_server::server::AdminAction;
use fs_server::{ServerError, client, server};
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

const USAGE: &str = "usage: fusescope-engine --config <file> [--role <ingester|querier|compactor|all>] [--check] [--version]
       fusescope-engine --config <file> admin tenant create <name>
       fusescope-engine --config <file> admin token create <tenant-id> <name> [ingest|query]
       FS_QUERY_TOKEN=<token> fusescope-engine admin query <addr> <sql>
       FS_QUERY_TOKEN=<token> fusescope-engine admin action <addr> <type> [json]";

#[derive(Debug)]
struct Args {
    config: Option<PathBuf>,
    role: Option<Role>,
    check: bool,
    admin: Option<Admin>,
}

/// What `admin` does: a catalog action through the configuration, or a
/// client call against a running querier that needs no configuration.
#[derive(Debug, PartialEq)]
enum Admin {
    Catalog(AdminAction),
    Query {
        addr: String,
        sql: String,
    },
    Action {
        addr: String,
        kind: String,
        body: String,
    },
}

#[derive(Debug)]
enum Early {
    Version,
    Help,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Result<Args, Early>, String> {
    let mut out = Args {
        config: None,
        role: None,
        check: false,
        admin: None,
    };
    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "admin" => {
                let rest: Vec<String> = args.by_ref().collect();
                out.admin = Some(parse_admin(&rest)?);
            }
            "--version" => return Ok(Err(Early::Version)),
            "-h" | "--help" => return Ok(Err(Early::Help)),
            "--check" => out.check = true,
            "--config" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("--config needs a path\n{USAGE}"))?;
                out.config = Some(PathBuf::from(value));
            }
            "--role" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("--role needs a value\n{USAGE}"))?;
                out.role = Some(value.parse().map_err(|e| format!("{e}\n{USAGE}"))?);
            }
            other => return Err(format!("unexpected argument {other:?}\n{USAGE}")),
        }
    }
    Ok(Ok(out))
}

fn parse_admin(rest: &[String]) -> Result<Admin, String> {
    let words: Vec<&str> = rest.iter().map(String::as_str).collect();
    match words.as_slice() {
        ["tenant", "create", name] => Ok(Admin::Catalog(AdminAction::CreateTenant {
            name: (*name).to_owned(),
        })),
        ["query", addr, sql] => Ok(Admin::Query {
            addr: (*addr).to_owned(),
            sql: (*sql).to_owned(),
        }),
        ["action", addr, kind] | ["action", addr, kind, _] => Ok(Admin::Action {
            addr: (*addr).to_owned(),
            kind: (*kind).to_owned(),
            body: words.get(3).map_or("{}", |b| b).to_owned(),
        }),
        ["token", "create", tenant, name] | ["token", "create", tenant, name, _] => {
            let tenant = tenant
                .parse()
                .map_err(|e| format!("tenant id {tenant:?}: {e}\n{USAGE}"))?;
            let scope = words
                .get(4)
                .map_or(Ok(fs_catalog::TokenScope::Ingest), |s| {
                    s.parse().map_err(|e| format!("{e}\n{USAGE}"))
                })?;
            Ok(Admin::Catalog(AdminAction::CreateToken {
                tenant,
                name: (*name).to_owned(),
                scope,
            }))
        }
        _ => Err(format!("unknown admin command\n{USAGE}")),
    }
}

fn init_tracing(level: &str, format: LogFormat) {
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false);
    match format {
        LogFormat::Json => builder.json().init(),
        LogFormat::Text => builder.init(),
    }
}

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

async fn shutdown_signal(tx: watch::Sender<bool>) {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = %e, "SIGTERM handler unavailable");
                    let _ = ctrl_c.await;
                    let _ = tx.send(true);
                    return;
                }
            };
        tokio::select! {
            _ = ctrl_c => tracing::info!("SIGINT received"),
            _ = term.recv() => tracing::info!("SIGTERM received"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
    let _ = tx.send(true);
}

async fn real_main(args: Args) -> Result<(), ServerError> {
    let query_token = || {
        Secrets::from_env(&env).query_token.ok_or_else(|| {
            ServerError::InvalidConfig(format!(
                "FS_QUERY_TOKEN is required for admin query and admin action\n{USAGE}"
            ))
        })
    };
    let admin = match args.admin {
        Some(Admin::Query { addr, sql }) => {
            let batches = client::query(&addr, &query_token()?, &sql).await?;
            println!("{}", client::render(&batches)?);
            return Ok(());
        }
        Some(Admin::Action { addr, kind, body }) => {
            let batches = client::action(&addr, &query_token()?, &kind, &body).await?;
            println!("{}", client::render(&batches)?);
            return Ok(());
        }
        Some(Admin::Catalog(action)) => Some(action),
        None => None,
    };
    let path = args
        .config
        .ok_or_else(|| ServerError::InvalidConfig(format!("--config is required\n{USAGE}")))?;
    let mut config = Config::load(&path)?;
    config.apply_env(&env)?;
    let secrets = Secrets::from_env(&env);
    init_tracing(&config.log.level, config.log.format);
    let node_id = config.node_id(&env);
    let role = args.role.unwrap_or(config.server.role);

    if let Some(action) = admin {
        for line in server::admin(config, secrets, action).await? {
            println!("{line}");
        }
        return Ok(());
    }
    if args.check {
        let report = server::check(config, secrets, node_id).await?;
        println!(
            "ok: node_id={} bucket={} catalog={} live_ingesters={}",
            report.node_id, report.bucket_ok, report.catalog_ok, report.live_ingesters
        );
        return Ok(());
    }

    let (tx, rx) = watch::channel(false);
    tokio::spawn(shutdown_signal(tx));
    server::run(config, secrets, role, node_id, rx).await
}

fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(Ok(args)) => args,
        Ok(Err(Early::Version)) => {
            println!("fusescope-engine {}", fs_server::VERSION);
            return ExitCode::SUCCESS;
        }
        Ok(Err(Early::Help)) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(2);
        }
    };
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(real_main(args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("fusescope-engine: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> impl Iterator<Item = String> {
        list.iter()
            .map(|s| (*s).to_owned())
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[test]
    fn parses_flags() {
        let parsed = parse_args(args(&[
            "--config", "x.toml", "--role", "querier", "--check",
        ]))
        .unwrap()
        .ok()
        .unwrap();
        assert_eq!(parsed.config, Some(PathBuf::from("x.toml")));
        assert_eq!(parsed.role, Some(Role::Querier));
        assert!(parsed.check);
    }

    #[test]
    fn rejects_unknown_role_and_flags() {
        assert!(
            parse_args(args(&["--role", "nope"]))
                .unwrap_err()
                .contains("unknown role")
        );
        assert!(
            parse_args(args(&["--bogus"]))
                .unwrap_err()
                .contains("unexpected argument")
        );
        assert!(
            parse_args(args(&["--config"]))
                .unwrap_err()
                .contains("needs a path")
        );
    }

    #[test]
    fn parses_admin_commands() {
        let parsed = parse_args(args(&[
            "--config", "x.toml", "admin", "tenant", "create", "acme",
        ]))
        .unwrap()
        .ok()
        .unwrap();
        assert_eq!(
            parsed.admin,
            Some(Admin::Catalog(AdminAction::CreateTenant {
                name: "acme".into()
            }))
        );
        let id = "00000000-0000-0000-0000-000000000001";
        let parsed = parse_args(args(&[
            "--config",
            "x.toml",
            "admin",
            "token",
            "create",
            id,
            "collector",
        ]))
        .unwrap()
        .ok()
        .unwrap();
        assert!(
            matches!(parsed.admin, Some(Admin::Catalog(AdminAction::CreateToken { name, .. })) if name == "collector")
        );
        let parsed = parse_args(args(&["admin", "query", "127.0.0.1:8815", "select 1"]))
            .unwrap()
            .ok()
            .unwrap();
        assert_eq!(
            parsed.admin,
            Some(Admin::Query {
                addr: "127.0.0.1:8815".into(),
                sql: "select 1".into(),
            })
        );
        let parsed = parse_args(args(&[
            "admin",
            "action",
            "127.0.0.1:8815",
            "metadata.services",
        ]))
        .unwrap()
        .ok()
        .unwrap();
        assert!(matches!(parsed.admin, Some(Admin::Action { ref body, .. }) if body == "{}"));
        assert!(
            parse_args(args(&[
                "admin",
                "query",
                "127.0.0.1:8815",
                "fst_x",
                "select 1"
            ]))
            .unwrap_err()
            .contains("unknown admin command"),
            "a token on the command line is not accepted"
        );
        assert!(
            parse_args(args(&["admin", "token", "create", "not-a-uuid", "x"]))
                .unwrap_err()
                .contains("tenant id")
        );
        assert!(
            parse_args(args(&["admin", "frobnicate"]))
                .unwrap_err()
                .contains("unknown admin command")
        );
    }

    #[test]
    fn version_and_help_exit_early() {
        assert!(matches!(
            parse_args(args(&["--version"])),
            Ok(Err(Early::Version))
        ));
        assert!(matches!(
            parse_args(args(&["--help"])),
            Ok(Err(Early::Help))
        ));
    }
}
