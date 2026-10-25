//! FuseScope Engine entry point.
//!
//! ```text
//! fusescope-engine --config <file> [--role ingester|querier|compactor|all] [--check]
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use fs_server::config::{Config, LogFormat, Secrets};
use fs_server::role::Role;
use fs_server::{ServerError, server};
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

const USAGE: &str = "usage: fusescope-engine --config <file> [--role <ingester|querier|compactor|all>] [--check] [--version]";

#[derive(Debug)]
struct Args {
    config: Option<PathBuf>,
    role: Option<Role>,
    check: bool,
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
    };
    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
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
    let path = args
        .config
        .ok_or_else(|| ServerError::InvalidConfig(format!("--config is required\n{USAGE}")))?;
    let mut config = Config::load(&path)?;
    config.apply_env(&env)?;
    let secrets = Secrets::from_env(&env);
    init_tracing(&config.log.level, config.log.format);
    let node_id = config.node_id(&env);
    let role = args.role.unwrap_or(config.server.role);

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
