//! FuseScope Engine entry point.
//!
//! Usage: `fusescope-engine --role <ingester|querier|compactor|all>`
//!
//! Phase 0: parses the role and reports the crate versions it would start.
//! Real servers are wired in as each crate lands.

mod role;

use std::process::ExitCode;

use role::Role;

const USAGE: &str = "usage: fusescope-engine --role <ingester|querier|compactor|all> [--version]";

fn main() -> ExitCode {
    match run(std::env::args().skip(1)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::from(2)
        }
    }
}

fn run(args: impl Iterator<Item = String>) -> Result<(), String> {
    let mut role: Option<Role> = None;
    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--version" => {
                println!("fusescope-engine {}", env!("CARGO_PKG_VERSION"));
                return Ok(());
            }
            "--role" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("--role needs a value\n{USAGE}"))?;
                role = Some(value.parse().map_err(|e| format!("{e}\n{USAGE}"))?);
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            other => return Err(format!("unexpected argument {other:?}\n{USAGE}")),
        }
    }
    let role = role.ok_or_else(|| format!("--role is required\n{USAGE}"))?;

    println!(
        "fusescope-engine {} starting role={role}",
        env!("CARGO_PKG_VERSION")
    );
    for r in role.expand() {
        println!("  {r}: not implemented yet (phase 0)");
    }
    println!(
        "  crates: schema={} catalog={} wal={} ingest={} index={} cache={} query={} compactor={} auth={}",
        fs_schema::VERSION,
        fs_catalog::VERSION,
        fs_wal::VERSION,
        fs_ingest::VERSION,
        fs_index::VERSION,
        fs_cache::VERSION,
        fs_query::VERSION,
        fs_compactor::VERSION,
        fs_auth::VERSION,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::run;

    fn args(list: &[&str]) -> impl Iterator<Item = String> {
        list.iter()
            .map(|s| (*s).to_owned())
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[test]
    fn requires_role() {
        let err = run(args(&[])).unwrap_err();
        assert!(err.contains("--role is required"));
    }

    #[test]
    fn rejects_unknown_role() {
        let err = run(args(&["--role", "nope"])).unwrap_err();
        assert!(err.contains("unknown role"));
    }

    #[test]
    fn rejects_unknown_flag() {
        let err = run(args(&["--bogus"])).unwrap_err();
        assert!(err.contains("unexpected argument"));
    }

    #[test]
    fn accepts_all() {
        assert!(run(args(&["--role", "all"])).is_ok());
    }
}
