//! `fs-bench`: measure before optimising.
//!
//! ```text
//! fs-bench stages [--requests N] [--spans-per-trace K] [--services S] [--sync batch|always]
//!                 [--store local|s3] [--json out.json]
//! fs-bench load   --endpoint host:4317 --token fst_… [--concurrency C] [--duration-secs D]
//!                 [--spans-per-trace K] [--services S] [--json out.json]
//! fs-bench query  --endpoint host:8815 --token fst_… [--iterations N] [--json out.json]
//! ```
//!
//! `--store s3` reads `FS_BUCKET_ENDPOINT`, `FS_BUCKET_NAME`, `FS_BUCKET_REGION`,
//! `FS_BUCKET_ACCESS_KEY` and `FS_BUCKET_SECRET_KEY`.

mod load;
mod query;
mod stages;
mod stats;
mod traffic;

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use fs_wal::SyncPolicy;
use object_store::ObjectStore;

use crate::traffic::Shape;

const USAGE: &str = "usage:
  fs-bench stages [--requests N] [--spans-per-trace K] [--services S] [--sync batch|always] [--store local|s3] [--wal-dir DIR] [--json out.json]
  fs-bench load   --endpoint host:4317 --token fst_... [--concurrency C] [--duration-secs D] [--spans-per-trace K] [--services S] [--metrics host:9464] [--json out.json]
  fs-bench query  --endpoint host:8815 --token fst_... [--iterations N] [--json out.json]";

type Flags = HashMap<String, String>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;
/// A store, its key prefix, and the temporary directory that backs a local one.
type StoreChoice = (Arc<dyn ObjectStore>, String, Option<tempfile::TempDir>);

fn parse(args: &[String]) -> Result<(String, Flags), String> {
    let mode = args.first().cloned().ok_or_else(|| USAGE.to_owned())?;
    let mut flags = Flags::new();
    let mut rest = args.iter().skip(1);
    while let Some(arg) = rest.next() {
        let key = arg
            .strip_prefix("--")
            .ok_or_else(|| format!("unexpected argument {arg:?}\n{USAGE}"))?;
        let value = rest
            .next()
            .ok_or_else(|| format!("--{key} needs a value\n{USAGE}"))?;
        flags.insert(key.to_owned(), value.clone());
    }
    Ok((mode, flags))
}

fn get<T: std::str::FromStr>(flags: &Flags, key: &str, default: T) -> Result<T, String> {
    match flags.get(key) {
        None => Ok(default),
        Some(v) => v.parse().map_err(|_| format!("--{key} {v:?} is not valid")),
    }
}

fn required(flags: &Flags, key: &str) -> Result<String, String> {
    flags
        .get(key)
        .cloned()
        .ok_or_else(|| format!("--{key} is required\n{USAGE}"))
}

fn shape(flags: &Flags) -> Result<Shape, String> {
    let defaults = Shape::default();
    Ok(Shape {
        services: get(flags, "services", defaults.services)?,
        operations: get(flags, "operations", defaults.operations)?,
        spans_per_trace: get(flags, "spans-per-trace", defaults.spans_per_trace)?,
        extra_attributes: get(flags, "extra-attributes", defaults.extra_attributes)?,
        error_rate: get(flags, "error-rate", defaults.error_rate)?,
        window_secs: get(flags, "window-secs", defaults.window_secs)?,
    })
}

fn store(flags: &Flags) -> Result<StoreChoice, BoxError> {
    match flags.get("store").map_or("local", String::as_str) {
        "local" => {
            let dir = tempfile::tempdir()?;
            let store = object_store::local::LocalFileSystem::new_with_prefix(dir.path())?;
            Ok((Arc::new(store), String::new(), Some(dir)))
        }
        "s3" => {
            let env = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
            let mut builder = object_store::aws::AmazonS3Builder::from_env()
                .with_allow_http(true)
                .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
                .with_virtual_hosted_style_request(false);
            if let Some(v) = env("FS_BUCKET_ENDPOINT") {
                builder = builder.with_endpoint(v);
            }
            if let Some(v) = env("FS_BUCKET_NAME") {
                builder = builder.with_bucket_name(v);
            }
            if let Some(v) = env("FS_BUCKET_REGION") {
                builder = builder.with_region(v);
            }
            if let (Some(ak), Some(sk)) = (env("FS_BUCKET_ACCESS_KEY"), env("FS_BUCKET_SECRET_KEY"))
            {
                builder = builder.with_access_key_id(ak).with_secret_access_key(sk);
            }
            Ok((Arc::new(builder.build()?), String::new(), None))
        }
        other => Err(format!("--store {other:?}: expected local or s3").into()),
    }
}

fn write_json<T: serde::Serialize>(
    flags: &Flags,
    value: &T,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(path) = flags.get("json") {
        std::fs::write(path, serde_json::to_vec_pretty(value)?)?;
        println!("json written to {path}");
    }
    Ok(())
}

async fn real_main(
    mode: &str,
    flags: &Flags,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match mode {
        "stages" => {
            let sync = match flags.get("sync").map_or("batch", String::as_str) {
                "batch" => SyncPolicy::Batch,
                "always" => SyncPolicy::Always,
                other => return Err(format!("--sync {other:?}: expected batch or always").into()),
            };
            let (store, prefix, _keep) = store(flags)?;
            // The WAL must live on a real disk: a tmpfs makes fsync free and the
            // numbers meaningless. Default to the working directory.
            let wal_dir = std::path::PathBuf::from(
                flags
                    .get("wal-dir")
                    .cloned()
                    .unwrap_or_else(|| "./data/bench-wal".to_owned()),
            );
            let _ = std::fs::remove_dir_all(&wal_dir);
            let report = stages::run(stages::StageParams {
                requests: get(flags, "requests", 2_000)?,
                shape: shape(flags)?,
                sync,
                wal_dir: wal_dir.clone(),
                store,
                prefix,
                seed: get(flags, "seed", 42)?,
            })
            .await?;
            stages::print(&report);
            write_json(flags, &report)?;
            let _ = std::fs::remove_dir_all(&wal_dir);
        }
        "load" => {
            let report = load::run(load::LoadParams {
                endpoint: required(flags, "endpoint")?,
                token: required(flags, "token")?,
                concurrency: get(flags, "concurrency", 8)?,
                duration: Duration::from_secs(get(flags, "duration-secs", 30)?),
                shape: shape(flags)?,
                seed: get(flags, "seed", 42)?,
                metrics: flags.get("metrics").cloned(),
            })
            .await?;
            load::print(&report);
            write_json(flags, &report)?;
        }
        "query" => {
            let results = query::run(query::QueryParams {
                endpoint: required(flags, "endpoint")?,
                token: required(flags, "token")?,
                iterations: get(flags, "iterations", 10)?,
            })
            .await?;
            query::print(&results);
            write_json(flags, &results)?;
        }
        other => return Err(format!("unknown mode {other:?}\n{USAGE}").into()),
    }
    Ok(())
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (mode, flags) = match parse(&args) {
        Ok(p) => p,
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
    match runtime.block_on(real_main(&mode, &flags)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("fs-bench: {e}");
            ExitCode::FAILURE
        }
    }
}
