---
paths:
  - "**/*.rs"
  - "**/Cargo.toml"
---

# Contribution guidelines

- Read [`DESIGN.md`](../../DESIGN.md) and the ADRs in [`docs/adr/`](../../docs/adr/) before changing a durable format, a limit, the Flight contract or the catalog schema. A change that contradicts an accepted ADR needs a new ADR first.
- Look for existing patterns in the workspace before implementing: `TestDb` for database tests, `Limits` for per-record bounds, `QueryWarnings` for partial results, the `watch::Receiver<bool>` shutdown signal, `spawn_blocking` for file I/O.
- Dependencies are declared once in `[workspace.dependencies]` and referenced with `{ workspace = true }`. Disable default features when they pull code we do not use (`datafusion` without `compression`, `sqlx` with native TLS roots). Run `cargo deny check` before proposing one.
- Crate boundaries: `fs-schema` owns Arrow schemas and OTLP conversion; `fs-wal` owns the log format; `fs-catalog` owns Postgres and nothing above it; `fs-ingest` and `fs-query` own their protocols; `fs-server` wires roles and configuration. Shared protocol types (`HotScanRequest`, headers) live in the crate that serves them.
- Within a file: constants, then public types and their methods, then private types and functions, then tests.
- Catalog migrations are append-only and import nothing from Rust; a migration file is complete on its own.
- Avoid breaking function calls into multiple lines for a couple of arguments; let `rustfmt` decide.
- Run `cargo fmt --all` before committing; CI checks it.
