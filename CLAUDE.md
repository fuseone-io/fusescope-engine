# FuseScope Engine · engineering rules

The engine is FuseScope's storage and query layer for OpenTelemetry traces, logs
and metrics: Parquet on object storage, DataFusion queries, a Postgres catalog,
Arrow Flight to the Go backend. It replaces ClickHouse. The design is in
[`DESIGN.md`](DESIGN.md); decisions are recorded in [`docs/adr/`](docs/adr/) and
are binding until superseded by a new ADR.

| Repo | What | License |
|---|---|---|
| `fuseone-io/fusescope-engine` (this) | Rust workspace: `crates/fs-*`, one binary `fusescope-engine` | Apache-2.0 |
| `fuseone-io/fusescope` | Go backend, React frontend; talks to the engine over Flight SQL | MIT |
| `fuseone-io/fusescope-otel-collector` | collector; exports plain OTLP to the engine | AGPL-3.0 |

These rules apply repo-wide. Topic rules live in [`.claude/rules/`](.claude/rules/)
(comments, Rust, tests, pull requests). Everything committed is written in
English.

## License posture (non-negotiable)

- Source in this repo is **Apache-2.0**. Dependencies must be MIT, BSD, ISC or
  Apache-2.0; `cargo deny check` enforces the allow-list in [`deny.toml`](deny.toml)
  and blocks RustSec advisories. Other permissive licenses (Boost, CDLA, bzip2)
  enter only with a dependency that has no MIT or Apache alternative, and the
  FuseScope backend policy changes with them. GPL, AGPL, SSPL, BUSL and
  Commons-Clause never enter.
- **Clean-room** ([ADR 0008](docs/adr/0008-clean-room-from-otlp.md)): the OTLP to
  schema mapping is specified from the OpenTelemetry protocol, the semantic
  conventions and observed data. Never open, copy, translate or summarise the
  AGPL collector's exporter source, including from git history. Every mapping
  rule is written down with its source in
  [`crates/fs-schema/MAPPING.md`](crates/fs-schema/MAPPING.md). Say "clean-room"
  in the commit message when touching the mapping.
- Advisory exceptions in `deny.toml` carry the advisory id, why the exposure is
  acceptable, what removes it and a revisit date.
- TLS clients use the operating system trust store (`rustls-native-certs`), not
  bundled roots; container images ship `ca-certificates`.

## Tests first

- **Behaviour changes start with a failing test**: new features, limits, SQL
  surface, file formats, recovery paths. Write the test, see it fail, implement.
- **Bug fixes start with a regression test** that fails on `main` and stays in
  the tree. A reviewer's crash scenario becomes a test before the fix.
- **Crash boundaries are tested explicitly.** Anything on the durability path
  (WAL, flush, checkpoint, catalog registration) gets a test per boundary:
  before, between and after each durable step, including a restart.
- Not required for docs, generated code or behaviour-preserving refactors; the
  existing tests must still pass.
- Unit tests next to the code in `#[cfg(test)] mod tests`; integration tests in
  `crates/<crate>/tests/`. Tests that need Postgres read `FS_TEST_DATABASE_URL`,
  work in their own schema and skip with a printed `skipped:` line when it is
  unset (`make dev-up` provides it). CI always sets it; never let a database test
  silently pass by skipping in CI.
- Local bucket tests use a temporary directory through `LocalFileSystem`; the
  S3 path is exercised by `make check-env` against the compose store.
- Everything that lands in the catalog or on disk must stay readable by the
  previous version: schema and format changes bump a version and keep a reader.

## Toolchain

- Rust stable (see [`rust-toolchain.toml`](rust-toolchain.toml)); minimum version
  in `Cargo.toml` `rust-version`. Edition 2024.
- `make dev-up` starts Postgres and an S3-compatible store; `make test`,
  `make lint`, `make deny` are what CI runs. Locally they are throttled
  (`JOBS`, `TEST_THREADS`, `nice`) so the machine stays usable; only the
  benchmarks (`make bench-*`) take every core, and `make test-full` runs the
  suite at full speed when a fast final check is wanted. `make check-env` and `make run`
  drive the real binary against them.
- Dependency versions are shared in `[workspace.dependencies]` and follow the
  pins of `datafusion` (arrow, parquet, object_store) and `opentelemetry-proto`
  (prost, tonic). Bump them together.

## Rust

The repo conventions win over generic Rust advice:

| Rule | Why |
|---|---|
| `cargo fmt` clean; `cargo clippy --workspace --all-targets -- -D warnings` clean with the workspace lints (pedantic on) | CI blocks otherwise |
| `#![forbid(unsafe_code)]` everywhere except `fs-cache` and `fs-index`, where each `unsafe` block carries a comment stating the invariant | memory safety is the point of Rust |
| No `unwrap`, `expect` or `panic` outside tests; errors are `thiserror` enums per crate, converted with `?` and `From` | a bad request or a bad file must never take the process down |
| Every limit is configurable, has a default from `DESIGN.md`, and is enforced before the expensive work: message size, buffer bytes, WAL bytes, scan budget, memory pool, timeout, row cap | the engine serves tenants that do not trust each other |
| Tenant scoping is structural: a session sees only its tenant's tables, a hot-window scan returns only its tenant's rows, a ticket is bound to the tenant that planned it | isolation does not depend on a `WHERE` clause |
| Secrets come from the environment, are held in types whose `Debug` redacts them, and never reach logs; URLs are logged through `redact_url` | operability without leaks |
| Logging via `tracing`, one event per line with structured fields, right level; `debug` logs ids and sizes, never payloads | observability of the engine itself |
| Blocking I/O (WAL, Parquet encoding) runs on `spawn_blocking`; the async runtime is never blocked | throughput |
| Durable state has one source of truth: the catalog for files, the WAL for unflushed rows. Nothing is overwritten; objects are created once, rows are swapped in a transaction | recovery reasoning stays simple |
| Functions ≲ 80 lines, files ≲ 500 lines; split by responsibility | reviewability |
| Long-running tasks take a shutdown signal (`watch::Receiver<bool>`) and stop with it | clean shutdown, no leaks |
| No new global mutable state; dependencies are passed through constructors and `Arc` | testability |
| Traits are introduced where a second implementation or a test double is needed, not before | YAGNI |
| Public items have a doc comment that says what is non-obvious; `# Errors` on fallible functions | `missing_docs` is on |
| Comments only for non-obvious behaviour ([`rust-comments`](.claude/rules/rust-comments.md)) | names carry the meaning |

## Schema and compatibility

- Logical field names follow the Go backend ([ADR 0007](docs/adr/0007-logical-schema-compatibility.md));
  physical encoding is chosen for Parquet. Changing a column name or type is a
  schema version bump with a reader for the old version.
- Catalog migrations are append-only SQL files in `crates/fs-catalog/migrations`;
  never edit a migration that has shipped. Codes stored as integers (`signal`,
  `level`, `state`) are never reused.
- WAL and sidecar formats carry a version byte and a checksum; a reader that
  meets an unknown version fails loudly, a reader that meets damage degrades to a
  scan and reports it.

## Commits and pull requests

- Conventional commits, scoped by crate and imperative: `feat(ingest): …`,
  `fix(wal): …`, `docs(design): …`. One logical change per commit.
- Body explains **why**, briefly. AI-assisted commits end with the
  `Co-Authored-By: Claude … <noreply@anthropic.com>` trailer.
- Amending and force-pushing are fine on PR branches before merge; **never
  rewrite `main`**. Merges are fast-forward and only with CI green.
- PRs follow [`.github/pull_request_template.md`](.github/pull_request_template.md)
  and the [`pull-requests`](.claude/rules/pull-requests.md) rule.

## When to ask

- **Ask** before: adding a dependency, anything touching licensing or the
  clean-room boundary, a new ADR or a change to an accepted one, a change to a
  durable format (WAL, Parquet layout, catalog schema), a change to the Flight
  contract with the Go backend.
- **Don't ask** about mechanical choices (naming, formatting, module layout
  within these rules) — decide and move on.
