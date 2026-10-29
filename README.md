# fusescope-engine

Storage and query engine for OpenTelemetry traces, logs and metrics on object
storage. Built in Rust on Apache Arrow, Parquet and DataFusion. It is the
candidate replacement for ClickHouse in [FuseScope](https://github.com/fuseone-io/fusescope).

Read [DESIGN.md](DESIGN.md) first. Decisions are recorded in [docs/adr](docs/adr);
the order of work is in [docs/ROADMAP.md](docs/ROADMAP.md).

## Status

Phase 1 in progress. Done: `fs-schema` (span schema v1, OTLP conversion),
`fs-wal` (segmented write-ahead log with recovery), `fs-catalog` (Postgres file
catalog, tenants and ingest tokens, atomic compaction swaps, change
notifications), `fs-ingest` (OTLP gRPC with bearer-token authentication and
per-tenant limits, WAL, buffers, sorted Parquet L0 flush with bloom filters,
catalog registration, hot window, WAL-ordered buffers with background
coalescing), `fs-query` (DataFusion over the catalog's files with
tenant-scoped read-only sessions, catalog-level pruning, byte budgets, the
hot-window union and an Arrow Flight SQL front end) and `fs-server`
(configuration, bootstrap, roles, admin commands, `/metrics`, graceful
shutdown). What comes next, in order, is in [docs/ROADMAP.md](docs/ROADMAP.md).

## Layout

| Crate | Responsibility |
|---|---|
| `fs-schema` | Versioned Arrow schemas per signal, OTLP to Arrow conversion |
| `fs-catalog` | File catalog in Postgres, in-memory cache, LISTEN/NOTIFY, Iceberg export |
| `fs-wal` | Append-only segments with crc32c, replay |
| `fs-ingest` | OTLP/OTel-Arrow server, limits, buffering, flush, hot window |
| `fs-index` | xor/bloom filters, ngram bloom, roaring postings, trace_id index |
| `fs-cache` | foyer wrapper for footers, page indexes, sidecars and pages |
| `fs-query` | DataFusion TableProviders, pruning, Flight SQL, typed actions, SQL sandbox |
| `fs-compactor` | Compaction levels, derived tables, promotions, retention |
| `fs-auth` | mTLS, tenant JWT, per-role identity |
| `fs-server` | Single binary, `--role ingester|querier|compactor|all` |
| `fs-bench` | Benchmarks: ingest stages in process, OTLP load generator, Flight SQL query suite |

## Development

```sh
make dev-up               # Postgres on :55432 (FS_DEV_POSTGRES_PORT) and an S3 store (RustFS) on :9100 via Docker
make test                 # cargo test, throttled (JOBS, TEST_THREADS, nice), with FS_TEST_DATABASE_URL pointing at that Postgres
make test-full            # the same suite with every core
make lint                 # fmt and clippy
make deny                 # licenses, advisories, sources
make check-env            # fusescope-engine --check: validates config and connectivity
make run                  # fusescope-engine --role all against the dev services
make tenant NAME=acme     # create a tenant; prints its id
make token TENANT=<id> NAME=collector   # issue an ingest token; printed once
make dev-down
```

Benchmarks live in `fs-bench` and results in [docs/benchmarks/](docs/benchmarks/):
`make bench-stages` decomposes the ingest path in process, `make bench-load`
drives a running ingester over OTLP, `make bench-query` runs the DESIGN.md query
suite over Flight SQL. The engine exposes Prometheus metrics on `:9464`.

Point an OpenTelemetry collector's OTLP gRPC exporter at `localhost:4317` with
`authorization: Bearer <token>` in its headers to send traces. Query with any
Arrow Flight SQL client against `localhost:8815`, sending a `query`-scope token
in the same header; the table is `spans`, the dialect is PostgreSQL-like, and
only read statements are accepted.

Configuration is a TOML file ([config/dev.toml](config/dev.toml) for the dev
services) plus environment overrides. Secrets come only from the environment:
`FS_CATALOG_URL`, `FS_BUCKET_ACCESS_KEY`, `FS_BUCKET_SECRET_KEY`. Without
bucket keys the S3 client uses the AWS environment and instance metadata, which
is how Kubernetes workload identity is picked up.

Database tests live in `crates/fs-catalog/tests` and run only when
`FS_TEST_DATABASE_URL` is set; otherwise they print `skipped` and pass. Each
test works in its own schema, so they run in parallel. CI provides a Postgres
service and always runs them.

## Clean-room rule

Parts of FuseScope's collector are AGPL-3.0. Nothing in this repository may be
derived from that code. The OTLP mapping is specified from the OpenTelemetry
protocol, the semantic conventions and observed data. See
[ADR 0008](docs/adr/0008-clean-room-from-otlp.md).

## License

Apache-2.0. Dependencies must be MIT, BSD, ISC or Apache-2.0; `cargo deny`
enforces it. Other permissive licenses (for example Boost, BSL-1.0) are added
to [deny.toml](deny.toml) only when a dependency needs them and no MIT or
Apache alternative exists, and the FuseScope backend policy changes with them.
