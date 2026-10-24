# fusescope-engine

Storage and query engine for OpenTelemetry traces, logs and metrics on object
storage. Built in Rust on Apache Arrow, Parquet and DataFusion. It is the
candidate replacement for ClickHouse in [FuseScope](https://github.com/fuseone-io/fusescope).

Read [DESIGN.md](DESIGN.md) first. Decisions are recorded in [docs/adr](docs/adr).

## Status

Phase 0, foundation. The workspace compiles; no crate does real work yet.

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

## Development

```sh
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo deny check          # licenses, advisories, sources
```

## License

Apache-2.0. Dependencies must be MIT, BSD, ISC or Apache-2.0; `cargo deny`
enforces it.
