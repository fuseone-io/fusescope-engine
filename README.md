# fusescope-engine

Storage and query engine for OpenTelemetry traces, logs and metrics on object
storage. Built in Rust on Apache Arrow, Parquet and DataFusion. It is the
candidate replacement for ClickHouse in [FuseScope](https://github.com/fuseone-io/fusescope).

Read [DESIGN.md](DESIGN.md) first. Decisions are recorded in [docs/adr](docs/adr).

## Status

Phase 0, foundation. Implemented: `fs-schema` (span schema v1, OTLP
conversion), `fs-wal` (segmented write-ahead log with recovery) and
`fs-catalog` (Postgres file catalog with atomic compaction swaps and change
notifications). The other crates are placeholders.

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
make dev-up               # Postgres on :5433 and MinIO on :9000 via Docker
make test                 # cargo test, with FS_TEST_DATABASE_URL pointing at that Postgres
make lint                 # fmt and clippy
make deny                 # licenses, advisories, sources
make dev-down
```

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
