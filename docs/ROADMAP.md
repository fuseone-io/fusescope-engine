# Roadmap

The order of work from the current state to a version that can be tested for
real, then to the phases that follow. [`DESIGN.md`](../DESIGN.md) says what
the engine is; this file says what to do next and in which order, and it is
updated in the same pull request as the work it describes.

## How to read it

- One line per item, in the order it should be done. Items in the same
  group can be reordered inside the group; groups are ordered by dependency.
- Each item names its exit test. Durability items name the crash boundary
  their test covers; performance items name the benchmark file that will
  show the result (`docs/benchmarks/`).
- Size is a rough effort: **S** under a day, **M** a few days, **L** a week
  or more.
- Status: `todo`, `doing`, `done` (with the commit), `dropped` (with the
  reason). Nothing is deleted from the list; it is marked.

## What "testable for real" means

Milestone **M1**: a FuseScope staging environment shows traces served by the
engine end to end, with the collector writing to ClickHouse and to the
engine at the same time, and a diff of query results between the two
backends on the same data.

Concretely:

1. The collector exports OTLP to the engine ingesters ([ADR 0006](adr/0006-collector-exports-otlp.md)).
2. The engine ingests, flushes, compacts L0 to L1 and serves the hot window
   for one or more tenants, surviving restarts without loss or duplicates.
3. The Go backend talks to the engine over Flight SQL through
   `flighttelemetrystore`, selected by `telemetrystore.provider: flight`,
   and the trace views of the UI work on it: trace by id, span list with
   filters, service list, latency per service, top operations.
4. The results of those views match ClickHouse on the same data, and the
   trace targets of DESIGN.md section 9.2 are within the same order of
   magnitude.
5. The engine runs in staging for two weeks with its metrics on a dashboard
   and without manual intervention.

Everything before item 3 is engine work in this repository. Items 3 to 5
need the Go backend and the collector too.

## Where we are

Done, in this repository (phase 0 and most of the engine side of phase 1):

- Workspace, lints, cargo-deny allow-list, CI (`check`, `deny`).
- `fs-schema`: span schema v1, clean-room OTLP conversion, limits, compacted
  batches.
- `fs-wal`: segments, crc32c, checkpoint file, instance id, replay.
- `fs-catalog`: migrations 0001 to 0003, files, tenants, tokens
  (`ingest` and `query` scopes), idempotent registration with content
  verification, atomic swaps, purge of quarantined files, change
  notifications.
- `fs-ingest`: OTLP gRPC with bearer tokens and per-tenant limits, group
  commit WAL writer, strict admission against per-tenant and global caps,
  flush intents and the gate, file ids derived from the log, retries that
  never change a file's content, hot window over internal Flight with final
  file-id announcements, WAL-ordered buffers with background coalescing,
  histograms for hot-scan and checkpoint latency.
- `fs-query`: tenant-scoped DataFusion sessions, SELECT-only gate, catalog
  pruning, byte budget, hot-window union with partial-result warnings,
  Flight SQL front end.
- `fs-server`: configuration, bootstrap, roles, admin commands, `/metrics`,
  graceful shutdown. `fs-compactor`: purge of quarantined files only.
- `fs-bench`: ingest stages, OTLP load generator with `/metrics` deltas,
  Flight SQL query suite. Benchmarks recorded for the baseline, group
  commit, flush stages, coalescing and the gate.

Measured today (32 senders, one tenant, NVMe WAL): about 8 000 requests/s,
99 000 spans/s alone and 88 000 with queries running; flush of 200 k rows in
about 0.9 s, 60% of it Parquet encode; 98% of request batches merged into
chunks before their flush; hot-window snapshot p95 within 2 ms; `trace by
id` up to 2 s p95 under load because nothing narrows the hot scan or the
file list by trace id; the per-tenant cap is met by 15 s of retained rows.

## M1 · traces end to end

### Group A · ingest path, finish what is open

| # | Item | Why | Exit test | Size | Status |
|---|---|---|---|---|---|
| A1 | Parallel coalescer: several coalesce ids per tenant over non-overlapping runs, a global semaphore (`max_coalesces`), group size by bytes, task and bytes gauges; flushes materialised off the flusher loop under `max_flushes` | one chunk at a time fell behind at 32 senders, so the retention held raw batches and the tenant cap tripped | buffer tests for disjoint runs and cancellation; `coalesced_flush_matches_replay_byte_for_byte` green; [benchmark](benchmarks/2026-10-26-parallel-coalesce.md): 98% of batches merged before their flush, 99 k spans/s | M | done (edbccfb, 12b85a4) |
| A2 | Persisted retention holds the file's rows as written, one sorted batch, and the default retention (8 s) is validated against the querier's file cache TTL plus a hot scan | retention was the main memory consumer | memory cap tests with retention; benchmark of persisted bytes pending | S | done, benchmark pending |
| A3 | Parquet encode by row group as the buffer fills (streaming), bounded encode concurrency | encode is 2.5 µs per row and the largest flush stage | flush stages benchmark; crash between row groups keeps idempotency (object created once) | M | todo |
| A4 | Flush concurrency across tenants with a bounded semaphore | one slow tenant must not delay the others | multi-tenant flush test; benchmark with 4 tenants | S | doing: `max_flushes` landed with A1 and covers new cuts and retries; the multi-tenant test and the four-tenant benchmark are missing |
| A5 | End-of-hour flush boundary so an L0 file never spans two hours | L1 compaction and the trace index are per hour | flush test with spans across an hour boundary produces two files | S | todo |
| A6 | Ingest health: `/healthz` with WAL, bucket and catalog state; shutdown drains in-flight requests with a deadline | staging needs liveness and readiness | server test for the endpoints and the drain | S | todo |

### Group B · query path for traces

| # | Item | Why | Exit test | Size | Status |
|---|---|---|---|---|---|
| B1 | Typed `trace_id` and `span_id` predicates in `HotScanRequest` (`#[serde(default)]`, optional), applied before rows leave the ingester | the hot scan shipped every retained row | `hot_scan_keeps_only_the_requested_traces_and_spans`, pruning unit test, SQL lookup test; query benchmark `trace by id` pending | S | done, benchmark pending |
| B2 | Hot trace index: `trace_id → batches` in the ingester, rebuilt per chunk | narrows the in-memory scan to a few batches | unit test on the index; benchmark | M | todo |
| B3 | `index/trace_id/*.tix` sidecar per hour written by the flush and the compactor, with the xxh3 header; querier opens only the listed files | thousands of files per trace lookup otherwise | sidecar round-trip test; corrupt sidecar degrades to a scan; `trace by id` within the 9.2 target | L | todo |
| B4 | Typed action `trace.lookup` over Flight `DoAction` returning the spans of one trace | the Go backend's trace view should not build SQL for the most common query | action test; Go client call | M | todo |
| B5 | Prepared-plan ticket: `GetFlightInfo` plans once, `DoGet` executes the ticket bound to the tenant | every statement is planned twice today, a 125 ms floor | query test asserting one planning per statement; query benchmark | M | todo |
| B6 | Service and operation metadata actions (`metadata.services`, `metadata.operations`) and the `attribute_keys` derived table | the UI's service list and filter suggestions need them | action tests; Go client call | M | todo |
| B7 | SQL sandbox denial tests for every blocked construct (DDL, DML, `COPY`, functions that touch the file system, cross-tenant references) | DESIGN.md section 10.2 promises it; CI must prove it | one test per construct | S | todo |

### Group C · compactor and durability

| # | Item | Why | Exit test | Size | Status |
|---|---|---|---|---|---|
| C1 | Compactor leader lease in the catalog, single active compactor per deployment | two compactors would race on the same files | lease test with two instances | S | todo |
| C2 | L0 to L1 per closed hour: re-sort, 512 MB targets, quarantine and swap in one transaction, `tix` rebuilt | small L0 files make queries open hundreds of files | compaction test with crash before and after the swap; query results identical before and after | L | todo |
| C3 | Orphan collector: objects without a catalog row are quarantined, then deleted after the TTL | a crash between upload and registration leaves objects behind | test creating an orphan and running the collector | S | todo |
| C4 | Retention by tenant: delete day prefixes beyond `retention_days`, catalog first, bucket second | staging keeps data for a bounded time | retention test with a crash between the two steps | S | todo |
| C5 | Content digest column on `files` (sha256 of the object), verified on re-registration | the no-overwrite guarantee rests on size and metadata today; needs a decision because it is a catalog schema change | migration 0004; register test with a different object of the same size fails | S | todo, decision pending |
| C6 | `trace_summary` and `service_graph_1m` derived tables updated by the compactor | the UI's trace list and service map read them | derived-table tests; Go client call | L | todo |

### Group D · Go backend and collector

Work in `fuseone-io/fusescope` and `fuseone-io/fusescope-otel-collector`,
tracked here so the order stays visible.

| # | Item | Why | Exit test | Size | Status |
|---|---|---|---|---|---|
| D1 | Collector: OTLP exporter to the engine next to the ClickHouse exporter, dual-write by configuration | both backends receive the same data during the comparison | collector config test; spans arrive in both | S | todo |
| D2 | Go: `telemetrystore` interface with `Query` and `Do`, `flighttelemetrystore` with mTLS and tenant token, `telemetrystore.provider` flag | the backend needs a second provider before any view can switch | Go unit tests with a Flight test server; integration against `fusescope-engine --role all` | L | todo |
| D3 | Go: DataFusion dialect for the trace statement builders, tested against the same cases as the ClickHouse dialect | the trace views build SQL through these builders | builder tests pass for both dialects | L | todo |
| D4 | Go: trace views on the Flight provider (trace by id, span list, services, latency, top operations) | the M1 user-facing check | end-to-end test in the backend repository | M | todo |
| D5 | Result diff tool: runs the same view queries against both providers and reports differences | parity is the exit criterion of phase 1 | tool in the backend repository, run in staging | M | todo |

### Group E · security before staging

| # | Item | Why | Exit test | Size | Status |
|---|---|---|---|---|---|
| E1 | mTLS on Flight SQL and the internal hot-window Flight, OS trust store, certificate reload | DESIGN.md section 10.4; staging is a shared network | handshake tests with a bad client certificate refused | M | todo |
| E2 | Tenant JWT from the Go backend to the querier, bound to the session and the ticket | the query token today is a static bearer; the backend acts for many tenants | token test: a ticket from one tenant is refused by another | M | todo |
| E3 | Token rotation and revocation in `admin`, cached tokens expire within the TTL | operations | catalog and cache tests | S | todo |

### Group F · operations for staging

| # | Item | Why | Exit test | Size | Status |
|---|---|---|---|---|---|
| F1 | Container image with `ca-certificates`, non-root, read-only file system except WAL and spill | deployment | image builds in CI; `--check` passes inside it | S | todo |
| F2 | Helm chart: one `values`, roles as deployments, WAL on a persistent volume, Postgres and bucket secrets from the environment | staging | chart lints; `helm template` in CI | M | todo |
| F3 | Grafana dashboard from `/metrics`: ingest rate, refusals, flush stages, coalescer, buffers, hot-scan and checkpoint histograms, query latency | the two-week soak needs eyes | dashboard JSON in `docs/ops/` | S | todo |
| F4 | Runbook: restart, WAL replay, checkpoint, quarantine, orphan collector, token issue, what each refusal means | the people on call during the soak | `docs/ops/runbook.md` | S | todo |

### M1 order

A1 → B1 → B2 → A2 → A3 → B3 → C1 → C2 → B5 → B4 → B6 → B7 → A4 → A5 →
A6 → C3 → C4 → C6 → E1 → E2 → E3 → F1 → F2 → F3 → F4, with D1 to D5 in
parallel in the other repositories once B4 and B6 define the actions.
C5 waits for a decision.

## M2 · logs and compactor (phase 2)

- Log schema v1 and OTLP logs conversion, with the same clean-room mapping
  rules, limits and tests as spans.
- Logs ingest path reusing the WAL, buffers, coalescer and flush, with its
  own sort key (`service_name, timestamp`) and bloom filters on `trace_id`
  and `severity`.
- Ngram and token bloom sidecars ([ADR 0003](adr/0003-ngram-bloom-before-tantivy.md)) for `LIKE` and `contains`.
- L1 to L2 compaction with zstd 7 and promoted columns; `usage_1h`.
- Go: logs views on the Flight provider; logs diff against ClickHouse.
- Exit: logs view and service map served by the engine in staging for two
  weeks without regression.

## M3 · metrics (phase 3)

- Series and samples schemas, fingerprints, postings sidecars
  (roaring bitmaps per hour).
- `metrics.select` typed action; PromQL stays in Go ([ADR 0002](adr/0002-promql-stays-in-go.md)) through `flightprometheus`.
- Rollups `rollup_5m` and `rollup_30m` from the compactor; per-tenant
  cardinality limit.
- Exit: alerts and dashboards match ClickHouse in shadow for two weeks.

## M4 · cutover (phase 4)

- History migration from ClickHouse through Parquet export into L2.
- Iceberg export of L2 ([ADR 0001](adr/0001-own-catalog-iceberg-export.md)).
- Remove `clickhousetelemetrystore`; ClickHouse shutdown.
- Exit: production without ClickHouse for 30 days.

## Cadence

- One item per branch and pull request, with its test in the same change;
  performance items add a benchmark file in `docs/benchmarks/` on the same
  hardware as the previous one.
- Every pull request updates this file: status, commit, and any item it
  adds or drops. The reviewer's findings become items here when they are
  not fixed in the same change.
- A short written checkpoint at the end of each group: what landed, what
  the benchmark says, what moved in the order and why.
- Exit criteria are not negotiated down silently. Missing one is a decision
  recorded in this file.
