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
- `fs-wal`: segments, crc32c, checkpoint file, instance id, replay, a
  commit marker per synced batch so recovery cuts only unacknowledged bytes
  and refuses to open on any damage it cannot prove unacknowledged.
- `fs-catalog`: migrations 0001 to 0003, files, tenants, tokens
  (`ingest` and `query` scopes), idempotent registration with content
  verification, atomic swaps, purge of quarantined files, change
  notifications.
- `fs-ingest`: OTLP gRPC with bearer tokens and per-tenant limits, group
  commit WAL writer, strict admission against per-tenant and global caps,
  flush intents and the gate, file ids derived from the log, retries that
  never change a file's content, a `trace_id` sidecar per file, hot window
  over internal Flight with final file-id announcements, WAL-ordered buffers with background coalescing,
  histograms for hot-scan and checkpoint latency.
- `fs-query`: tenant-scoped DataFusion sessions, SELECT-only gate, catalog
  pruning, `trace_id` sidecar pruning with an in-memory cache, byte budget,
  hot-window union with partial-result warnings, Flight SQL front end.
- `fs-server`: configuration, bootstrap, roles, admin commands, `/metrics`,
  graceful shutdown. `fs-compactor`: leader lease in the catalog, purge of quarantined files, L0 to L1 compaction of closed hours and the orphan sweep while holding it.
- `fs-bench`: ingest stages, OTLP load generator with `/metrics` deltas,
  Flight SQL query suite. Benchmarks recorded for the baseline, group
  commit, flush stages, coalescing and the gate.

Measured today (32 senders, one tenant, NVMe WAL): about 10 000 requests/s
and 125 000 spans/s with queries running; flush of 200 k rows in about 1 s,
60% of it Parquet encode; 98% of request batches merged into chunks before
their flush; hot-window snapshot p95 within 20 ms under load; `trace by id`
384 ms p95 under load, bounded by opening every overlapping file; 8 s of
retention holds about 570 bytes per span.

## M1 · traces end to end

### Group A · ingest path, finish what is open

| # | Item | Why | Exit test | Size | Status |
|---|---|---|---|---|---|
| A1 | Parallel coalescer: several coalesce ids per tenant over non-overlapping runs, a global semaphore (`max_coalesces`), group size by bytes, task and bytes gauges; flushes materialised off the flusher loop under `max_flushes` | one chunk at a time fell behind at 32 senders, so the retention held raw batches and the tenant cap tripped | buffer tests for disjoint runs and cancellation; `coalesced_flush_matches_replay_byte_for_byte` green; [benchmark](benchmarks/2026-10-26-parallel-coalesce.md): 98% of batches merged before their flush, 99 k spans/s | M | done (edbccfb, 12b85a4) |
| A2 | Persisted retention holds the file's rows as written, one sorted batch, and the default retention (8 s) is validated against the querier's file cache TTL plus a hot scan | retention was the main memory consumer | memory cap tests with retention; [benchmark](benchmarks/2026-10-26-retention-and-trace-lookup.md): 571 MiB for 8 s at 125 k spans/s, refusals 2 648 → 760 | S | done (e6fb243) |
| A3 | Parquet encode by row group from a k-way merge of per-batch sorts; the writer holds one row group at a time and its peak is measured per flush and published; row groups are what the hot window retains, each with its trace set | encode was the largest flush stage and the writer's memory an estimate | `merge_orders_rows_across_inputs_and_splits_row_groups` (same bytes whatever the batching), `flush_writes_row_groups_and_retains_each`, replay digest test; benchmark pending, after which the three-copy lease is to be replaced by inputs + one row group + measured writer peak | M | done (96fb1d3), lease retune after benchmark |
| A4 | Flush concurrency across tenants with a bounded semaphore | one slow tenant must not delay the others | multi-tenant flush test; benchmark with 4 tenants | S | test done: a store wrapper holds one tenant's uploads while the others flush through the permits left, twice, and the held one lands once released (`a_slow_tenants_flush_does_not_hold_the_others`, anchored on the upload reaching the gate before the others export); the four-tenant benchmark is pending authorisation |
| A5 | End-of-hour flush boundary so an L0 file never spans two hours | L1 compaction and the trace index are per hour | flush test with spans across an hour boundary produces two files | S | done: a cut makes one file per hour, at most `max_hours_per_cut`, each under an intent naming its hour (`FlushIntent.hour`, WAL record version 2, id per hour) and replay partitions each record by it (`a_flush_with_spans_across_an_hour_boundary_writes_one_file_per_hour`, `crash_between_the_files_of_a_split_flush_is_resumed`, `a_part_left_live_by_a_failed_intent_keeps_its_hour_through_a_replay`, `a_stop_flushes_every_hour_however_many_a_cut_takes`, `requests_are_bounded_by_the_hours_they_span_and_the_hours_live`); the hours of a batch are kept with it, requests are bounded by `max_hours_per_batch` and tenants by `max_live_hours`; the hot window announces every id the live rows can take |
| A6 | Ingest health: `/healthz` with WAL, bucket and catalog state; shutdown drains in-flight requests with a deadline | staging needs liveness and readiness | server test for the endpoints and the drain | S | done: `fs_server::health::Health` fed by the roles and a prober every `server.health_interval_secs`, `/healthz` and `/readyz` on the metrics listener, which binds before any role starts, is watched like one and outlives them so `/readyz` answers 503 through the drain, `server.drain_timeout_secs` aborts what the roles still hold (`readiness_follows_the_roles_and_the_drain`, `the_drain_has_a_deadline`, `a_health_listener_that_cannot_bind_stops_the_process`) |

### Group B · query path for traces

| # | Item | Why | Exit test | Size | Status |
|---|---|---|---|---|---|
| B1 | Typed `trace_id` and `span_id` predicates in `HotScanRequest` (`#[serde(default)]`, optional), applied before rows leave the ingester | the hot scan shipped every retained row | `hot_scan_keeps_only_the_requested_traces_and_spans`, pruning unit test, SQL lookup test; [benchmark](benchmarks/2026-10-26-retention-and-trace-lookup.md): `trace by id` p95 2.1 s → 384 ms under load | S | done (e6fb243) |
| B2 | Hot trace index: every chunk and retained file batch carries its set of trace ids, built when the batch is made; a lookup skips batches holding none of the requested traces | narrows the in-memory scan to a few batches | `hot_scan_skips_batches_without_the_trace`; benchmark pending | M | done, benchmark pending |
| B3 | `index/trace_id/*.tix` sidecar per hour written by the flush and the compactor, with the xxh3 header; querier opens only the listed files | thousands of files per trace lookup otherwise | sidecar round-trip test; corrupt sidecar degrades to a scan; `trace by id` within the 9.2 target | L | done for L0: `fs-index::trace_index` format v1, one sidecar per flush (`flush_writes_a_trace_index_sidecar_listing_its_traces`, `crash_between_parquet_and_sidecar_is_resumed`), querier cache with byte budget (`trace_index_prunes_files_without_the_trace`, `missing_or_damaged_sidecars_degrade_to_a_scan`); the per-hour merge lands with C2; review rounds closed the exact-bytes rule for an existing sidecar, the cache ceiling with leases and measured decode cost, and the weak back-reference; benchmark pending |
| B4 | Typed action `trace.lookup` over Flight `DoAction` returning the spans of one trace | the Go backend's trace view should not build SQL for the most common query | action test; Go client call | M | engine side done: `fs_query::actions` (JSON body, Arrow IPC stream across the result bodies), `Querier::trace_lookup` through the spans table without SQL, `ListActions` lists it, metric `fusescope_query_trace_lookups_total` (`trace_lookup_action_returns_the_spans_of_one_trace`); the Go client call lives in `fuseone-io/fusescope` |
| B5 | Prepared-plan ticket: `GetFlightInfo` plans once, `DoGet` executes the ticket bound to the tenant | every statement is planned twice today, a 125 ms floor | query test asserting one planning per statement; query benchmark | M | done: `Querier::prepare`/`execute_prepared` keep the plan keyed by tenant and SQL for one execution, the ticket stays tenant id plus SQL as before (`a_statement_is_planned_once_between_get_flight_info_and_do_get` presents a ticket of the previous version), a reaper drops expired plans (`a_ticket_whose_plan_is_gone_plans_again`), `query.prepared_plans`, `query.prepared_bytes` and `query.prepared_ttl_secs` (`prepared_plans_are_bounded_by_the_bytes_they_retain`); metrics `fusescope_query_plannings_total`, `fusescope_query_prepared_{hits,misses}_total`, gauges `fusescope_query_prepared_{plans,bytes}`; benchmark pending |
| B6 | Service and operation metadata actions (`metadata.services`, `metadata.operations`) and the `attribute_keys` derived table | the UI's service list and filter suggestions need them | action tests; Go client call | M | engine side done: `metadata.services` from the catalog and the hot window, `metadata.operations` through the spans table, `metadata.keys` over the `attribute_keys` table (`fs-schema::attribute_keys` format v1, rebuilt by every L0 to L1 pass and swapped in with the outputs, laid out under `derived/attribute_keys` and swept like any object), `attribute_keys` in SQL (`metadata_services_action_lists_the_services_of_the_tenant`, `metadata_operations_action_lists_the_operations_of_a_service`, `metadata_keys_action_reads_the_attribute_keys_table`, `a_pass_rebuilds_the_attribute_keys_table_from_its_inputs_and_the_table_before`); the Go client call lives in `fuseone-io/fusescope` |
| B7 | SQL sandbox denial tests for every blocked construct (DDL, DML, `COPY`, functions that touch the file system, cross-tenant references) | DESIGN.md section 10.2 promises it; CI must prove it | one test per construct | S | done: `sandbox` module in `crates/fs-query/tests/query.rs`, one test per construct (DDL, DML, `MERGE`, `TRUNCATE`, `COPY` both ways, `CREATE EXTERNAL TABLE`, five path-taking table functions also nested, `SET`, `RESET`, `SHOW`, transactions, `PREPARE`/`EXECUTE`/`DEALLOCATE`, two statements, other catalogs and schemas, `information_schema` scope); found and fixed `EXPLAIN` of a write passing the statement gate: the gate now looks through `EXPLAIN` and the plan is walked whole with an exhaustive allow-list of reading nodes; `SELECT INTO` exercises that second gate alone |

### Group C · compactor and durability

| # | Item | Why | Exit test | Size | Status |
|---|---|---|---|---|---|
| C1 | Compactor leader lease in the catalog, single active compactor per deployment | two compactors would race on the same files | lease test with two instances | S | done: `engine_lease` (migration 0004), `Catalog::acquire_lease/release_lease/lease`, `fs_compactor::Leadership` and `run` (`two_instances_hand_the_lease_over_by_release_or_expiry`, `the_loop_purges_only_while_it_holds_the_lease`), `compactor.lease_ttl_secs`; review rounds added the per-incarnation holder, the generation fence checked under a row lock (`swap_files_fenced`), renewal on its own loop bounded by its period with the work aborted and awaited on loss, sidecar purge, the database-clock purge cutoff and the quarantine TTL floor |
| C2 | L0 to L1 per closed hour: re-sort, 512 MB targets, quarantine and swap in one transaction, `tix` rebuilt | small L0 files make queries open hundreds of files | compaction test with crash before and after the swap; query results identical before and after | L | done: streaming k-way merge in `fs-compactor::merge` over `fs-schema::parquet`, passes bounded by `max_inputs_per_pass` and `max_input_bytes`, fenced swap, per-output sidecars (`a_closed_hour_with_several_l0_files_becomes_one_sorted_l1_file`, `outputs_close_at_the_target_size`, `a_crash_before_the_swap_leaves_inputs_visible_and_outputs_orphaned`, `a_stale_fence_publishes_nothing`); the per-hour merged `.tix` waits for a final file set per hour; review rounds added the cold-hour rule from one repeatable-read snapshot with UTC bins, the mandatory fence, one output in memory at a time, the strict input cap that passes over oversized hours, null services first, and orphan accounting through a guard that survives cancellation |
| C3 | Orphan collector: objects without a catalog row are quarantined, then deleted after the TTL | a crash between upload and registration leaves objects behind | test creating an orphan and running the collector | S | done: `engine_orphan` (migrations 0006 to 0009) remembers first sightings, marks under the path's advisory lock every registration checks (`ObjectDoomed`), retries deletes with backoff and keeps a permanent tombstone; `fs_compactor::Sweeper` walks a lazily listed inventory in bounded passes with a strict layout parser and finishes other sweepers' marks (`orphans_are_deleted_only_after_a_ttl_without_a_row`, `a_sweep_walks_in_bounded_passes_and_finishes_what_it_marked`, `an_object_written_behind_a_tombstone_is_deleted_again`); a refused flush supersedes its intent under a fresh id and keeps its `origin` (migrations 0010 to 0012), which the hot window announces and the planner excludes; an hour closes only after every file was confirmed by its writer once its rows are retained (migrations 0013 to 0015), with the confirmations off the flusher on their own permits and the rollout order queriers, compactors, ingesters; metrics `fusescope_orphan_*`, `fusescope_flush_superseded_total`, `fusescope_flush_unconfirmed` |
| C4 | Retention by tenant: quarantine the files beyond `retention_days`, catalog first, bucket at the purge; rebuild the tenant's `attribute_keys` table from the L1 files that remain | staging keeps data for a bounded time | `retention_expires_whole_utc_days_under_the_fence_and_finds_the_stale_table`, `expired_days_leave_the_catalog_first_and_the_bucket_at_the_purge` (restart between the catalog and the purge), `the_attribute_keys_table_is_rebuilt_from_the_l1_files_that_remain` (swap failure, lost lease, read cap, unreadable L1), `a_tenant_whose_days_are_all_gone_loses_its_table`, `the_loop_expires_and_rebuilds_while_it_holds_the_lease` | S | done (c8ce697, 1c4d52a, 67315e6, 17ec451): UTC day cut on the catalog's clock (`expire_files_fenced`, `stale_derived_table`, `rebuild_inputs`), `fs_compactor::Retention` on the leader's loop, `KeyStats::absorb_known`, `compactor.retention_interval_secs`, `retention_files_per_pass`, `rebuild_max_bytes`, `fusescope_retention_*`; a per-day contribution to the table would make the rebuild a subtraction and lift the read cap, a format change for later; review round 1 (P0 purge, P1 held-back tenant, P1 memory budget): the purge marks path and sidecar in the row's transaction and deletes under claims with tombstones (`a_purge_dooms_the_path_and_its_sidecar_with_the_row`, `a_replay_during_the_purge_is_refused_and_never_leaves_a_row_without_an_object`), a failed rebuild holds its tenant back for the tick and a tenant with no L1 left loses its table unread, and a rebuild is admitted by `rebuild_footprint` under `rebuild_max_bytes` and by `rebuild_max_input_bytes` (`the_footprint_adds_every_resident_under_the_default_limits`); round 2: the leader's loop goes on past a tenant's own failure (`the_loop_expires_and_rebuilds_while_it_holds_the_lease` runs two tenants through `run`, one unreadable), the inputs are listed as path and size a page at a time in one snapshot with the budget seeing each page (`rebuild_inputs`, list bytes in the footprint), and the purge replay test waits on the path's advisory lock in `pg_stat_activity` with deadlines; round 3: a table with no L1 left goes whatever the caps, a failed tenant's backoff (`rebuild_backoff_secs`, times its failures) outlives the tick and `rebuilds_per_tick` bounds a tick (the loop test runs one rebuild per tick with the broken tenant first), and the purge replay test waits on the lock of the exact path (`hashtextextended`); round 4: the stale tables are walked by a keyset cursor across calls and ticks (`StaleKey`, one query per table a round; the loop test runs the broken tenant first with a zero backoff) and `rebuilds_per_tick` is capped at `MAX_REBUILDS_PER_TICK`; round 5: one stale table per `rebuild_once` (`Rebuild::{Done, Skipped, End}`), a passed-over one consuming a pass of the tick and the round ending without a wrap, and `rebuilds_per_tick` clamped inside the crate (`rebuilds_per_tick_is_bounded_inside_the_crate`); round 6: the walk's step past a table is confirmed only once the table was dealt with, so a catalog failure anywhere in the call, the input listing included, leaves the walk on it (`a_failed_listing_leaves_the_walk_on_the_same_table`) |
| C5 | Content digest column on `files` (sha256 of the object), verified on re-registration | the no-overwrite guarantee rests on size and metadata today; needs a decision because it is a catalog schema change | migration 0004; register test with a different object of the same size fails | S | done: migration 0017 adds a nullable `content_sha256` to `engine_file`, filled by the flush and by every compactor output and derived file, compared on a second registration of an id (`a_registration_keeps_the_content_digest_and_old_rows_have_none`); a retried flush that meets other bytes at its path is refused by the digest, takes a fresh id like a doomed path and counts `fusescope_flush_replay_mismatch_total` (`a_retried_flush_refuses_an_object_with_other_bytes_and_takes_a_fresh_id`); checking compaction inputs against the column is a later item |
| C6 | `trace_summary` and `service_graph_1m` derived tables updated by the compactor | the UI's trace list and service map read them | derived-table tests; Go client call | L | engine side done: exact versioned formats in `fs-schema`, with committed v1 reader fixtures; one immutable contribution of each per L0-to-L1 pass, uploaded and published in the same fenced swap as L1; whole-contribution count and layout-derived resident-byte caps, interned names, distinct-edge finalization charges, metrics and a deduplicated warning on skip; direct and inferred service edges keyed by trace plus span id; start-pruned SQL summaries and `quantile_tdigest`; retention and orphan sweep cover both layouts, including a distant span end (`a_pass_publishes_trace_and_service_graph_contributions_with_the_l1`, `two_passes_leave_partial_derived_files_that_queries_aggregate`, `derived_caps_skip_whole_contributions_and_count_them`, `a_distant_span_end_does_not_keep_its_hourly_summary_past_retention`, `derived_contributions_are_pruned_by_time_and_unknown_versions_fail_closed`); graph resolution and derived encoding run on the blocking pool; the Go client/query lives in `fuseone-io/fusescope` |

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
| E1 | mTLS on Flight SQL and the internal hot-window Flight, OS trust store, certificate reload | DESIGN.md section 10.4; staging is a shared network | handshake tests with a bad client certificate refused | M | done: `fs-auth` loads and validates the identity and client CAs (`tonic` `tls-ring` and `tls-native-roots`, `tokio-rustls` for the listener), serves both Flights through a listener that hands each new connection the config current at accept time and reloads the files every `tls.reload_interval_secs` keeping the last good config on failure (`a_reload_serves_new_connections_and_leaves_open_ones`, `a_failed_reload_keeps_the_last_good_config_and_marks_it_stale`, `silent_and_excess_connections_are_dropped_within_the_limits`, `the_hot_window_over_mtls_admits_only_certified_queriers`, `with_tls_on_each_flight_admits_only_its_own_clients`); clients trust the system store only; `/readyz` shows `tls=`, metrics `fusescope_tls_*`; review round 1: Flight SQL and the hot window take disjoint client CA bundles (`tls.public_client_ca`, `tls.hot_client_ca`), refused when they share a CA, so a backend certificate never opens the hot window (`the_public_and_hot_listeners_admit_disjoint_clients`, `with_tls_on_each_flight_admits_only_its_own_clients`), and `tls.max_handshakes` is bounded at 65536 in the server and clamped in `fs-auth` (`max_handshakes_is_clamped_to_its_bounds`, `a_listener_with_an_unbounded_handshake_limit_still_serves`); round 2: the bundles are compared as trust domains, every client CA a self-signed root (checked by its own signature through the rustls provider) and no public key in both, so a CA reissued under the same key, renamed, an intermediate or a self-issued lookalike is refused (`a_ca_reissued_under_the_same_key_is_the_same_trust_domain`, `invalid_mismatched_or_overlapping_files_refuse_the_load`), and the rotation tests go B, B+B2, B2 on Flight SQL and A, A+A2, A2 on the hot window without moving a CA between listeners; refusing an expired certificate at start is required before staging; test certificates in `crates/fs-auth/tests/fixtures` with their generator; the OTLP listener stays out |
| E2 | Tenant JWT from the Go backend to the querier, bound to the session and the ticket | the query token today is a static bearer; the backend acts for many tenants | token test: a ticket from one tenant is refused by another | M | done: `fs_auth::JwtVerifier` checks compact JWS `EdDSA` tokens against a reloaded Ed25519 JWK set through the rustls provider, no new crate (`every_refusal_has_its_reason`, `a_key_rotation_overlaps_without_a_restart`); `auth.mode = "jwt"` takes only those tokens, checks the tenant alive in the catalog and binds the ticket (`FST2`) to tenant and subject (`tenant_jwts_bind_the_ticket_to_tenant_and_subject`, `in_jwt_mode_the_querier_takes_only_backend_tokens`); `token` mode and its ticket are unchanged; one `fusescope::audit` event per execution (`the_outcome_follows_how_the_stream_ends`); test keys in `crates/fs-auth/tests/fixtures`; review round 1: `jwt` requires `[tls]` on (`jwt_mode_requires_tls`, the server test runs over mTLS with the backend certificate), refused tokens and tickets are audited with `refused`, an unreachable catalog or a deadline with `failed` (`the_outcome_follows_how_the_stream_ends`), and client strings escaped, and the key set refuses private material and any `key_ops` but `verify`; the Go signer lives in `fuseone-io/fusescope` |
| E3 | Token rotation and revocation in `admin`, cached tokens expire within the TTL | operations | catalog and cache tests | S | done: no migration, `revoked_at` in the future is a scheduled end; tokens are named by the first 8 bytes of their hash (`TenantToken::id`), `Catalog::revoke_token_by_id` and `rotate_token` work in one transaction and refuse an ambiguous id, `resolve_token_until` tells a cache how long a token has left on the catalog's clock (`tokens_are_revoked_and_rotated_by_their_id`, `an_ambiguous_token_id_changes_nothing`); a revocation or rotation reaches an ingester within `ingest.token_cache_secs`, an answer cached after a rotation never outliving its end (`revoked_and_rotated_tokens_stop_within_the_cache_ttl`), invalidation on change left for a migration; `admin token list`, `revoke` and `rotate [overlap-secs]` with `fusescope::audit` events (`admin_rotates_and_revokes_tokens_by_id`) |

### Group F · operations for staging

| # | Item | Why | Exit test | Size | Status |
|---|---|---|---|---|---|
| F1 | Container image with `ca-certificates`, non-root, read-only file system except WAL and spill | deployment | image builds in CI; `--check` passes inside it | S | done: `Dockerfile` (release build on bookworm, runtime `gcr.io/distroless/cc-debian12:nonroot` with the Debian CA bundle, user 65532, volumes `/var/lib/fusescope/{wal,spill}`), `config/container.toml` as its default and validated with `config/image-check.toml` in `the_image_configurations_validate`; the CI `image` job builds it and runs `scripts/image-check.sh`: the user, a CA bundle holding certificates, and with a read-only root and only the state paths writable, `--check` and `--role all` up to readiness and a clean stop on SIGTERM (spill not forced); `make image`, `make image-check`; `docs/ops/image.md` |
| F2 | Helm chart: one `values`, roles as deployments, WAL on a persistent volume, Postgres and bucket secrets from the environment | staging | chart lints; `helm template` in CI | M | done: `deploy/helm/fusescope-engine`, ingesters as a StatefulSet with a WAL claim per pod and a headless Service each ingester advertises for the hot window, queriers and the compactor as Deployments, one rendered `config.toml` with `--role` per workload, secrets from an existing Secret as environment, mTLS and JWT on by default, read-only root as 65532; the chart refuses a release without its secrets, JWT without TLS or an unknown auth mode; `scripts/chart-check.sh` (strict lint, `kubeconform`, the engine's new `--validate` on the rendered configuration for every role, the refusals) runs in the CI `image` job and as `make chart-check`; `docs/ops/helm.md`; review round 1: per-role env Secrets, ServiceAccounts with no API token, per-role TLS identities over shared client CA bundles (none for the compactor), one configuration per role, a startup probe ahead of liveness and readiness, `rolloutTokens` for rotated Secrets, all checked on the rendered manifests by `chart-check`; round 2: no role migrates, a hook Job runs the new `admin migrate` (catalog only, `admin_migrate_migrates_the_catalog_alone`) as `secrets.migrator`, every Secret name differs from every other (client CAs included), and `startupTimeoutSecs` is bounded 1 to 86400 |
| F3 | Grafana dashboard from `/metrics`: ingest rate, refusals, flush stages, coalescer, buffers, hot-scan and checkpoint histograms, query latency | the two-week soak needs eyes | dashboard JSON in `docs/ops/` | S | todo |
| F4 | Runbook: restart, WAL replay, checkpoint, quarantine, orphan collector, token issue, what each refusal means | the people on call during the soak | `docs/ops/runbook.md` | S | todo |
| F5 | SBOM generated at image build and published with the image | DESIGN.md section 10.5; supply chain | the CI `image` job produces an SBOM listing the binary's crates | S | todo |

### M1 order

A1 → B1 → B2 → A2 → A3 → B3 → C1 → C2 → B5 → B4 → B6 → B7 → A4 → A5 →
A6 → C3 → C4 → C6 → E1 → E2 → E3 → F1 → F2 → F3 → F4 → F5, with D1 to D5 in
parallel in the other repositories once B4 and B6 define the actions.
C5 landed after C6.

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
