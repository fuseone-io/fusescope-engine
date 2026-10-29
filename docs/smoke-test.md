# Smoke test with the real binary

Drives one `fusescope-engine` process with the three roles against the
development services, with the tools a developer machine already has:
`grpcurl` to export OTLP and the binary's own `admin query` and
`admin action` to read back over Flight. Every step is what the Go backend
and the collector do, so a failure here is theirs too.

Run it after a change to any role, the catalog or a durable format; it is
not part of CI.

## Services

`make dev-up` starts Postgres on `localhost:55432` (`FS_DEV_POSTGRES_PORT`
changes it, in compose and in every make target) and an S3-compatible
store on `localhost:9100`, and creates the `fusescope` bucket. A prefix
belongs to one catalog: the engine marks it with the catalog's identity on
first start and refuses a prefix marked for another, since the orphan sweep
deletes any object under the prefix its catalog does not know. The smoke
test uses `prefix = "smoke"`, the development configuration `"dev"`.

## Configuration

`config/smoke.toml` is `config/dev.toml` with its own prefix and the clocks
shortened so the whole path runs in minutes; the validator enforces the
order between them:

```toml
[ingest]
flush_interval_secs = 5
persisted_retain_secs = 10     # >= file cache TTL (5 s) + a hot scan

[query]
timeout_secs = 10

[compactor]
compaction_interval_secs = 15
hour_close_delay_secs = 30     # >= persisted_retain + query timeout + file cache TTL
purge_interval_secs = 15
```

## Steps

```sh
export FS_CATALOG_URL=postgres://fusescope:fusescope@localhost:55432/fusescope
export FS_BUCKET_ACCESS_KEY=fusescope FS_BUCKET_SECRET_KEY=fusescope-dev
B=target/debug/fusescope-engine
C=config/smoke.toml

$B --config $C --check                                  # reports the prefix ownership
$B --config $C admin tenant create smoke                # prints tenant_id
$B --config $C admin token create <tenant_id> collector ingest
$B --config $C admin token create <tenant_id> backend query
export FS_QUERY_TOKEN=<the query token>                 # never on the command line
$B --config $C --role all &
```

Export spans with `grpcurl`, using the protos the engine builds against;
`traceId` and `spanId` are base64 of 16 and 8 bytes:

```sh
P=$(ls -d ~/.cargo/registry/src/*/opentelemetry-proto-*/src/proto/opentelemetry-proto | tail -1)
grpcurl -plaintext -import-path "$P" \
  -proto opentelemetry/proto/collector/trace/v1/trace_service.proto \
  -H "authorization: Bearer $INGEST_TOKEN" -d @ 127.0.0.1:4317 \
  opentelemetry.proto.collector.trace.v1.TraceService/Export < spans.json
```

Export two batches stamped now and two stamped in an hour that has
already passed, under different services and traces; a closed hour needs
at least two L0 files before a pass takes it.

Then read back, in this order, and compare with what was exported:

1. Before the first flush: `admin query <addr> "select service_name,
   count(*) from spans group by 1"` answers from the hot window;
   `admin action <addr> trace.lookup '{"trace_id": "<hex>"}'` returns the
   trace's spans.
2. An ingest token on the query side is `Unauthenticated`.
3. After `flush_interval_secs`: the same counts, now from files; the
   catalog holds one L0 file per hour exported.
4. After `hour_close_delay_secs + compaction_interval_secs`: the closed
   hour's L0 files are quarantined, one L1 file is visible, the
   `attribute_keys` table lists the keys exported in that hour with
   `admitted_hour` set to it, and `admin action <addr> metadata.keys
   '{"signal": "traces", "prefix": "http."}'` answers from it.
5. `metadata.services` and `metadata.operations` answer from the catalog
   and the spans table.
6. `SIGTERM` stops every role within a second with exit code 0; the log
   ends with `fusescope-engine stopped`.
7. Restarted on the same WAL, catalog and bucket, the counts are the same
   with no hot window, a trace in the L1 file is found through its sidecar
   (`fusescope_query_trace_index_pruned_total` grows), a new export is
   ingested, and a body with a field an action does not know is
   `InvalidArgument`.

`curl 127.0.0.1:9464/metrics` shows each step: `fusescope_ingest_requests_total`,
`fusescope_flush_total`, `fusescope_compaction_passes_total`,
`fusescope_compaction_derived_total`, `fusescope_query_*`.

Last run: 2026-10-29, every step as described, no warning or error in the
log across both runs.
