# Runbook

For whoever is on call for the engine. Each section says what the engine
does by itself, what to look at, and what to do. The image, the chart and
the dashboard are in `docs/ops/image.md`, `docs/ops/helm.md` and
`docs/ops/dashboard.md`.

## Where to look

- **`GET /readyz`** on port 9464 answers `ready` or `not ready`, then one
  line per fact: `roles=up/expected`, `catalog=ok|down`, `bucket=ok|down`,
  `wal=open|closed|none`, `draining=true|false`, `tls=off|current|stale`,
  `jwt_keys=off|current|stale`. Not ready means a role is not up, the
  catalog or the bucket did not answer the last probe, the WAL is closed,
  or a stop is draining. `stale` is reported but does not fail readiness.
- **`GET /metrics`** on the same port, read through the dashboard.
- **Logs**, one JSON object per line in the image. Audit events, one per
  query and per token or migration action, carry the target
  `fusescope::audit`.

## Restart

`SIGTERM` (a pod deletion, a rollout) starts a drain: `/readyz` turns
`not ready` at once, the roles finish what they hold, and the process exits
within `server.drain_timeout_secs`, aborting what is left. An ingester
flushes what it can on the way out; anything not flushed is in its WAL.
Restarting is always safe: nothing the engine acknowledged lives only in
memory.

On start a pod checks the bucket and the catalog, verifies the bucket
prefix is this catalog's, and only then opens `/healthz`; the chart's
startup probe allows `startupTimeoutSecs` for that. The roles never
migrate: the chart's migration Job does, before a release rolls, or
`fusescope-engine --config <file> admin migrate` as the migration user.

## WAL replay and checkpoint

An ingester writes every accepted request to its WAL before acknowledging
it, flushes rows to the bucket as Parquet L0 files, registers them in the
catalog, and moves the checkpoint past them. On start it replays the WAL
from the checkpoint, rebuilding the rows not yet flushed;
`fusescope_ingest_flushes_recovered_total` counts the flushes it resumes.
A replayed flush produces the same file under the same id, so nothing is
written twice. Expect a start to take longer the more the WAL holds.

- **The WAL refuses to open** with damage recovery cannot prove
  unacknowledged: damage before the last commit marker, or a read failure
  that keeps recovery from proving the damage lies after it. The suffix of
  `wal: damage at segment … that recovery must not cut (…)` says which:
  corrupt bytes, or an I/O error while reading. Either way the records may
  be acknowledged and the engine will not cut them: keep the volume, do not
  delete segments, and escalate, since they are the only copy of rows the
  clients were told were stored.
- **Replay stops at damage anywhere else**, in an earlier segment or in a
  record, and the ingester refuses to start with
  `ingest: wal record: wal damage at …`: the records past it may hold a
  flush intent, and replaying without it would write those rows again
  under an id already taken. Keep the volume, do not delete segments, and
  escalate, as above.
- **Replay stops at a record this version cannot read**, with
  `ingest: wal record: wal record at … cannot be read by this engine` or
  `ingest: wal record: flush intent at … cannot be read by this engine`:
  a newer engine wrote it. Roll the ingester forward to that version
  rather than back.
- **A torn tail** after the last commit marker belongs to a batch whose
  sync never finished and whose clients were never acknowledged; it is the
  only damage cut, silently, and appends continue.
- **The WAL volume is lost** (a deleted claim): rows the ingester
  acknowledged and had not flushed are gone. The ingester starts with a new
  WAL instance and new file ids.
- **The WAL fills** (`ingest.wal_max_bytes`): ingest is refused with
  `UNAVAILABLE: write-ahead log full` until flushes move the checkpoint.
  Look at flush failures and unconfirmed files on the dashboard; usually
  the bucket or the catalog is failing.

`fusescope_flush_unconfirmed` above zero for long means registrations
whose confirmation has not landed; their hour stays open to compaction
until it does. `fusescope_flush_replay_mismatch_total` above zero means a
retried flush found other bytes at its path: the rows took a fresh id and
the stray object is left to the orphan sweep. Investigate the bucket.

## Quarantine and purge

Compaction swaps L0 files for L1 in one catalog transaction and leaves the
inputs quarantined; retention quarantines files past the tenant's
`retention_days`. Quarantined files stay readable for
`compactor.quarantine_ttl_secs`, so queries that listed them before the
swap still finish, then the purge deletes their rows and objects. A purge
marks each path before deleting it: a writer that replays such a file later
is refused with `ObjectDoomed` and gives its rows a fresh id, so no row ever
points at a deleted object. Nothing here needs a hand; do not delete
objects from the bucket directly.

One compactor works at a time, holding the `compactor` lease; others stand
by. `fusescope_compaction_lease_lost_total` rising means the holder lost
the catalog or stalled; a standby takes over within
`compactor.lease_ttl_secs`.

## Orphan collector

An object in the bucket prefix with no catalog row, left by a crash between
upload and registration or by a refused swap, is an orphan. The compactor's
sweep marks it the first time it sees it and deletes it once it has stayed
unregistered for `compactor.orphan_ttl_secs`, leaving a tombstone that keeps
the path from being written again. `fusescope_orphan_delete_failures_total`
rising means deletes are failing (bucket permissions, throttling); they are
retried with backoff. `fusescope_orphan_forgotten_total` counts remembered
paths the sweep dropped and left in the bucket, for one of two reasons: a
catalog row owns them after all, the benign case of a registration that
landed late, or they are not in the engine's layout at all. The second
means something else writes into the engine's prefix; find out what, since
the sweep never deletes what it does not recognise.

The bucket prefix belongs to one catalog. A start refused with
`bucket prefix … belongs to catalog …; this is not it` means the pod points
at another deployment's prefix: fix `bucket.prefix` or the catalog URL. A
start refused with `… holds objects but no catalog marker` means the
prefix has objects from before ownership was recorded; set
`bucket.adopt_prefix = true` for one start, only after confirming those
objects are this deployment's.

## Tokens, TLS and JWT keys

- **Issue a token:** `admin token create <tenant-id> <name> [ingest|query]`
  prints the secret once.
- **List:** `admin token list <tenant-id>` shows ids, names, scopes and
  state, never secrets.
- **Rotate:** `admin token rotate <id> [overlap-secs]` issues a successor
  and keeps the old one for the overlap (3600 s by default).
- **Revoke:** `admin token revoke <id>` stops it at once in the catalog.
  Queriers see a change at once; an ingester may accept a token for up to
  `ingest.token_cache_secs` after a revocation.
- **TLS files and the JWT key set** are read again every reload interval.
  Rotate a CA through a bundle holding the old and the new root until every
  certificate is reissued, never by a direct switch. A rotation that does
  not load keeps the last good files and shows `tls=stale` or
  `jwt_keys=stale` on `/readyz` and in `fusescope_tls_stale` and
  `fusescope_jwt_keys_stale`: fix the Secret or ConfigMap. A rotated
  environment Secret (catalog or bucket credentials) needs a rollout; see
  `rolloutTokens` in `docs/ops/helm.md`.

## What each refusal means

### OTLP ingest (gRPC status and message)

| Status | Message | Meaning | Action |
|---|---|---|---|
| `UNAUTHENTICATED` | `missing bearer token`, `malformed token`, `unknown or revoked token` | the collector's token | check the collector's token with `admin token list` |
| `UNAVAILABLE` | `token lookup failed: …` | the catalog did not answer | catalog health |
| `RESOURCE_EXHAUSTED` | `tenant throughput limit` | the tenant's rate limit | the collector retries; raise the limit if it is legitimate |
| `RESOURCE_EXHAUSTED` | `ingester busy: wal queue bytes`, `… wal queue records`, `… conversions closed` | the WAL writer or the conversions are behind | transient; persistent means a slow WAL disk |
| `RESOURCE_EXHAUSTED` | `tenant buffer full, flush pending`, `tenant rows span too many hours, flush pending` | the tenant's buffer waits for a flush | transient; persistent means flushes fail |
| `RESOURCE_EXHAUSTED` | `tenant memory full, hot-window retention pending`, `ingester memory full, …` | flushed rows are still served from memory | transient; persistent means memory is too small for the load |
| `UNAVAILABLE` | `write-ahead log full` | flushes are not moving the checkpoint | see WAL replay and checkpoint |
| `UNAVAILABLE` | `write-ahead log unavailable` | an append failed | the WAL disk; the log has the cause |
| `INVALID_ARGUMENT` | `spans fall in N calendar hours, more than the M allowed` | one request spans too many hours | the sender's clock or batching |
| `INTERNAL` | `conversion failed`, `ingest task failed` | a bug | the log has the cause; report it |

A success with `partial_success.rejected_spans` means spans with malformed
trace or span ids were dropped and the rest stored.

### Flight SQL (gRPC status and message)

| Status | Message | Meaning | Action |
|---|---|---|---|
| `UNAUTHENTICATED` | `missing bearer token`, `unknown or revoked token`, a JWT reason, `unknown or deleted tenant` | the caller's token | in `jwt` mode, the backend's signing key and clock |
| `PERMISSION_DENIED` | `ticket does not belong to this tenant`, `… issued to another subject`, `… not issued for a tenant token` | a ticket used by another caller, or an old ticket in `jwt` mode | the backend's ticket handling |
| `PERMISSION_DENIED` | `only SELECT statements are allowed: …` | a statement that would write | the query |
| `RESOURCE_EXHAUSTED` | `query would scan N bytes, over the budget of M`, `result exceeded N rows` | the scan budget or the row cap | narrow the query |
| `DEADLINE_EXCEEDED` | `query exceeded …` | `query.timeout_secs` | narrow the query |
| `INVALID_ARGUMENT` | `bad ticket`, `invalid action: …`, a planning error | malformed input | the caller |
| `UNIMPLEMENTED` | `unknown action "…"` | a `DoAction` type this engine does not serve | the backend calls an action of a newer engine, or a typo |
| `UNAVAILABLE` | `token lookup failed: …`, `tenant lookup failed: …` | the catalog did not answer | catalog health |

A result carrying the `x-fusescope-partial` header is complete for what it
read but missed something, named in the header, such as an ingester whose
hot window did not answer.

### Start

| Message | Meaning |
|---|---|
| `invalid config: …` | the configuration; `--validate` reproduces it without connecting |
| `tls: …`, `jwt keys …` | a certificate, key, CA bundle or key set that does not load |
| `bucket prefix … belongs to catalog …` | another deployment's prefix |
| `… holds objects but no catalog marker` | see the orphan collector above |
| `wal: damage at segment … that recovery must not cut (…)` | damage recovery cannot prove unacknowledged, corrupt bytes or a read failure as the suffix says; keep the volume and escalate |
| `wal: io at …` | the WAL directory cannot be read or written: the volume is missing, read-only or not the pod's to write |
| `wal: …`, any other | the WAL cannot open; the message names why |
| `ingest: wal record: wal damage at …` | replay met damage it will not skip; keep the volume and escalate |
| `ingest: wal record: wal record at …` or `… flush intent at … cannot be read by this engine` | a newer engine wrote the WAL; roll forward |
| `catalog: …`, `bucket: …` | unreachable at start; the pod restarts until they answer |
