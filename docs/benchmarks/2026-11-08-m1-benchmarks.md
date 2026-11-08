# 2026-11-08 · row-group encode, tenant flushes, trace indexes, prepared plans

The benchmarks roadmap items A3, A4, B2, B3 and B5 left pending. Code at
54bd761. Raw output, JSON, `/metrics` snapshots before and after each run,
the configuration, the server log and the script in
[`2026-11-08-m1-benchmarks/`](2026-11-08-m1-benchmarks/).

## Setup

| | |
|---|---|
| Commit | 54bd761 |
| CPU | AMD Ryzen 9 5950X, 16 cores / 32 threads, 64 MiB L3 |
| RAM | 125 GiB |
| Kernel | Linux 6.18.60-2-cachyos-lts |
| WAL disk | disk A of the [baseline](2026-10-25-baseline.md): NVMe (Netac 2 TB) under LUKS, btrfs, `./data/bench-20261108/wal`. Disk B (`/mnt/fs-bench`) was not mounted |
| Bucket | RustFS 1.0.3 in Docker over the S3 API (`compose.yaml`), prefix `bench-20261108` |
| Catalog | Postgres 17.8 in Docker, schema `bench_20261108`, empty at start |
| Build | `cargo build --release -p fs-server -p fs-bench` (fat LTO, one codegen unit), rustc 1.99.1 |
| Server | `fusescope-engine --config bench.toml --role all`: `config/dev.toml` with its own prefix, WAL, spill dir and node id; flush every 10 s; TLS off |
| Traffic | default shape: 20 services, 15 operations, 12 spans per trace, 6 extra attributes, 2% errors |
| Host | five unrelated containers running; load average 1.6 at the start |

Commands are in [`run.sh`](2026-11-08-m1-benchmarks/run.sh), in order:
`fs-bench stages --requests 5000`; then `fs-bench load` for 30 s with
`--metrics` at 32 senders alone, at 32 senders and at 8 senders each with
`fs-bench query --iterations 40` running at the same time against the same
tenant; four tenants of 8 senders each at once; and the query suite again
40 times at rest after the last flushes, with 15 to 30 s between runs.

## Ingest stages (in process, 5000 requests, 60 000 spans)

| stage | total | ns/span |
|---|---:|---:|
| protobuf encode | 56 ms | 937 |
| wal append + sync, per request | 3.59 s | 59 760 |
| wal group commit, 64 senders | 146 ms | 2 430 |
| otlp → arrow | 395 ms | 6 584 |
| concat + sort | 158 ms | 2 632 |
| parquet encode | 104 ms | 1 737 |
| upload (local store) | 1 ms | 18 |
| hot-window filter + project | 105 ms | 1 754 |

## Load

| run | req/s | spans/s | p50 | p95 | p99 | errors | flushes | rows per flush | concat | sort | encode | upload | total per flush | writer peak | buffers at end, live / persisted |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 tenant, 32 senders, alone | 4 965 | 59 583 | 2.81 ms | 14.13 ms | 41.64 ms | 0 | 16 | 101 394 | 93 ms | 60 ms | 214 ms | 434 ms | 871 ms | 45 MiB | 85 / 237 MiB |
| 1 tenant, 32 senders, with queries | 4 552 | 54 629 | 3.36 ms | 18.29 ms | 42.91 ms | 0 | 15 | 96 881 | 121 ms | 89 ms | 266 ms | 267 ms | 803 ms | 47 MiB | 0 / 238 MiB |
| 1 tenant, 8 senders, with queries | 2 462 | 29 543 | 1.64 ms | 7.12 ms | 40.90 ms | 0 | 8 | 102 050 | 145 ms | 88 ms | 286 ms | 298 ms | 918 ms | 47 MiB | 47 / 118 MiB |
| 4 tenants × 8 senders, sum | 5 254 | 63 047 | 3.86–3.89 ms | 23.28–23.57 ms | 48.36–48.71 ms | 0 | 16 | 84 431 | 92 ms | 50 ms | 213 ms | 100 ms | 520 ms | 48 MiB | 306 / 374 MiB |

Every flush wrote one row group. The four tenants, per tenant:

| tenant | req/s | spans/s | p95 |
|---|---:|---:|---:|
| t1 | 1 316 | 15 790 | 23.35 ms |
| t2 | 1 312 | 15 748 | 23.40 ms |
| t3 | 1 311 | 15 727 | 23.57 ms |
| t4 | 1 315 | 15 782 | 23.28 ms |

Files at the end: t1 52 L0 files, 4 792 992 rows, 249 MB; t2, t3 and t4 6
L0 files each, about 472 000 rows and 25 MB.

## Queries, p50 / p95 / max

| query | during 32 senders | during 8 senders | at rest |
|---|---:|---:|---:|
| trace by id | 153 / 198 / 222 ms | 139 / 159 / 179 ms | 139 / 147 / 319 ms |
| span list, 1 service, 15 min, promoted attribute | 161 / 345 / 918 ms | 104 / 159 / 162 ms | 156 / 168 ms / 4.72 s |
| p99 latency per service, 24 h | 213 / 450 / 772 ms | 212 / 441 / 665 ms | 218 / 225 / 232 ms |
| dashboard: rate and errors per service, 5 min | 146 / 319 / 397 ms | 123 / 172 / 196 ms | 145 / 149 / 186 ms |
| top operations by p95, 1 h | 230 / 275 / 536 ms | 268 / 367 / 834 ms | 246 / 257 / 260 ms |

Counters over each suite of 201 statements (5 queries × 40 and the lookup
of sample ids), from the `/metrics` snapshots:

| suite | plannings | prepared hits | prepared misses | files pruned by the trace index | hot batches skipped | hot scans unavailable |
|---|---:|---:|---:|---:|---:|---:|
| during 32 senders | 201 | 201 | 0 | 665 | 1 098 | 0 |
| during 8 senders | 201 | 201 | 0 | 1 360 | 743 | 0 |
| at rest | 201 | 201 | 0 | 1 920 | 0 | 0 |

No hot scan timed out. The server log holds one warning besides TLS off: an
ingester heartbeat that took 1.47 s while the catalog was loaded.

## Reading

**B5: one planning per statement.** Every statement planned once in
`GetFlightInfo` and executed from the prepared plan in `DoGet`: plannings
equal statements and every `DoGet` hit, in all three suites.

**B2 and B3: the indexes prune.** The trace-id sidecar pruned 665 to 1 920
files per suite, growing with the files the tenant had, and the hot trace
index skipped 1 098 batches under 32 senders. `trace by id` p95 is 198 ms
under 32 senders, against 384 ms in the
[retention run](2026-10-26-retention-and-trace-lookup.md), and 147 ms at
rest; the floor of about 140 ms is the same at rest and under load.

**A4: tenants share the ingester evenly.** Four tenants of 8 senders each
got the same throughput within 0.4% and the same latency, and together
moved as much as one tenant with 32 senders. Flushes were shorter (520 ms
against 871 ms) with smaller uploads, but live buffers ended at 306 MiB
against 85 MiB with one coalesce still running: four tenants keep four sets
of buffers.

**A3: the writer holds about 47 MiB per flush** of 100 000 rows and 5 MiB of
Parquet, one row group each. That is the measured peak the roadmap needs to
retune the flush lease, not done here.

**Throughput is below the earlier runs and this run does not explain it.**
59 583 spans/s at 32 senders alone against 99 280 in the
[parallel coalescer run](2026-10-26-parallel-coalesce.md) and 125 028 with
queries in the retention run. Those ran on disk B and two weeks of code
earlier; this one ran on disk A with other containers on the host. The
per-request fsync on disk A measures as in the baseline (0.72 ms against
0.69 ms) and the group-commit stage is not the bound. Recorded as
inconclusive, not as a confirmed regression: the load runs alone are to be
repeated on disk B at 54bd761 with the same `bench.toml`, without the other
containers if possible, to tell the disk from the code.

**Against DESIGN.md section 9.2** the query p95s are inside their targets
(trace by id 198 ms of 300, span list 345 ms of 500, p99 per service 450 ms
of 2 s), but on 5.7 million spans and a local store, not 1 TB per tenant on
S3: this does not validate the targets.
