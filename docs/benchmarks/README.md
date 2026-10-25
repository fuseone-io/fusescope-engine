# Benchmarks

Numbers in `DESIGN.md` are targets until a file in this directory shows them
measured. Every result file names the commit, the date, the hardware and the
exact commands, so a later run on the same machine is comparable and a run on
a different machine is recognisably different.

## What is measured

| Mode | Command | Measures |
|---|---|---|
| Ingest stages | `make bench-stages` | Each stage of the ingest path in process over the same generated traffic: protobuf encode, WAL append and fsync, OTLP to Arrow, concat and sort, Parquet encode, upload, hot-window filter. Pure per-stage cost, no concurrency. |
| Load | `make bench-load TOKEN=…` | Requests and spans per second, latency percentiles and errors of a running ingester under concurrent OTLP senders. |
| Query suite | `make bench-query TOKEN=…` | The DESIGN.md queries over Flight SQL against a running querier: trace by id, span list with a promoted attribute, p99 per service over 24 h, the 5 minute dashboard, top operations. |

The traffic generator (`crates/fs-bench/src/traffic.rs`) is seeded and shaped:
20 services, 15 operations, 12 spans per trace in a parent chain, HTTP
attributes plus 6 extra string attributes, 2% errors, timestamps spread over a
window ending now. Change the shape with flags and say so in the result file.

## Rules

- Release builds only. The WAL directory must be on a real disk; `/tmp` is a
  tmpfs on many machines and makes fsync free. `make bench-stages` writes to
  `./data/bench-wal`; pass `BENCH_ARGS="--wal-dir /mnt/…"` to choose a disk.
- One result file per run worth keeping: `YYYY-MM-DD-<topic>.md`, with the
  JSON from `--json` next to it when the run feeds a comparison.
- A result that motivates a change links the commit that made the change and
  is followed by a result on the same hardware after it.
