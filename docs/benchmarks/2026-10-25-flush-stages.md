# 2026-10-25 · flush stages under load

Same machine, disks, services and traffic shape as the
[baseline](2026-10-25-baseline.md); code at the group-commit result plus the
flush instrumentation. Raw JSON in
[`2026-10-25-flush-stages/`](2026-10-25-flush-stages/).

Command: `fs-bench load --endpoint 127.0.0.1:4317 --duration-secs 30 --concurrency C --metrics 127.0.0.1:9464`

The engine now reports, per flush, the time in sort, Parquet encode, upload
and catalog registration, plus the bytes held live, flushing and persisted
for the hot window. `fs-bench load` reads `/metrics` before and after the run
and prints the per-flush averages.

## Results

| senders | req/s | spans/s | flushes | rows per flush | Parquet per flush | sort | encode | upload | register | total per flush |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | 3 991 | 47 895 | 17 | 83 329 | 5.0 MiB | 314 ms | 238 ms | 31 ms | 3 ms | 599 ms |
| 32 | 7 918 | 95 017 | 17 | 159 336 | 9.2 MiB | 851 ms | 400 ms | 47 ms | 2 ms | 1 318 ms |

Buffers at the end of the run, in MiB:

| senders | live | flushing | persisted (hot window retention) |
|---:|---:|---:|---:|
| 8 | 58 | 0 | 1 025 |
| 32 | 241 | 0 | 2 051 |

Request throughput in this run is below the group-commit result (3 991
against 5 490 at 8 senders) because each run here waited only 8 s after the
previous one and the engine was still flushing and holding persisted rows;
the stage numbers are the point of this run, not the throughput.

## Second run: concat measured apart from sort

The first run timed concatenation and sort together. Split, with the stage
times published only for completed flushes and the memory cap counting the
hot-window retention (`load-c32-concat-split.json`):

| senders | req/s | spans/s | flushes | rows per flush | concat | sort | encode | upload | register | total per flush |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 32 | 7 393 | 88 719 | 20 | 133 166 | 498 ms | 95 ms | 353 ms | 42 ms | 3 ms | 1 005 ms |

Errors in this run were 20 107 `RESOURCE_EXHAUSTED`, most of them the new
`ingester memory full, hot-window retention pending`: persisted rows reached
the 2 GiB cap and admission waited instead of letting memory grow.

## Reading

**Concatenation, not sort, is the largest flush cost.** The first run
blamed sort because the clock covered both; apart, concat is 498 ms and sort
95 ms of a 1 005 ms flush. A flush merges about 11 000 request batches, and
for every one of them the concat merges 18 dictionary columns and the nested
map and list columns. Sort over the merged batch is 0.7 µs per row, which is
not a problem. Upload and registration stay negligible: the flush path is
CPU, in concat and Parquet encode.

**The hot-window retention is the real memory consumer.** Rows stay in
memory for 15 s after their file is registered so queriers with a stale file
list still see them. At 95 k spans/s that is 2 GiB of Arrow per tenant, held
outside every cap: the capacity check counts live and flushing bytes only.
Persisted memory needs a byte bound in addition to the time bound, and the
retention should shrink to what the querier's file cache actually needs.

**What to change, in order of measured impact:**

1. Make the flush see few large batches instead of thousands of small ones:
   coalesce request batches in the buffer as they arrive, or hold the
   low-cardinality columns as plain strings in memory so concat is a copy
   rather than a dictionary merge (Parquet dictionary-encodes them on write
   regardless). The second option changes the in-memory schema and needs its
   own decision.
2. Parquet encode, 2.7 µs per row, is next; streaming row groups spreads it
   over the flush rather than shrinking it.
3. Flush concurrently across tenants with a bounded semaphore. Upload is 4%
   of the flush; parallelism buys little until concat and encode shrink.

The memory cap is in place: live, flushing and persisted rows together are
bounded, persisted rows are never evicted before their retention, and the
ingester answers `RESOURCE_EXHAUSTED` until the retention frees memory.
Sort will not be touched on the strength of the first measurement.
