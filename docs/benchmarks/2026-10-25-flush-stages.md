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

## Reading

**Sort is the largest flush cost**, 5.3 µs per row at 160 k rows, twice the
Parquet encode and 18× the upload. Upload and registration are negligible on
this bucket: the flush path is CPU, not I/O. At 95 k spans/s a 160 k-row file
represents 1.7 s of arrivals and takes 1.3 s to write, on one core, one at a
time, so the live buffer grows between flushes and the 512 MiB cap turns into
`RESOURCE_EXHAUSTED`. The in-process stage benchmark measured sort at 2.6 µs
per row on 60 k rows; the per-row cost grows with the file because the sort
runs through a row converter over dictionary columns.

**The hot-window retention is the real memory consumer.** Rows stay in
memory for 15 s after their file is registered so queriers with a stale file
list still see them. At 95 k spans/s that is 2 GiB of Arrow per tenant, held
outside every cap: the capacity check counts live and flushing bytes only.
Persisted memory needs a byte bound in addition to the time bound, and the
retention should shrink to what the querier's file cache actually needs.

**What to change, in order of measured impact:**

1. Sort by `(service_name, timestamp)` without the row converter: sort the
   dictionary keys and the timestamp as two primitive columns, or sort each
   request batch at conversion and merge sorted batches at flush.
2. Bound persisted bytes per tenant and sample the memory into the capacity
   check, so the cap covers everything the ingester holds.
3. Flush concurrently across tenants with a bounded semaphore, and stream
   Parquet row groups instead of encoding the whole file in memory. Upload
   is 4% of the flush; parallelism buys little until sort and encode shrink.
