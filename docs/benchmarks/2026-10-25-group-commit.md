# 2026-10-25 · WAL group commit

Same machine, disks, services and traffic shape as the
[baseline](2026-10-25-baseline.md). Raw JSON in
[`2026-10-25-group-commit/`](2026-10-25-group-commit/).

## Change

One writer thread owns WAL appends. It drains everything queued, appends the
batch, fsyncs once and acknowledges every request in it. No timer: idle
requests wait only for their own fsync, and under load the batch grows while
the previous fsync runs. The flush gate and intents are unchanged: a request
still holds the gate until its record is durable.

Two things had to change with it, found by the first measurement after the
writer landed, which showed 1.45 records per fsync in the server although the
in-process stage showed 30:

- `check_capacity` read the WAL size by locking the WAL, which the writer holds
  through every fsync. Every request therefore waited for the fsync in flight
  before it could even queue, so arrivals were synchronised into batches of
  one. The writer now publishes the size in an atomic.
- `fs-bench load` multiplexed every sender over one HTTP/2 connection, which
  capped the generator near 3 000 requests per second. Each sender now has its
  own connection.

## Ingest stage (in process, 5 000 requests, 60 000 spans)

| WAL stage | disk B (plain NVMe) | disk A (NVMe + LUKS) |
|---|---:|---:|
| append + fsync per request, 1 sender (baseline) | 2.08 s | 3.66 s |
| group commit, 64 senders | 90 ms | 160 ms |
| records per fsync | 29.4 | 30.1 |

23× on both disks; the per-span cost of the WAL drops from 33 µs to 1.5 µs,
below the conversion cost.

## Load (release server, all roles, WAL on disk B, bucket RustFS, 30 s)

| senders | req/s | spans/s | p50 | p95 | p99 | errors |
|---:|---:|---:|---:|---:|---:|---:|
| 8, baseline | 2 196 | 26 354 | 3.20 ms | 5.23 ms | 6.95 ms | 0 |
| 32, baseline | 2 216 | 26 589 | 13.40 ms | 24.73 ms | 49.62 ms | 0 |
| 8 | 5 520 | 66 237 | 0.85 ms | 1.20 ms | 40.45 ms | 0 |
| 32 | 9 524 | 114 290 | 1.59 ms | 2.51 ms | 40.82 ms | 10 660 (3.6%) |
| 128 | 7 796 | 93 551 | 7.03 ms | 11.61 ms | 47.52 ms | 67 873 (22%) |

Across the three runs: 686 619 records in 109 428 batches, 6.3 records per
fsync, 51 s of fsync time over 90 s of load. 8.2 million spans ingested, 61
files flushed, no flush failure.

**Reading.** Throughput now scales with concurrency: 4.3× at 32 senders, with
p50 under 2 ms. The errors at 32 and 128 senders are all
`RESOURCE_EXHAUSTED: tenant buffer full, flush pending`: at 114 k spans/s the
tenant produces about 180 MB/s of Arrow, the flusher writes one file at a
time through sort, Parquet, upload and registration, and the 512 MiB cap on
live plus flushing rows is reached. That is backpressure working as designed,
and it names the next bottleneck: the flush path, which is sequential per
tenant and encodes a whole file in memory. The p99 of about 40 ms in every
run is the same thing seen from the other side, a flush competing for CPU.

Against the DESIGN.md target of 150 MB/s of OTLP per ingester: 9 500
requests × 4 KiB is about 39 MB/s, from 9 MB/s at the baseline. The WAL is no
longer the limit; the flusher is.
