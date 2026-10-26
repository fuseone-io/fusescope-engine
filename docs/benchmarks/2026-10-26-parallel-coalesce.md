# 2026-10-26 · parallel coalescer

Same machine, disks, services and traffic shape as the
[baseline](2026-10-25-baseline.md); code at 12b85a4 (several coalesces per
tenant under `max_coalesces`, flushes materialised on their own tasks under
`max_flushes`, counted waiting batches, edge-triggered wake-ups). Raw JSON
and the histogram page in
[`2026-10-26-parallel-coalesce/`](2026-10-26-parallel-coalesce/). Compare with
the [coalesce](2026-10-25-coalesce.md) and [gate](2026-10-26-gate-costs.md)
runs.

Commands as in the gate run: load for 30 s with `--metrics`, and for the
"with queries" rows `fs-bench query --iterations 40` against the same tenant
at the same time.

## Results

| run | req/s | spans/s | refused | flushes | rows per flush | concat | sort | encode | total per flush | batches merged |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 32 senders, one chunk at a time (gate run) | 6 637 | 79 650 | 10 673 | 9 | 229 487 | 164 ms | 197 ms | 607 ms | 1 076 ms | 86% |
| 32 senders, first parallel version (edbccfb) | 4 503 | 54 033 | 28 054 | 10 | 172 790 | 317 ms | 128 ms | 441 ms | 952 ms | 42% |
| 32 senders, alone (12b85a4) | 8 273 | 99 280 | 10 156 | 15 | 201 059 | 54 ms | 158 ms | 550 ms | 857 ms | 98% |
| 32 senders, with queries (12b85a4) | 7 345 | 88 138 | 2 648 | 13 | 201 776 | 71 ms | 187 ms | 592 ms | 969 ms | 98% |
| 8 senders, with queries (12b85a4) | 4 215 | 50 577 | 0 | 7 | 206 988 | 42 ms | 175 ms | 625 ms | 935 ms | 100% |

Coalescer, with queries at 32 senders: 417 chunks of 520 batches, 46 ms
each, p95 within 100 ms, never more than a few in flight. Hot-window
snapshot: within the 32-sender window, 15 snapshots with p95 within 20 ms;
over the whole session (warm-up and both runs, 402 snapshots) 95% within
2 ms and the worst within 20 ms. No hot scan timed out. Checkpoints p95
within 20 ms.

Buffers at the end of the 32-sender run with queries: live 69 MiB, flushing
0, persisted 735 MiB.

## Reading

**The first parallel version was slower than the serial one, and the trace
said why.** Three things compounded: the request path woke the flusher on
every request while a backlog lasted, and each wake-up walked every entry
under the buffer lock; the flusher encoded and uploaded each file inline,
so during that second the live buffer grew raw past the flush threshold;
and the cut that followed took the runs being merged, whose chunks were
then thrown away. Groups of 3 467 batches were taken four at a time and
discarded 20 ms later.

**With flushes off the loop the coalescer keeps up.** 98% of request
batches are merged before their flush, concat fell to 0.27 µs per row, and
throughput is the highest measured so far: 99 k spans/s alone, 88 k with
queries running. Parquet encode, 2.9 µs per row here, is now 60% of the
flush.

**Refusals are the retention cap.** The remaining `RESOURCE_EXHAUSTED`
answers are the per-tenant cap met by 15 s of persisted rows: 735 MiB to
981 MiB of chunks, about 700 bytes per span of this traffic shape in Arrow.
Tying the retention to the querier's file cache TTL (roadmap A2) is the
next step on memory; a chunk finished after its rows were cut now replaces
them in the flushing rows, so the retention holds no raw batches.

**`trace by id` is unchanged at 1.4 to 2.1 s p95**, which is the read path:
no trace-id predicate reaches the hot window and no index narrows the file
list (roadmap B1 to B3).
