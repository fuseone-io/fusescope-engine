# 0011 · `quantile_compat`: exact up to 10,010 values, t-digest above

- Status: accepted
- Date: 2026-11-14

## Context

The parity judges each engine quantile against the exact distribution of
the group, read from ClickHouse. While one value is coarser than its rank
tolerance, `0.1·q·(1 − q) ≤ 1/n`, the value must lie between the two order
statistics around rank `q·(n − 1)`. Above that it may be off by
`0.1·q·(1 − q)` in rank. ClickHouse's own `quantile` interpolates at that
rank over every value up to 8,192 values and samples a reservoir past them.

DataFusion's `approx_percentile_cont` estimates at rank `q·n`. In the order
statistic regime this half-value shift puts about half of the groups outside
the interval, whatever the number of centroids (1000 centroids, fusescope#408,
still left 17 blocking differences in a demo round). The exact
`percentile_cont` passes, but holds every value of every group, so a query
ClickHouse answers can exhaust the memory pool.

## Decision

The engine registers `quantile_compat(value, q)`, a compatibility function,
not a reimplementation of ClickHouse's `quantile`:

- Up to 10,010 values a group is in the parity's exact regime: it keeps them
  all and answers the linear interpolation at rank `q·(n − 1)`. 10,010 is the
  largest `n` the parity judges by order statistics for the backend's highest
  quantile, p999: `0.1·0.999·0.001 ≤ 1/10 010` and `> 1/10 011`.
- The algorithm diverges from ClickHouse's from 8,193 values: ClickHouse
  samples there, the function stays exact up to 10,010 and then folds the
  values into a t-digest of 1000 centroids, which stays within the rank
  tolerance. Only the parity's rule is shared.
- Partial states carry either the values or the digest; merging exact with
  exact, exact with digest and digest with digest gives the same answer
  whatever the split across partitions, exactly while the group is at most
  10,010 values.
- Each accumulator (one per group, quantile and partition) holds at most
  about 80 KB of values or 16 KB of digest, charged to the memory pool, plus
  a copy while a batch crosses the limit or a state is serialized.
- The limit and the centroid count are constants. A quantile finer than p999
  would need a larger limit, decided by the same evidence.

## Consequences

- Quantiles of raw values match ClickHouse within the parity's rules at every
  group size, with bounded memory per group.
- The engine's SQL surface gains a function the backend's dialect calls; the
  backend switches from `approx_percentile_cont` once a release carries it.
- An aggregate without groups whose one accumulator exceeds the pool fails
  with `RESOURCE_EXHAUSTED`; grouped aggregates emit partial states early and
  spill.
