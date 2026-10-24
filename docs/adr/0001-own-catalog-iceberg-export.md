# 0001 · Own catalog in Postgres, Iceberg only as L2 export

- Status: accepted
- Date: 2026-10-24

## Context

Apache Iceberg gives atomic commits and broad tool compatibility, but its optimistic concurrency on a single metadata pointer and manifest growth suit batch workloads. Ingesters flush every few seconds and the engine already depends on Postgres.

## Decision

The file catalog lives in Postgres and is the single source of truth. The compactor writes Iceberg metadata for level L2 asynchronously so Trino, DuckDB and Spark can read compacted data.

## Consequences

No Lakekeeper or Polaris to operate. Hot path writes never contend on Iceberg commits. External tools see data with the L2 delay, which is acceptable for analytics.
