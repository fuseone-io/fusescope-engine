# 0002 · PromQL stays in the Go Prometheus engine

- Status: accepted
- Date: 2026-10-24

## Context

The Go backend already runs the Prometheus PromQL engine over a storage.Queryable implemented on ClickHouse. GreptimeDB shows PromQL on DataFusion is possible but it is a full reimplementation.

## Decision

The engine exposes a typed metrics.select action (label matchers, range, step) returning series as Arrow. A new flightprometheus package in Go implements storage.Queryable over it.

## Consequences

No PromQL semantics to maintain in Rust. Metric performance depends on the postings index and rollups, not on a new evaluator.
