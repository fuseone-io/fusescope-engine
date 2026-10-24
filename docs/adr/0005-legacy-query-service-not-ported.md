# 0005 · Legacy query-service v3/v4 is not ported

- Status: accepted
- Date: 2026-10-24

## Context

pkg/query-service/app/{logs,traces,metrics}/{v3,v4} in the Go backend contain raw ClickHouse SQL. SigNoz upstream is retiring them in favor of querier v5 and the statementbuilder packages.

## Decision

The engine integrates through telemetrystore and statementbuilder only. Legacy paths keep running on ClickHouse until removed with the cutover.

## Consequences

Less code to port. Any feature that only exists in the legacy paths must be rebuilt on v5 before cutover, which is tracked as migration work.
