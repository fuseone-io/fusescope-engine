# 0006 · Collector exports standard OTLP to the engine

- Status: accepted
- Date: 2026-10-24

## Context

fusescope-otel-collector (AGPL-3.0) owns the ClickHouse exporter and the telemetry schema. The Go backend imports collector packages, which the repository rules want to eliminate.

## Decision

The collector uses the stock OTLP (and later OTel-Arrow) exporter pointed at the ingesters. The engine owns the schema in fs-schema.

## Consequences

The schema leaves the AGPL repository. The collector becomes a plain upstream build with FuseScope processors, easier to track.
