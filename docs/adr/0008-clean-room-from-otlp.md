# 0008 · Clean-room mapping from OTLP; AGPL exporter code is never read

- Status: accepted
- Date: 2026-10-24

## Context

The rules that turn OTLP into the current ClickHouse rows (which attributes go
to resources_string, how resource_fingerprint is computed, how histograms are
exploded) live in the ClickHouse exporter of fusescope-otel-collector, which is
AGPL-3.0. The engine is Apache-2.0 and must not derive from that code. The
FuseScope repository rules already demand clean-room reimplementation for
proprietary upstream code.

## Decision

The OTLP to schema mapping is specified from three sources only: the
OpenTelemetry protocol definitions, the OpenTelemetry semantic conventions, and
observed data produced by the current pipeline (input OTLP next to output rows).
Contributors do not open, copy, translate or summarize the exporter source,
including from git history. Every mapping decision is written down in
fs-schema documentation with its source. Pull requests touching the mapping say
"clean-room" in the description.

Internal values such as resource_fingerprint are engine-defined and never
compared with ClickHouse values. Result comparison during migration happens on
trace_id, span_id, timestamps and attribute values.

## Consequences

Some mapping details will differ from the current pipeline and may need
adjustment when observed data disagrees with expectation. In exchange the
engine's license posture is clean and the schema stops living in the AGPL
repository (ADR 0006).
