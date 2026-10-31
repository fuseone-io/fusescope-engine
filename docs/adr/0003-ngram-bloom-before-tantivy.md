# 0003 · Ngram bloom for log text first, Tantivy later

- Status: superseded by [0009](0009-text-bloom-per-row-group-tantivy-by-measure.md)
- Date: 2026-10-24

## Context

ClickHouse in SigNoz serves log text search with ngrambf_v1 skip indexes, not an inverted index. Tantivy is MIT and mature but adds a second indexing pipeline and storage format.

## Decision

Phase 2 ships an ngram and token bloom sidecar per file. Tantivy is added only when the product needs ranked full-text or phrase search.

## Consequences

Feature parity with today's log search at a fraction of the complexity. Full-text with ranking is explicitly deferred.
