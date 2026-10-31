# 0009 · Text bloom per row group now, Tantivy at L1/L2 when measured

- Status: accepted
- Date: 2026-10-31
- Supersedes: [0003](0003-ngram-bloom-before-tantivy.md)

## Context

ADR 0003 chose an ngram and token bloom for log text search before Tantivy,
without saying at what granularity or what would justify Tantivy later. The
backend searches bodies with `lower(body) LIKE '%x%'` and `hasToken`. A bloom
per file prunes nothing inside a 512 MB L1 file, and a rare match then reads
every row group. Common terms (`error`, `timeout`), fragments shorter than an
ngram, regex, phrases and ranking are what a bloom cannot serve; how much
they cost is unknown until measured.

Prior art (timefusion, MIT, Tantivy per Parquet file) records manifest
read-modify-write contention in the bucket, index writer memory charged
against the ingest buffer, and a wrong-result bug in an id prefilter.

## Decision

- Every log file, L0 and L1 (L2 when it exists), gets one `.ngb` sidecar
  holding, per row group, a trigram bloom and a token bloom over the
  ASCII-lowercased body. It is bound to the exact Parquet bytes by their
  SHA-256 and to the file's row-group geometry; any doubt reads the row
  group.
- The index sits behind one contract that answers per file `All`, `Skip`
  or a list of row groups, and can later answer row positions.
- Tantivy enters only at L1/L2, built by the compactor over closed hours as
  a derived, rebuildable sidecar behind the same contract; never in the
  ingest path.
- The trigger for Tantivy is measured: seven consecutive days in staging,
  each with at least 100 eligible text searches, where the median precision
  (files with a real match over files the index kept) is below 0.2, the p95
  of `fusescope_query_text_seconds` is above 1 s, and a profile shows scan
  and filter as a material part of that latency.
- From the prior art: sidecars are created once per file, the catalog is
  the only manifest; index memory is a flush lease of its own, not hidden
  in the buffer; every index is checked by a differential test (same rows
  with and without it).

## Consequences

Rare matches in large files read only the row groups that may hold them.
The sidecar format, the normalization and the bloom sizing are durable and
versioned; flush intents record the sizing so a replay writes the same
bytes. Case-insensitive search prunes only ASCII row groups. Regex, `NOT`
and short fragments are not pruned. Tantivy stays out until the numbers say
otherwise.
