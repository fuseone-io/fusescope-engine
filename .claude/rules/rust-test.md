---
paths:
  - "**/tests/*.rs"
  - "**/src/**/*.rs"
---

# Rust tests

- **Unit tests** live in `#[cfg(test)] mod tests` next to the code and test one module's behaviour without infrastructure. **Integration tests** live in `crates/<crate>/tests/` and drive the crate's public API end to end.
- **Database tests** read `FS_TEST_DATABASE_URL`, create a schema named `<prefix>_<uuid>` through the `TestDb` helper, migrate into it and drop it at the end, so they run in parallel and leave nothing behind. When the variable is unset they print `skipped: FS_TEST_DATABASE_URL is not set (run \`make dev-up\`)` and return. CI always sets it.
- **Buckets in tests** are temporary directories behind `LocalFileSystem`. Create the directory before opening the store.
- **Ports in tests** are `127.0.0.1:0`; servers report the bound address through a oneshot channel. Never hard-code a port.
- **Assert on behaviour, not logs.** Count rows, read the Parquet footer, query the catalog, check the gRPC status code. A test that passes because a log line appeared is not a test.
- **Crash scenarios are first-class tests.** Simulate the crash by aborting the task, by disabling the step after the one under test through a configuration knob that exists only for tests (`checkpoint_after_flush`), or by making the next resource fail (an unwritable directory). Then restart on the same directories and assert the exact end state: files, rows, catalog entries, no duplicates.
- **Lints in tests.** Integration test files start with `#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::panic, clippy::too_many_lines)]`; everything else from the workspace lint set still applies. In `#[cfg(test)]` modules `clippy.toml` already allows `unwrap`/`expect`/`panic`.
- **Clippy pedantic shapes assertions**: `assert_eq!(x.len(), 0)` rather than `assert!(x.is_empty())`, `RecordBatch::num_rows` rather than `|b| b.num_rows()`, `ref` bindings in `matches!` guards that borrow.
- **Fixtures are code, not files.** Build OTLP requests and batches with small helper functions in the test file; vary values across cases instead of hoisting shared constants.
- **Time in fixtures is explicit.** Use a named constant (`T0`) with its ISO date in a comment, or `Utc::now()` when the test depends on "recent"; never mix the two in one assertion.
