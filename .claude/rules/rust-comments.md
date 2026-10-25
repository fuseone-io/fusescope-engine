---
paths:
  - "**/*.rs"
---

# Rust comments

The bar is the [`comments`](comments.md) rule: nothing where the code is self-explanatory.

- **Names carry the meaning.** Make function, type, and variable names self-explanatory so the comment is unnecessary in the first place. If a comment is needed to explain what a function does, fix the name, not the comment.
- **Doc comments (`///`, `//!`)** exist because `missing_docs` is on. They state what is non-obvious: units, invariants, ordering, what happens on failure. A doc that restates the identifier is worse than none; write the one sentence that the name cannot carry.
- **`# Errors`** on every public fallible function names the conditions, not the type.
- **Module docs (`//!`)** explain the protocol or format the module implements when it has one (WAL framing, flush protocol, ticket encoding), in the module that owns it, once.
- **`unsafe` blocks** carry a `// SAFETY:` comment stating the invariant that makes them sound. Only `fs-cache` and `fs-index` may contain them.
- **No `TODO` without an owner and a reference** (issue or ADR). Deferred work goes in `DESIGN.md` or the ADR, not in the source.
