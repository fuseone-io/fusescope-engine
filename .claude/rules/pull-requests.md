# Pull requests

- **Follow the template** (`.github/pull_request_template.md`): fill in its headings (Description / Test plan / Issues closed by this PR / Additional Information). Don't add sections the template doesn't have.
- **Keep only the headings that apply.** Delete every heading that has nothing under it, along with its `<!--...-->` placeholder comment. The body must never contain an empty heading.
- **Keep the description concise and human-readable.** A few non-repetitive bullets saying what changed and why, for a reviewer skimming it — not a wall of text, not a restatement of the diff, not generated boilerplate.
- **Test plan names the crash boundaries covered** when the change touches the durability path.
- **Reference issues with `Closes #issue-number`** under "Issues closed by this PR" so they auto-close on merge. This goes in the PR description only — never in commit messages.
- **Breaking changes** to a durable format, a configuration key or the Flight contract go under Additional Information, with the migration path.
- **AI-assisted commits end with a `Co-Authored-By:` trailer** naming the model (e.g. `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`).
- **Conventional commit format** for commits and PR title, scoped by crate: `feat(ingest): …`, `fix(wal): …`, `docs(design): …`, `chore(deps): …`.
- **Keep the commit body short**, focused on the decision made; skip it when the title says everything.
- **Amending and force-pushing are fine on PR branches** before merge. Never rewrite `main`; merges are fast-forward and only with CI green (`check` and `deny` jobs).
