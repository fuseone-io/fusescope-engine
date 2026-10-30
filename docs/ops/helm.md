# Helm chart

The engine's chart is `charts/fusescope-engine` in
[fuseone-io/fusescope-charts](https://github.com/fuseone-io/fusescope-charts),
where every FuseScope component installs from. The `fusescope` chart lists
it under the alias `engine`, disabled by default while the engine is
experimental; its README there describes what it installs, the Secrets,
identities and key set a release names, `rolloutTokens`, and what it
refuses to render.

The chart renders this engine's configuration, so the two must agree. The
CI `image` job here checks out fusescope-charts and runs its
`scripts/fusescope-engine-chart-check.sh` against the image it just built:
the chart lints, renders valid objects and keeps the roles apart, alone and
under the `fusescope` chart, and every role's rendered configuration
passes this engine's `--validate`. A configuration change that breaks the
chart fails this repository's CI, and changes to the chart land in
fusescope-charts. `make chart-check` runs the same against a local build,
with fusescope-charts checked out at `CHARTS_DIR` (`../fusescope-charts`
by default).
