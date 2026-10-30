# Helm chart

`deploy/helm/fusescope-engine` runs the three roles from one `values.yaml`:

| Role | Workload | State | Service |
|---|---|---|---|
| ingester | StatefulSet, `podManagementPolicy: Parallel` | WAL on a `ReadWriteOnce` claim per pod (`ingester.wal.size`) | `<release>-ingest` (OTLP 4317), headless `<release>-ingester-hot` (8816) |
| querier | Deployment | spill on an `emptyDir` (`querier.spillSizeLimit`) | `<release>-flight` (Flight SQL 8815) |
| compactor | Deployment, one replica holding the lease | none | none |

Every pod runs the image as `65532:65532` with a read-only root, no
privilege escalation, all capabilities dropped and the runtime's default
seccomp profile; it writes only to its WAL claim and its spill `emptyDir`.
Each role has its own ServiceAccount, whose annotations bind workload
identity, and no pod mounts its API token. Each role gets its own
`config.toml` from one ConfigMap, the node id from the pod name. Probes run
on the metrics port 9464: a startup probe on `GET /healthz` allows
`startupTimeoutSecs` (600) for the start, migrations and the bucket's
ownership check included, before liveness (`/healthz`) and readiness
(`/readyz`) begin; a pod gets `drainTimeoutSecs` plus 30 seconds to stop.

The roles are kept apart, since the hot window trusts the tenant in its
ticket and each role needs different rights:

- `secrets.ingester`, `secrets.querier`, `secrets.compactor`: one Secret
  per role, created outside the chart, whose keys are `FS_CATALOG_URL`,
  `FS_BUCKET_ACCESS_KEY` and `FS_BUCKET_SECRET_KEY`, read as environment.
  Each names its own Postgres user and bucket identity (DESIGN.md 10.4):
  the ingester writes L0, the querier reads, the compactor reads, writes
  and deletes. With workload identity the bucket keys stay out and the
  role's ServiceAccount carries the annotation. The three must differ.
- With `tls.enabled` (the default), `tls.clientCASecret` holds
  `public-ca.crt`, the backend's client CAs for Flight SQL, and
  `hot-ca.crt`, the queriers' for the hot window, roots that share no key.
  `tls.identities.ingester` and `tls.identities.querier` are each a Secret
  with `tls.crt` and `tls.key`, and must differ. The ingester's serves the
  hot window, names `*.<release>-ingester-hot.<namespace>.svc` (it
  advertises `<pod>.<release>-ingester-hot.<namespace>.svc:8816`) and must
  not chain to `hot-ca.crt`; the querier's serves Flight SQL, names
  `<release>-flight`, and is the only identity issued under `hot-ca.crt`,
  with `serverAuth` and `clientAuth`. The compactor serves and dials no
  Flight and gets no identity.
- With `auth.mode=jwt` (the default), `auth.jwksConfigMap` and
  `auth.issuer`: a ConfigMap with `jwks.json`, the backend's Ed25519 keys,
  mounted on the queriers only.

The chart refuses to render a release missing any of these, with two roles
sharing a Secret, the ingester and the querier sharing an identity,
`auth.mode=jwt` and TLS off, or an unknown `auth.mode`. A pod reads its
Secrets' environment at start: after rotating one, set the role's
`rolloutTokens` entry to a new value, or `kubectl rollout restart` the
workload; a change to the configuration rolls it by itself.
`ci/staging-values.yaml` is the shape of a staging release;
`ci/dev-values.yaml` turns TLS and JWTs off for a development cluster.

`make chart-check` (`scripts/chart-check.sh`) lints the chart strictly
with each `ci/` values file, validates the rendered objects with
`kubeconform`, runs the engine's `--validate` on each role's rendered
configuration, checks on the rendered workloads that the roles keep their
own Secret, ServiceAccount and identity (none for the compactor), mount no
API token and have a startup probe, and checks the refusals; the CI
`image` job runs the same against the built image. `--validate` reads no
certificate and connects nowhere, so certificates themselves are proven at
the first start.
