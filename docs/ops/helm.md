# Helm chart

`deploy/helm/fusescope-engine` runs the three roles from one `values.yaml`:

| Role | Workload | State | Service |
|---|---|---|---|
| ingester | StatefulSet, `podManagementPolicy: Parallel` | WAL on a `ReadWriteOnce` claim per pod (`ingester.wal.size`) | `<release>-ingest` (OTLP 4317), headless `<release>-ingester-hot` (8816) |
| querier | Deployment | spill on an `emptyDir` (`querier.spillSizeLimit`) | `<release>-flight` (Flight SQL 8815) |
| compactor | Deployment, one replica holding the lease | none | none |

Every pod runs the image as `65532:65532` with a read-only root, no
privilege escalation, all capabilities dropped and the runtime's default
seccomp profile; it writes only to its WAL claim and its spill
`emptyDir`. Probes are `GET /healthz` and `GET /readyz` on the metrics
port 9464, and a pod gets `drainTimeoutSecs` plus 30 seconds to stop. The
configuration is one rendered `config.toml`, the role given by `--role`,
the node id by the pod name; a change to it rolls every workload.

What the chart does not create, a release names:

- `secretName`: a Secret whose keys are `FS_CATALOG_URL`,
  `FS_BUCKET_ACCESS_KEY` and `FS_BUCKET_SECRET_KEY`, read as environment.
- `tls.secretName`, with `tls.enabled` (the default): `tls.crt` and
  `tls.key`, an identity with both `serverAuth` and `clientAuth`;
  `public-ca.crt`, the backend's client CAs; `hot-ca.crt`, the queriers',
  roots that share no key with the backend's. An ingester advertises
  `<pod>.<release>-ingester-hot.<namespace>.svc:8816`, so its certificate
  names `*.<release>-ingester-hot.<namespace>.svc`; the querier's names
  `<release>-flight` as the backend dials it.
- `auth.jwksConfigMap` and `auth.issuer`, with `auth.mode=jwt` (the
  default): a ConfigMap with `jwks.json`, the backend's Ed25519 keys.

The chart refuses to render a release missing any of these, one with
`auth.mode=jwt` and TLS off, or an unknown `auth.mode`.
`ci/staging-values.yaml` is the shape of a staging release;
`ci/dev-values.yaml` turns TLS and JWTs off for a development cluster.

`make chart-check` (`scripts/chart-check.sh`) lints the chart strictly
with each `ci/` values file, validates the rendered objects with
`kubeconform`, runs the engine's `--validate` on the rendered
configuration for every role, and checks the refusals; the CI `image` job
runs the same against the built image.
