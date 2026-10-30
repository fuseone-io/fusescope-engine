# Container image

The `Dockerfile` at the repository root builds the engine's image: a
release build of `fusescope-engine` on Debian bookworm, copied onto
`gcr.io/distroless/cc-debian12:nonroot`. The runtime has no shell and no
package manager; it carries glibc, libgcc and the Debian CA bundle at
`/etc/ssl/certs/ca-certificates.crt`, which every TLS client of the engine
reads (the bucket, Postgres, the internal Flight), so a private CA is added
to that bundle in a derived image, not configured in the engine.

| | |
|---|---|
| User | `65532:65532` (distroless `nonroot`) |
| Entrypoint | `/usr/local/bin/fusescope-engine` |
| Default arguments | `--config /etc/fusescope/config.toml` |
| Default configuration | `config/container.toml`: every role, all interfaces, `[tls]` and `[auth]` off |
| Writable paths | `/var/lib/fusescope/wal`, `/var/lib/fusescope/spill`, both declared volumes |
| Ports | 4317 OTLP gRPC, 8815 Flight SQL, 8816 hot-window Flight, 9464 metrics and probes |
| SBOM | `/usr/share/doc/fusescope-engine/sbom.cdx.json`, CycloneDX 1.5 JSON of the binary's crates |

Run it with a read-only root and the two state paths mounted; nothing else
is written. The WAL holds rows not yet flushed and must survive a restart,
so in Kubernetes it is a persistent volume per ingester (`docs/ops/helm.md`); the spill path is
scratch for large sorts and joins and may be an `emptyDir`.

```sh
docker run --read-only \
  -v fusescope-wal:/var/lib/fusescope/wal \
  --tmpfs /var/lib/fusescope/spill:uid=65532,gid=65532 \
  -v "$PWD/config/staging.toml:/etc/fusescope/config.toml:ro" \
  -e FS_CATALOG_URL=... -e FS_BUCKET_ACCESS_KEY=... -e FS_BUCKET_SECRET_KEY=... \
  fusescope-engine:dev
```

Configuration comes from the mounted file and the environment overrides
(`FS_ROLE`, `FS_NODE_ID`, `FS_BUCKET_NAME`, `FS_BUCKET_ENDPOINT`,
`FS_INGEST_HOT_ADVERTISE`, …); secrets only from the environment. Liveness is
`GET /healthz` and readiness `GET /readyz` on the metrics port, since the
image has no shell for an exec probe.

`make image` builds it as `fusescope-engine:dev`. `make image-check`
(`scripts/image-check.sh`), and the CI `image` job on every push, prove the
image against a throwaway Postgres the script starts on an ephemeral
loopback port and removes, never a catalog given from outside, since
`--role all` runs a compactor: it runs as `65532:65532`, its CA bundle holds
certificates, and with a read-only root, only the WAL, spill and a tmpfs
bucket writable, `--check` passes and `--role all` starts, reports ready on
`/readyz` and stops cleanly on SIGTERM. Nothing forces a spill, so that
path is mounted but not proven written. The Rust version of the build stage
is the `RUST_VERSION` argument, bumped with the toolchain.

The SBOM is generated in the build stage by `cargo-cyclonedx` 0.5.9,
installed with `--locked` and left in that stage, describing the
`fusescope-engine` binary for the build's target with its full dependency
graph and no development dependencies (`--describe binaries --all`). It is
copied into the image readable by every user and published by the CI
`image` job as the `fusescope-engine-sbom` artifact; `scripts/sbom-check.py`
checks it is CycloneDX 1.5 JSON whose root is the binary, with a non-empty
graph naming the engine's runtime crates. It lists crates only: the
distroless base's Debian packages are recorded by the base image itself.

## Releases

A tag `v<version>`, equal to the workspace version, publishes the image
as `ghcr.io/fuseone-io/fusescope-engine:<version>` (`.github/workflows/release.yml`).
The workflow builds it, runs the same `scripts/image-check.sh` as CI and
publishes nothing if that fails; then it pushes, signs the digest with
cosign keyless, attests the build provenance and the CycloneDX SBOM by
digest, and creates a GitHub pre-release with the SBOM attached. Images are
`linux/amd64` only for now. The engine is experimental and its releases
are pre-releases.

Verify an image before running it:

```sh
cosign verify ghcr.io/fuseone-io/fusescope-engine:<version> \
  --certificate-identity-regexp '^https://github.com/fuseone-io/fusescope-engine/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
gh attestation verify oci://ghcr.io/fuseone-io/fusescope-engine:<version> --owner fuseone-io
```
