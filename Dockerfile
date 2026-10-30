# syntax=docker/dockerfile:1.7
# The engine's image: one static-ish binary on distroless, running as the
# image's non-root user, with the operating system's CA bundle for every
# TLS client, and the CycloneDX SBOM of the binary's crates at
# /usr/share/doc/fusescope-engine/sbom.cdx.json. Only /var/lib/fusescope/wal
# and /var/lib/fusescope/spill are written; run it with a read-only root
# (docs/ops/image.md).

ARG RUST_VERSION=1.99.1

FROM rust:${RUST_VERSION}-bookworm AS build
ARG RUST_VERSION
# rust-toolchain.toml asks for "stable"; build with the image's toolchain
# instead of fetching another.
ENV RUSTUP_TOOLCHAIN=${RUST_VERSION}
# The SBOM generator, its own dependencies locked by its published
# lockfile; it stays in this stage and only its output reaches the image.
ARG CYCLONEDX_VERSION=0.5.9
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo install cargo-cyclonedx --version ${CYCLONEDX_VERSION} --locked --root /opt/cyclonedx
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p fs-server --bin fusescope-engine \
    && PATH="/opt/cyclonedx/bin:$PATH" cargo cyclonedx \
        --manifest-path crates/fs-server/Cargo.toml --describe binaries --all \
        --format json --spec-version 1.5 --target "$(rustc -vV | sed -n 's/^host: //p')" \
    && install -D -m 0644 crates/fs-server/fusescope-engine_bin.cdx.json /out/sbom.cdx.json \
    && install -D target/release/fusescope-engine /out/fusescope-engine \
    && install -d -o 65532 -g 65532 /out/state/wal /out/state/spill

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /out/fusescope-engine /usr/local/bin/fusescope-engine
COPY --from=build /out/sbom.cdx.json /usr/share/doc/fusescope-engine/sbom.cdx.json
COPY --from=build --chown=65532:65532 /out/state /var/lib/fusescope
COPY config/container.toml /etc/fusescope/config.toml
USER 65532:65532
VOLUME ["/var/lib/fusescope/wal", "/var/lib/fusescope/spill"]
# OTLP gRPC, Flight SQL, the internal hot-window Flight, metrics and probes.
EXPOSE 4317 8815 8816 9464
ENTRYPOINT ["/usr/local/bin/fusescope-engine"]
CMD ["--config", "/etc/fusescope/config.toml"]
