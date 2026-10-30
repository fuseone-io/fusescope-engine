#!/bin/sh
# Checks a built image: it runs as the non-root user and carries a
# non-empty system CA bundle; with a read-only root, writing only to its
# WAL and spill paths and to a tmpfs bucket, `--check` passes, and
# `--role all` starts, reports ready, and stops cleanly on SIGTERM. Spill
# is not forced, so its path is mounted but not proven written.
#
# usage: scripts/image-check.sh <image> <catalog-url>
set -eu
image=$1
catalog=$2
name="fusescope-image-check-$$"

user=$(docker image inspect "$image" --format '{{.Config.User}}')
[ "$user" = "65532:65532" ] || { echo "image runs as '$user', not 65532:65532" >&2; exit 1; }

created=$(docker create "$image")
trap 'docker rm -fv "$created" "$name" >/dev/null 2>&1 || true' EXIT
certificates=$(docker cp -L "$created:/etc/ssl/certs/ca-certificates.crt" - | tar -xOf - | grep -c 'BEGIN CERTIFICATE' || true)
[ "$certificates" -gt 0 ] || { echo "image has no certificate in its CA bundle" >&2; exit 1; }

tmpfs="uid=65532,gid=65532,mode=0700"
run() {
    docker run --read-only --network host \
        --tmpfs "/var/lib/fusescope/wal:$tmpfs" \
        --tmpfs "/var/lib/fusescope/spill:$tmpfs" \
        --tmpfs "/var/lib/fusescope/bucket:$tmpfs" \
        -v "$(pwd)/config/image-check.toml:/etc/fusescope/config.toml:ro" \
        -e FS_CATALOG_URL="$catalog" \
        "$@"
}

run --rm "$image" --config /etc/fusescope/config.toml --check

run -d --name "$name" "$image" --config /etc/fusescope/config.toml --role all >/dev/null
ready=""
for _ in $(seq 1 60); do
    if curl -fsS http://127.0.0.1:19464/readyz >/dev/null 2>&1; then
        ready=yes
        break
    fi
    sleep 1
done
if [ -z "$ready" ]; then
    docker logs "$name" >&2
    echo "the engine never reported ready" >&2
    exit 1
fi
docker stop -t 30 "$name" >/dev/null
status=$(docker inspect "$name" --format '{{.State.ExitCode}}')
if [ "$status" != "0" ]; then
    docker logs "$name" >&2
    echo "the engine exited $status on SIGTERM" >&2
    exit 1
fi
echo "image $image: non-root, $certificates CA certificates, --check and --role all ready and stopped cleanly with a read-only root"
