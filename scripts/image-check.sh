#!/bin/sh
# Checks a built image: it runs as the non-root user, carries the system
# CA bundle, and passes `--check` with a read-only root, writing only to
# its WAL and spill volumes and to a tmpfs bucket.
#
# usage: scripts/image-check.sh <image> <catalog-url>
set -eu
image=$1
catalog=$2

user=$(docker image inspect "$image" --format '{{.Config.User}}')
[ "$user" = "65532:65532" ] || { echo "image runs as '$user', not 65532:65532" >&2; exit 1; }

container=$(docker create "$image")
trap 'docker rm -f "$container" >/dev/null' EXIT
docker cp "$container:/etc/ssl/certs/ca-certificates.crt" - | tar -tf - >/dev/null \
    || { echo "image has no system CA bundle" >&2; exit 1; }

tmpfs="uid=65532,gid=65532,mode=0700"
docker run --rm --read-only --network host \
    --tmpfs "/var/lib/fusescope/wal:$tmpfs" \
    --tmpfs "/var/lib/fusescope/spill:$tmpfs" \
    --tmpfs "/var/lib/fusescope/bucket:$tmpfs" \
    -v "$(pwd)/config/image-check.toml:/etc/fusescope/config.toml:ro" \
    -e FS_CATALOG_URL="$catalog" \
    "$image" --config /etc/fusescope/config.toml --check
echo "image $image: non-root, CA bundle present, --check passed read-only"
