#!/bin/sh
# Checks a built image: it runs as the non-root user, carries a non-empty
# system CA bundle and a readable CycloneDX SBOM of its binary that
# scripts/sbom-check.py accepts (copied to $SBOM_OUT when set); with a read-only root, writing only to its
# WAL and spill paths and to a tmpfs bucket, `--check` passes, and
# `--role all` starts, reports ready, and stops cleanly on SIGTERM. Spill
# is not forced, so its path is mounted but not proven written.
#
# The catalog is a Postgres of its own, started here on an ephemeral
# loopback port and removed on exit: `--role all` runs a compactor, which
# would expire, purge and compact a real catalog's rows against the empty
# test bucket, so no catalog URL is ever taken from outside.
#
# usage: scripts/image-check.sh <image>
set -eu
image=$1
name="fusescope-image-check-$$"
postgres="fusescope-image-check-pg-$$"
created=""
trap 'docker rm -fv "$created" "$name" "$postgres" >/dev/null 2>&1 || true' EXIT

docker run -d --name "$postgres" \
    -e POSTGRES_USER=fusescope -e POSTGRES_PASSWORD=fusescope -e POSTGRES_DB=fusescope \
    -p 127.0.0.1::5432 postgres:17-alpine >/dev/null
# Over TCP: the image's init server listens on the socket only and is
# restarted once the database exists, so socket readiness can be the
# init server's.
for _ in $(seq 1 60); do
    docker exec "$postgres" pg_isready -h 127.0.0.1 -U fusescope -d fusescope >/dev/null 2>&1 && break
    sleep 1
done
docker exec "$postgres" pg_isready -h 127.0.0.1 -U fusescope -d fusescope >/dev/null \
    || { echo "the throwaway Postgres never became ready" >&2; exit 1; }
port=$(docker port "$postgres" 5432/tcp | head -n 1 | sed 's/.*://')
catalog="postgres://fusescope:fusescope@127.0.0.1:$port/fusescope"

user=$(docker image inspect "$image" --format '{{.Config.User}}')
[ "$user" = "65532:65532" ] || { echo "image runs as '$user', not 65532:65532" >&2; exit 1; }

created=$(docker create "$image")
certificates=$(docker cp -L "$created:/etc/ssl/certs/ca-certificates.crt" - | tar -xOf - | grep -c 'BEGIN CERTIFICATE' || true)
[ "$certificates" -gt 0 ] || { echo "image has no certificate in its CA bundle" >&2; exit 1; }

sbom_path=/usr/share/doc/fusescope-engine/sbom.cdx.json
mode=$(docker cp "$created:$sbom_path" - | tar -tvf - | cut -c1-10)
case "$mode" in
    -r??r??r??) ;;
    *) echo "image SBOM is not readable by every user: '$mode'" >&2; exit 1 ;;
esac
sbom=${SBOM_OUT:-$(mktemp)}
docker cp "$created:$sbom_path" - | tar -xOf - > "$sbom"
python3 "$(dirname "$0")/sbom-check.py" "$sbom"

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
echo "image $image: non-root, $certificates CA certificates, a valid SBOM, --check and --role all ready and stopped cleanly with a read-only root"
