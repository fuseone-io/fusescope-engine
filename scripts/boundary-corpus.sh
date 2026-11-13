#!/usr/bin/env bash
# Records the D7 observation corpus against a ClickHouse pipeline reached over SSH: a throwaway
# collector on the target's Docker network takes every case's first export, then, past its batch
# flush, every second export; the top_level_operations rows it wrote become the fixture.
#
#   CORPUS_SSH="ssh -l root -i KEY HOST" scripts/boundary-corpus.sh OUT.json
set -euo pipefail

: "${CORPUS_SSH:?the ssh command that reaches the host of the pipeline}"
COLLECTOR=${COLLECTOR:-ghcr.io/fuseone-io/fusescope-collector:0.6.1@sha256:bca9dd696fb69558dff92eb52fc7f3400f79b38ea645501acb7afc3394e53baf}
NETWORK=${NETWORK:-fusescope-demo}
CLICKHOUSE=${CLICKHOUSE:-fusescope-demo-clickhouse-1}
# Seconds between exports; past the collector's flush (1s), each export is a batch of its own.
SPACING=${SPACING:-2.5}
out=${1:?output fixture}
here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

run=$(od -An -N4 -tx4 /dev/urandom | tr -d ' ')
python3 "$here/boundary-corpus.py" requests "$run" "$(date +%s%N)" > "$work/requests.json"
mkdir "$work/exports"
python3 - "$work/requests.json" "$work/exports" <<'SPLIT'
import json, sys
for i, e in enumerate(json.load(open(sys.argv[1]))):
    json.dump(e["request"], open(f"{sys.argv[2]}/{e['export']}-{i:03d}.json", "w"))
SPLIT

dir=$($CORPUS_SSH 'mktemp -d /tmp/d7-corpus.XXXXXX')
tar -C "$work/exports" -cf - . | $CORPUS_SSH "tar -C $dir -xf - && chmod -R a+rX $dir"
env="COLLECTOR='$COLLECTOR' NETWORK='$NETWORK' CLICKHOUSE='$CLICKHOUSE' RUN='$run' dir='$dir' SPACING='$SPACING'"
$CORPUS_SSH "$env bash -s" > "$work/remote.txt" <<'REMOTE'
set -euo pipefail
name=d7-corpus-$RUN
trap 'docker rm -f "$name" >/dev/null 2>&1 || true; rm -rf "$dir"' EXIT
cat > "$dir/config.yaml" <<YAML
receivers:
  otlp:
    protocols:
      http: {endpoint: 0.0.0.0:4318}
exporters:
  fusescope_clickhouse:
    endpoint: $CLICKHOUSE:9000
    tls: {insecure: true}
    sending_queue:
      batch: {flush_timeout: 1s}
service:
  pipelines:
    traces: {receivers: [otlp], exporters: [fusescope_clickhouse]}
YAML
chmod 644 "$dir/config.yaml"
docker run -d --name "$name" --network "$NETWORK" -v "$dir:/corpus:ro" "$COLLECTOR" --config=file:/corpus/config.yaml >/dev/null
post() {
    docker run --rm --network "$NETWORK" -v "$dir:/corpus:ro" curlimages/curl:8.17.0 -sS --retry 20 --retry-connrefused \
        --retry-delay 1 -o /dev/null -w '%{http_code}\n' -H 'Content-Type: application/json' \
        --data-binary "@/corpus/$1" "http://$name:4318/v1/traces" >&2
}
for f in $(cd "$dir" && ls 0-*.json); do post "$f"; sleep "$SPACING"; done
sleep 6
for f in $(cd "$dir" && ls 1-*.json 2>/dev/null); do post "$f"; sleep "$SPACING"; done
sleep 6
query() { docker exec "$CLICKHOUSE" clickhouse-client -q "$1"; }
before=-1
for _ in $(seq 30); do
    n=$(query "SELECT count() FROM signoz_traces.distributed_top_level_operations WHERE startsWith(serviceName, 'd7c-$RUN-')")
    [ "$n" = "$before" ] && [ "$n" -gt 0 ] && break
    before=$n
    sleep 3
done
echo '---rows'
query "SELECT DISTINCT name, serviceName FROM signoz_traces.distributed_top_level_operations
       WHERE startsWith(serviceName, 'd7c-$RUN-') FORMAT JSONEachRow"
echo '---versions'
printf '{"collector":"%s","clickhouse":"%s","table":"signoz_traces.distributed_top_level_operations","flush_timeout":"1s","second_export_after":"6s","spacing_s":"%s","run":"%s"}\n' \
    "$COLLECTOR" "$(query 'SELECT version()')" "$SPACING" "$RUN"
REMOTE
sed -n '/^---rows$/,/^---versions$/p' "$work/remote.txt" | sed '1d;$d' > "$work/rows.json"
sed -n '/^---versions$/,$p' "$work/remote.txt" | sed '1d' > "$work/versions.json"
python3 "$here/boundary-corpus.py" fixture "$work/requests.json" "$work/rows.json" "$work/versions.json" > "$out"
echo "$(wc -l < "$work/rows.json") rows of run $run in $out" >&2
