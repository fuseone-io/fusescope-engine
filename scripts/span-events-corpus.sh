#!/usr/bin/env bash
# Records the D12 span-event corpus against a ClickHouse pipeline reached over SSH:
# a throwaway collector on the target's Docker network takes the corpus over OTLP/HTTP
# and writes it with its ClickHouse exporter; the events it stored become a run of the
# fixture. An existing OUT gains the new run.
#
#   CORPUS_SSH="ssh -l root -i KEY HOST" [COLLECTOR=IMAGE] scripts/span-events-corpus.sh OUT.json
set -euo pipefail

: "${CORPUS_SSH:?the ssh command that reaches the host of the pipeline}"
COLLECTOR=${COLLECTOR:-ghcr.io/fuseone-io/fusescope-collector:0.6.1@sha256:bca9dd696fb69558dff92eb52fc7f3400f79b38ea645501acb7afc3394e53baf}
NETWORK=${NETWORK:-fusescope-demo}
CLICKHOUSE=${CLICKHOUSE:-fusescope-demo-clickhouse-1}
out=${1:?output fixture}
here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

run=$(od -An -N4 -tx4 /dev/urandom | tr -d ' ')
python3 "$here/span-events-corpus.py" request "$run" "$(date +%s%N)" > "$work/request.json"

dir=$($CORPUS_SSH 'mktemp -d /tmp/d12-corpus.XXXXXX')
$CORPUS_SSH "cat > $dir/request.json" < "$work/request.json"
env="COLLECTOR='$COLLECTOR' NETWORK='$NETWORK' CLICKHOUSE='$CLICKHOUSE' RUN='$run' dir='$dir'"
$CORPUS_SSH "$env bash -s" > "$work/remote.txt" <<'REMOTE'
set -euo pipefail
name=d12-corpus-$RUN
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
chmod 755 "$dir"; chmod 644 "$dir/config.yaml" "$dir/request.json"
docker run -d --name "$name" --network "$NETWORK" -v "$dir/config.yaml:/etc/d12/config.yaml:ro" \
    "$COLLECTOR" --config=file:/etc/d12/config.yaml >/dev/null
docker run --rm --network "$NETWORK" -v "$dir/request.json:/request.json:ro" curlimages/curl:8.17.0 \
    -sS --retry 20 --retry-connrefused --retry-delay 1 -o /dev/null -w '%{http_code}\n' \
    -H 'Content-Type: application/json' --data-binary @/request.json "http://$name:4318/v1/traces" >&2
query() { docker exec "$CLICKHOUSE" clickhouse-client -q "$1"; }
for _ in $(seq 60); do
    n=$(query "SELECT count() FROM signoz_traces.distributed_signoz_index_v3 WHERE startsWith(trace_id, 'd12e$RUN')")
    [ "$n" -gt 0 ] && break
    sleep 2
done
sleep 5
echo '---rows'
query "SELECT span_id, status_code, has_error, events
       FROM signoz_traces.distributed_signoz_index_v3 WHERE startsWith(trace_id, 'd12e$RUN') FORMAT JSONEachRow"
echo '---versions'
printf '{"collector":"%s","clickhouse":"%s","table":"signoz_traces.distributed_signoz_index_v3","run":"%s"}\n' \
    "$COLLECTOR" "$(query 'SELECT version()')" "$RUN"
REMOTE
sed -n '/^---rows$/,/^---versions$/p' "$work/remote.txt" | sed '1d;$d' > "$work/rows.json"
sed -n '/^---versions$/,$p' "$work/remote.txt" | sed '1d' > "$work/versions.json"
existing=()
[ -s "$out" ] && existing=("$out")
python3 "$here/span-events-corpus.py" fixture "$run" "$work/rows.json" "$work/versions.json" "${existing[@]}" > "$work/fixture.json"
mv "$work/fixture.json" "$out"
echo "$(wc -l < "$work/rows.json") rows of run $run in $out" >&2
