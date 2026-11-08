#!/bin/sh
# Reproduces this run against a server started with bench.toml (`--role all`).
# Tokens come from the environment: T1_INGEST, T1_QUERY, T2_INGEST, T3_INGEST, T4_INGEST.
set -eu
out=$(dirname "$0")
bench=./target/release/fs-bench
snap() { curl -s 127.0.0.1:9464/metrics > "$out/metrics-$1.txt"; }

$bench stages --requests 5000 --json "$out/stages.json" > "$out/stages.txt"

snap before-1t-c32
$bench load --endpoint 127.0.0.1:4317 --token "$T1_INGEST" --duration-secs 30 --concurrency 32 \
    --metrics 127.0.0.1:9464 --json "$out/load-1t-c32.json" > "$out/load-1t-c32.txt"
snap after-1t-c32
sleep 15

snap before-1t-c32-q
$bench query --endpoint 127.0.0.1:8815 --token "$T1_QUERY" --iterations 40 \
    --json "$out/query-during-c32.json" > "$out/query-during-c32.txt" &
q=$!
$bench load --endpoint 127.0.0.1:4317 --token "$T1_INGEST" --duration-secs 30 --concurrency 32 \
    --metrics 127.0.0.1:9464 --json "$out/load-1t-c32-q.json" > "$out/load-1t-c32-q.txt"
wait $q
snap after-1t-c32-q
sleep 15

snap before-1t-c8-q
$bench query --endpoint 127.0.0.1:8815 --token "$T1_QUERY" --iterations 40 \
    --json "$out/query-during-c8.json" > "$out/query-during-c8.txt" &
q=$!
$bench load --endpoint 127.0.0.1:4317 --token "$T1_INGEST" --duration-secs 30 --concurrency 8 \
    --metrics 127.0.0.1:9464 --json "$out/load-1t-c8-q.json" > "$out/load-1t-c8-q.txt"
wait $q
snap after-1t-c8-q
sleep 15

snap before-4t-c8
for t in 2 3 4; do
    eval "token=\$T${t}_INGEST"
    $bench load --endpoint 127.0.0.1:4317 --token "$token" --duration-secs 30 --concurrency 8 \
        --json "$out/load-4t-c8-t$t.json" > "$out/load-4t-c8-t$t.txt" &
done
$bench load --endpoint 127.0.0.1:4317 --token "$T1_INGEST" --duration-secs 30 --concurrency 8 \
    --metrics 127.0.0.1:9464 --json "$out/load-4t-c8-t1.json" > "$out/load-4t-c8-t1.txt"
wait
snap after-4t-c8
sleep 30

snap before-rest
$bench query --endpoint 127.0.0.1:8815 --token "$T1_QUERY" --iterations 40 \
    --json "$out/query-rest.json" > "$out/query-rest.txt"
snap after-rest
