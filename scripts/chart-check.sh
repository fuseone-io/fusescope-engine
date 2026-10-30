#!/bin/sh
# Checks the Helm chart: it lints strictly and renders valid Kubernetes
# objects for every values file under ci/, the configuration it renders
# passes the engine's own --validate, and the releases it must refuse
# fail to render.
#
# usage: scripts/chart-check.sh --binary <path>
#        scripts/chart-check.sh --image <image>
# The engine validates each rendered configuration for every role, as a
# local binary or inside the image.
set -eu
chart=deploy/helm/fusescope-engine
work=$(mktemp -d)
chmod 755 "$work"
trap 'rm -rf "$work"' EXIT

validate() {
    case "$mode" in
        --binary) "$engine" "$@" ;;
        --image) docker run --rm -v "$work:$work:ro" \
            -e FS_CATALOG_URL -e FS_BUCKET_ACCESS_KEY -e FS_BUCKET_SECRET_KEY \
            "$engine" "$@" ;;
    esac
}
mode=$1
engine=$2
case "$mode" in
    --binary|--image) ;;
    *) echo "usage: $0 --binary <path> | --image <image>" >&2; exit 2 ;;
esac
export FS_CATALOG_URL=postgres://check@localhost/check
export FS_BUCKET_ACCESS_KEY=check FS_BUCKET_SECRET_KEY=check

for values in "$chart"/ci/*-values.yaml; do
    name=$(basename "$values" -values.yaml)
    helm lint --strict "$chart" -f "$values" >/dev/null
    helm template check "$chart" -f "$values" --namespace fusescope > "$work/$name.yaml"
    kubeconform -strict -summary "$work/$name.yaml"
    helm template check "$chart" -f "$values" --namespace fusescope \
        --show-only templates/configmap.yaml \
        | python3 -c 'import sys, yaml; sys.stdout.write(yaml.safe_load(sys.stdin)["data"]["config.toml"])' \
        > "$work/$name.toml"
    chmod 644 "$work/$name.toml"
    for role in ingester querier compactor; do
        validate --config "$work/$name.toml" --role "$role" --validate >/dev/null
    done
    echo "chart with $name values: lints, renders valid objects, configuration valid for every role"
done

refuse() {
    if helm template check "$chart" "$@" >/dev/null 2>"$work/err"; then
        echo "the chart rendered what it must refuse: $*" >&2
        exit 1
    fi
}
refuse
refuse -f "$chart/ci/staging-values.yaml" --set tls.enabled=false
refuse -f "$chart/ci/staging-values.yaml" --set auth.mode=oauth
refuse -f "$chart/ci/staging-values.yaml" --set tls.secretName=
echo "chart refuses a release without its secrets, jwt without tls, or an unknown auth mode"
