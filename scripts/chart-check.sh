#!/bin/sh
# Checks the Helm chart: it lints strictly and renders valid Kubernetes
# objects for every values file under ci/; each role's rendered
# configuration passes the engine's own --validate; the rendered workloads
# keep the roles apart (their own Secret, ServiceAccount and TLS identity,
# none for the compactor, no API token, no DDL: a Job migrates as its own
# user) and wait for a startup probe; and
# the releases it must refuse fail to render. --validate reads no
# certificate and connects nowhere, so the separation is checked here, on
# the manifests.
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
    python3 - "$work/$name.yaml" "$work" "$name" <<'PY'
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
work, name = sys.argv[2], sys.argv[3]
fail = lambda why: sys.exit(f"chart with {name} values: {why}")
config = next(d for d in docs if d["kind"] == "ConfigMap"
              and d["metadata"]["name"].endswith("-config")
              and not d["metadata"]["name"].endswith("-migrate-config"))["data"]
for role in ("ingester", "querier", "compactor"):
    if "migrate = false" not in config[f"{role}.toml"]:
        fail(f"{role} migrates on start, so its user needs DDL")
jobs = [d for d in docs if d["kind"] == "Job"]
if len(jobs) != 1:
    fail(f"{len(jobs)} migration Jobs")
job = jobs[0]["spec"]["template"]["spec"]
migrate = job["containers"][0]
if migrate["args"][-2:] != ["admin", "migrate"] or job.get("automountServiceAccountToken") is not False:
    fail(f"the migration Job runs {migrate['args']}")
for role in ("ingester", "querier", "compactor"):
    with open(f"{work}/{name}-{role}.toml", "w") as out:
        out.write(config[f"{role}.toml"])
pods = {d["metadata"]["labels"]["app.kubernetes.io/component"]: d["spec"]["template"]["spec"]
        for d in docs if d["kind"] in ("StatefulSet", "Deployment")}
if set(pods) != {"ingester", "querier", "compactor"}:
    fail(f"workloads {sorted(pods)}")
secrets, accounts, identities = {}, {}, {}
for role, pod in pods.items():
    if pod.get("automountServiceAccountToken") is not False:
        fail(f"{role} mounts its ServiceAccount token")
    accounts[role] = pod["serviceAccountName"]
    container = pod["containers"][0]
    if "startupProbe" not in container or "livenessProbe" not in container:
        fail(f"{role} has no startup probe before its liveness")
    secrets[role] = container["envFrom"][0]["secretRef"]["name"]
    identity = [v["secret"]["secretName"] for v in pod["volumes"] if v["name"] == "tls"]
    identities[role] = identity[0] if identity else None
secrets["migrator"] = migrate["envFrom"][0]["secretRef"]["name"]
accounts["migrator"] = job.get("serviceAccountName", "default")
if len(set(secrets.values())) != 4:
    fail(f"roles share a Secret: {secrets}")
if len(set(v for k, v in accounts.items() if k != "migrator")) != 3:
    fail(f"roles share a ServiceAccount: {accounts}")
if identities["compactor"] is not None:
    fail("the compactor has a TLS identity")
tls_on = "enabled = true" in config["querier.toml"]
if tls_on:
    cas = {v["secret"]["secretName"] for pod in pods.values() for v in pod["volumes"] if v["name"] == "client-ca"}
    named = [identities["ingester"], identities["querier"], *cas, *secrets.values()]
    if None in named or len(named) != len(set(named)):
        fail(f"identities, client CAs and Secrets overlap: {named}")
PY
    for role in ingester querier compactor; do
        chmod 644 "$work/$name-$role.toml"
        validate --config "$work/$name-$role.toml" --validate >/dev/null
    done
    echo "chart with $name values: lints, renders valid objects, each role's configuration valid, roles kept apart, startup probed"
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
refuse -f "$chart/ci/staging-values.yaml" --set tls.clientCASecret=
refuse -f "$chart/ci/staging-values.yaml" --set secrets.querier=fusescope-engine-ingester
refuse -f "$chart/ci/staging-values.yaml" --set tls.identities.querier=fusescope-engine-ingester-tls
refuse -f "$chart/ci/staging-values.yaml" --set tls.clientCASecret=fusescope-engine-querier-tls
refuse -f "$chart/ci/staging-values.yaml" --set tls.clientCASecret=fusescope-engine-ingester-tls
refuse -f "$chart/ci/staging-values.yaml" --set tls.clientCASecret=fusescope-engine-compactor
refuse -f "$chart/ci/staging-values.yaml" --set secrets.migrator=fusescope-engine-querier
refuse -f "$chart/ci/staging-values.yaml" --set secrets.migrator=
refuse -f "$chart/ci/staging-values.yaml" --set startupTimeoutSecs=0
refuse -f "$chart/ci/staging-values.yaml" --set startupTimeoutSecs=86401
for bound in 1 86400; do
    helm template check "$chart" -f "$chart/ci/staging-values.yaml" --set startupTimeoutSecs=$bound >/dev/null
done
echo "chart refuses a release without its secrets, any two Secrets alike (roles, migrator, client CAs, identities), a startup budget outside 1..86400, jwt without tls, or an unknown auth mode"
