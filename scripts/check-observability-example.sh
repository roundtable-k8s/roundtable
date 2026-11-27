#!/usr/bin/env bash
# Checks examples/observability: values-telemetry.yaml renders the chart
# collector with only the Jaeger trace exporter, the operator, gateway and
# dashboard export to that collector, and the manifests are well-formed (checked offline with yq).
#
# Usage: HELM=bin/helm scripts/check-observability-example.sh
set -euo pipefail

HELM="${HELM:-helm}"
root="$(cd "$(dirname "$0")/.." && pwd)"
chart="$root/charts/roundtable-operator"
dir="$root/examples/observability"
fail() { echo "FAIL: $*" >&2; exit 1; }

rendered="$("$HELM" template rt "$chart" -n roundtable -f "$dir/values-telemetry.yaml")"

endpoint="http://rt-roundtable-operator-otel-collector.roundtable:4317"
# endpoint_of <manifests> <deployment>: the OTLP endpoint a Deployment exports to.
endpoint_of() {
  yq -r 'select(.kind == "Deployment" and .metadata.name == "'"$2"'")
    | .spec.template.spec.containers[].env[] | select(.name == "OTEL_EXPORTER_OTLP_ENDPOINT") | .value' <<<"$1"
}
for d in rt-roundtable-operator rt-roundtable-operator-gateway rt-roundtable-operator-dashboard; do
  [ "$(endpoint_of "$rendered" "$d")" = "$endpoint" ] || fail "$d must export to $endpoint"
done
# Negative test: a Deployment exporting elsewhere must be caught.
[ "$(endpoint_of "${rendered//$endpoint/http://elsewhere:4317}" rt-roundtable-operator-gateway)" != "$endpoint" ] \
  || fail "negative test: wrong endpoint not detected"

cfg="$(mktemp)"; trap 'rm -f "$cfg"' EXIT
yq 'select(.kind == "ConfigMap" and .metadata.name == "rt-roundtable-operator-otel-collector") | .data["config.yaml"]' <<<"$rendered" >"$cfg"
yq -e '(.service.pipelines | keys | join(",")) == "traces"' "$cfg" >/dev/null || fail "only the traces pipeline may be rendered"
yq -e '(.service.pipelines.traces.exporters | join(",")) == "otlp/jaeger"' "$cfg" >/dev/null || fail "traces must export to otlp/jaeger only"
yq -e '.exporters["otlp/jaeger"].endpoint == "jaeger.roundtable.svc.cluster.local:4317"' "$cfg" >/dev/null || fail "Jaeger exporter endpoint does not match jaeger.yaml"
yq -e '.exporters | has("debug") | not' "$cfg" >/dev/null || fail "default debug exporter still rendered"

# The manifests: every document has apiVersion, kind and a name, and the Service
# exposes the OTLP port the exporter points at.
manifests="$dir/jaeger.yaml"
if yq -e 'select(.apiVersion == null or .kind == null or .metadata.name == null)' "$manifests" >/dev/null 2>&1; then fail "jaeger.yaml has a document without apiVersion, kind or name"; fi
yq -e 'select(.kind == "Service" and .metadata.name == "jaeger") | .spec.ports[] | select(.port == 4317)' "$manifests" >/dev/null || fail "jaeger Service lacks port 4317"
yq -e 'select(.kind == "Deployment") | .spec.template.spec.containers[0].image | test(":[0-9.]+$")' "$manifests" >/dev/null || fail "jaeger image must be pinned to a version tag"
echo "observability example OK"
