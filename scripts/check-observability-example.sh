#!/usr/bin/env bash
# Checks examples/observability: values-telemetry.yaml renders the chart
# collector with only the Jaeger trace exporter, the operator, gateway and
# dashboard export to that collector, and the manifests are valid Kubernetes YAML.
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
[ "$(grep -c "value: \"$endpoint\"" <<<"$rendered")" = 3 ] || fail "operator, gateway and dashboard must export to $endpoint"

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
if command -v kubectl >/dev/null; then
  # Client-side only: no kubeconfig, so kubectl never contacts a cluster.
  KUBECONFIG=/dev/null kubectl create --dry-run=client --validate=false -f "$manifests" -o name >/dev/null || fail "kubectl create --dry-run=client rejected jaeger.yaml"
fi
echo "observability example OK"
