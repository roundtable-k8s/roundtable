#!/usr/bin/env bash
# Checks the chart's values.schema.json: the default values, values-minimal.yaml
# and the example values files lint and render, a wrong type fails naming the
# key, and the schema never forbids unknown keys.
#
# Usage: HELM=bin/helm scripts/check-values-schema.sh
set -euo pipefail

HELM="${HELM:-helm}"
root="$(cd "$(dirname "$0")/.." && pwd)"
chart="$root/charts/roundtable-operator"
fail() { echo "FAIL: $*" >&2; exit 1; }

[ -f "$chart/values.schema.json" ] || fail "values.schema.json missing"
! grep -q '"additionalProperties": *false' "$chart/values.schema.json" || fail "schema must stay permissive (additionalProperties: false)"

"$HELM" lint "$chart" >/dev/null || fail "helm lint with default values"
"$HELM" template rt "$chart" >/dev/null || fail "helm template with default values"
for f in "$chart/values-minimal.yaml" \
  "$root/examples/recipes/dev-factory/values.yaml" \
  "$root/examples/observability/values-telemetry.yaml"; do
  "$HELM" lint "$chart" -f "$f" >/dev/null || fail "helm lint with $f"
  "$HELM" template rt "$chart" -f "$f" >/dev/null || fail "helm template with $f"
done

# Loose values stay accepted, unknown keys too.
"$HELM" template rt "$chart" --set sandboxBackend=true --set keda=false \
  --set isolation.egress.mode=off --set someUnknownKey=1 >/dev/null \
  || fail "loosely typed values rejected"

err="$("$HELM" template rt "$chart" --set gateway.replicas=abc 2>&1)" && fail "gateway.replicas=abc was accepted"
grep -q 'gateway.replicas' <<<"$err" || fail "error does not name gateway.replicas: $err"
echo "values schema OK"
