#!/usr/bin/env bash
# Renders the chart with telemetry off and on and checks the OTEL_* env on the
# operator, gateway and dashboard Deployments.
#
# Usage: HELM=bin/helm scripts/check-telemetry-env.sh
set -euo pipefail

HELM="${HELM:-helm}"
chart="$(cd "$(dirname "$0")/.." && pwd)/charts/roundtable-operator"
render() { "$HELM" template rt "$chart" "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
has() { grep -q -- "$2" <<<"$1" || fail "$3"; }
lacks() { ! grep -q -- "$2" <<<"$1" || fail "$3"; }

def="$(render)"
lacks "$def" 'OTEL_EXPORTER_OTLP_ENDPOINT' "endpoint rendered by default"
lacks "$def" 'OTEL_SERVICE_NAME' "service name rendered when disabled"
lacks "$def" 'TRACE_URL_TEMPLATE' "trace URL rendered when disabled"
[ "$(grep -c 'name: OTEL_SDK_DISABLED' <<<"$def")" = 3 ] || fail "OTEL_SDK_DISABLED must be on all three Deployments"

on="$(render --set telemetry.enabled=true --set telemetry.otlp.endpoint=http://x:4317 \
  --set telemetry.traceUrlTemplate='https://g/{traceId}' \
  --set telemetry.resourceAttributes.zeta=1 --set telemetry.resourceAttributes.alpha=b)"
lacks "$on" 'OTEL_SDK_DISABLED' "SDK disabled while enabled"
for v in OTEL_EXPORTER_OTLP_ENDPOINT OTEL_EXPORTER_OTLP_PROTOCOL OTEL_TRACES_SAMPLER_ARG OTEL_RESOURCE_ATTRIBUTES ROUNDTABLE_TELEMETRY_CAPTURE_CONTENT OTEL_SERVICE_NAME; do
  [ "$(grep -c "name: $v\$" <<<"$on")" = 3 ] || fail "$v must be on all three Deployments"
done
for s in roundtable-operator roundtable-gateway roundtable-ui; do
  has "$on" "value: \"$s\"" "OTEL_SERVICE_NAME $s missing"
done
has "$on" 'value: parentbased_traceidratio' "sampler missing"
has "$on" 'value: "alpha=b,zeta=1"' "resource attributes not sorted/joined"
has "$on" 'name: ROUNDTABLE_TELEMETRY_CAPTURE_CONTENT' "capture content missing"
[ "$(grep -A1 'name: ROUNDTABLE_TELEMETRY_CAPTURE_CONTENT' <<<"$on" | grep -c 'value: "true"')" = 3 ] || fail "capture content should default true on all three Deployments"
# Negative test: a value: "true" elsewhere must not satisfy the anchored check.
neg="$(printf '%s\n' '- name: OTHER' '  value: "true"' '- name: ROUNDTABLE_TELEMETRY_CAPTURE_CONTENT' '  value: "false"')"
[ "$(grep -A1 'name: ROUNDTABLE_TELEMETRY_CAPTURE_CONTENT' <<<"$neg" | grep -c 'value: "true"')" = 0 ] || fail "negative test: loose value: \"true\" matched"
has "$on" 'name: TRACE_URL_TEMPLATE' "dashboard trace URL missing"
[ "$(grep -c 'name: TRACE_URL_TEMPLATE' <<<"$on")" = 1 ] || fail "TRACE_URL_TEMPLATE only on dashboard"

off="$(render --set telemetry.enabled=true --set telemetry.captureContent=false)"
[ "$(grep -A1 'name: ROUNDTABLE_TELEMETRY_CAPTURE_CONTENT' <<<"$off" | grep -c 'value: "false"')" = 3 ] || fail "captureContent=false not honoured"
lacks "$off" 'OTEL_EXPORTER_OTLP_ENDPOINT' "empty endpoint rendered"
echo "telemetry env OK"

# --- optional collector (#386) ---
lacks "$def" 'otel-collector' "collector rendered by default"
col="$(render -n obs --set telemetry.enabled=true --set telemetry.collector.enabled=true)"
cdocs="$(render --set telemetry.collector.enabled=true --show-only templates/otel-collector.yaml)"
for k in Deployment Service ConfigMap; do has "$cdocs" "^kind: $k\$" "collector $k missing"; done
has "$cdocs" 'readOnlyRootFilesystem: true' "collector not read-only"
has "$cdocs" 'automountServiceAccountToken: false' "collector mounts SA token"
[ "$(grep -c 'value: "http://rt-roundtable-operator-otel-collector.obs:4317"' <<<"$col")" = 3 ] || fail "default endpoint must point at collector Service on all three Deployments"
tmp="$(mktemp)"; trap 'rm -f "$tmp"' EXIT
yq 'select(.kind == "ConfigMap") | .data["config.yaml"]' <<<"$cdocs" >"$tmp"
yq -e '(.service.pipelines | keys | join(",")) == "logs,metrics,traces"' "$tmp" >/dev/null || fail "collector config does not parse or lacks pipelines"
yq -e '[.service.pipelines[].processors | join(",") == "memory_limiter,redaction,batch"] | all' "$tmp" >/dev/null || fail "redaction missing from a pipeline"
yq -e '.processors.redaction.allow_all_keys == true and (.processors.redaction.blocked_values | length) > 0' "$tmp" >/dev/null || fail "redaction processor misconfigured"
custom="$(render --set telemetry.collector.enabled=true --set telemetry.collector.exporters.debug=null --set telemetry.collector.exporters.otlp.endpoint=tempo:4317 --set 'telemetry.collector.pipelines.traces={otlp}' --set 'telemetry.collector.pipelines.metrics=null' --set 'telemetry.collector.pipelines.logs=null' --show-only templates/otel-collector.yaml)"
has "$custom" 'endpoint: tempo:4317' "custom exporter missing"
lacks "$custom" 'debug' "default exporter still rendered"
! render --set telemetry.collector.enabled=true --set 'telemetry.collector.pipelines.traces={nope}' >/dev/null 2>&1 || fail "undefined exporter in a pipeline must fail rendering"
cnp="$(render --set telemetry.collector.enabled=true --set networkPolicy.enabled=true --show-only templates/networkpolicy.yaml)"
has "$cnp" 'name: rt-roundtable-operator-otel-collector' "collector NetworkPolicy missing"
has "$cnp" 'port: 4317' "collector NetworkPolicy lacks 4317"
has "$cnp" 'port: 4318' "collector NetworkPolicy lacks 4318"
# collector pods, Service and Deployment selector must not match the operator's selector labels
[ "$(grep -c 'app.kubernetes.io/name: roundtable-operator$' <<<"$cdocs")" = 3 ] || fail "only the 3 metadata labels may carry the operator name"
[ "$(grep -c 'app.kubernetes.io/name: roundtable-otel-collector' <<<"$cdocs")" = 3 ] || fail "collector Service selector, Deployment selector and pod labels need their own name"
if command -v otelcol-contrib >/dev/null; then
  otelcol-contrib validate --config "$tmp" || fail "otelcol-contrib validate failed"
fi
echo "telemetry collector OK"
