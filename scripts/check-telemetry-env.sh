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
has "$on" 'value: "true"' "capture content should default true"
has "$on" 'name: TRACE_URL_TEMPLATE' "dashboard trace URL missing"
[ "$(grep -c 'name: TRACE_URL_TEMPLATE' <<<"$on")" = 1 ] || fail "TRACE_URL_TEMPLATE only on dashboard"

off="$(render --set telemetry.enabled=true --set telemetry.captureContent=false)"
[ "$(grep -A1 'name: ROUNDTABLE_TELEMETRY_CAPTURE_CONTENT' <<<"$off" | grep -c 'value: "false"')" = 3 ] || fail "captureContent=false not honoured"
lacks "$off" 'OTEL_EXPORTER_OTLP_ENDPOINT' "empty endpoint rendered"
echo "telemetry env OK"
