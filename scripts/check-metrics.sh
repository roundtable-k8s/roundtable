#!/usr/bin/env bash
# Checks that defaults render no metrics resources and that enabling metrics
# renders two ServiceMonitors and one PodMonitor.
#
# Usage: HELM=bin/helm scripts/check-metrics.sh
set -euo pipefail

HELM="${HELM:-helm}"
chart="$(cd "$(dirname "$0")/.." && pwd)/charts/roundtable-operator"
fail() { echo "FAIL: $*" >&2; exit 1; }
count() { grep -c "^kind: $2\$" <<<"$1" || true; }

default="$("$HELM" template rt "$chart" --api-versions monitoring.coreos.com/v1)"
for k in ServiceMonitor PodMonitor; do
  [ "$(count "$default" $k)" = 0 ] || fail "defaults render $k"
done
! grep -q 'metrics-bind-address\|name: metrics' <<<"$default" || fail "defaults render a metrics port"

on="$("$HELM" template rt "$chart" --api-versions monitoring.coreos.com/v1 \
  --set metrics.enabled=true,metrics.serviceMonitor.enabled=true,metrics.podMonitor.enabled=true \
  --set dashboard.enabled=true --set metrics.labels.release=prom)"
[ "$(count "$on" ServiceMonitor)" = 2 ] || fail "expected 2 ServiceMonitors"
[ "$(count "$on" PodMonitor)" = 1 ] || fail "expected 1 PodMonitor"
grep -q -- '--metrics-bind-address=:8080' <<<"$on" || fail "metrics flag missing"
grep -q 'release: prom' <<<"$on" || fail "metrics.labels not applied"
grep -q 'honorLabels: true' <<<"$on" || fail "honorLabels missing"

# Without the CRD API the monitors are skipped.
nocrd="$("$HELM" template rt "$chart" --set metrics.enabled=true,metrics.serviceMonitor.enabled=true,metrics.podMonitor.enabled=true)"
[ "$(count "$nocrd" ServiceMonitor)" = 0 ] || fail "monitors rendered without the API"
echo "ok"
