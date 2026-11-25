#!/usr/bin/env bash
# Checks that defaults render no metrics resources and that enabling metrics
# renders three ServiceMonitors and one PodMonitor.
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
[ "$(count "$on" ServiceMonitor)" = 3 ] || fail "expected 3 ServiceMonitors"
[ "$(count "$on" PodMonitor)" = 1 ] || fail "expected 1 PodMonitor"
grep -q -- '--metrics-bind-address=:8080' <<<"$on" || fail "metrics flag missing"
grep -q 'release: prom' <<<"$on" || fail "metrics.labels not applied"
grep -q 'honorLabels: true' <<<"$on" || fail "honorLabels missing"

# The gateway (on by default) exposes metrics: flag, Service and policy rule.
gwon="$("$HELM" template rt "$chart" --set metrics.enabled=true,gateway.networkPolicy.enabled=true \
  --set 'networkPolicy.metricsFrom[0].namespaceSelector.matchLabels.x=y')"
grep -q -- '--metrics-listen=:8081' <<<"$gwon" || fail "gateway metrics flag missing"
grep -q -- '-gateway-metrics$' <<<"$gwon" || fail "gateway metrics Service missing"
grep -q -- '- port: metrics$' <<<"$gwon" || fail "gateway network policy does not allow the metrics port"
! grep -q -- '--metrics-listen' <<<"$default" || fail "defaults render the gateway metrics flag"
! grep -q -- '-gateway-metrics' <<<"$default" || fail "defaults render the gateway metrics Service"
gwdef="$("$HELM" template rt "$chart" --set gateway.networkPolicy.enabled=true \
  --set 'networkPolicy.metricsFrom[0].namespaceSelector.matchLabels.x=y')"
! grep -q -- '- port: metrics$' <<<"$gwdef" || fail "defaults render a gateway metrics policy rule"

# The operator NetworkPolicy must allow the metrics port the operator binds.
np="$("$HELM" template rt "$chart" --set metrics.enabled=true,networkPolicy.enabled=true \
  --set 'networkPolicy.metricsFrom[0].namespaceSelector.matchLabels.x=y')"
grep -q -- '- port: 8080$' <<<"$np" || fail "network policy does not allow metrics port 8080"
! grep -q -- '- port: 8443$' <<<"$np" || fail "network policy still allows 8443 with metrics.enabled"

# Without the CRD API the monitors are skipped.
nocrd="$("$HELM" template rt "$chart" --set metrics.enabled=true,metrics.serviceMonitor.enabled=true,metrics.podMonitor.enabled=true)"
[ "$(count "$nocrd" ServiceMonitor)" = 0 ] || fail "monitors rendered without the API"
echo "ok"
