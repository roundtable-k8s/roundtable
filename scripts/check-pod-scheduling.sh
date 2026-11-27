#!/usr/bin/env bash
# Renders the chart and checks the operator probes, topologySpreadConstraints
# on every Deployment, and the operator PodDisruptionBudget.
#
# Usage: HELM=bin/helm scripts/check-pod-scheduling.sh
set -euo pipefail

HELM="${HELM:-helm}"
chart="$(cd "$(dirname "$0")/.." && pwd)/charts/roundtable-operator"
render() { "$HELM" template rt "$chart" "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
has() { grep -q -- "$2" <<<"$1" || fail "$3"; }
lacks() { ! grep -q -- "$2" <<<"$1" || fail "$3"; }
count() { grep -c -- "$2" <<<"$1" || true; }

def="$(render)"
# The gateway also serves /healthz and /readyz, so compare against probes off.
[ "$(count "$def" 'path: /healthz')" = 2 ] || fail "operator liveness probe missing"
[ "$(count "$def" 'path: /readyz')" = 2 ] || fail "operator readiness probe missing"
[ "$(count "$def" 'port: 8081')" = 2 ] || fail "operator probes not on port 8081"
lacks "$def" 'topologySpreadConstraints' "constraints rendered by default"
lacks "$def" 'kind: PodDisruptionBudget' "PDB rendered by default"
off="$(render --set probes.enabled=false)"
[ "$(count "$off" 'path: /healthz')" = 1 ] || fail "probes rendered while disabled"

glob=(--set telemetry.collector.enabled=true
  --set 'topologySpreadConstraints[0].maxSkew=1'
  --set 'topologySpreadConstraints[0].topologyKey=kubernetes.io/hostname'
  --set 'topologySpreadConstraints[0].whenUnsatisfiable=ScheduleAnyway')
g="$(render "${glob[@]}")"
[ "$(count "$g" '^ *topologySpreadConstraints:')" = 4 ] || fail "global constraint not in all four Deployments"
for app in roundtable-operator roundtable-gateway roundtable-dashboard roundtable-otel-collector; do
  has "$g" "app.kubernetes.io/name: $app" "selector for $app missing"
done

p="$(render "${glob[@]}" --set 'gateway.topologySpreadConstraints[0].maxSkew=3' \
  --set 'gateway.topologySpreadConstraints[0].topologyKey=topology.kubernetes.io/zone' \
  --set 'gateway.topologySpreadConstraints[0].whenUnsatisfiable=DoNotSchedule')"
has "$p" 'maxSkew: 3' "per-component constraint missing"
[ "$(count "$p" 'maxSkew: 1')" = 3 ] || fail "per-component constraint did not replace the global one only for the gateway"

lacks "$(render --set podDisruptionBudget.enabled=true)" 'kind: PodDisruptionBudget' "operator PDB rendered with replicaCount=1"
lacks "$(render --set replicaCount=2)" 'kind: PodDisruptionBudget' "operator PDB rendered while disabled"
pdb="$(render --set replicaCount=2 --set podDisruptionBudget.enabled=true)"
has "$pdb" 'kind: PodDisruptionBudget' "operator PDB missing with replicaCount=2"
has "$pdb" 'app.kubernetes.io/component: operator' "operator PDB not labelled"
echo "ok"
