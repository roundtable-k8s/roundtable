#!/usr/bin/env bash
# Renders the dashboard Role with and without the KEDA and metrics APIs served
# and checks the read access the dashboard needs (Deployments, HPAs, events,
# plus ScaledObjects and pod metrics only when served) and that none of it can
# write.
#
# Usage: HELM=bin/helm scripts/check-dashboard-rbac.sh
set -euo pipefail

HELM="${HELM:-helm}"
chart="$(cd "$(dirname "$0")/.." && pwd)/charts/roundtable-operator"

role() { # [extra helm args...] -> the dashboard Role document
  "$HELM" template rt "$chart" --set dashboard.enabled=true "$@" |
    awk '/^kind: Role$/{r=1} r{print} /^---$/{if(r)exit}'
}
fail() { echo "FAIL: $*" >&2; exit 1; }
has() { grep -q "$2" <<<"$1" || fail "$3"; }
lacks() { ! grep -q "$2" <<<"$1" || fail "$3"; }

minimal="$(role)"
[ -n "$minimal" ] || fail "dashboard Role not rendered"
for r in '"deployments", "replicasets"' '"horizontalpodautoscalers"' '"events"'; do
  has "$minimal" "$r" "minimal install: Role lacks $r"
done
lacks "$minimal" 'scaledobjects' "minimal install: Role must not reference keda.sh"
lacks "$minimal" 'metrics.k8s.io' "minimal install: Role must not reference metrics.k8s.io"

full="$(role --api-versions keda.sh/v1alpha1 --api-versions metrics.k8s.io/v1beta1)"
has "$full" 'scaledobjects' "KEDA served: Role lacks scaledobjects"
has "$full" 'metrics.k8s.io' "metrics served: Role lacks pod metrics"

# Nothing outside the Mission rule may write.
for doc in "$minimal" "$full"; do
  writes="$(awk '/- apiGroups:/{g=$0} /verbs:.*(create|update|patch|delete)/{print g}' <<<"$doc")"
  [ "$(wc -l <<<"$writes")" -eq 1 ] && grep -q ai.roundtable.io <<<"$writes" ||
    fail "dashboard Role has write verbs beyond missions: $writes"
done
echo "dashboard RBAC OK"
