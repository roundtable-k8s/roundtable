#!/usr/bin/env bash
# Checks that defaults render no trigger pod scheduling flags and that
# triggers.* values reach the operator flags.
#
# Usage: HELM=bin/helm scripts/check-trigger-scheduling.sh
set -euo pipefail

HELM="${HELM:-helm}"
chart="$(cd "$(dirname "$0")/.." && pwd)/charts/roundtable-operator"
fail() { echo "FAIL: $*" >&2; exit 1; }

default="$("$HELM" template rt "$chart")"
! grep -q -- '--trigger-\(image-pull-secrets\|node-selector\|tolerations\|priority-class-name\)' <<<"$default" \
  || fail "defaults render trigger scheduling flags"

on="$("$HELM" template rt "$chart" \
  --set 'triggers.imagePullSecrets={a,b}' --set triggers.nodeSelector.pool=ops \
  --set 'triggers.tolerations[0].key=dedicated' --set 'triggers.tolerations[0].operator=Exists' \
  --set triggers.priorityClassName=high)"
grep -q -- '--trigger-image-pull-secrets=a,b' <<<"$on" || fail "imagePullSecrets flag missing"
grep -q -- '--trigger-node-selector={"pool":"ops"}' <<<"$on" || fail "nodeSelector flag missing"
grep -q -- '--trigger-tolerations=\[{"key":"dedicated","operator":"Exists"}\]' <<<"$on" || fail "tolerations flag missing"
grep -q -- '--trigger-priority-class-name=high' <<<"$on" || fail "priorityClassName flag missing"
echo "ok"
