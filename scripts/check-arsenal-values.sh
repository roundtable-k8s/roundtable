#!/usr/bin/env bash
# Checks that a leftover pre-arsenals `arsenal` value on a knight or the planner
# fails the render naming `arsenals`, instead of being silently ignored.
#
# Usage: HELM=bin/helm scripts/check-arsenal-values.sh
set -euo pipefail

HELM="${HELM:-helm}"
chart="$(cd "$(dirname "$0")/.." && pwd)/charts/roundtable-operator"
render() { "$HELM" template rt "$chart" "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

render >/dev/null || fail "default render must succeed"
render --set 'knights.x.domain=d' --set 'knights.x.skills={a}' --set 'knights.x.arsenals[0].name=a' >/dev/null \
  || fail "knight with arsenals must render"

if out="$(render --set 'knights.x.domain=d' --set 'knights.x.skills={a}' --set 'knights.x.arsenal[0].name=a' 2>&1)"; then
  fail "knights.x.arsenal must fail"
fi
grep -q 'knights.x.arsenals' <<<"$out" || fail "unclear error for knights.x.arsenal: $out"

if out="$(render --set 'planner.arsenal[0].name=a' 2>&1)"; then
  fail "planner.arsenal must fail"
fi
grep -q 'planner.arsenals' <<<"$out" || fail "unclear error for planner.arsenal: $out"
echo "ok"
