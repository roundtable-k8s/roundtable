#!/usr/bin/env bash
# Renders the chart and checks mission.resultsTTL: a Go duration reaches the
# --mission-results-ttl flag, anything else (e.g. 30d) fails the render instead
# of crash-looping the operator.
#
# Usage: HELM=bin/helm scripts/check-mission-ttl.sh
set -euo pipefail

HELM="${HELM:-helm}"
chart="$(cd "$(dirname "$0")/.." && pwd)/charts/roundtable-operator"
render() { "$HELM" template rt "$chart" "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
has() { grep -q -- "$2" <<<"$1" || fail "$3"; }

has "$(render)" '--mission-results-ttl=720h$' "default TTL missing"
has "$(render --set mission.resultsTTL=1h30m)" '--mission-results-ttl=1h30m$' "compound duration rejected or ignored"

for bad in 30d 2w 720 abc ""; do
  [ -n "$bad" ] || continue
  if out="$(render --set-string mission.resultsTTL="$bad" 2>&1)"; then
    fail "mission.resultsTTL=$bad must fail"
  fi
  has "$out" '720h' "unclear error for mission.resultsTTL=$bad"
done
echo "ok"
