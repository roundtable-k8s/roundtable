#!/usr/bin/env bash
# Renders the chart and checks the isolation env on the operator Deployment
# (#387): gvisor and default-deny by default, and the install-wide off switch
# dapper-cluster relies on (isolation.runtimeClassName: "" plus
# isolation.egress.mode: "off").
#
# Usage: HELM=bin/helm scripts/check-isolation-env.sh
set -euo pipefail

HELM="${HELM:-helm}"
chart="$(cd "$(dirname "$0")/.." && pwd)/charts/roundtable-operator"
render() { "$HELM" template rt "$chart" --show-only templates/deployment.yaml "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
# envval <rendered> <name>: the value of env var <name>.
envval() { grep -A1 -- "- name: $2\$" <<<"$1" | sed -n 's/^ *value: //p' | head -1; }
expect() { [ "$(envval "$1" "$2")" = "$3" ] || fail "$4: $2=$(envval "$1" "$2"), want $3"; }

def="$(render)"
expect "$def" KNIGHT_DEFAULT_RUNTIME_CLASS '"gvisor"' "defaults"
expect "$def" KNIGHT_DEFAULT_EGRESS '"deny"' "defaults"
expect "$def" KNIGHT_EGRESS_BASE_HOSTS '"openrouter.ai,api.anthropic.com"' "defaults"

# The exact pair of values dapper-cluster's HelmRelease sets.
tmp="$(mktemp)"; trap 'rm -f "$tmp"' EXIT
cat >"$tmp" <<'YAML'
isolation:
  runtimeClassName: ""
  egress:
    mode: "off"
YAML
off="$(render -f "$tmp")"
expect "$off" KNIGHT_DEFAULT_RUNTIME_CLASS '""' "off switch"
expect "$off" KNIGHT_DEFAULT_EGRESS '"off"' "off switch"

# An unquoted YAML off (false) means off too.
printf 'isolation:\n  egress:\n    mode: off\n' >"$tmp"
expect "$(render -f "$tmp")" KNIGHT_DEFAULT_EGRESS '"off"' "bare off"

expect "$(render --set 'isolation.egress.baseHosts=null')" KNIGHT_EGRESS_BASE_HOSTS '""' "no base hosts"
! render --set isolation.egress.mode=allow >/dev/null 2>&1 || fail "invalid egress mode must fail rendering"
echo "isolation env OK"
