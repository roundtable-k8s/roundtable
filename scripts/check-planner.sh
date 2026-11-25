#!/usr/bin/env bash
# Renders the chart and checks the planner upgrade path (#566): by default no
# install-wide planner Knight is rendered (each RoundTable owns its planner)
# and the operator gets the planner.* values as table planner defaults;
# planner.enabled=true still renders the old install-wide planner.
#
# Usage: HELM=bin/helm scripts/check-planner.sh
set -euo pipefail

HELM="${HELM:-helm}"
chart="$(cd "$(dirname "$0")/.." && pwd)/charts/roundtable-operator"
render() { "$HELM" template rt "$chart" "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
has() { grep -q -- "$2" <<<"$1" || fail "$3"; }
lacks() { ! grep -q -- "$2" <<<"$1" || fail "$3"; }

def="$(render)"
lacks "$def" 'name: rt-roundtable-operator-planner$' "install-wide planner rendered by default"
lacks "$def" 'ai.roundtable.io/role: planner' "role-labelled planner rendered by default"
has "$def" 'name: TABLE_PLANNER_DEFAULTS' "operator not given the table planner defaults"
has "$def" '\\"model\\":\\"claude-sonnet-4-20250514\\"' "planner.model not in the table planner defaults"
has "$def" '\\"repo\\":\\"https://github.com/roundtable-k8s/arsenal.git\\"' "planner.arsenals not in the table planner defaults"

custom="$(render --set planner.model=opus --set 'planner.prompt.instructions=plan small')"
has "$custom" '\\"model\\":\\"opus\\"' "planner.model ignored"
has "$custom" '\\"prompt\\":{\\"instructions\\":\\"plan small\\"}' "planner.prompt ignored"

on="$(render --set planner.enabled=true)"
has "$on" 'name: rt-roundtable-operator-planner$' "planner.enabled=true does not render the install-wide planner"
has "$on" 'ai.roundtable.io/role: planner' "install-wide planner lost its role label"
echo "planner defaults OK"
