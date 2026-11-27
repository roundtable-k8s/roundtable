#!/usr/bin/env bash
# Checks the Grafana dashboards (#333): defaults render nothing; enabling them
# renders five sidecar ConfigMaps whose JSON parses; the grafana-operator path
# renders GrafanaDashboard CRs only when the API exists; and every PromQL
# expression uses only known roundtable_* metrics (plus kube-state-metrics
# HPA series) and filters on $table.
#
# Usage: HELM=bin/helm scripts/check-grafana-dashboards.sh
set -euo pipefail

HELM="${HELM:-helm}"
chart="$(cd "$(dirname "$0")/.." && pwd)/charts/roundtable-operator"

fail() { echo "FAIL: $*" >&2; exit 1; }
render() { "$HELM" template rt "$chart" "$@"; }
count() { grep -c "^kind: $1\$" <<<"$2" || true; }
# Dashboard objects: ConfigMaps carrying the sidecar label, or GrafanaDashboards.
cms() { grep -c '^    grafana_dashboard: "1"$' <<<"$1" || true; }

[ "$(cms "$(render)")" = 0 ] || fail "defaults render dashboard ConfigMaps"
[ "$(count GrafanaDashboard "$(render)")" = 0 ] || fail "defaults render GrafanaDashboards"

on="$(render --set dashboards.enabled=true)"
[ "$(cms "$on")" = 5 ] || fail "enabled: expected 5 labelled ConfigMaps"
custom="$(render --set dashboards.enabled=true --set dashboards.label=foo --set-string dashboards.labelValue=bar --set dashboards.folder=RT)"
grep -q '^    foo: "bar"$' <<<"$custom" || fail "custom label not applied"
grep -q '^    grafana_folder: "RT"$' <<<"$custom" || fail "folder annotation missing"

# Without the CRD the operator option falls back to ConfigMaps.
nocrd="$(render --set dashboards.enabled=true --set dashboards.grafanaOperator.enabled=true)"
[ "$(cms "$nocrd")" = 5 ] || fail "grafanaOperator without CRD should render ConfigMaps"
withcrd="$(render --set dashboards.enabled=true --set dashboards.grafanaOperator.enabled=true --api-versions grafana.integreatly.org/v1beta1)"
[ "$(count GrafanaDashboard "$withcrd")" = 5 ] || fail "grafanaOperator with CRD: expected 5 GrafanaDashboards"
[ "$(cms "$withcrd")" = 0 ] || fail "grafanaOperator with CRD still renders ConfigMaps"

known='roundtable_knights roundtable_table_cost_usd
roundtable_knight_tasks_total roundtable_knight_active_tasks roundtable_knight_nats_connected
roundtable_knight_llm_cost_usd_total roundtable_knight_llm_tokens_total
roundtable_chain_step_cost_usd_total roundtable_chain_step_tokens_total
roundtable_chain_runs_total roundtable_chain_step_duration_seconds_bucket roundtable_chain_provider_retries_total roundtable_missions
roundtable_build_info roundtable_dead_letters_open roundtable_knight_consumer_ready roundtable_trigger_firings_total roundtable_trigger_runs_finished_total roundtable_trigger_last_fired_timestamp_seconds roundtable_gateway_requests_total roundtable_gateway_request_duration_seconds_bucket roundtable_tasks_dead_lettered_total roundtable_knight_isolated roundtable_knight_wake_seconds_bucket roundtable_mission_cost_usd_total
roundtable_knight_task_duration_seconds_bucket roundtable_knight_tool_calls_total roundtable_knight_tool_call_duration_seconds_bucket roundtable_knight_tool_calls_blocked_total roundtable_knight_nats_reconnects_total
kube_horizontalpodautoscaler_status_desired_replicas kube_horizontalpodautoscaler_status_current_replicas keda_scaler_metrics_value'

# True when any panel or target datasource uid is a literal rather than
# ${datasource} or another template variable ($var, ${var}, [[var]]).
bad_ds() {
  jq -e '[.. | objects | select(has("datasource")) | .datasource
          | if type == "object" then .uid else . end
          | select(type == "string" and test("^(\\$\\{?[A-Za-z_][A-Za-z0-9_]*\\}?|\\[\\[[A-Za-z_][A-Za-z0-9_]*\\]\\])$") | not)]
         | length > 0' "$1" >/dev/null
}

# Negative test: a literal datasource uid on one panel must be caught.
neg="$(mktemp)"; trap 'rm -f "$neg"' EXIT
jq '.panels[0].targets[0].datasource.uid = "abc123"' "$chart/dashboards/fleet-overview.json" >"$neg"
bad_ds "$neg" || fail "negative test: literal datasource uid not detected"

# A text panel without targets and a row with nested panels must not break the
# expression walk, and a nested panel's expression must still be seen.
jq '.panels += [{"type":"text","title":"t"},{"type":"row","title":"r","panels":[{"type":"timeseries","targets":[{"expr":"NESTED_EXPR"}]}]}]' \
  "$chart/dashboards/fleet-overview.json" >"$neg"
[ "$(jq -r '.. | objects | select(has("targets")) | .targets[]?.expr // empty' "$neg" | grep -c NESTED_EXPR)" = 1 ] ||
  fail "negative test: nested row panel expression not found"

for f in "$chart"/dashboards/*.json; do
  jq empty "$f" || fail "$f is not valid JSON"
  jq -e '[.templating.list[].name] | contains(["datasource","namespace","table"])' "$f" >/dev/null ||
    fail "$f lacks datasource/namespace/table variables"
  jq -e '.templating.list[] | select(.name=="table") | .multi' "$f" >/dev/null || fail "$f: table must be multi-select"
  bad_ds "$f" && fail "$f hard-codes a datasource"
  # Every defined variable must be referenced by at least one panel expression.
  for v in $(jq -r '.templating.list[].name' "$f"); do
    [ "$v" = datasource ] && continue
    jq -e --arg v "$v" '[.. | objects | select(has("targets")) | .targets[]?.expr // empty | select(contains("$" + $v))] | length > 0' "$f" >/dev/null ||
      fail "$f: variable \$$v is not used by any panel"
  done
  while IFS= read -r expr; do
    for m in $(grep -oE '\b(roundtable|kube|keda)_[a-z_]+' <<<"$expr"); do
      grep -qw "$m" <<<"$known" || fail "$f: unknown metric $m in: $expr"
    done
    case "$expr" in
      *kube_horizontalpodautoscaler*|*keda_scaler_metrics_value*) ;;
      # The gateway duration histogram carries no table label (only route).
      *roundtable_gateway_request_duration_seconds_bucket*) ;;
      # Build info is per component, not per table.
      *roundtable_build_info*) ;;
      *'table=~"$table"'*) ;;
      *) fail "$f: expression does not filter on \$table: $expr" ;;
    esac
  done < <(jq -r '.. | objects | select(has("targets")) | .targets[]?.expr // empty' "$f")
done
echo OK
