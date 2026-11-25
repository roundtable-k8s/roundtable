#!/usr/bin/env bash
# Checks the Grafana dashboards (#333): defaults render nothing; enabling them
# renders two sidecar ConfigMaps whose JSON parses; the grafana-operator path
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
[ "$(cms "$on")" = 2 ] || fail "enabled: expected 2 labelled ConfigMaps"
custom="$(render --set dashboards.enabled=true --set dashboards.label=foo --set-string dashboards.labelValue=bar --set dashboards.folder=RT)"
grep -q '^    foo: "bar"$' <<<"$custom" || fail "custom label not applied"
grep -q '^    grafana_folder: "RT"$' <<<"$custom" || fail "folder annotation missing"

# Without the CRD the operator option falls back to ConfigMaps.
nocrd="$(render --set dashboards.enabled=true --set dashboards.grafanaOperator.enabled=true)"
[ "$(cms "$nocrd")" = 2 ] || fail "grafanaOperator without CRD should render ConfigMaps"
withcrd="$(render --set dashboards.enabled=true --set dashboards.grafanaOperator.enabled=true --api-versions grafana.integreatly.org/v1beta1)"
[ "$(count GrafanaDashboard "$withcrd")" = 2 ] || fail "grafanaOperator with CRD: expected 2 GrafanaDashboards"
[ "$(cms "$withcrd")" = 0 ] || fail "grafanaOperator with CRD still renders ConfigMaps"

known='roundtable_knights roundtable_table_cost_usd
roundtable_knight_tasks_total roundtable_knight_active_tasks roundtable_knight_nats_connected
roundtable_knight_llm_cost_usd_total roundtable_knight_llm_tokens_total
roundtable_chain_step_cost_usd_total roundtable_chain_step_tokens_total roundtable_mission_cost_usd_total
kube_horizontalpodautoscaler_status_desired_replicas kube_horizontalpodautoscaler_status_current_replicas'

for f in "$chart"/dashboards/*.json; do
  jq empty "$f" || fail "$f is not valid JSON"
  jq -e '[.templating.list[].name] | contains(["datasource","namespace","table"])' "$f" >/dev/null ||
    fail "$f lacks datasource/namespace/table variables"
  jq -e '.templating.list[] | select(.name=="table") | .multi' "$f" >/dev/null || fail "$f: table must be multi-select"
  grep -q '"uid": "[a-z]' "$f" && ! grep -q '"uid": "\${datasource}"' "$f" && fail "$f hard-codes a datasource"
  while IFS= read -r expr; do
    for m in $(grep -oE '\b(roundtable|kube|keda)_[a-z_]+' <<<"$expr"); do
      grep -qw "$m" <<<"$known" || fail "$f: unknown metric $m in: $expr"
    done
    case "$expr" in
      *kube_horizontalpodautoscaler*) ;;
      *'table=~"$table"'*) ;;
      *) fail "$f: expression does not filter on \$table: $expr" ;;
    esac
  done < <(jq -r '.panels[].targets[].expr' "$f")
done
echo OK
