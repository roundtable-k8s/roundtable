#!/usr/bin/env bash
# Checks the opt-in PrometheusRule: defaults render nothing, enabling it without
# the monitoring.coreos.com/v1 API renders nothing, enabling it with the API
# renders a PrometheusRule whose rules pass `promtool check rules`, and the
# tunables and label overrides take effect.
#
# Usage: HELM=bin/helm [PROMTOOL=promtool] scripts/check-prometheusrule.sh
# Without promtool on PATH it downloads a pinned release (linux/amd64); if that
# fails it skips locally but fails when CI is set.
set -euo pipefail

HELM="${HELM:-helm}"
PROMTOOL="${PROMTOOL:-promtool}"
chart="$(cd "$(dirname "$0")/.." && pwd)/charts/roundtable-operator"
api=(--api-versions monitoring.coreos.com/v1)

render() { "$HELM" template rt "$chart" "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
count() { grep -c '^kind: PrometheusRule$' <<<"$1" || true; }

[ "$(count "$(render "${api[@]}")")" = 0 ] || fail "defaults render a PrometheusRule"
[ "$(count "$(render --set alerts.enabled=true)")" = 0 ] || fail "rendered without the monitoring.coreos.com/v1 API"

on="$(render --set alerts.enabled=true "${api[@]}" \
  --set alerts.costSpikeUSDPer24h=42 --set alerts.taskFailuresPerHour=7 \
  --set alerts.labels.team=ops --set alerts.labels.severity=critical \
  --set alerts.gatewayErrorRatio=0.3 --set alerts.natsReconnectsPer30m=9 \
  --namespace rtns)"
[ "$(count "$on")" = 1 ] || fail "enabled: expected 1 PrometheusRule"
grep -q '> 42$' <<<"$on" || fail "costSpikeUSDPer24h not applied"
grep -q '> 7$' <<<"$on" || fail "taskFailuresPerHour not applied"
grep -q 'severity: critical' <<<"$on" || fail "alerts.labels severity override not applied"
! grep -q 'severity: warning' <<<"$on" || fail "default severity not overridden"
grep -q 'team: ops' <<<"$on" || fail "alerts.labels not applied"
grep -q 'namespace="rtns"' <<<"$on" || fail "release namespace not used in pod rules"
grep -q 'status=~"error|publish_failed"' <<<"$on" || fail "failure alert does not match emitted statuses"
! grep -q 'error|timeout' <<<"$on" || fail "failure alert still matches the never-emitted timeout status"
! grep -qE '(pi|claude)_knight' <<<"$on" || fail "old runtime metric names in rules"
# Every knight aggregation keeps table.
! grep -E 'sum by \(' <<<"$on" | grep -E 'roundtable_(knight|chain)' | grep -qv 'table' \
  || fail "an aggregation over knight/chain metrics drops table"

for a in RoundtableDeadLettersOpen KnightConsumerNotReady KnightNotIsolated RoundtableChainRunsFailing KnightNotReady RoundtableToolsImageBuildFailed; do
  grep -q "alert: $a\$" <<<"$on" || fail "alert $a missing"
done
for a in RoundtableGatewayErrors KnightNatsFlapping RoundtableReconcileErrors; do
  grep -q "alert: $a\$" <<<"$on" || fail "alert $a missing"
done
grep -q 'roundtable_knight_nats_reconnects_total\[30m\]' <<<"$on" || fail "KnightNatsFlapping expr missing"
grep -q '> 9$' <<<"$on" || fail "natsReconnectsPer30m not applied"
grep -q '> 0.3$' <<<"$on" || fail "gatewayErrorRatio not applied"
grep -q 'code=~"5\.\."' <<<"$on" || fail "gateway rule does not count 5xx"
grep -q 'sum by (namespace, table, route) (increase(roundtable_gateway_requests_total{code' <<<"$on" \
  || fail "gateway rule does not keep route and table"
! grep -qE 'sum by \(namespace, table\) \(increase\(roundtable_gateway' <<<"$on" \
  || fail "gateway rule aggregates away route"
grep -q 'sum by (namespace, controller) (rate(controller_runtime_reconcile_errors_total{namespace="rtns", job="rt-roundtable-operator-metrics"}\[10m\])) > 0' <<<"$on" \
  || fail "RoundtableReconcileErrors expr missing"
oj="$(render --set alerts.enabled=true "${api[@]}" --set alerts.operatorJob=my-op --set alerts.gatewayMinRequests=50 --namespace rtns)"
grep -q 'reconcile_errors_total{namespace="rtns", job="my-op"}' <<<"$oj" || fail "alerts.operatorJob not applied"
grep -q '\[15m\])) >= 50$' <<<"$oj" || fail "alerts.gatewayMinRequests not applied"
grep -q '\[15m\])) >= 20$' <<<"$on" || fail "default gatewayMinRequests not 20"
grep -q 'roundtable.operator' <<<"$on" || fail "roundtable.operator group missing"
grep -q 'roundtable.platform' <<<"$on" || fail "roundtable.platform group missing"
grep -q 'for: 15m' <<<"$(grep -A12 'alert: KnightNotReady$' <<<"$on")" || fail "default knightNotReadyMinutes not 15m"
kn="$(render --set alerts.enabled=true "${api[@]}" --set alerts.knightNotReadyMinutes=30)"
grep -q 'for: 30m' <<<"$(grep -A12 'alert: KnightNotReady$' <<<"$kn")" || fail "knightNotReadyMinutes not applied"
grep -q 'kube_job_failed{condition="true", job_name=~".+-toolsimg-.+"}' <<<"$on" || fail "tools image build alert expr missing"
for a in RoundtableScheduleMissed RoundtableScheduleJobFailed RoundtableTriggerFiringErrors RoundtableScheduledRunFailed; do
  grep -q "alert: $a\$" <<<"$on" || fail "alert $a missing"
done
grep -q 'roundtable_trigger_runs_finished_total{outcome=~"failed|lost"}' <<<"$on" || fail "RoundtableScheduledRunFailed expr missing"
grep -q 'roundtable.triggers' <<<"$on" || fail "roundtable.triggers group missing"
grep -q 'kube_cronjob_spec_suspend == 0' <<<"$on" || fail "RoundtableScheduleMissed does not skip suspended CronJobs"
grep -q 'cronjob=~"trigger-.+"' <<<"$on" || fail "schedule alerts not limited to trigger- CronJobs"
grep -q '< time() - 600$' <<<"$on" || fail "default scheduleMissedSeconds not 600"
sched="$(render --set alerts.enabled=true "${api[@]}" --set alerts.scheduleMissedSeconds=1200)"
grep -q '< time() - 1200$' <<<"$sched" || fail "scheduleMissedSeconds not applied"
! grep -E 'sum by \(' <<<"$on" | grep -E 'roundtable_trigger_firings' | grep -qv 'table' \
  || fail "trigger firings aggregation drops table"
grep -q '> 0.5$' <<<"$(render --set alerts.enabled=true "${api[@]}")" || fail "default chainFailureRatio not 0.5"
tuned="$(render --set alerts.enabled=true "${api[@]}" --set alerts.chainFailureRatio=0.2 --set alerts.deadLettersOpenMinutes=30)"
grep -q '> 0.2$' <<<"$tuned" || fail "chainFailureRatio not applied"
grep -q 'for: 30m' <<<"$tuned" || fail "deadLettersOpenMinutes not applied"
! grep -E 'sum by \(' <<<"$on" | grep -E 'roundtable_(dead|knight_isolated|chain_runs)' | grep -qv 'table' \
  || fail "a platform aggregation drops table"

off="$(render --set alerts.enabled=true --set alerts.scaling.enabled=false "${api[@]}")"
! grep -q 'roundtable.scaling' <<<"$off" || fail "alerts.scaling.enabled=false still renders scaling group"

! grep -q 'RoundtableQueueBacklog' <<<"$on" || fail "RoundtableQueueBacklog rendered by default"
qb="$(render --set alerts.enabled=true "${api[@]}" --set alerts.queueBacklog.enabled=true --set alerts.queueBacklog.pending=25 --set alerts.queueBacklog.forMinutes=7)"
grep -q 'alert: RoundtableQueueBacklog$' <<<"$qb" || fail "RoundtableQueueBacklog missing when enabled"
grep -q '> 25$' <<<"$qb" || fail "queueBacklog.pending not applied"
grep -q 'for: 7m' <<<"$(grep -A12 'alert: RoundtableQueueBacklog$' <<<"$qb")" || fail "queueBacklog.forMinutes not applied"
on="$qb"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
if ! command -v "$PROMTOOL" >/dev/null 2>&1; then
  # CI runners have no promtool: fetch a pinned release (linux/amd64 only).
  v=3.5.0
  sum=e811827af26d822afb09a4f28314f61b618b12cff5369835a67f674d8b46f39a
  if [ "$(uname -sm)" = "Linux x86_64" ] && curl -fsSL -o "$tmp/prom.tgz" \
      "https://github.com/prometheus/prometheus/releases/download/v${v}/prometheus-${v}.linux-amd64.tar.gz"; then
    echo "$sum  $tmp/prom.tgz" | sha256sum -c - >/dev/null || fail "promtool download checksum mismatch"
    tar -xzf "$tmp/prom.tgz" -C "$tmp" --strip-components=1 "prometheus-${v}.linux-amd64/promtool"
    PROMTOOL="$tmp/promtool"
  elif [ -n "${CI:-}" ]; then
    fail "promtool not found and not downloadable; refusing to skip rule syntax check in CI"
  else
    echo "SKIP: promtool not found and not downloadable, rule syntax not checked"
    exit 0
  fi
fi
# The PrometheusRule spec is a rule file: take the spec: block, drop the indent.
awk '/^kind: PrometheusRule$/{pr=1} pr&&/^spec:$/{s=1;next} s&&/^---$/{exit} s{sub(/^  /,"");print}' <<<"$on" >"$tmp/rules.yaml"
"$PROMTOOL" check rules "$tmp/rules.yaml"
echo "OK: PrometheusRule checks passed"
