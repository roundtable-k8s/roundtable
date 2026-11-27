#!/usr/bin/env bash
# Renders the chart with the flags hack/quickstart-kind.sh passes (shared in
# hack/quickstart-flags.sh) and fails if a chart default NATS URL
# (nats.database.svc) or any PVC claimName leaks into the result.
#
# Usage: HELM=bin/helm scripts/check-quickstart-values.sh
set -euo pipefail

HELM="${HELM:-helm}"
root="$(cd "$(dirname "$0")/.." && pwd)"
chart="$root/charts/roundtable-operator"
fail() { echo "FAIL: $*" >&2; exit 1; }

NATS_URL="nats://nats.roundtable.svc:4222"
# shellcheck source=hack/quickstart-flags.sh
. "$root/hack/quickstart-flags.sh"

out="$("$HELM" template roundtable-operator "$chart" -n roundtable "${QUICKSTART_CHART_FLAGS[@]}")"

! grep -q 'nats\.database\.svc' <<<"$out" || fail "nats.database.svc rendered"
! grep -q 'claimName' <<<"$out" || fail "a claimName is rendered (missing PVC)"
grep -q -- "--nats-url=$NATS_URL" <<<"$out" || fail "gateway not pointed at $NATS_URL"
dash_nats() { grep -A1 -E '^ +- name: NATS_URL$' <<<"$1" | grep -qE "value: \"?$NATS_URL\"?$"; }
dash_nats "$out" || fail "dashboard NATS_URL not $NATS_URL"
# Negative test: the operator's MISSION_DEFAULT_NATS_URL must not satisfy the check.
! dash_nats "$(printf '%s\n' '- name: MISSION_DEFAULT_NATS_URL' "  value: \"$NATS_URL\"")" || fail "negative test: operator env matched dashboard NATS_URL"
echo "quickstart values OK"
