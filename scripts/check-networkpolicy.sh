#!/usr/bin/env bash
# Renders the opt-in ingress NetworkPolicies and checks that: defaults render
# none; enabling them renders the operator, dashboard and knight policies with
# selectors matching the pod labels; and no rule is rendered without peers (an
# ingress rule with no `from` would allow every source).
#
# Usage: HELM=bin/helm scripts/check-networkpolicy.sh
set -euo pipefail

HELM="${HELM:-helm}"
chart="$(cd "$(dirname "$0")/.." && pwd)/charts/roundtable-operator"

render() { # [helm args...] -> rendered manifests
  "$HELM" template rt "$chart" "$@"
}
fail() { echo "FAIL: $*" >&2; exit 1; }
has() { grep -q -- "$2" <<<"$1" || fail "$3"; }
lacks() { ! grep -q -- "$2" <<<"$1" || fail "$3"; }
# Counts the NetworkPolicy documents rendered by templates/networkpolicy.yaml
# (the gateway renders its own policy from templates/gateway.yaml).
policies() { grep -c '^# Source: roundtable-operator/templates/networkpolicy.yaml$' <<<"$1" || true; }
# Prints the NetworkPolicy document whose metadata.name is $2.
doc() {
  awk -v want="  name: $2" '
    /^---$/ { if (hit) exit; buf=""; np=0; next }
    { buf = buf $0 "\n" }
    /^kind: NetworkPolicy$/ { np=1 }
    np && $0 == want { hit=1 }
    END { if (hit) printf "%s", buf }' <<<"$1"
}
# The release is named rt, so the policies are rt-roundtable-operator-<suffix>.
base=rt-roundtable-operator

default="$(render)"
[ "$(policies "$default")" = 0 ] || fail "defaults render NetworkPolicy objects"

on="$(render --set networkPolicy.enabled=true --set dashboard.enabled=true)"
[ "$(policies "$on")" = 3 ] || fail "enabled: expected 3 NetworkPolicies, got $(policies "$on")"

op="$(doc "$on" "$base-operator")"
[ -n "$op" ] || fail "operator NetworkPolicy not rendered"
has "$op" 'app.kubernetes.io/name: roundtable-operator' "operator policy selector lacks the operator pod label"
has "$op" 'kubernetes.io/metadata.name: monitoring' "operator policy lacks the default metricsFrom peer"
has "$op" 'port: 8443' "operator policy lacks the metrics port"
has "$op" 'port: 8081' "operator policy lacks the health port"

dash="$(doc "$on" "$base-dashboard")"
[ -n "$dash" ] || fail "dashboard NetworkPolicy not rendered"
has "$dash" 'app.kubernetes.io/name: roundtable-dashboard' "dashboard policy selector lacks the dashboard pod label"
has "$dash" 'port: http' "dashboard policy lacks the http port"

kn="$(doc "$on" "$base-knights")"
[ -n "$kn" ] || fail "knight NetworkPolicy not rendered"
has "$kn" 'app.kubernetes.io/name: knight$' "knight policy selector lacks app.kubernetes.io/name: knight"
has "$kn" 'port: 3000' "knight policy lacks the metrics port 3000"
for p in "$op" "$dash" "$kn"; do
  has "$p" 'policyTypes' "policy lacks policyTypes"
  has "$p" '    - Ingress' "policy is not Ingress-only"
  lacks "$p" 'Egress' "policy restricts egress"
done

# Dashboard policy is skipped when the dashboard is disabled.
nodash="$(render --set networkPolicy.enabled=true --set dashboard.enabled=false)"
[ "$(policies "$nodash")" = 2 ] || fail "dashboard disabled: expected 2 NetworkPolicies, got $(policies "$nodash")"

# Empty peer lists render no ingress rule: no rule may exist without `from`.
empty="$(render --set networkPolicy.enabled=true --set networkPolicy.metricsFrom=null --set dashboard.enabled=true)"
[ "$(policies "$empty")" = 3 ] || fail "empty metricsFrom: expected 3 NetworkPolicies, got $(policies "$empty")"
for name in operator dashboard knights; do
  lacks "$(doc "$empty" "$base-$name")" '^  ingress:' "empty peer lists still render an ingress rule ($name)"
done

echo "check-networkpolicy: ok"
