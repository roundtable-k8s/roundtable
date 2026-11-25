#!/usr/bin/env bash
# Renders the chart and checks the leader election flags come from values, and
# that disabling leader election forces Recreate and a single replica.
#
# Usage: HELM=bin/helm scripts/check-leader-election.sh
set -euo pipefail

HELM="${HELM:-helm}"
chart="$(cd "$(dirname "$0")/.." && pwd)/charts/roundtable-operator"
render() { "$HELM" template rt "$chart" "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
has() { grep -q -- "$2" <<<"$1" || fail "$3"; }
lacks() { ! grep -q -- "$2" <<<"$1" || fail "$3"; }

def="$(render)"
has "$def" '- --leader-elect$' "leader election not on by default"
has "$def" '--leader-election-lease-duration=60s' "default lease duration missing"
has "$def" '--leader-election-renew-deadline=40s' "default renew deadline missing"
has "$def" '--leader-election-retry-period=5s' "default retry period missing"
lacks "$def" 'type: Recreate' "Recreate used with leader election on"

custom="$(render --set leaderElection.leaseDuration=30s --set leaderElection.renewDeadline=20s --set leaderElection.retryPeriod=3s)"
has "$custom" '--leader-election-lease-duration=30s' "lease duration value ignored"
has "$custom" '--leader-election-renew-deadline=20s' "renew deadline value ignored"
has "$custom" '--leader-election-retry-period=3s' "retry period value ignored"

off="$(render --set leaderElection.enabled=false)"
lacks "$off" '--leader-elect' "leader election flags rendered while disabled"
has "$off" 'type: Recreate' "Recreate strategy not enforced when disabled"

if out="$(render --set leaderElection.enabled=false --set replicaCount=2 2>&1)"; then
  fail "disabled leader election with replicaCount=2 must fail"
fi
has "$out" 'replicaCount must be 1' "unclear error for disabled election with replicas > 1"
echo "ok"
