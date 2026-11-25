#!/usr/bin/env bash
# Lints and renders the chart with values-minimal.yaml and checks the result
# needs nothing beyond Kubernetes and NATS: no KEDA or agent-sandbox objects,
# no gateway, no vault volume, and the operator told to leave KEDA, sandbox and
# the default RuntimeClass off.
#
# Usage: HELM=bin/helm scripts/check-minimal-values.sh
set -euo pipefail

HELM="${HELM:-helm}"
chart="$(cd "$(dirname "$0")/.." && pwd)/charts/roundtable-operator"
values="$chart/values-minimal.yaml"
fail() { echo "FAIL: $*" >&2; exit 1; }
has() { grep -q -- "$2" <<<"$1" || fail "$3"; }
lacks() { ! grep -q -- "$2" <<<"$1" || fail "$3"; }

"$HELM" lint "$chart" -f "$values" >/dev/null || fail "helm lint with values-minimal.yaml"
out="$("$HELM" template rt "$chart" -f "$values")"

lacks "$out" '^apiVersion: keda.sh' "KEDA object rendered"
lacks "$out" '^apiVersion: agents.x-k8s.io' "agent-sandbox object rendered"
lacks "$out" '^apiVersion: monitoring.coreos.com' "Prometheus Operator object rendered"
has "$out" '--enable-keda=false' "KEDA not disabled"
has "$out" '--enable-sandbox-backend=false' "sandbox backend not disabled"
lacks "$out" 'name: rt-roundtable-operator-gateway$' "gateway rendered"
lacks "$out" 'roundtable-vault' "vault volume rendered"
lacks "$out" '^[^#]*gvisor' "gVisor RuntimeClass still the default"
has "$out" 'name: rt-roundtable-operator-planner' "planner missing"
echo "minimal values OK"
