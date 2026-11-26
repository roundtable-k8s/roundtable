#!/usr/bin/env bash
# Local quickstart: a kind cluster with NATS (JetStream) and the Round Table
# operator + dashboard, ready for examples/quickstart/. See docs/getting-started.md.
#
# Usage: hack/quickstart-kind.sh [--cluster NAME] [--namespace NS]
#                                [--chart-version VERSION] [--delete]
#   --chart-version  install that version of the OCI chart instead of the local
#                    charts/roundtable-operator
#   --delete         delete the kind cluster and exit
# Needs: kind, helm, kubectl, docker (or another kind provider).
set -euo pipefail

CLUSTER=roundtable
NAMESPACE=roundtable
CHART_VERSION=
DELETE=false
NATS_CHART_VERSION=${NATS_CHART_VERSION:-2.12.3}
OCI_CHART=oci://ghcr.io/roundtable-k8s/charts/roundtable-operator
RELEASE=roundtable-operator   # the dashboard Service is <release>-dashboard

usage() { sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'; }

while [ $# -gt 0 ]; do
  case $1 in
    --cluster) CLUSTER=${2:?}; shift 2 ;;
    --namespace) NAMESPACE=${2:?}; shift 2 ;;
    --chart-version) CHART_VERSION=${2:?}; shift 2 ;;
    --delete) DELETE=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

for tool in kind helm kubectl; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 1; }
done

if $DELETE; then
  kind delete cluster --name "$CLUSTER"
  exit 0
fi

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
# The chart flags for this install live in one file that
# scripts/check-quickstart-values.sh also renders.
NATS_URL="nats://nats.$NAMESPACE.svc:4222"
# shellcheck source=hack/quickstart-flags.sh
. "$REPO_ROOT/hack/quickstart-flags.sh"
CTX="kind-$CLUSTER"

echo "==> kind cluster '$CLUSTER'"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "cluster already exists, reusing it"
else
  kind create cluster --name "$CLUSTER" --wait 120s
fi

echo "==> NATS with JetStream (chart $NATS_CHART_VERSION) in '$NAMESPACE'"
helm repo add nats https://nats-io.github.io/k8s/helm/charts/ --force-update >/dev/null
helm --kube-context "$CTX" upgrade --install nats nats/nats \
  --version "$NATS_CHART_VERSION" \
  --namespace "$NAMESPACE" --create-namespace \
  --set config.jetstream.enabled=true \
  --set config.jetstream.fileStore.pvc.enabled=false \
  --set natsBox.enabled=false \
  --wait --timeout 5m

# kind has no gVisor and no Cilium, so turn the isolation defaults off for this
# local install (see docs/runtime-isolation.md and docs/egress.md).
if [ -n "$CHART_VERSION" ]; then
  CHART=$OCI_CHART
  CHART_ARGS=(--version "$CHART_VERSION")
else
  CHART=$REPO_ROOT/charts/roundtable-operator
  CHART_ARGS=()
fi
echo "==> Round Table operator ($CHART ${CHART_VERSION:+$CHART_VERSION})"
helm --kube-context "$CTX" upgrade --install "$RELEASE" "$CHART" ${CHART_ARGS[@]+"${CHART_ARGS[@]}"} \
  --namespace "$NAMESPACE" --create-namespace \
  "${QUICKSTART_CHART_FLAGS[@]}" \
  --wait --timeout 5m

echo "==> waiting for the operator, dashboard and gateway"
kubectl --context "$CTX" -n "$NAMESPACE" wait deployment \
  --for=condition=Available --timeout=300s \
  "$RELEASE" "$RELEASE-dashboard" "$RELEASE-gateway"

cat <<DONE

Ready. Next (see docs/getting-started.md):
  kubectl --context $CTX -n $NAMESPACE create secret generic llm-api-keys \\
    --from-literal=ANTHROPIC_API_KEY=<your key>
  kubectl --context $CTX apply -f examples/quickstart/
  kubectl --context $CTX -n $NAMESPACE port-forward svc/$RELEASE-dashboard 8080:80
Gateway (docs/gateway.md), after a port-forward of svc/$RELEASE-gateway to 8081:
  kubectl --context $CTX -n $NAMESPACE port-forward svc/$RELEASE-gateway 8081:80
  curl -H "Authorization: Bearer \$(kubectl --context $CTX -n $NAMESPACE get secret $RELEASE-gateway-token -o jsonpath='{.data.token}' | base64 -d)" http://localhost:8081/openapi.json

Tear down with: hack/quickstart-kind.sh --cluster $CLUSTER --delete
DONE
