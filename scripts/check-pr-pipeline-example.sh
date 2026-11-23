#!/usr/bin/env bash
# Renders the PR pipeline Chain from the Helm chart with prPipeline.enabled=true
# and checks that examples/chain-pr-pipeline.yaml is that output, minus comments,
# document separators and the Helm labels. The example itself is decoded
# strictly against the API types by api/v1alpha1/examples_test.go and applied in
# internal/controller/chain_pr_pipeline_test.go, so together they check that the
# chart renders a Chain the CRDs accept.
#
# Usage: HELM=bin/helm scripts/check-pr-pipeline-example.sh
set -euo pipefail

HELM="${HELM:-helm}"
root="$(cd "$(dirname "$0")/.." && pwd)"
chart="$root/charts/roundtable-operator"
example="$root/examples/chain-pr-pipeline.yaml"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

cat > "$tmp/values.yaml" <<'EOV'
prPipeline:
  enabled: true
  coders: [rt-operator, rt-ui]
  input: '{"repo": "roundtable-k8s/roundtable", "issue": 225}'
EOV

"$HELM" lint "$chart" --quiet
"$HELM" lint "$chart" --quiet -f "$tmp/values.yaml"
"$HELM" template rt "$chart" -n roundtable -f "$tmp/values.yaml" \
  --show-only templates/pr-pipeline.yaml > "$tmp/rendered.yaml"

strip() { grep -v -e '^#' -e '^---$' "$1" || true; }
strip "$tmp/rendered.yaml" |
  grep -v -e '^  labels:$' -e '^    helm.sh/chart:' -e '^    app.kubernetes.io/' > "$tmp/chart.yaml"
strip "$example" > "$tmp/example.yaml"

if ! diff -u "$tmp/example.yaml" "$tmp/chart.yaml"; then
  echo "examples/chain-pr-pipeline.yaml differs from the chart output (diff above: - example, + chart)." >&2
  echo "Re-render it as described in docs/pr-pipeline.md." >&2
  exit 1
fi
echo "examples/chain-pr-pipeline.yaml matches the chart output."
