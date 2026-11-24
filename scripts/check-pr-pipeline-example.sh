#!/usr/bin/env bash
# Renders the PR pipeline, PR feedback and smoke Chains from the Helm chart with
# prPipeline.enabled and prPipeline.feedback.enabled and checks that
# examples/chain-pr-pipeline.yaml and examples/chain-pr-feedback.yaml are that output, minus comments,
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
  feedback:
    enabled: true
smoke:
  enabled: true
EOV

"$HELM" lint "$chart" --quiet
"$HELM" lint "$chart" --quiet -f "$tmp/values.yaml"

# Trailing blank lines are dropped: Helm versions differ in how many they emit.
strip() { { grep -v -e '^#' -e '^---$' "$1" || true; } | sed -e :a -e '/^\n*$/{$d;N;ba' -e '}'; }

# check <template> <example>: the example is the rendered template.
check() {
  local template="$1" example="$root/examples/$2"
  "$HELM" template rt "$chart" -n roundtable -f "$tmp/values.yaml" \
    --show-only "templates/$template" > "$tmp/rendered.yaml"
  strip "$tmp/rendered.yaml" |
    grep -v -e '^  labels:$' -e '^    helm.sh/chart:' -e '^    app.kubernetes.io/' > "$tmp/chart.yaml"
  strip "$example" > "$tmp/example.yaml"
  if ! diff -u "$tmp/example.yaml" "$tmp/chart.yaml"; then
    echo "examples/$2 differs from the chart output (diff above: - example, + chart)." >&2
    echo "Re-render it as described in docs/pr-pipeline.md / docs/pr-feedback.md." >&2
    exit 1
  fi
  echo "examples/$2 matches the chart output."
}

check pr-pipeline.yaml chain-pr-pipeline.yaml
check pr-feedback.yaml chain-pr-feedback.yaml
check smoke.yaml chain-smoke.yaml
