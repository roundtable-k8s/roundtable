#!/usr/bin/env bash
# Renders the PR pipeline and PR feedback Chains from the Helm chart with
# prPipeline.enabled and prPipeline.feedback.enabled and checks that
# examples/chain-pr-pipeline.yaml, examples/chain-pr-pipeline-postrun.yaml
# (prPipeline.postRun: chain) and examples/chain-pr-feedback.yaml are that output, minus comments,
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
EOV

"$HELM" lint "$chart" --quiet
"$HELM" lint "$chart" --quiet -f "$tmp/values.yaml"

# Trailing blank lines are dropped: Helm versions differ in how many they emit.
strip() { { grep -v -e '^#' -e '^---$' "$1" || true; } | sed -e :a -e '/^\n*$/{$d;N;ba' -e '}'; }

# check <template> <example> [helm args...]: the example is the rendered template.
check() {
  local template="$1" name="$2" example="$root/examples/$2"
  shift 2
  "$HELM" template rt "$chart" -n roundtable -f "$tmp/values.yaml" "$@" \
    --show-only "templates/$template" > "$tmp/rendered.yaml"
  strip "$tmp/rendered.yaml" |
    grep -v -e '^  labels:$' -e '^    helm.sh/chart:' -e '^    app.kubernetes.io/' > "$tmp/chart.yaml"
  strip "$example" > "$tmp/example.yaml"
  if ! diff -u "$tmp/example.yaml" "$tmp/chart.yaml"; then
    echo "examples/$name differs from the chart output (diff above: - example, + chart)." >&2
    echo "Re-render it as described in docs/pr-pipeline.md / docs/pr-feedback.md." >&2
    exit 1
  fi
  echo "examples/$name matches the chart output."
}

check pr-pipeline.yaml chain-pr-pipeline.yaml
check pr-feedback.yaml chain-pr-feedback.yaml
# postRun: chain (#538) adds the verify, close_parent and handback steps.
check pr-pipeline.yaml chain-pr-pipeline-postrun.yaml --set prPipeline.postRun=chain

# The plan prompt describes only the model tiers the implement steps can run
# (#360): a tier it never describes is one it never picks, and the planner's
# default is fast when allowed, else the first allowed tier.
# assert_plan <rendered> <present|absent> <text>: the rendered chart contains text.
assert_plan() {
  local found=absent
  if grep -q -F -- "$3" "$1"; then found=present; fi
  if [[ "$found" != "$2" ]]; then
    echo "plan prompt ($1): expected $2: $3" >&2
    exit 1
  fi
}

render_tiers() { # render_tiers <out> [--set ...]
  local out="$1"; shift
  "$HELM" template rt "$chart" -n roundtable -f "$tmp/values.yaml" "$@" \
    --show-only templates/pr-pipeline.yaml > "$out"
}

render_tiers "$tmp/default.yaml"
assert_plan "$tmp/default.yaml" absent '"cheap" (Haiku drafts'
assert_plan "$tmp/default.yaml" present '"fast": the default.'
assert_plan "$tmp/default.yaml" present '"deep": design-heavy'

render_tiers "$tmp/all.yaml" --set 'prPipeline.implementModels={cheap,fast,deep}'
assert_plan "$tmp/all.yaml" present '"cheap" (Haiku drafts): only for'
assert_plan "$tmp/all.yaml" present '"fast": the default.'
assert_plan "$tmp/all.yaml" present '"deep": design-heavy'
assert_plan "$tmp/all.yaml" absent '(the default)'
assert_plan "$tmp/all.yaml" absent ', the default)'

render_tiers "$tmp/cheap-deep.yaml" --set 'prPipeline.implementModels={cheap,deep}'
assert_plan "$tmp/cheap-deep.yaml" present '"cheap" (Haiku drafts, the default): only for'
assert_plan "$tmp/cheap-deep.yaml" present '"deep": design-heavy'
assert_plan "$tmp/cheap-deep.yaml" absent '"fast": the default.'
echo "plan prompt describes only the allowed model tiers."
