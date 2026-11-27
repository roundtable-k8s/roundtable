#!/usr/bin/env bash
# Checks the chart against what the code asks for, using only helm and yq and
# the committed files (run `make manifests` to refresh them):
#   1. the operator ClusterRole in the chart grants every `group resource verb`
#      triple of config/rbac/role.yaml (kubebuilder markers) and of the
#      leader-election role, and nothing beyond them except the allowlist below;
#   2. charts/roundtable-operator/crds equals config/crd/bases.
#
# Usage: HELM=bin/helm scripts/check-operator-rbac.sh
set -euo pipefail

HELM="${HELM:-helm}"
root="$(cd "$(dirname "$0")/.." && pwd)"
chart="$root/charts/roundtable-operator"

# Chart triples that no marker or leader-election role names, on purpose, as
# extended regexes over "group resource verb". Keep this short and comment
# every entry.
ALLOWED_CHART_ONLY=(
  # The chart gives the status and finalizers subresources of our CRDs the
  # same verb list as the resources; the markers name only the verbs used.
  '^ai\.roundtable\.io [a-z]+/(status|finalizers) '
)

fail() { echo "FAIL: $*" >&2; exit 1; }
command -v yq >/dev/null 2>&1 || fail "yq (v4) is required"
yq --version 2>&1 | grep -Eq 'version v?4\.' || fail "yq v4 is required (mikefarah/yq), found: $(yq --version 2>&1 | head -1)"

triples() { # yq-expression file-or-stdin -> sorted "group resource verb" lines
  yq eval-all '[.] | .[] | select(.kind == "ClusterRole" or .kind == "Role") | .rules[] |
    . as $r | $r.apiGroups[] as $g | $r.resources[] as $res | $r.verbs[] as $v |
    (if $g == "" then "core" else $g end) + " " + $res + " " + $v' "$@" | sort -u
}

chart_t="$(mktemp)"; code_t="$(mktemp)"
trap 'rm -f "$chart_t" "$code_t"' EXIT

"$HELM" template rt "$chart" |
  yq eval-all 'select(.kind == "ClusterRole" and .metadata.name == "rt-roundtable-operator")' - |
  triples - | sort -u >"$chart_t"
[ -s "$chart_t" ] || { echo "FAIL: operator ClusterRole not rendered" >&2; exit 1; }
triples "$root/config/rbac/role.yaml" "$root/config/rbac/leader_election_role.yaml" >"$code_t"

status=0
missing="$(comm -13 "$chart_t" "$code_t")"
if [ -n "$missing" ]; then
  echo "FAIL: the code needs these permissions (config/rbac) but the chart ClusterRole lacks them:" >&2
  sed 's/^/  /' <<<"$missing" >&2
  status=1
fi
extra="$(comm -23 "$chart_t" "$code_t")"
if [ ${#ALLOWED_CHART_ONLY[@]} -gt 0 ]; then
  extra="$(grep -vEf <(printf '%s\n' "${ALLOWED_CHART_ONLY[@]}") <<<"$extra" || true)"
fi
if [ -n "$extra" ]; then
  echo "FAIL: the chart ClusterRole grants these but no kubebuilder marker or leader-election role asks for them:" >&2
  sed 's/^/  /' <<<"$extra" >&2
  status=1
fi

if ! diff -r -x kustomization.yaml "$chart/crds" "$root/config/crd/bases" >&2; then
  echo "FAIL: chart CRDs differ from config/crd/bases; run 'make manifests' and copy them to the chart" >&2
  status=1
fi

[ "$status" -eq 0 ] || exit 1
echo "operator RBAC and CRDs OK"
