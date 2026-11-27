#!/usr/bin/env bash
# Checks the main-build chart version (<Chart.yaml version>-dev.g<short sha>) is valid SemVer
# for SHAs made only of digits with a leading zero (e.g. 0954706), where the bare form
# -dev.<sha> is rejected by helm ("chart.metadata.version is invalid"). Also checks the
# workflow derives the version with the same "g" form.
#
# Usage: HELM=bin/helm scripts/check-dev-chart-version.sh
set -euo pipefail

HELM="${HELM:-helm}"
root="$(cd "$(dirname "$0")/.." && pwd)"
chart="$root/charts/roundtable-operator"
fail() { echo "FAIL: $*" >&2; exit 1; }

# Must match "Set chart version from SHA" in .github/workflows/ci.yml.
dev_version() { echo "$1-dev.g${2:0:7}"; }

# lint_version <version> prints nothing and returns 0 when helm accepts the chart version.
lint_version() {
  local tmp rc=0
  tmp="$(mktemp -d)"
  cp -r "$chart" "$tmp/chart"
  sed -i "s/^version:.*/version: $1/" "$tmp/chart/Chart.yaml"
  "$HELM" lint "$tmp/chart" >/dev/null 2>&1 || rc=$?
  rm -rf "$tmp"
  return "$rc"
}

grep -q -- '-dev.g${SHORT_SHA}' "$root/.github/workflows/ci.yml" ||
  fail "ci.yml does not set the main-build chart version to <version>-dev.g<short sha>"

base="$(sed -n 's/^version: *//p' "$chart/Chart.yaml" | tr -d '"')"
[ -n "$base" ] || fail "no version in Chart.yaml"

for sha in 0954706f0000000000000000000000000000000 abcdef12000000000000000000000000000000000; do
  v="$(dev_version "$base" "$sha")"
  lint_version "$v" || fail "chart version $v for $sha is invalid"
done

# Negative test: the bare short SHA form is invalid for an all-digit SHA with a leading zero,
# so the check above is meaningful.
bare="$base-dev.0954706"
if lint_version "$bare"; then
  fail "helm accepted $bare; the negative test no longer shows the all-digit SHA problem"
fi

echo "ok"
