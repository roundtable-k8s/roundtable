#!/usr/bin/env bash
# Checks docs/observability.md: the telemetry values block between the
# telemetry-values markers equals the `telemetry:` block of the chart's
# values.yaml, and every relative link in the doc points at an existing file.
#
# Usage: scripts/check-observability-doc.sh
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
doc="$root/docs/observability.md"
values="$root/charts/roundtable-operator/values.yaml"
fail() { echo "FAIL: $*" >&2; exit 1; }

# values.yaml: from `telemetry:` up to (not including) the next top-level key or comment,
# with trailing blank lines dropped.
want="$(awk '/^telemetry:/{on=1} on && /^[A-Za-z#]/ && !/^telemetry:/{exit} on' "$values" | sed -e :a -e '/^\n*$/{$d;N;ba' -e '}')"
[ -n "$want" ] || fail "no telemetry block in $values"

# The doc: the fenced yaml between the markers.
got="$(awk '/<!-- telemetry-values:begin/{on=1; next} /<!-- telemetry-values:end/{on=0} on' "$doc" | sed -e '/^```/d')"
[ -n "$got" ] || fail "no telemetry-values block in $doc"

if [ "$want" != "$got" ]; then
  diff <(echo "$want") <(echo "$got") >&2 || true
  fail "docs/observability.md telemetry block differs from values.yaml (left: values.yaml)"
fi

# Relative links resolve.
while IFS= read -r link; do
  target="${link%%#*}"
  [ -z "$target" ] && continue
  [ -e "$root/docs/$target" ] || fail "broken link in docs/observability.md: $link"
done < <(grep -o '](\([^)]*\))' "$doc" | sed -e 's/^](//' -e 's/)$//' | grep -v '^https\?://')

echo "observability doc OK"
