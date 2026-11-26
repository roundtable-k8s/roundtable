#!/usr/bin/env bash
# Checks docs/observability.md against the Go code: the "Operator and gateway
# metrics" table lists every roundtable_* metric registered in
# pkg/metrics/metrics.go and internal/gateway/metrics.go, with exactly the
# labels the code declares, and no metric the code does not register. It also
# fails on stale "warm pool" text (the warm pool was removed in #483).
#
# Usage: scripts/check-metrics-doc.sh
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
doc="$root/docs/observability.md"
src=("$root/pkg/metrics/metrics.go" "$root/internal/gateway/metrics.go")
fail() { echo "FAIL: $*" >&2; exit 1; }

# One line per metric: "name label...". The labels are the first []string{...}
# after the metric's Name: line.
code="$(awk '
  /Name:/ && match($0, /"roundtable_[a-z0-9_]+"/) {
    name = substr($0, RSTART + 1, RLENGTH - 2); pending = 1; next
  }
  pending && /\[\]string\{/ {
    labels = $0; sub(/.*\[\]string\{/, "", labels); sub(/\}.*/, "", labels)
    gsub(/[^A-Za-z0-9_]+/, " ", labels)
    print name, labels; pending = 0
  }' "${src[@]}")"
[ -n "$code" ] || fail "no roundtable_* metrics found in the Go sources"

# The table: rows of the "Operator and gateway metrics" section, whose first
# cell is the metric name in backticks and whose third cell lists the labels.
section="$(awk '/^## Operator and gateway metrics/{on=1; next} on && /^## /{exit} on' "$doc")"
[ -n "$section" ] || fail "no 'Operator and gateway metrics' section in $doc"
docrows="$(awk -F'|' '
  /^\| `roundtable_/ {
    name = $2; gsub(/[ `]/, "", name)
    labels = $4; gsub(/[^A-Za-z0-9_]+/, " ", labels)
    print name, labels
  }' <<<"$section")"
[ -n "$docrows" ] || fail "no metric rows in the 'Operator and gateway metrics' section of $doc"

# Compare name and labels as sets, so label order does not matter.
norm() {
  while read -r name labels; do
    printf '%s %s\n' "$name" "$(printf '%s\n' $labels | sort | paste -sd' ' -)"
  done | sort
}
want="$(norm <<<"$code")"
got="$(norm <<<"$docrows")"
if [ "$want" != "$got" ]; then
  diff <(echo "$want") <(echo "$got") >&2 || true
  fail "the metrics table in docs/observability.md differs from the Go code (left: code, right: doc)"
fi

! grep -qi 'warm pool' "$doc" || fail "stale 'warm pool' text in docs/observability.md (removed in #483)"

echo "metrics doc OK ($(wc -l <<<"$want" | tr -d ' ') metrics)"
