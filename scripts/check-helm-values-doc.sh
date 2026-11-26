#!/usr/bin/env bash
# Checks docs/helm-values.md against the chart's values.yaml (needs yq v4):
#  1. every leaf key path of values.yaml (scalars, empty maps and lists; a list
#     is a leaf, its items are collapsed to it) appears in a backticked
#     first-column cell of the doc, directly or under a documented parent row;
#  2. every backticked first-column cell of the form a.b.c (or a bare top-level
#     key) resolves to a key in values.yaml.
#
# Usage: scripts/check-helm-values-doc.sh
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
doc="$root/docs/helm-values.md"
values="$root/charts/roundtable-operator/values.yaml"
fail() { echo "FAIL: $*" >&2; exit 1; }
command -v yq >/dev/null 2>&1 || fail "yq (v4) is required"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# Leaf paths: not a non-empty map, and not inside a list.
yq e '.. | select((tag == "!!map" and length > 0) | not) | select(path | map(tag) | all_c(. != "!!int")) | path | join(".")' "$values" \
  | grep -v '^$' | sort -u > "$tmp/leaves" || true
# All key paths (any node, lists collapsed) to resolve doc cells.
yq e '.. | select(path | map(tag) | all_c(. != "!!int")) | path | join(".")' "$values" \
  | grep -v '^$' | sort -u > "$tmp/all" || true
[ -s "$tmp/leaves" ] || fail "no keys read from $values"

# Backticked tokens in the first column of the doc's `| Key |` tables (only
# tables whose header starts with `| Key |` are read; others are skipped silently).
# A token such as `.tag` in `gateway.image.repository` / `.tag` is relative to the
# previous token in the cell (its last path segment is replaced).
awk '
  /^\| Key \|/ { on = 1; next }
  !/^\|/ { on = 0 }
  on && /^\| `/ {
    cell = $0; sub(/^\| /, "", cell); sub(/ \|.*/, "", cell)
    prev = ""
    while (match(cell, /`[^`]*`/)) {
      tok = substr(cell, RSTART + 1, RLENGTH - 2); cell = substr(cell, RSTART + RLENGTH)
      if (substr(tok, 1, 1) == "." && prev != "") { base = prev; sub(/\.?[^.]*$/, "", base); tok = base tok }
      print tok; prev = tok
    }
  }' "$doc" | sort -u > "$tmp/doc"
[ -s "$tmp/doc" ] || fail "no table keys found in $doc"

rc=0
while IFS= read -r leaf; do
  found=0
  while IFS= read -r tok; do
    if [ "$leaf" = "$tok" ] || [[ "$leaf" == "$tok".* ]]; then found=1; break; fi
  done < "$tmp/doc"
  if [ "$found" = 0 ]; then
    echo "FAIL: values.yaml key not documented in docs/helm-values.md: $leaf" >&2; rc=1
  fi
done < "$tmp/leaves"

while IFS= read -r tok; do
  grep -qxF -- "$tok" "$tmp/all" || { echo "FAIL: docs/helm-values.md key not in values.yaml: $tok" >&2; rc=1; }
done < "$tmp/doc"

[ "$rc" = 0 ] || exit 1
echo "helm values doc OK"
