#!/usr/bin/env bash
# Fail on dead relative links in Markdown files and on examples missing from
# examples/README.md. External (http) links and anchors are not checked.
# Usage: scripts/check-doc-links.sh [repo-root]
set -euo pipefail
root="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$root"
fail=0

while IFS= read -r -d '' f; do
  dir=$(dirname "$f")
  # Drop fenced code blocks, then pull out ](target) occurrences.
  while IFS= read -r target; do
    t="${target%%#*}"
    t="${t%% *}"
    t="${t#<}"; t="${t%>}"
    case "$t" in
      ''|http://*|https://*|mailto:*|*://*) continue ;;
    esac
    if [[ "$t" == /* ]]; then path="$root$t"; else path="$dir/$t"; fi
    if [ ! -e "$path" ]; then
      echo "dead link: $f -> $target"
      fail=1
    fi
  done < <(awk '/^[ \t]*(```|~~~)/{c=!c; next} !c' "$f" | grep -oE '\]\([^)]+\)' | sed -E 's/^\]\(//; s/\)$//' || true)
done < <(find . \( -name node_modules -o -name vendor -o -name .git \) -prune -o -name '*.md' -type f -print0)

readme=examples/README.md
for p in examples/*.yaml examples/*/; do
  [ -e "$p" ] || continue
  name=$(basename "$p")
  [ "$name" = dev ] && continue
  [ -d "$p" ] && name="$name/"
  if ! grep -qF -- "$name" "$readme"; then
    echo "unlisted example: $p is not mentioned in $readme"
    fail=1
  fi
done

# Every docs/*.md page must be linked from docs/README.md, the index.
index=docs/README.md
for p in docs/*.md; do
  name=$(basename "$p")
  [ "$name" = README.md ] && continue
  esc=${name//./\\.}
  if ! grep -qE -- "\]\(${esc}(#[^)]*)?\)" "$index"; then
    echo "unlisted doc: $p is not linked from $index"
    fail=1
  fi
done

[ "$fail" -eq 0 ] && echo "doc links OK"
exit "$fail"
