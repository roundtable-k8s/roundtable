#!/usr/bin/env bash
# Fail on dead relative links in Markdown files and on examples missing from
# examples/README.md. External (http) links and anchors are not checked. Links
# inside fenced blocks and inline code spans are ignored; inline ](target) and
# reference-style [x]: target definitions are checked.
# Usage: scripts/check-doc-links.sh [repo-root]
set -euo pipefail
root="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$root"
fail=0

# links <file>: one link target per line. Drops fenced blocks and inline code
# spans, then prints inline ](target) links and reference-style definitions.
links() {
  local text
  text="$(awk '/^[ \t]*(```|~~~)/{c=!c; next} !c' "$1" | sed -E 's/`+[^`]*`+//g')"
  { grep -oE '\]\([^)]+\)' <<<"$text" | sed -E 's/^\]\(//; s/\)$//' || true
    grep -E '^ {0,3}\[[^]]+\]:[ \t]*[^ \t]' <<<"$text" | sed -E 's/^ {0,3}\[[^]]+\]:[ \t]*//' || true
  }
}

# Self-test: a link in a code span is ignored, reference definitions are found.
st="$(mktemp)"; trap 'rm -f "$st"' EXIT
printf '%s\n' 'use `[x](code-span.md)` and [y](real.md)' '' '[ref]: ref-target.md "title"' >"$st"
[ "$(links "$st" | paste -sd, -)" = "real.md,ref-target.md \"title\"" ] || { echo "FAIL: link extraction self-test"; exit 1; }

while IFS= read -r -d '' f; do
  dir=$(dirname "$f")
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
  done < <(links "$f")
done < <(find . \( -name node_modules -o -name vendor -o -name .git \) -prune -o -name '*.md' -type f -print0)

readme=examples/README.md
for p in examples/*.yaml examples/*/; do
  [ -e "$p" ] || continue
  name=$(basename "$p")
  [ "$name" = dev ] && continue
  [ -d "$p" ] && name="$name/"
  # Listed as a link target ](name) or a backticked `name`: not a substring of
  # another entry such as bar-foo.yaml.
  if ! grep -qF -e "]($name)" -e "\`$name\`" "$readme"; then
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
