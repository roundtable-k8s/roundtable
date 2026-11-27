#!/usr/bin/env bash
# Checks docs/observability.md: the telemetry values block between the
# telemetry-values markers equals the `telemetry:` block of the chart's
# values.yaml, every relative link in the doc points at an existing file, and
# every #anchor fragment matches a heading of the target file (GitHub slug rules).
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

# Heading slugs of a markdown file, one per line, by GitHub's rules: lowercase,
# punctuation dropped (letters, digits, `-` and `_` kept), spaces to `-`, and a
# repeated heading gets `-1`, `-2`... Headings inside code fences are skipped.
slugs() {
  awk '
    /^(```|~~~)/ { fence = !fence; next }
    !fence && /^#+[ \t]/ {
      h = $0; sub(/^#+[ \t]+/, "", h); sub(/[ \t#]+$/, "", h)
      h = tolower(h); gsub(/[^a-z0-9 _-]/, "", h); gsub(/ /, "-", h)
      n = seen[h]++
      print (n ? h "-" n : h)
    }' "$1"
}

# has_anchor FILE FRAGMENT: succeeds when FILE has a heading with that slug.
has_anchor() { slugs "$1" | grep -qxF -- "$2"; }

# Negative test: punctuation is dropped, repeats are suffixed, fenced "headings" are ignored.
neg="$(mktemp)"; trap 'rm -f "$neg"' EXIT
printf '%s\n' '# A `Fancy` Title: (v2)!' '## Dup' '## Dup' '```' '# not a heading' '```' >"$neg"
[ "$(slugs "$neg" | paste -sd'|' -)" = "a-fancy-title-v2|dup|dup-1" ] || fail "negative test: slug rules mishandled"
has_anchor "$neg" nope && fail "negative test: a missing anchor was accepted"

# Relative links resolve, and their #fragments name a heading in the target.
while IFS= read -r link; do
  target="${link%%#*}"
  frag=""
  case "$link" in *'#'*) frag="${link#*#}" ;; esac
  if [ -z "$target" ]; then
    file="$doc"
  else
    [ -e "$root/docs/$target" ] || fail "broken link in docs/observability.md: $link"
    file="$root/docs/$target"
  fi
  [ -n "$frag" ] || continue
  case "$file" in *.md) ;; *) continue ;; esac
  has_anchor "$file" "$frag" || fail "broken anchor in docs/observability.md: $link (no such heading in ${file#"$root"/})"
done < <(grep -o '](\([^)]*\))' "$doc" | sed -e 's/^](//' -e 's/)$//' | grep -v '^https\?://')

echo "observability doc OK"
