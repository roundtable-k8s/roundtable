#!/usr/bin/env bash
# Checks the release pins in the install docs: the three pins must name the
# same minor line, the range text in the ocirepository.yaml comment must match
# the semver pin, they must not be newer than the chart version, and (when the
# clone has tags) must name a released v<version> tag.
#
# Usage: scripts/check-install-versions.sh
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
fail() { echo "FAIL: $*" >&2; exit 1; }

flux="$root/examples/flux/ocirepository.yaml"
doc="$root/docs/install.md"
chart="$root/charts/roundtable-operator/Chart.yaml"
ver_re='[0-9]+\.[0-9]+\.[0-9]+'

semver_pin="$(sed -nE "s/^[[:space:]]*semver:[[:space:]]*\"?\^?($ver_re)\"?.*/\1/p" "$flux" | head -n1)"
doc_semver="$(sed -nE "s/.*\`semver\` constraint.*\(\`\^($ver_re)\`\).*/\1/p" "$doc" | head -n1)"
if [ -z "$doc_semver" ]; then
  doc_semver="$(grep -oE "\(\`\^$ver_re\`\)" "$doc" | head -n1 | grep -oE "$ver_re" || true)"
fi
# The chart pin is the --version of the `helm install roundtable` command. The NATS
# install above it also has a --version (its chart, not ours), so start at the command.
helm_pin="$(awk '/^helm install roundtable[[:space:]]/{f=1} f && /--version/{print; exit}' "$doc" |
  sed -nE "s/.*--version[[:space:]]+($ver_re).*/\1/p")"
chart_ver="$(sed -nE "s/^version:[[:space:]]*\"?($ver_re)\"?.*/\1/p" "$chart" | head -n1)"

[ -n "$semver_pin" ] || fail "no semver pin found in $flux"
[ -n "$doc_semver" ] || fail "no semver constraint found in $doc"
[ -n "$helm_pin" ] || fail "no --version pin found in $doc"
[ -n "$chart_ver" ] || fail "no version found in $chart"

minor() { echo "${1%.*}"; }
for p in "$doc_semver" "$helm_pin"; do
  [ "$(minor "$p")" = "$(minor "$semver_pin")" ] ||
    fail "pins name different minor lines: flux=$semver_pin docs-semver=$doc_semver helm=$helm_pin"
done

# The comment above the semver pin spells the range out (`^0.23.0 means
# >=0.23.0 <0.24.0`); the caret version must be the pin and the range must be
# what a caret on that version means: next major for ^1.x, next minor for ^0.x,
# next patch for ^0.0.x.
range_comment() { sed -nE "s/.*\`?\^($ver_re) means (>=$ver_re <$ver_re).*/\1 \2/p" "$1" | head -n1; }
want_range() {
  local maj min pat up
  IFS=. read -r maj min pat <<<"$1"
  if [ "$maj" -gt 0 ]; then up="$((maj + 1)).0.0"
  elif [ "$min" -gt 0 ]; then up="0.$((min + 1)).0"
  else up="0.0.$((pat + 1))"; fi
  echo "$1 >=$1 <$up"
}
# Negative tests: a stale range, a caret version that is not the pin, and the
# non-0.x bounds must all be told apart.
neg="$(mktemp)"; trap 'rm -f "$neg"' EXIT
printf '%s\n' '# For 0.x versions ^0.23.0 means >=0.22.0 <0.24.0, so' >"$neg"
[ "$(range_comment "$neg")" != "$(want_range 0.23.0)" ] || fail "negative test: stale range comment accepted"
printf '%s\n' '# For 0.x versions ^0.22.0 means >=0.22.0 <0.23.0, so' >"$neg"
[ "$(range_comment "$neg")" != "$(want_range 0.23.0)" ] || fail "negative test: caret version differing from the pin accepted"
printf '%s\n' '# ^1.2.3 means >=1.2.3 <1.3.0' >"$neg"
[ "$(range_comment "$neg")" != "$(want_range 1.2.3)" ] || fail "negative test: ^1.x minor-bounded range accepted"
[ "$(want_range 0.23.0)" = "0.23.0 >=0.23.0 <0.24.0" ] || fail "negative test: want_range wrong for ^0.x"
[ "$(want_range 1.2.3)" = "1.2.3 >=1.2.3 <2.0.0" ] || fail "negative test: want_range wrong for ^1.x"
[ "$(want_range 0.0.4)" = "0.0.4 >=0.0.4 <0.0.5" ] || fail "negative test: want_range wrong for ^0.0.x"
got_range="$(range_comment "$flux")"
[ -n "$got_range" ] || fail "no '^X.Y.Z means >=X.Y.Z <X.Y.Z' range comment found in $flux"
[ "${got_range%% *}" = "$semver_pin" ] ||
  fail "range comment in $flux names ^${got_range%% *} but the semver pin is ^$semver_pin"
[ "$got_range" = "$(want_range "$semver_pin")" ] ||
  fail "range comment in $flux says '${got_range#* }' but semver ^$semver_pin means '$(want_range "$semver_pin" | cut -d' ' -f2-)'"

highest="$(printf '%s\n%s\n%s\n' "$semver_pin" "$doc_semver" "$helm_pin" | sort -V | tail -n1)"
[ "$(printf '%s\n%s\n' "$highest" "$chart_ver" | sort -V | tail -n1)" = "$chart_ver" ] ||
  fail "pinned version $highest is higher than chart version $chart_ver"

if [ -n "$(git -C "$root" tag --list)" ]; then
  for p in "$semver_pin" "$doc_semver" "$helm_pin"; do
    git -C "$root" rev-parse -q --verify "refs/tags/v$p" >/dev/null ||
      fail "no tag v$p exists (pin names an unreleased version)"
  done
fi
echo "install version pins OK ($helm_pin)"
