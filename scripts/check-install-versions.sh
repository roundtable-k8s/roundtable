#!/usr/bin/env bash
# Checks the release pins in the install docs: the three pins must name the
# same minor line, must not be newer than the chart version, and (when the
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
