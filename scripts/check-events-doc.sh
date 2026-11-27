#!/usr/bin/env bash
# Checks docs/events.md against the Go code: every Warning event reason the
# operator emits (an EventTypeWarning call in internal/, Event, Eventf or the
# controllers' event helper) is in a `Reason` cell of the table in
# docs/events.md, and the table lists no reason the code does not emit.
#
# A call whose reason is a quoted string is read directly. A call whose reason
# is a variable or constant must carry a marker comment on the line directly
# above it, naming every reason it can emit:
#   // event-reasons: ReasonA ReasonB
# A call with neither fails the check.
#
# Usage: scripts/check-events-doc.sh
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
doc="$root/docs/events.md"
fail() { echo "FAIL: $*" >&2; exit 1; }

# extract <file>...: one reason per line (unsorted); "UNMARKED file:line" for a
# Warning call whose reason is not a quoted string and has no marker.
extract() {
  awk '
    FNR == 1 { m1 = "" }
    {
      if ($0 ~ /EventTypeWarning/ && $0 !~ /^[ \t]*\/\//) {
        if (match($0, /EventTypeWarning,[ \t]*"[A-Za-z0-9]+"/)) {
          t = substr($0, RSTART, RLENGTH); sub(/^[^"]*"/, "", t); sub(/".*/, "", t); print t
        } else if (m1 ~ /event-reasons:/) {
          t = m1; sub(/.*event-reasons:[ \t]*/, "", t)
          n = split(t, a, /[ \t]+/); for (i = 1; i <= n; i++) if (a[i] ~ /^[A-Za-z0-9]+$/) print a[i]
        } else {
          print "UNMARKED " FILENAME ":" FNR
        }
      }
      m1 = ($0 ~ /event-reasons:/) ? $0 : ""
    }' "$@"
}

# Negative test: the three cases, on a scratch file.
neg="$(mktemp)"; trap 'rm -f "$neg"' EXIT
printf '%s\n' \
  'r.Recorder.Eventf(a, corev1.EventTypeWarning, "Literal", "x")' \
  '// event-reasons: MarkA MarkB' \
  'r.Recorder.Event(a, corev1.EventTypeWarning, reason, msg)' \
  'r.Recorder.Event(a, corev1.EventTypeWarning, reason, msg)' >"$neg"
[ "$(extract "$neg" | sed "s#$neg#F#" | paste -sd'|' -)" = "Literal|MarkA|MarkB|UNMARKED F:4" ] \
  || fail "negative test: extraction mishandled"

mapfile -t files < <(find "$root/internal" -name '*.go' ! -name '*_test.go' | sort)
out="$(extract "${files[@]}")"
unmarked="$(grep '^UNMARKED ' <<<"$out" | sed "s#UNMARKED $root/#  #" || true)"
[ -z "$unmarked" ] || fail "Warning event with a non-literal reason and no '// event-reasons:' marker:
$unmarked"
code="$(sort -u <<<"$out" | sed '/^$/d')"
[ -n "$code" ] || fail "no Warning event reasons found in the Go sources"

[ -f "$doc" ] || fail "$doc does not exist"
docrows="$(awk -F'|' '/^\| `[A-Za-z0-9]+` \|/ { r = $2; gsub(/[ `]/, "", r); print r }' "$doc" | sort)"
[ -n "$docrows" ] || fail "no reason rows in $doc"
dups="$(uniq -d <<<"$docrows")"
[ -z "$dups" ] || fail "reasons listed more than once in docs/events.md: $(paste -sd' ' - <<<"$dups")"

missing="$(comm -23 <(echo "$code") <(echo "$docrows") | paste -sd' ' -)"
stale="$(comm -13 <(echo "$code") <(echo "$docrows") | paste -sd' ' -)"
[ -z "$missing" ] || fail "Warning reasons emitted by the code but missing from docs/events.md: $missing"
[ -z "$stale" ] || fail "reasons in docs/events.md that no code emits: $stale"

echo "events doc OK ($(wc -l <<<"$code" | tr -d ' ') Warning reasons)"
