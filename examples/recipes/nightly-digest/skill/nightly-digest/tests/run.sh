#!/usr/bin/env bash
# Tests for scripts/file and scripts/collect. Run: tests/run.sh
set -uo pipefail
here=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
failures=0
check() { if [ "$2" = "$3" ]; then :; else echo "FAIL: $1 (want '$2', got '$3')"; failures=$((failures+1)); fi; }

export DIGEST_DIR=$tmp/out
valid='{"namespace":"web","date":"2026-11-25","summary":"Quiet night.","changes":[{"kind":"Deployment","name":"web","detail":"rolled out"}],"concerns":[]}'

# file: valid input -> exit 0, last line is the ok line, outputs written.
echo "$valid" >"$tmp/valid.json"
out=$("$here/scripts/file" "$tmp/valid.json" 2>&1); rc=$?
check "valid exit" 0 "$rc"
check "valid last line" "nightly-digest: ok" "$(echo "$out" | tail -n1)"
[ -f "$DIGEST_DIR/web-2026-11-25.json" ] && [ -f "$DIGEST_DIR/web-2026-11-25.md" ] || { echo "FAIL: outputs missing"; failures=$((failures+1)); }

# file: invalid inputs -> non-zero, no ok line, nothing written.
rm -rf "$DIGEST_DIR"
echo 'not json' >"$tmp/bad1.json"
echo '{"namespace":"web","date":"yesterday","summary":"x","changes":[],"concerns":[]}' >"$tmp/bad2.json"
echo '{"namespace":"web","date":"2026-11-25","summary":"","changes":[],"concerns":[]}' >"$tmp/bad3.json"
echo '{"namespace":"../x","date":"2026-11-25","summary":"x","changes":[],"concerns":[]}' >"$tmp/bad4.json"
echo '{"namespace":"web","date":"2026-11-25","summary":"x","changes":[{"kind":"Pod"}],"concerns":[]}' >"$tmp/bad5.json"
for f in bad1 bad2 bad3 bad4 bad5 missing; do
  out=$("$here/scripts/file" "$tmp/$f.json" 2>&1); rc=$?
  [ "$rc" -ne 0 ] || { echo "FAIL: $f exited 0"; failures=$((failures+1)); }
  echo "$out" | grep -q 'nightly-digest: ok' && { echo "FAIL: $f printed ok"; failures=$((failures+1)); }
done
[ ! -e "$DIGEST_DIR" ] || { echo "FAIL: invalid input wrote output"; failures=$((failures+1)); }
"$here/scripts/file" >/dev/null 2>&1; check "no args exit" 2 "$?"

# collect: bad arguments are refused before kubectl runs; with a fake kubectl
# the output is the JSON the skill describes.
"$here/scripts/collect" 'Bad_NS' >/dev/null 2>&1; check "bad namespace exit" 2 "$?"
"$here/scripts/collect" web x >/dev/null 2>&1; check "bad hours exit" 2 "$?"
mkdir -p "$tmp/bin"
cat >"$tmp/bin/kubectl" <<'FAKE'
#!/usr/bin/env bash
case "$*" in
  *deployments*) echo '{"items":[{"kind":"Deployment","metadata":{"name":"web","creationTimestamp":"2026-11-24T10:00:00Z"},"spec":{"replicas":2},"status":{"readyReplicas":1}}]}' ;;
  *pods*) echo '{"items":[{"metadata":{"name":"web-1"},"status":{"phase":"Running","containerStatuses":[{"restartCount":3,"ready":true}]}},{"metadata":{"name":"web-2"},"status":{"phase":"Running","containerStatuses":[{"restartCount":0,"ready":true}]}}]}' ;;
  *events*) echo '{"items":[{"involvedObject":{"kind":"Pod","name":"web-1"},"reason":"BackOff","message":"x","count":2,"lastTimestamp":"2999-01-01T00:00:00Z"},{"involvedObject":{"kind":"Pod","name":"old"},"reason":"Old","message":"y","lastTimestamp":"2000-01-01T00:00:00Z"}]}' ;;
  *) exit 1 ;;
esac
FAKE
chmod +x "$tmp/bin/kubectl"
res=$(PATH="$tmp/bin:$PATH" "$here/scripts/collect" web 24 2>&1); rc=$?
check "collect exit" 0 "$rc"
check "collect workloads" 1 "$(echo "$res" | jq '.workloads | length')"
check "collect unhealthy pods" web-1 "$(echo "$res" | jq -r '.unhealthyPods[].name')"
check "collect warnings in window" 1 "$(echo "$res" | jq '.warnings | length')"

if [ "$failures" -ne 0 ]; then echo "tests: $failures failure(s)"; exit 1; fi
echo "tests: ok"
