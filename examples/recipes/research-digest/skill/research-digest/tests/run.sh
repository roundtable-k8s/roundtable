#!/usr/bin/env bash
# Tests for scripts/seen. Run: tests/run.sh
set -uo pipefail
here=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
failures=0
check() { if [ "$2" = "$3" ]; then :; else echo "FAIL: $1 (want '$2', got '$3')"; failures=$((failures+1)); fi; }

export KNIGHT_MEMORY_DIR=$tmp/mem
input=$'https://a.example/1\nhttps://a.example/2\n\nhttps://a.example/1\n  https://a.example/3  \n'

out=$(printf '%s' "$input" | "$here/scripts/seen"); rc=$?
check "first exit" 0 "$rc"
check "first run prints each item once" $'https://a.example/1\nhttps://a.example/2\nhttps://a.example/3' "$out"

out=$(printf '%s' "$input" | "$here/scripts/seen"); rc=$?
check "second exit" 0 "$rc"
check "second run prints nothing (idempotent)" "" "$out"

out=$(printf 'https://a.example/2\nhttps://a.example/4\n' | "$here/scripts/seen")
check "only the new item" "https://a.example/4" "$out"
check "seen.txt has 4 lines" 4 "$(wc -l <"$KNIGHT_MEMORY_DIR/seen.txt" | tr -d ' ')"
check "no temp files left" 1 "$(find "$KNIGHT_MEMORY_DIR" -type f | wc -l | tr -d ' ')"

# A hand-edited seen.txt without a trailing newline must not merge URLs.
printf 'https://a.example/9' >"$KNIGHT_MEMORY_DIR/seen.txt"
out=$(printf 'https://a.example/10\n' | "$here/scripts/seen")
check "no-newline: new item printed" "https://a.example/10" "$out"
check "no-newline: lines kept apart" $'https://a.example/9\nhttps://a.example/10' "$(cat "$KNIGHT_MEMORY_DIR/seen.txt")"

out=$(printf 'https://a.example/1\n' | KNIGHT_MEMORY_DIR= "$here/scripts/seen" 2>&1); rc=$?
[ "$rc" -ne 0 ] || { echo "FAIL: unset KNIGHT_MEMORY_DIR exited 0"; failures=$((failures+1)); }

if [ "$failures" -eq 0 ]; then echo "tests: ok"; else echo "tests: $failures failed"; exit 1; fi
