#!/usr/bin/env bash
# Checks the operator image tag defaults to the chart appVersion (never latest),
# and that image.tag still overrides it.
#
# Usage: HELM=bin/helm scripts/check-image-tag.sh
set -euo pipefail

HELM="${HELM:-helm}"
chart="$(cd "$(dirname "$0")/.." && pwd)/charts/roundtable-operator"
fail() { echo "FAIL: $*" >&2; exit 1; }
has() { grep -q -- "$2" <<<"$1" || fail "$3"; }
lacks() { ! grep -q -- "$2" <<<"$1" || fail "$3"; }

tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
cp -r "$chart" "$tmp/chart"
sed -i 's/^appVersion:.*/appVersion: "v9.9.9"/' "$tmp/chart/Chart.yaml"

def="$("$HELM" template rt "$tmp/chart")"
has "$def" 'image: "ghcr.io/roundtable-k8s/roundtable:v9.9.9"' "operator image not at appVersion"
has "$def" '--trigger-image=ghcr.io/roundtable-k8s/roundtable:v9.9.9' "trigger image not at appVersion"
lacks "$def" 'roundtable:latest' "a latest tag is rendered"

over="$("$HELM" template rt "$tmp/chart" --set image.tag=custom)"
has "$over" 'roundtable:custom"' "image.tag override ignored"
echo "ok"
