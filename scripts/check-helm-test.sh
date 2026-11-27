#!/usr/bin/env bash
# Renders the chart and checks the `helm test` Pod (right URLs, one Pod, none
# with both components off) and that NOTES.txt names the real Service and Secret.
#
# Usage: HELM=bin/helm scripts/check-helm-test.sh
set -euo pipefail

HELM="${HELM:-helm}"
chart="$(cd "$(dirname "$0")/.." && pwd)/charts/roundtable-operator"
render() { "$HELM" template "$@" "$chart" --namespace roundtable; }
fail() { echo "FAIL: $*" >&2; exit 1; }
has() { grep -q -- "$2" <<<"$1" || fail "$3"; }
lacks() { ! grep -q -- "$2" <<<"$1" || fail "$3"; }
count() { grep -c 'helm.sh/hook: test' <<<"$1" || true; }

def="$(render rt)"
[ "$(count "$def")" = 1 ] || fail "expected exactly one test hook Pod with default values"
has "$def" 'http://rt-roundtable-operator-gateway\.roundtable\.svc:80/readyz' "gateway URL wrong"
has "$def" 'http://rt-roundtable-operator-dashboard\.roundtable\.svc:80/api/health' "dashboard URL wrong"
has "$def" 'image: "busybox:1.37.0"' "test image not from tests.image"
lacks "$def" 'busybox:latest' "test image must be pinned"

ov="$(render x --set fullnameOverride=y)"
[ "$(count "$ov")" = 1 ] || fail "expected one test Pod with fullnameOverride"
has "$ov" 'http://y-gateway\.roundtable\.svc:80/readyz' "fullnameOverride not used in gateway URL"
has "$ov" 'http://y-dashboard\.roundtable\.svc:80/api/health' "fullnameOverride not used in dashboard URL"

gw="$(render rt --set dashboard.enabled=false)"
has "$gw" '/readyz' "gateway check missing"
lacks "$gw" '/api/health' "dashboard check rendered while disabled"

off="$(render rt --set gateway.enabled=false --set dashboard.enabled=false)"
[ "$(count "$off")" = 0 ] || fail "test Pod rendered with both components off"

# `helm template` drops NOTES.txt (and --show-only cannot select it), so render
# it from a temporary chart copy where it is wrapped in a ConfigMap.
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
cp -r "$chart" "$tmp/chart"
{ echo '{{ define "notes" }}'; cat "$tmp/chart/templates/NOTES.txt"; echo '{{ end }}'; } > "$tmp/chart/templates/_notes.tpl"
rm "$tmp/chart/templates/NOTES.txt"
cat > "$tmp/chart/templates/notes-cm.yaml" <<'CM'
apiVersion: v1
kind: ConfigMap
metadata:
  name: notes
data:
  notes: |
{{ include "notes" . | indent 4 }}
CM
notes() { "$HELM" template "$@" "$tmp/chart" --namespace roundtable --show-only templates/notes-cm.yaml; }
n="$(notes roundtable-operator)"
has "$n" 'svc/roundtable-operator-gateway 8081:80' "NOTES lacks gateway Service"
has "$n" 'secret roundtable-operator-gateway-token ' "NOTES lacks token Secret"
has "$n" 'svc/roundtable-operator-dashboard 8080:80' "NOTES lacks dashboard Service"
n="$(notes x --set fullnameOverride=y)"
has "$n" 'svc/y-gateway ' "NOTES ignores fullnameOverride (gateway)"
has "$n" 'secret y-gateway-token ' "NOTES ignores fullnameOverride (secret)"
n="$(notes x --set gateway.auth.existingSecret=mine --set dashboard.ingress.enabled=true --set dashboard.ingress.host=rt.example.com)"
has "$n" 'secret mine ' "NOTES ignores existingSecret"
has "$n" 'http://rt.example.com' "NOTES lacks ingress host"
echo "ok"
