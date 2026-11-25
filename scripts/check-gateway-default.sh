#!/usr/bin/env bash
# Renders the chart and checks the gateway is on by default with a generated,
# kept token Secret wired into the dashboard and the webhook endpoint with
# get-only Secret access, and that a user-supplied Secret or
# gateway.enabled=false suppress the generated one.
#
# Usage: HELM=bin/helm scripts/check-gateway-default.sh
set -euo pipefail

HELM="${HELM:-helm}"
chart="$(cd "$(dirname "$0")/.." && pwd)/charts/roundtable-operator"
render() { "$HELM" template rt "$chart" "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
has() { grep -q -- "$2" <<<"$1" || fail "$3"; }
lacks() { ! grep -q -- "$2" <<<"$1" || fail "$3"; }

def="$(render)"
has "$def" 'name: rt-roundtable-operator-gateway$' "gateway not deployed by default"
has "$def" 'name: rt-roundtable-operator-gateway-token' "generated token Secret missing"
has "$def" 'helm.sh/resource-policy: keep' "token Secret not kept"
has "$def" 'name: GATEWAY_TOKEN_FILE' "dashboard not given the token file"
lacks "$def" 'name: GATEWAY_TOKEN$' "token passed as env var"
has "$def" 'name: GATEWAY_URL' "dashboard not given the gateway URL"

own="$(render --set gateway.auth.existingSecret=mine)"
lacks "$own" 'rt-roundtable-operator-gateway-token' "generated Secret rendered despite existingSecret"
[ "$(grep -c 'secretName: mine' <<<"$own")" = 2 ] || fail "gateway and dashboard must both mount existingSecret"

dash="$(render --set dashboard.gateway.tokenSecret=dash --set dashboard.gateway.tokenKey=k)"
has "$dash" 'secretName: dash' "dashboard.gateway.tokenSecret ignored"
has "$dash" '- key: k$' "dashboard.gateway.tokenKey ignored"

has "$def" 'name: rt-roundtable-operator-gateway-hook-secrets' "webhook secret Role missing by default"
has "$def" '--hooks=true' "webhook endpoint not enabled by default"
hook_role="$(awk 'BEGIN{RS="\n---\n"} /kind: Role\n/ && /name: rt-roundtable-operator-gateway-hook-secrets/' <<<"$def")"
has "$hook_role" 'resources: \["secrets"\]' "webhook secret Role does not cover Secrets"
[ "$(grep 'verbs:' <<<"$hook_role")" = '    verbs: ["get"]' ] || fail "webhook secret Role must grant get only"
nohooks="$(render --set gateway.hooks.enabled=false)"
lacks "$nohooks" 'gateway-hook-secrets' "webhook secret Role rendered with hooks disabled"
has "$nohooks" '--hooks=false' "hooks flag not passed when disabled"

# ServiceAccount auth: on by default with a TokenReview-only ClusterRole.
has "$def" '--service-account-auth=true' "ServiceAccount auth not enabled by default"
has "$def" '--service-account-audience=roundtable-gateway' "default ServiceAccount audience not passed"
has "$def" '--service-account-namespaces=\*' "ServiceAccounts not allowed from any namespace by default"
tr_role="$(awk 'BEGIN{RS="\n---\n"} /kind: ClusterRole\n/ && /name: rt-roundtable-operator-gateway-sa-auth/' <<<"$def")"
has "$tr_role" 'resources: \["tokenreviews"\]' "TokenReview ClusterRole missing by default"
[ "$(grep 'verbs:' <<<"$tr_role")" = '    verbs: ["create"]' ] || fail "TokenReview ClusterRole must grant create only"
# ClusterRole name, ClusterRoleBinding name and its roleRef.
[ "$(grep -c 'name: rt-roundtable-operator-gateway-sa-auth$' <<<"$def")" = 3 ] || fail "TokenReview ClusterRoleBinding missing"
sa="$(render --set gateway.serviceAccountAuth.audience=other --set 'gateway.serviceAccountAuth.allowedNamespaces={jobs,ops}')"
has "$sa" '--service-account-audience=other' "serviceAccountAuth.audience ignored"
has "$sa" '--service-account-namespaces=jobs,ops' "serviceAccountAuth.allowedNamespaces ignored"
nosa="$(render --set gateway.serviceAccountAuth.enabled=false)"
lacks "$nosa" 'gateway-sa-auth' "TokenReview ClusterRole rendered with ServiceAccount auth disabled"
lacks "$nosa" 'tokenreviews' "tokenreviews granted with ServiceAccount auth disabled"
has "$nosa" '--service-account-auth=false' "ServiceAccount auth flag not passed when disabled"

off="$(render --set gateway.enabled=false)"
# The operator still records knight-target Trigger tasks in the gateway's task
# bucket (--trigger-task-bucket), whose default name says gateway.
has "$off" '--trigger-task-bucket=roundtable_gateway_tasks' "operator not given the task bucket with the gateway disabled"
lacks "$(grep -v -- '--trigger-task-' <<<"$off")" 'gateway' "gateway resources rendered when disabled"
echo "gateway defaults OK"
