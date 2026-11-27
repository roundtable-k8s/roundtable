#!/usr/bin/env bash
# Renders the chart and checks the gateway is on by default with a generated,
# kept token Secret wired into the dashboard and the webhook endpoint with
# get-only Secret access and the schedule endpoints with get, list, watch,
# create, update, patch and delete on Triggers, no write verb on Knights unless
# gateway.knightSnapshots.enabled=true, and that a user-supplied Secret or
# gateway.enabled=false suppress the generated one.
#
# Usage: HELM=bin/helm scripts/check-gateway-default.sh
set -euo pipefail

HELM="${HELM:-helm}"
chart="$(cd "$(dirname "$0")/.." && pwd)/charts/roundtable-operator"
# Pin the release namespace: helm otherwise takes it from the kubeconfig (or
# "default" in CI), and the Service URL assertions below name it.
render() { "$HELM" template rt "$chart" --namespace roundtable "$@"; }
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

# Dashboard ServiceAccount token: opt-in; default keeps the Secret volume.
lacks "$def" 'serviceAccountToken' "projected token rendered by default"
dsa="$(render --set dashboard.gateway.serviceAccountToken=true)"
has "$dsa" 'audience: "roundtable-gateway"$' "dashboard projected token lacks the gateway audience"
has "$dsa" 'expirationSeconds: 3600' "dashboard projected token expiration not 3600"
has "$dsa" 'name: GATEWAY_TOKEN_FILE' "GATEWAY_TOKEN_FILE dropped with the projected token"
[ "$(grep -c 'secretName: rt-roundtable-operator-gateway-token' <<<"$dsa")" = 1 ] || fail "dashboard still mounts the token Secret (only the gateway should)"
has "$(render --set dashboard.gateway.serviceAccountToken=true --set gateway.serviceAccountAuth.audience=other)" 'audience: "other"$' "projected token audience ignores serviceAccountAuth.audience"
has "$(render --set dashboard.gateway.serviceAccountToken=true --set gateway.serviceAccountAuth.audience=)" 'audience: "roundtable-gateway"$' "empty audience must fall back to roundtable-gateway"
if err="$(render --set dashboard.gateway.serviceAccountToken=true --set gateway.serviceAccountAuth.enabled=false 2>&1)"; then
  fail "render succeeded with serviceAccountToken and serviceAccountAuth disabled"
fi
has "$err" 'requires gateway.serviceAccountAuth.enabled=true' "missing fail message"

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
has "$def" '--schedules=true' "schedule endpoints not enabled by default"
sched_role="$(awk 'BEGIN{RS="\n---\n"} /kind: Role\n/ && /name: rt-roundtable-operator-gateway-schedules\n/' <<<"$def")"
has "$sched_role" 'resources: \["triggers"\]' "schedule Role missing or not on Triggers"
for verb in get list watch create update patch delete; do
  has "$sched_role" "\"$verb\"" "schedule Role lacks $verb on Triggers"
done
nosched="$(render --set gateway.schedules.enabled=false)"
lacks "$nosched" 'gateway-schedules' "schedule Role rendered with schedules disabled"
has "$nosched" '--schedules=false' "schedules flag not passed when disabled"

# Schedule run history: on by default, results kept 30 days by both the
# gateway and the operator, the retention value configurable.
has "$def" '--schedule-runs=true' "schedule run history not enabled by default"
[ "$(grep -c -- '--schedule-result-retention=720h' <<<"$def")" = 2 ] || fail "gateway and operator must both default the result retention to 720h"
ret="$(render --set gateway.scheduleRuns.resultRetention=48h)"
[ "$(grep -c -- '--schedule-result-retention=48h' <<<"$ret")" = 2 ] || fail "gateway.scheduleRuns.resultRetention ignored"
has "$(render --set gateway.scheduleRuns.enabled=false)" '--schedule-runs=false' "schedule-runs flag not passed when disabled"
# Scheduled Triggers: the firing pod calls the chart's gateway Service with
# the gateway's ServiceAccount audience, or triggers.gatewayURL.
has "$def" '--trigger-gateway-url=http://rt-roundtable-operator-gateway.roundtable.svc:80$' "trigger gateway URL not the gateway Service"
has "$def" '--trigger-gateway-audience=roundtable-gateway$' "trigger gateway audience not passed"
has "$def" '--trigger-image=' "trigger image not passed"
has "$sa" '--trigger-gateway-audience=other$' "trigger audience does not follow serviceAccountAuth.audience"
ext="$(render --set gateway.enabled=false --set triggers.gatewayURL=https://gw.example)"
has "$ext" '--trigger-gateway-url=https://gw.example$' "triggers.gatewayURL ignored"

# Knight snapshots: off by default, so the gateway has no write verb on
# Knights; opting in renders the -snapshots Role with get and patch.
lacks "$def" 'name: rt-roundtable-operator-gateway-snapshots' "snapshot Role rendered by default"
has "$def" '--knight-snapshots=false' "knight snapshots not disabled by default"
# knight_writes prints "<role>: <verbs>" for each rule, in a Role or
# ClusterRole bound to the gateway ServiceAccount, that covers knights.
knight_writes() {
  local roles
  roles="$(awk 'BEGIN{RS="\n---\n"} /\nkind: (Cluster)?RoleBinding\n/ && /\n    name: rt-roundtable-operator-gateway\n    namespace: roundtable($|\n)/' <<<"$1" |
    awk '/^roleRef:/{r=1} r && /^  name:/{print $2; r=0}' | sort -u)"
  [ -n "$roles" ] || fail "no Role bound to the gateway ServiceAccount found"
  awk -v roles=" $(tr '\n' ' ' <<<"$roles") " 'BEGIN{RS="\n---\n"}
    /\nkind: (Cluster)?Role\n/ {
      if (!match($0, /\n  name: [^\n]*/)) next
      name = substr($0, RSTART + 9, RLENGTH - 9)
      if (index(roles, " " name " ") == 0) next
      n = split($0, rules, "\n  - ")
      for (i = 2; i <= n; i++) if (rules[i] ~ /resources: \[[^]]*"knights"/ && match(rules[i], /verbs: \[[^]]*\]/))
        print name ": " substr(rules[i], RSTART, RLENGTH)
    }' <<<"$1"
}
def_knights="$(knight_writes "$def")"
has "$def_knights" '"get"' "gateway lost read access to knights"
lacks "$def_knights" '"\(create\|update\|patch\|delete\|deletecollection\|\*\)"' "default gateway Roles grant a write verb on knights: $def_knights"
snap="$(render --set gateway.knightSnapshots.enabled=true)"
has "$snap" '--knight-snapshots=true' "knight snapshots flag not passed when enabled"
snap_role="$(awk 'BEGIN{RS="\n---\n"} /kind: Role\n/ && /name: rt-roundtable-operator-gateway-snapshots\n/' <<<"$snap")"
has "$snap_role" 'resources: \["knights"\]' "snapshot Role missing or not on Knights"
[ "$(grep 'verbs:' <<<"$snap_role")" = '    verbs: ["get","patch"]' ] || fail "snapshot Role must grant get and patch only"
has "$(knight_writes "$snap")" 'gateway-snapshots: verbs: \["get","patch"\]' "snapshot Role not bound to the gateway"
snapall="$(render --set gateway.knightSnapshots.enabled=true --set 'gateway.allowedNamespaces={*}')"
has "$(knight_writes "$snapall")" 'gateway-snapshots: verbs: \["get","patch"\]' "snapshot ClusterRole missing with allowedNamespaces=*"
lacks "$(knight_writes "$(render --set 'gateway.allowedNamespaces={*}')")" '"patch"' "patch on knights granted by default with allowedNamespaces=*"
off="$(render --set gateway.enabled=false)"
lacks "$off" '--trigger-gateway-url' "trigger gateway URL passed with the gateway disabled"
# The operator still records knight-target Trigger tasks in the gateway's task
# bucket (--trigger-task-bucket), whose default name says gateway.
has "$off" '--trigger-task-bucket=roundtable_gateway_tasks' "operator not given the task bucket with the gateway disabled"
lacks "$(grep -v -- '--trigger-task-' <<<"$off")" 'gateway' "gateway resources rendered when disabled"
echo "gateway defaults OK"
