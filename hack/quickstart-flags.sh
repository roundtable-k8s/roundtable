# Chart flags of hack/quickstart-kind.sh, shared with
# scripts/check-quickstart-values.sh. Source it with NATS_URL set to the NATS
# the quickstart installs (nats://nats.<namespace>.svc:4222).
QUICKSTART_CHART_FLAGS=(
  --set-string isolation.runtimeClassName=""
  --set isolation.egress.mode=off
  --set-string missionDefaultNatsURL="$NATS_URL"
  --set-string dashboard.env.NATS_URL="$NATS_URL"
  --set-string gateway.natsURL="$NATS_URL"
  --set dashboard.vault.enabled=false
)
