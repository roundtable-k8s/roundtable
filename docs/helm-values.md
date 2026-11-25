# Helm Chart Configuration

## Installation

```bash
# From OCI registry
helm install roundtable oci://ghcr.io/roundtable-k8s/charts/roundtable-operator \
  --namespace roundtable --create-namespace

# From local chart
helm install roundtable charts/roundtable-operator \
  --namespace roundtable --create-namespace
```

## Values Reference

### Operator

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `replicaCount` | int | `1` | Operator replicas (leader election handles HA) |
| `image.repository` | string | `ghcr.io/roundtable-k8s/roundtable` | Operator image |
| `image.tag` | string | `latest` | Operator image tag |
| `image.pullPolicy` | string | `Always` | Image pull policy |
| `leaderElect` | bool | `true` | Enable leader election for HA |
| `resources.requests.cpu` | string | `50m` | CPU request |
| `resources.requests.memory` | string | `64Mi` | Memory request |
| `resources.limits.cpu` | string | `200m` | CPU limit |
| `resources.limits.memory` | string | `128Mi` | Memory limit |
| `sandboxBackend` | string | `auto` | agent-sandbox runtime backend: `auto` (enable only if `agents.x-k8s.io/v1beta1` Sandbox is served), `true`, or `false`. Supports agent-sandbox v1.0.0 or later (developed against v1.0.12) |
| `keda` | string | `auto` | KEDA autoscaling for knights with `maxReplicas > 1` or `lifecycle.suspendPolicy: auto`: `auto` (enable only if `keda.sh/v1alpha1` ScaledObject is served), `true`, or `false` |
| `natsMonitoringEndpoint` | string | `""` | NATS HTTP monitoring endpoint (`host:port`) for KEDA's JetStream scaler; empty derives `<spec.nats.url host>:8222` per knight |
| `extraArgs` | list | `[]` | Additional args for the manager binary (e.g. `--github-api-url=https://ghe.example.com/api/v3` for [issue pickup](issue-pickup.md)) |

### Mission NetworkPolicy

The egress NetworkPolicy the operator creates for each mission's knights. The NATS rule is derived from the mission's NATS URL (port, and namespace for `<svc>.<namespace>.svc[.cluster.local]` hosts; any other host allows the port to any destination). TCP 443 is allowed to anywhere. The policy is created once and never updated. Rendered into the operator's `MISSION_NETPOL_*` env vars.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `missionNetworkPolicy.enabled` | bool | `true` | Create the policy (`MISSION_NETPOL_ENABLED`); disable on clusters without NetworkPolicy support or with their own policies |
| `missionNetworkPolicy.nats.podLabels` | map | `{app.kubernetes.io/name: nats}` | Pod labels narrowing the NATS peer (`MISSION_NETPOL_NATS_POD_LABELS`, `k=v,k=v`); empty means the whole namespace |
| `missionNetworkPolicy.dns.namespace` | string | `kube-system` | Namespace of cluster DNS (`MISSION_NETPOL_DNS_NAMESPACE`) |
| `missionNetworkPolicy.dns.podLabels` | map | `{k8s-app: kube-dns}` | Pod labels of cluster DNS (`MISSION_NETPOL_DNS_POD_LABELS`) |

### Ingress NetworkPolicy

Opt-in ingress NetworkPolicies for the operator, the dashboard and knight pods. Off by default: the defaults render no extra policy and the pods accept any ingress, as before. Each policy selects its pods in the release namespace and sets `policyTypes: [Ingress]`, so egress is not restricted. A policy whose peer list is empty allows no ingress at all.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `networkPolicy.enabled` | bool | `false` | Render the operator, dashboard and knight policies |
| `networkPolicy.metricsFrom` | list | monitoring namespace | NetworkPolicyPeers allowed to scrape metrics: the operator's metrics port and knights' `:3000/metrics`. The dashboard also accepts these peers. Empty denies all scraping |
| `networkPolicy.dashboardFrom` | list | `[]` | NetworkPolicyPeers allowed to reach the dashboard's `http` port, in addition to `metricsFrom`. Empty means only `metricsFrom` |
| `networkPolicy.operatorMetricsPort` | int | `8443` | Port of the operator's metrics endpoint; must match `--metrics-bind-address` in `extraArgs` (metrics are off by default) |

What each policy allows:

- **Operator** (`<release>-roundtable-operator-operator`): TCP 8081 (health probes) and `operatorMetricsPort`, from `metricsFrom`.
- **Dashboard** (`<release>-roundtable-operator-dashboard`, only when `dashboard.enabled`): the `http` port, from `dashboardFrom` and `metricsFrom`. Knights are reached through NATS and the Kubernetes API, so nothing else needs to connect to the dashboard.
- **Knights** (`<release>-roundtable-operator-knights`): TCP 3000 from `metricsFrom`. It selects every pod labelled `app.kubernetes.io/name: knight`, which covers Deployment knights and `runtime: sandbox` knights once their pods carry that label (#366).

Limits and caveats:

- Policies only cover pods in the release namespace. Knights in other namespaces are not covered; write their policies yourself.
- Kubelet probes come from the node, not from a pod. With Cilium they are allowed by default. Other CNIs may need an `ipBlock` for the node CIDRs in the operator's policy; the chart does not guess node CIDRs.
- Egress is not restricted here (see #168) and NATS-level authorization is not covered (see #234).
- The gateway has its own NetworkPolicy (`gateway.networkPolicy`), separate from this one.

Example, scraping from a Prometheus namespace and exposing the dashboard to an ingress controller:

```yaml
networkPolicy:
  enabled: true
  metricsFrom:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: monitoring
  dashboardFrom:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: ingress-nginx
```

### Service Account

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `serviceAccount.create` | bool | `true` | Create a ServiceAccount |
| `serviceAccount.name` | string | `roundtable-operator` | ServiceAccount name |
| `serviceAccount.annotations` | object | `{}` | ServiceAccount annotations |

### Global Images

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `images.piKnight.repository` | string | `ghcr.io/roundtable-k8s/pi-knight` | Default pi-knight image for all knights |
| `images.piKnight.tag` | string | `latest` | pi-knight image tag |
| `images.dashboard.repository` | string | `ghcr.io/roundtable-k8s/roundtable-ui` | Dashboard image |
| `images.dashboard.tag` | string | `latest` | Dashboard image tag |

### Dashboard

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `dashboard.enabled` | bool | `true` | Deploy the Round Table dashboard |
| `dashboard.replicas` | int | `1` | Dashboard replicas |
| `dashboard.ingress.enabled` | bool | `false` | Enable ingress for dashboard |
| `dashboard.ingress.className` | string | `""` | Ingress class name |
| `dashboard.ingress.host` | string | `""` | Dashboard hostname |
| `dashboard.env.NATS_URL` | string | `nats://nats.database.svc:4222` | NATS server URL |
| `dashboard.env.NAMESPACE` | string | `roundtable` | Namespace to watch |
| `dashboard.vault.enabled` | bool | `true` | Mount shared Obsidian vault |
| `dashboard.vault.claimName` | string | `roundtable-vault` | Vault PVC name |

### Invoke gateway

`dashboard.gateway.tokenSecret` / `tokenKey` (default empty = the gateway's Secret and key) give the dashboard its own token.

Synchronous HTTP API + MCP endpoint for knights (see [gateway.md](gateway.md)).
On by default. With no `gateway.auth.existingSecret` the chart generates a token Secret
(`<fullname>-gateway-token`, annotated `helm.sh/resource-policy: keep`, reused on upgrade via `lookup`)
and gives the dashboard `GATEWAY_URL` and `GATEWAY_TOKEN_FILE` (the token Secret mounted read-only, so rotation needs no restart).
Upgrade note: installs that never set `gateway.enabled` start a gateway on upgrade. Set `gateway.enabled=false` to turn it off.
Note: `helm template` has no cluster to `lookup`, so it renders a fresh random token each time.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `gateway.enabled` | bool | `true` | Deploy the invoke gateway |
| `gateway.replicas` | int | `1` | Gateway replicas (stateless; task records live in NATS KV) |
| `gateway.image.repository` / `.tag` | string | `""` | Image; defaults to the operator image (`/gateway` binary) |
| `gateway.natsURL` | string | `nats://nats.database.svc:4222` | NATS server URL |
| `gateway.namespace` | string | `""` | Default namespace for knights (empty = release namespace) |
| `gateway.allowedNamespaces` | list | `[]` | Extra namespaces callers may target (`"*"` = any) |
| `gateway.defaultTimeout` | duration | `60s` | Wait when the caller passes no `timeoutSeconds` |
| `gateway.maxTimeout` | duration | `10m` | Upper bound on a caller's wait |
| `gateway.maxInFlight` | int | `64` | Concurrently waiting invocations per replica (429 beyond) |
| `gateway.taskBucket` | string | `roundtable_gateway_tasks` | NATS KV bucket for task records |
| `gateway.taskTTL` | duration | `168h` | How long task records are kept |
| `gateway.auth.existingSecret` | string | `""` | Secret holding the bearer token(s); empty = chart generates one |
| `gateway.auth.secretKey` | string | `token` | Key in that Secret; one token per line (two during a rotation) |
| `gateway.service.type` / `.port` | string / int | `ClusterIP` / `80` | Service |
| `gateway.networkPolicy.enabled` | bool | `true` | Restrict ingress to the gateway |
| `gateway.networkPolicy.from` | list | `[{podSelector: {}}]` | NetworkPolicyPeers allowed in (default: release namespace). An empty list allows any source. |
| `gateway.resources` | object | 20m/32Mi req, 128Mi limit | Resources |
| `gateway.extraArgs` | list | `[]` | Extra gateway flags |
| `gateway.missions.enabled` | bool | `true` | Serve the mission endpoints and grant the gateway create/delete on Missions in its namespaces ([gateway.md](gateway.md#missions)) |
| `gateway.missions.maxBudgetUSD` | number | `10` | Upper bound on a mission's `costBudgetUSD` |
| `gateway.missions.maxTTLSeconds` | int | `86400` | Upper bound on a mission's `ttlSeconds` |
| `gateway.missions.maxTimeoutSeconds` | int | `21600` | Upper bound on a mission's `timeoutSeconds` |
| `gateway.missions.allowSkillGeneration` | bool | `false` | Let callers set `planner.allowSkillGeneration` |

### Knights

Knights with `coder: true` get `codingKnightDefaults.resources` (including
`ephemeral-storage`) and `codingKnightDefaults.workspace` unless they set their
own `resources` / `workspace`. Knights in a RoundTable (label
`ai.roundtable.io/round-table`) also inherit `RoundTable.spec.defaults.resources`
and `spec.defaults.workspace` when unset.

```yaml
codingKnightDefaults:
  resources:
    requests: {cpu: "1", memory: 2Gi, ephemeral-storage: 4Gi}
    limits: {cpu: "4", memory: 8Gi, ephemeral-storage: 20Gi}
  workspace:
    size: 20Gi
```

These match the chart's `values.yaml` defaults. The default applies as a whole
block: if a knight sets its own `resources` (or `workspace`), that block is used
as written and no fields are merged in from the default.

Set `knights.<name>.runtimeClassName` (and `planner.runtimeClassName`; both
default to empty, meaning the cluster's default runtime) to run a knight under a
RuntimeClass such as `gvisor`. Knights in a RoundTable also inherit
`RoundTable.spec.defaults.runtimeClassName` when unset. See
[runtime-isolation.md](runtime-isolation.md).

Define knights inline in values to deploy them with the operator:

```yaml
knights:
  galahad:
    domain: security
    model: claude-sonnet-4-20250514
    skills: [security, threat-intel]
    tools:
      apt: [nmap, whois]
    nats:
      subjects: ["fleet-a.tasks.security.>"]
    taskTimeoutSeconds: 300
    # Platform skills from roundtable-k8s/arsenal, domain packs from your own repo.
    # Merged per skill into /skills; later arsenals win on a clash.
    arsenal: {}
    arsenals:
      - name: personal
        repo: https://github.com/<you>/<your-skills>
    # Mount a refreshing GitHub token file instead of a frozen env token.
    github:
      tokenSecretRef:
        name: roundtable-github-token
        key: token
```

### PR pipeline

The PR pipeline Chain (plan → implement → review → fix loop → merge or
escalate). See [pr-pipeline.md](pr-pipeline.md).

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `prPipeline.enabled` | bool | `false` | Create the Chain |
| `prPipeline.name` | string | `pr-pipeline` | Chain name |
| `prPipeline.roundTableRef.name` | string | `roundtable-dev` | RoundTable; must define the model aliases used (`deep`, `fast`, and `cheap` if listed; for the coders) |
| `prPipeline.lead` | string | `rt-lead` | Knight that plans, reviews, merges and escalates |
| `prPipeline.leadModel` | string | `""` | Model (alias or runtime model ID) for the lead's steps; empty = the lead's default model |
| `prPipeline.coders` | list | `[rt-operator]` | Coder knights the planner may pick; one implement/split step and fix loop each |
| `prPipeline.implementModels` | list | `[deep, fast]` | Model aliases the planner may pick for the implementation (`cheap` = Haiku drafts, `fast`, `deep`) |
| `prPipeline.fixModel` | string | `deep` | Model alias for every fix round, so a cheap draft is fixed by a stronger model |
| `prPipeline.fallbackCoder` | string | `""` | Fallback coder knight for plan-quota refusals (#321): the refused implement/split/fix step re-runs on this knight with no model override; empty = defer until the plan window resets |
| `prPipeline.maxIterations` | int | `3` | Fix → re-review rounds before escalating |
| `prPipeline.timeoutSeconds` | int | `86400` | Chain timeout (seconds) |
| `prPipeline.maxQueueWaitSeconds` | int | `3600` | Max seconds a task may wait for its knight |
| `prPipeline.input` | string | `""` | Initial input, `{"repo": "<owner>/<repo>", "issue": <n>}` |
| `prPipeline.feedback.enabled` | bool | `false` | Create the feedback Chain the operator's PR feedback watcher feeds ([pr-feedback.md](pr-feedback.md)); the watcher itself is switched on per RoundTable (`spec.prFeedback`) |
| `prPipeline.feedback.name` | string | `pr-pipeline-feedback` | Feedback Chain name (the RoundTable's `spec.prFeedback.chainRef`) |
| `prPipeline.feedback.timeoutSeconds` | int | `7200` | Feedback Chain timeout (seconds) |

### Planner

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `planner.enabled` | bool | `true` | Deploy the built-in planner knight |
| `planner.domain` | string | `operator` | Planner domain |
| `planner.model` | string | `claude-sonnet-4-20250514` | Planner model |
| `planner.skills` | list | `[operator, shared]` | Planner skills |
| `planner.concurrency` | int | `1` | Max concurrent tasks |
| `planner.taskTimeoutSeconds` | int | `600` | Task timeout in seconds |
| `planner.arsenal.repo` | string | `https://github.com/roundtable-k8s/arsenal.git` | Skills git repo |
| `planner.arsenals` | list | `[]` | Extra skill repos merged after `arsenal` (`name`, `repo`, `ref`, `periodSeconds`; per-skill merge, later wins) |
| `planner.github.tokenSecretRef.name` | string | unset | Secret holding a GitHub token, mounted at `/var/run/secrets/roundtable/github/token` (`GITHUB_TOKEN_FILE`). The planner belongs to no RoundTable, so it is configured here |
| `planner.github.tokenSecretRef.key` | string | required | Key within that Secret (`corev1.SecretKeySelector`; `optional` is honoured too) |

### GitHub token file

GitHub App installation tokens expire after an hour, but env vars are frozen at
pod start. Set `github.tokenSecretRef` (on a RoundTable, a Knight, or
`planner.github` here) and the operator mounts that Secret read-only at
`/var/run/secrets/roundtable/github/token` and sets
`GITHUB_TOKEN_FILE` to that path. The kubelet refreshes the file when the
Secret rotates (about a minute later). The knight images' git credential helper
and `gh` shim read the file on every call. The token is never put in the env.

Resolution order: the Knight's own `spec.github.tokenSecretRef`, then the
RoundTable named by the knight's `ai.roundtable.io/round-table` label (or the
older `ai.roundtable.io/table` label). Mission and warm-pool knights get the
table's value copied into their spec, and a mission's ephemeral RoundTable
inherits it from `spec.roundTableRef`.

### Scheduling

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `nodeSelector` | object | `{}` | Node selector for operator pod |
| `tolerations` | list | `[]` | Tolerations for operator pod |
| `affinity` | object | `{}` | Affinity rules for operator pod |

## Operator metrics

The operator exposes Prometheus metrics on its metrics endpoint, including:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `roundtable_knight_wake_seconds` | histogram | `knight`, `namespace` | Wake latency of scaled-to-zero knights: time from entering `Waking` (tasks queued) until the knight is `Ready`. Buckets 1s to ~8.5m. Condition timestamps have 1s resolution, so values are whole seconds. |
