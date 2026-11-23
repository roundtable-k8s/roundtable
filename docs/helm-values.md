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
| `sandboxBackend` | string | `auto` | agent-sandbox runtime backend: `auto` (enable only if `agents.x-k8s.io/v1alpha1` Sandbox is served), `true`, or `false` |
| `keda` | string | `auto` | KEDA autoscaling for knights with `maxReplicas > 1` or `lifecycle.suspendPolicy: auto`: `auto` (enable only if `keda.sh/v1alpha1` ScaledObject is served), `true`, or `false` |
| `natsMonitoringEndpoint` | string | `""` | NATS HTTP monitoring endpoint (`host:port`) for KEDA's JetStream scaler; empty derives `<spec.nats.url host>:8222` per knight |
| `extraArgs` | list | `[]` | Additional args for the manager binary |

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

Synchronous HTTP API + MCP endpoint for knights (see [gateway.md](gateway.md)).
Off by default; enabling it requires a bearer-token Secret.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `gateway.enabled` | bool | `false` | Deploy the invoke gateway |
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
| `gateway.auth.existingSecret` | string | `""` | **Required.** Secret holding the bearer token(s) |
| `gateway.auth.secretKey` | string | `token` | Key in that Secret; one token per line (two during a rotation) |
| `gateway.service.type` / `.port` | string / int | `ClusterIP` / `80` | Service |
| `gateway.networkPolicy.enabled` | bool | `true` | Restrict ingress to the gateway |
| `gateway.networkPolicy.from` | list | `[{podSelector: {}}]` | NetworkPolicyPeers allowed in (default: release namespace). An empty list allows any source. |
| `gateway.resources` | object | 20m/32Mi req, 128Mi limit | Resources |
| `gateway.extraArgs` | list | `[]` | Extra gateway flags |

### Knights

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
| `prPipeline.roundTableRef.name` | string | `roundtable-dev` | RoundTable; must define the `deep` and `fast` model aliases (for the coders) |
| `prPipeline.lead` | string | `rt-lead` | Knight that plans, reviews, merges and escalates |
| `prPipeline.leadModel` | string | `""` | Model (alias or runtime model ID) for the lead's steps; empty = the lead's default model |
| `prPipeline.coders` | list | `[rt-operator]` | Coder knights the planner may pick; one implement/split step and fix loop each |
| `prPipeline.implementModels` | list | `[deep, fast]` | Model aliases the planner may pick for the implementation |
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
