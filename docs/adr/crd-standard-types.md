# ADR: standard Kubernetes types in the CRDs

Status: accepted (roundtable-k8s/roundtable#232)

## Context

Our CRDs (`Knight`, `RoundTable`, `Chain`, `Mission`, all `ai.roundtable.io/v1alpha1`)
grew several bespoke shapes where Kubernetes already has a standard one. The
known case: `Knight.spec.resources` was a flat `{cpu, memory}` ("limits") that
reads like `corev1.ResourceRequirements` but isn't, and the pod builder ignored
it, so the values never reached the pod (dapperdivers/dapper-cluster#5010).
Bespoke shapes also confuse tooling (kubeconform, IDE schemas, `kubectl explain`).

We are the only users, so this is a breaking change: no conversion webhook, no
deprecated aliases. Manifests are updated in the same rollout window.

## Conventions adopted

1. **Durations are `<name>Seconds` integers** (`int32`), like
   `activeDeadlineSeconds`, `terminationGracePeriodSeconds`,
   `startingDeadlineSeconds`. Every duration field says its unit in its name;
   none are Go-duration strings or unitless ints. (Chosen over `metav1.Duration`
   because most of our durations were already integer seconds with min/max
   bounds that OpenAPI validates without CEL, and the upstream core/batch APIs
   use `*Seconds`.)
2. **References to other API objects are `corev1.LocalObjectReference`**
   (`{name: ...}`) and are named `<kind>Ref`, like `configMapRef`/`secretRef`
   in `envFrom`, Tekton `taskRef` or KEDA `scaleTargetRef`. A field ending in
   `Ref` is always an object. Names of entries inside a spec (not API
   objects) are plain strings named `<thing>Name`.
3. **Secret keys are `corev1.SecretKeySelector`** (`{name, key, optional}`).
4. **Compute resources are `corev1.ResourceRequirements`**; sizes are
   `resource.Quantity`.
5. **"Pause" is `suspend: bool`**, like `CronJob.spec.suspend` /
   `Job.spec.suspend` / Flux `spec.suspend`.
6. **Container images are a single `image` string** (`repo:tag` or
   `repo@digest`), as in `corev1.Container`. The Helm chart keeps the Helm
   convention (`repository` + `tag` values) and renders them into that string.
7. **Status** keeps `[]metav1.Condition` (`listType=map`, key `type`) with
   `observedGeneration` set on every condition and `status.observedGeneration`
   on every kind. Printer columns must point at scalar fields.

## Audit

Decision `change` rows are implemented in the same PR as this document.

### Knight (`spec`; also embedded as `RoundTable.spec.knightTemplates`, Mission `ephemeralSpec`/`knightTemplates[].spec`)

| Field | Current | Standard | Decision |
|---|---|---|---|
| `resources` | `{cpu, memory}` Quantities, "limits", defaulted 200m/256Mi, **ignored by the pod builder** | `corev1.ResourceRequirements` | **change**: `resources: {requests, limits, claims}` applied verbatim to the `app` container. Unset keeps the operator default (requests 100m / 256Mi, no limits). |
| `taskTimeout` | `int32` seconds, unitless name | `*Seconds` int | **change** → `taskTimeoutSeconds` (same bounds 30–3600, default 120) |
| `lifecycle.idleTimeout` | Go-duration string (`"30m"`), parsed at runtime, silently ignored if invalid | `*Seconds` int | **change** → `lifecycle.idleTimeoutSeconds` (`int32`, min 0); becomes the ScaledObject `cooldownPeriod` (default 300) |
| `arsenal.period`, `arsenals[].period` | string `"300s"` | `*Seconds` int (cf. probe `periodSeconds`) | **change** → `periodSeconds` (`int32`, min 1, default 300), rendered as `GITSYNC_PERIOD=<n>s` |
| `workspace.size` | string `"1Gi"` | `resource.Quantity` (as PVC `resources.requests.storage`) | **change** → Quantity (same YAML, now validated at admission) |
| `github.tokenSecretRef` | bespoke `GitHubTokenSecretRef{name, key=token}` | `corev1.SecretKeySelector` | **change** → `SecretKeySelector`; `key` is now required (no `token` default) and `optional` is honoured on the projected volume |
| `suspended` | bool | `suspend` (CronJob/Job/Flux) | **change** → `suspend` |
| `nixPackages` | deprecated, ignored | — | **change**: removed (use `tools.nix`) |
| `image` | single string | `corev1.Container.image` | keep: already the container convention |
| `env` / `envFrom` | `[]corev1.EnvVar` / `[]corev1.EnvFromSource` | same | keep |
| `serviceAccountName` | string | `PodSpec.serviceAccountName` | keep |
| `prompt.configMapRef` | `corev1.LocalObjectReference` | same | keep |
| `vault.claimName` | string | `PersistentVolumeClaimVolumeSource.claimName` | keep: upstream name |
| `workspace.existingClaim` | string | (Helm `persistence.existingClaim`) | keep: says "not created by us"; not a type issue |
| `concurrency`, `replicas`, `maxReplicas` | int32 / *int32 | `replicas` as in Deployment | keep |
| `volumes`, `volumeMounts`, `securityContext`, `nodeSelector`, `tolerations`, `affinity`, `imagePullPolicy` | absent (operator owns the pod; security context is fleet-wide via chart `knightSecurity`) | upstream types | keep (absent): add as the upstream types if/when needed, never bespoke |
| enum values (`runtime: deployment`, `suspendPolicy: auto`, `workspace.mode: pvc`, ...) | lower-case | API conventions prefer CamelCase | keep: not a type/shape issue; a separate rename if we want it |
| `status.conditions` | `[]metav1.Condition` (`Available`, `RuntimeAvailable`, `Autoscaled`, `PlanCapLimited`), observedGeneration set | same | keep |
| `status.observedGeneration` | present | same | keep |
| `status.totalCost` | decimal string | — | keep: CRDs avoid floats; a Quantity would misrender USD |
| printer columns | Domain, Model, Phase, Ready, Replicas, Tasks, Age | scalar JSONPaths | keep |

### RoundTable

| Field | Current | Standard | Decision |
|---|---|---|---|
| `defaults.resources` | `{cpu, memory}` | `corev1.ResourceRequirements` | **change** (same type as Knight) |
| `defaults.taskTimeout` | int32 seconds | `*Seconds` | **change** → `defaults.taskTimeoutSeconds` |
| `warmPool.maxIdleTime` | Go-duration string `"1h"` | `*Seconds` | **change** → `warmPool.maxIdleTimeSeconds` (default 3600); the whole field was later removed (#483) |
| `missionRef` | string (set by the mission controller) | `LocalObjectReference` | **change** → `missionRef: {name}` |
| `suspended` | bool | `suspend` | **change** → `suspend` |
| `github.tokenSecretRef` | bespoke | `SecretKeySelector` | **change** (shared `GitHubConfig`) |
| `knightSelector` | `metav1.LabelSelector` | same | keep |
| `secrets` | `[]corev1.LocalObjectReference` | same | keep |
| `sharedWorkspace.claimName` | string | PVC `claimName` | keep |
| `policies.costBudgetUSD` | decimal string | — | keep (no floats in CRDs) |
| `knightTemplates` | `map[string]KnightSpec` | — | keep (inherits the Knight changes) |
| printer column `Knights` | `type=string` on the integer `knightsReady`, header says "Ready/Total" | correct type | **change** → `Ready` (`integer`) |
| conditions / observedGeneration | standard | — | keep |

### Chain

| Field | Current | Standard | Decision |
|---|---|---|---|
| `timeout` | int32 seconds | `*Seconds` | **change** → `timeoutSeconds` |
| `maxQueueWait` | int32 seconds | `*Seconds` | **change** → `maxQueueWaitSeconds` |
| `steps[].timeout`, `steps[].loop.steps[].timeout` | int32 seconds | `*Seconds` | **change** → `timeoutSeconds` |
| `steps[].knightRef`, `loop.steps[].knightRef` | string name | `LocalObjectReference` | **change** → `knightRef: {name}` |
| `roundTableRef` | string | `LocalObjectReference` | **change** → `roundTableRef: {name}` |
| `missionRef` | string | `LocalObjectReference` | **change** → `missionRef: {name}` |
| `outputKnight` | string, default `gawain` | `LocalObjectReference`, `*Ref` name | **change** → `outputKnightRef: {name}` (default still `gawain`, applied by the controller) |
| `suspended` | bool | `suspend` | **change** → `suspend` |
| `startingDeadlineSeconds` | `*int64` | CronJob's field | keep |
| `retryPolicy.backoffSeconds`, `steps[].retry.backoffSeconds` | int32 | `*Seconds` | keep (already conformant) |
| `notify.webhook.tokenSecretRef` | `corev1.SecretKeySelector` | same | keep |
| printer column `Steps` | `type=integer` on the `spec.steps` array (prints garbage) | scalar JSONPath | **change**: removed |
| conditions / observedGeneration | standard | — | keep |

### Mission

| Field | Current | Standard | Decision |
|---|---|---|---|
| `ttl` | int32 seconds | `*Seconds` (cf. `ttlSecondsAfterFinished`) | **change** → `ttlSeconds` |
| `timeout` | int32 seconds | `*Seconds` | **change** → `timeoutSeconds` |
| `planner.timeout` | int32 seconds | `*Seconds` | **change** → `planner.timeoutSeconds` |
| `generatedChains[].timeout` | `*int32` seconds | `*Seconds` | **change** → `timeoutSeconds` |
| `roundTableRef` | string | `LocalObjectReference` | **change** → `roundTableRef: {name}` |
| `planner.knightRef` | string, marked required although `templateRef`/`ephemeralSpec` are alternatives | `*LocalObjectReference` | **change** → optional `knightRef: {name}` (one of knightRef / templateName / ephemeralSpec, checked by the controller as before) |
| `knights[].templateRef`, `planner.templateRef` | string naming a spec entry, not an API object | `<thing>Name` string | **change** → `templateName` |
| `secrets` | `[]corev1.LocalObjectReference` | same | keep |
| `costBudgetUSD` | decimal string | — | keep |
| `knights[].specOverrides.env` | `[]corev1.EnvVar` | same | keep |
| printer columns | `Knights` `integer` on an array; `TTL` on `spec.ttl` | scalar JSONPaths | **change**: `Knights` removed, `TTL` → `spec.ttlSeconds` |
| conditions / observedGeneration | standard | — | keep |

### Helm chart (`charts/roundtable-operator`)

| Value | Current | Standard | Decision |
|---|---|---|---|
| `planner.taskTimeout`, `knights.<n>.taskTimeout` | seconds | mirror the CRD | **change** → `taskTimeoutSeconds` |
| `planner.arsenal.period` | `5m` | mirror the CRD | **change** → `planner.arsenal.periodSeconds` (300) |
| `knights.<n>.resources` | passed through as `{cpu, memory}` | mirror the CRD | **change**: passed through as `ResourceRequirements` |
| `knights.<n>.suspended` | bool | mirror the CRD | **change** → `suspend` |
| `image`, `images.*`, `gateway.image` | `repository` + `tag` (+ `pullPolicy`) | Helm convention | keep; rendered into the Knight `image` string |
| `resources`, `nodeSelector`, `tolerations`, `affinity`, `dashboard.resources`, `gateway.resources`, `gateway.networkPolicy.from` | upstream shapes | same | keep |
| `knightSecurity` | `PodSecurityContext` subset (`runAsUser`, `runAsGroup`, `fsGroup`, `fsGroupChangePolicy`) | upstream field names | keep (names already upstream) |
| `gateway.defaultTimeout`, `maxTimeout`, `taskTTL` | Go-duration strings | — | keep: these are operator flags, not CRD fields |
| `gateway.auth.existingSecret` / `secretKey` | Helm convention | — | keep |

## Consequences

- Every Knight, RoundTable, Chain and Mission manifest that uses a changed
  field has to be rewritten; the PR carries the migration note for the
  dapper-cluster manifests.
- roundtable-ui reads some of the renamed fields (`suspended`, `taskTimeout`,
  `roundTableRef`, `missionRef`, `outputKnight`, `planner.knightRef`) and needs
  a follow-up.
- `RoundTable.spec.defaults` is still not applied to standing knights (it only
  seeds ephemeral mission tables); that is a pre-existing gap, not changed here.
