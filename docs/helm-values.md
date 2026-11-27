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

For Flux (`OCIRepository` + `HelmRelease`) and CRD upgrade notes, see [installing Round Table](install.md).

Optional add-ons (KEDA, agent-sandbox, a tools-image registry, gVisor) and the minimal
install file `values-minimal.yaml` are described in [dependencies.md](dependencies.md).

## Values Reference

The chart ships a permissive `values.schema.json`: `helm install`, `helm template` and `helm lint` reject a value of the wrong type (for example `gateway.replicas=abc`), but unknown keys are accepted. `sandboxBackend`, `keda` and `isolation.egress.mode` accept a string or a boolean.

### Operator

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `nameOverride` | string | `""` | Overrides the chart name in resource names |
| `fullnameOverride` | string | `""` | Overrides the full resource name prefix |
| `replicaCount` | int | `1` | Operator replicas (leader election handles HA) |
| `image.repository` | string | `ghcr.io/roundtable-k8s/roundtable` | Operator image |
| `image.tag` | string | `""` | Operator image tag; empty uses the chart `appVersion` |
| `image.pullPolicy` | string | `Always` | Image pull policy |
| `nodeSelector` / `affinity` / `tolerations` | map / object / list | `{}` / `{}` / `[]` | Scheduling for the operator, gateway, dashboard and collector pods. `gateway.*`, `dashboard.*` and `telemetry.collector.*` take their own and, when non-empty, replace the global value |
| `topologySpreadConstraints` | list | `[]` | `topologySpreadConstraints` of the operator, gateway, dashboard and collector pods. `gateway.*`, `dashboard.*` and `telemetry.collector.*` take their own and, when non-empty, replace the global value. A constraint without `labelSelector` gets the component's pod selector labels; set `labelSelector` to spread by other labels |
| `gateway.topologySpreadConstraints` / `dashboard.topologySpreadConstraints` / `telemetry.collector.topologySpreadConstraints` | list | `[]` | Per-component spread constraints replacing the global `topologySpreadConstraints` when non-empty |
| `probes.enabled` | bool | `true` | Liveness and readiness probes on the operator `manager` container (`/healthz`, `/readyz`). Neither depends on holding the leader lease. Set `false` if `extraArgs` moves the health probe address and you do not want probes |
| `probes.port` | int | `8081` | Health probe port (`--health-probe-bind-address`); change it together with an `extraArgs` override of that flag |
| `probes.liveness.initialDelaySeconds` / `probes.liveness.periodSeconds` | int | `15` / `20` | Liveness probe timing |
| `probes.readiness.initialDelaySeconds` / `probes.readiness.periodSeconds` | int | `5` / `10` | Readiness probe timing |
| `probes.liveness.timeoutSeconds` / `probes.liveness.failureThreshold` | int | unset | Optional; omitted from the probe when unset (Kubernetes defaults 1 / 3) |
| `probes.readiness.timeoutSeconds` / `probes.readiness.failureThreshold` | int | unset | Optional; omitted from the probe when unset (Kubernetes defaults 1 / 3) |
| `priorityClassName` | string | `""` | `priorityClassName` of the operator, gateway, dashboard and collector pods |
| `imagePullSecrets` | list | `[]` | Pull secrets (`[{name: regcred}]`) for the operator, gateway, dashboard and collector pods |
| `podAnnotations` / `podLabels` | map | `{}` | Extra annotations/labels on the operator, gateway and dashboard pods |
| `podDisruptionBudget.enabled` | bool | `false` | PodDisruptionBudget for the operator (when `replicaCount` is above 1), gateway and dashboard (each when its `replicas` is above 1) |
| `podDisruptionBudget.minAvailable` | int/string | `1` | `minAvailable` (ignored when `maxUnavailable` is set) |
| `podDisruptionBudget.maxUnavailable` | int/string | `""` | `maxUnavailable` |
| `leaderElect` | bool | `true` | Enable leader election for HA |
| `leaderElection.enabled` | bool | `true` | Set `false` to turn leader election off. Requires `replicaCount: 1` (the chart fails otherwise) and switches the Deployment to `Recreate`, because a rolling update would otherwise run a second operator concurrently with the old one. Trade-off: updates have a short gap with no operator running |
| `leaderElection.leaseDuration` | duration | `60s` | How long a non-leader waits before taking over an unrenewed lease. A normal stop (rollout, node drain, SIGTERM) releases the lease so the new pod takes over at once; a crash or kill still waits the full `leaseDuration` |
| `leaderElection.renewDeadline` | duration | `40s` | How long the leader retries renewing before giving up and restarting. Must be less than `leaseDuration` and greater than 1.2 × `retryPeriod`; invalid values fail operator start-up. A slow API for less than this does not restart the operator |
| `leaderElection.retryPeriod` | duration | `5s` | Retry interval for acquiring/renewing the lease |
| `resources.requests.cpu` | string | `50m` | CPU request |
| `resources.requests.memory` | string | `64Mi` | Memory request |
| `resources.limits.cpu` | string | `200m` | CPU limit |
| `resources.limits.memory` | string | `128Mi` | Memory limit |
| `mission.resultsTTL` | duration | `720h` | How long retained mission results (`spec.retainResults`) are kept in the `mission-results` NATS KV bucket (Go duration; 720h = 30 days). An existing bucket with a different TTL is updated |
| `chains.scheduleMode` | string | `inprocess` | How `Chain.spec.schedule` runs (`--chain-schedule-mode`): `inprocess` (the operator's cron runs the Chain itself) or `trigger` (an owned schedule Trigger `<chain>-schedule` and its CronJob start a copy of the Chain per fire; needs the gateway, see `triggers.gatewayURL`). See [triggers.md](triggers.md#chain-schedules) |
| `chains.maxConcurrentReconciles` | int | `1` | How many Chains the operator reconciles in parallel (`--chain-max-concurrent-reconciles`). `1` keeps the single worker; a given Chain is never reconciled twice at once |
| `maxConcurrentRollouts` | int | `0` | Limit on knights of a namespace rolling out a changed pod template at once (`--max-concurrent-rollouts`), so an image bump does not replace every knight pod together. `0` is unlimited. A knight over the limit keeps its Deployment unchanged, shows the `RolloutPending` condition (reason `WaitingForOtherRollouts`) and retries every 30s. Scaled-to-zero and suspended knights are not counted. A rolling knight's surge pod (maxSurge 1) needs one pod of headroom, so size the limit to the spare capacity of the node pool |
| `providerOverload.maxRetries` | int | `4` | Retries of a chain step whose task fails with HTTP 529/503/502/500 or an `overloaded_error` message, after a 1m, 2m, 4m, 8m... backoff. After 2 retries the step moves to `prPipeline.fallbackCoder` when set. `0` disables the retry |
| `notify.allowedURLPrefixes` | list | `[]` | URL prefixes a Chain's `notify.url` may POST to (the SSRF guard, since anyone who can create a Chain controls the URL). An empty list rejects every notification |
| `sandboxBackend` | string | `auto` | agent-sandbox runtime backend: `auto` (enable only if `agents.x-k8s.io/v1beta1` Sandbox is served), `true`, or `false`. Supports agent-sandbox v1.0.0 or later (developed against v1.0.12) |
| `keda` | string | `auto` | KEDA autoscaling for knights with `maxReplicas > 1` or `lifecycle.suspendPolicy: auto`: `auto` (enable only if `keda.sh/v1alpha1` ScaledObject is served), `true`, or `false` |
| `natsMonitoringEndpoint` | string | `""` | NATS HTTP monitoring endpoint (`host:port`) for KEDA's JetStream scaler; empty derives `<spec.nats.url host>:8222` per knight |
| `cards.bucket` | string | `roundtable-cards` | NATS KV bucket of the [agent cards](agent-cards.md) (`CARDS_KV_BUCKET`; knight pods get `KNIGHT_CARDS_BUCKET`) |
| `extraArgs` | list | `[]` | Additional args for the manager binary (e.g. `--github-api-url=https://ghe.example.com/api/v3` for [issue pickup](issue-pickup.md)) |

### Metrics

Opt-in scrape config. Monitors are rendered only when the `monitoring.coreos.com/v1` API is served. When `metrics.enabled` is set the operator runs with `--metrics-bind-address=:<metrics.port> --metrics-secure=false` (plain HTTP, so no TLS/authz config is needed on the monitor), with a `metrics` container port and a `<release>-metrics` Service. Do not also set `--metrics-bind-address` in `extraArgs`: the flag would be passed twice, the last value wins, and the operator would bind a port other than the declared `metrics.port`. The knight PodMonitor only scrapes knight pods in the release namespace.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `metrics.enabled` | bool | `false` | Enable the operator metrics endpoint, port and Service; gates all monitors |
| `metrics.port` | int | `8080` | Operator metrics port: the `--metrics-bind-address` flag, the `metrics` container port, the `<release>-metrics` Service port and the NetworkPolicy port (when `metrics.enabled`) |
| `metrics.interval` | string | `1m` | Scrape interval for all monitors |
| `metrics.labels` | map | `{}` | Extra labels on every monitor (for Prometheus selectors) |
| `metrics.serviceMonitor.enabled` | bool | `false` | ServiceMonitors for the operator, the gateway (if `gateway.enabled`; port 8081 `/metrics`, served when `metrics.enabled`, unauthenticated, allowed from `networkPolicy.metricsFrom`) and, if `dashboard.enabled`, the dashboard |
| `metrics.podMonitor.enabled` | bool | `false` | PodMonitor for knights (`podIP:3000/metrics`, container `app`); fills in `table` from the `ai.roundtable.io/round-table` pod label without overriding a `table` label the knight sets |

### OpenTelemetry tracing

The operator sets up an OTel tracer provider from the standard `OTEL_*` environment variables. With no endpoint (or `OTEL_SDK_DISABLED=true`) tracing is off: no exporter, no overhead. The chart has no first-class values for these yet (they arrive with #156); until then set them on the operator Deployment's container env.

| Variable | Default | Description |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | unset | OTLP/gRPC endpoint; tracing is enabled only when one is set (headers/insecure/etc. are read by the exporter) |
| `OTEL_SDK_DISABLED` | `false` | `true` disables tracing even when an endpoint is set |
| `OTEL_SERVICE_NAME` | `roundtable-operator` | `service.name` resource attribute |
| `OTEL_RESOURCE_ATTRIBUTES` | unset | Extra resource attributes (`k=v,k2=v2`) |
| `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG` | `parentbased_always_on` | Sampler |

### Isolation defaults

Every knight runs under a sandboxing RuntimeClass and behind default-deny
egress unless its spec opts out (`spec.isolation`, with a reason). Rendered
into the operator's `KNIGHT_DEFAULT_RUNTIME_CLASS`, `KNIGHT_DEFAULT_EGRESS` and
`KNIGHT_EGRESS_BASE_HOSTS` env vars. See
[runtime-isolation.md](runtime-isolation.md) and [egress.md](egress.md).

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `isolation.runtimeClassName` | string | `gvisor` | RuntimeClass for every knight that neither sets `runtimeClassName` nor inherits one from its table. `""` turns the default off for the whole install. A knight whose RuntimeClass does not exist is parked (`Isolated=False`, `RuntimeClassNotFound`) |
| `isolation.egress.mode` | string | `deny` | `deny`: a knight without an allowlist (own or table) gets an empty one, i.e. the baseline only. `"off"`: no policy unless the knight or table sets `egress`. Quote `"off"` (a bare YAML `off` is read as `false`, also treated as off) |
| `isolation.egress.baseHosts` | list | `[openrouter.ai, api.anthropic.com]` | Hosts every knight's policy allows on 443, on top of DNS, NATS, the Kubernetes API and the knight's arsenal git hosts |

`NOTES.txt` warns after install or upgrade when the default RuntimeClass is not
in the cluster (a `lookup`; skipped by `helm template`), with the two ways out:
install gVisor, or set `isolation.runtimeClassName: ""`.

To upgrade a cluster without gVisor without changing any knight, set both
off switches first; knights that set `runtimeClassName` or `spec.egress`
themselves are still honoured:

```yaml
isolation:
  runtimeClassName: ""
  egress:
    mode: "off"
```

### Mission default NATS URL

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `missionDefaultNatsURL` | string | `nats://nats.database.svc:4222` | Fallback NATS URL for mission knights when neither the parent RoundTable nor `roundTableTemplate.natsURL` sets one (`MISSION_DEFAULT_NATS_URL`) |

### Mission NetworkPolicy

The egress NetworkPolicy the operator creates for each mission's knights. The NATS rule is derived from the mission's NATS URL (port, and namespace for `<svc>.<namespace>.svc[.cluster.local]` hosts; any other host allows the port to any destination). TCP 443 is allowed to anywhere. The policy is created once and never updated. It is not created when `isolation.egress.mode` is `deny`: each mission knight then gets its own default-deny policy, which this one would widen. Rendered into the operator's `MISSION_NETPOL_*` env vars.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `missionNetworkPolicy.enabled` | bool | `true` | Create the policy (`MISSION_NETPOL_ENABLED`); disable on clusters without NetworkPolicy support or with their own policies. Setting `false` does not delete existing `mission-*-isolation` policies; remove them manually |
| `missionNetworkPolicy.nats.podLabels` | map | `{app.kubernetes.io/name: nats}` | Pod labels narrowing the NATS peer (`MISSION_NETPOL_NATS_POD_LABELS`, `k=v,k=v`); maps merge, so `{}` keeps the default; set `null` to clear it (the variable is then omitted and the peer is the whole namespace) |
| `missionNetworkPolicy.dns.namespace` | string | `kube-system` | Namespace of cluster DNS (`MISSION_NETPOL_DNS_NAMESPACE`) |
| `missionNetworkPolicy.dns.podLabels` | map | `{k8s-app: kube-dns}` | Pod labels of cluster DNS (`MISSION_NETPOL_DNS_POD_LABELS`) |

### Ingress NetworkPolicy

Opt-in ingress NetworkPolicies for the operator, the dashboard and knight pods. Off by default: the defaults render no extra policy and the pods accept any ingress, as before. Each policy selects its pods in the release namespace and sets `policyTypes: [Ingress]`, so egress is not restricted. A policy whose peer list is empty allows no ingress at all.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `networkPolicy.enabled` | bool | `false` | Render the operator, dashboard and knight policies |
| `networkPolicy.metricsFrom` | list | monitoring namespace | NetworkPolicyPeers allowed to scrape metrics: the operator's metrics port and knights' `:3000/metrics`. The dashboard also accepts these peers. Empty denies all scraping |
| `networkPolicy.otlpFrom` | list | `[{podSelector: {}}]` | NetworkPolicyPeers allowed to send OTLP (4317/4318) to the collector; only rendered when `telemetry.collector.enabled`. Empty allows no one |
| `networkPolicy.dashboardFrom` | list | `[]` | NetworkPolicyPeers allowed to reach the dashboard's `http` port, in addition to `metricsFrom`. Empty means only `metricsFrom` |
| `networkPolicy.operatorMetricsPort` | int | `8443` | Port of the operator's metrics endpoint when enabled via `extraArgs`; ignored when `metrics.enabled` is true (the policy then allows `metrics.port`) |

What each policy allows:

- **Operator** (`<release>-roundtable-operator-operator`): TCP 8081 (health probes) and the metrics port (`metrics.port` when `metrics.enabled`, else `operatorMetricsPort`), from `metricsFrom`.
- **OpenTelemetry Collector** (`<release>-roundtable-operator-otel-collector`, only when `telemetry.collector.enabled`): ports 4317 and 4318, from `otlpFrom`. The collector pods use their own `app.kubernetes.io/name: roundtable-otel-collector`, so the operator policy and Deployment selector never match them.
- **Dashboard** (`<release>-roundtable-operator-dashboard`, only when `dashboard.enabled`): the `http` port, from `dashboardFrom`, `metricsFrom` and the `helm test` Pod (matched by `app.kubernetes.io/component: test` in the release). Knights are reached through NATS and the Kubernetes API, so nothing else needs to connect to the dashboard.
- **Knights** (`<release>-roundtable-operator-knights`): TCP 3000 from `metricsFrom`. It selects every pod labelled `app.kubernetes.io/name: knight`, which covers Deployment knights and `runtime: sandbox` knights once their pods carry that label (#366).

Limits and caveats:

- Policies only cover pods in the release namespace. Knights in other namespaces are not covered; write their policies yourself.
- Kubelet probes come from the node, not from a pod. With Cilium they are allowed by default. Other CNIs may need an `ipBlock` for the node CIDRs; the chart does not guess node CIDRs. This applies to every policy here that opens a port the kubelet probes, not only the operator's: add the `ipBlock` peer to `networkPolicy.metricsFrom` (which feeds the operator, dashboard and knight policies, and the gateway's metrics port rule in `templates/gateway.yaml`) or the probed pods are reported unready.
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
| `images.piKnight.tag` | string | pinned by the chart; bumped on release | pi-knight image tag |
| `images.dashboard.repository` | string | `ghcr.io/roundtable-k8s/roundtable-ui` | Dashboard image |
| `images.dashboard.tag` | string | pinned by the chart; bumped on release | Dashboard image tag |
| `nixpkgs.ref` | string | pinned by the chart; bumped on release | Pinned nixpkgs commit for knight tools images. Changing it changes every tools image tag, so every knight's tools are rebuilt |
| `toolsImage.repository` | string | `""` | Repository knights (the nodes) pull tools images from, e.g. `registry.example.com/knight-tools`. Empty disables tools images |
| `toolsImage.pushRepository` | string | `""` | Repository the build Job pushes to when it reaches the registry by a different address than the nodes (e.g. an in-cluster Service). Defaults to `toolsImage.repository` |
| `toolsImage.pushInsecure` | bool | `false` | Push over plain HTTP (in-cluster Service without TLS) |
| `toolsImage.builderImage` | string | `""` | Image the build Job runs. Empty uses the prebuilt builder CI publishes for the operator's built-in pins (`ghcr.io/roundtable-k8s/tools-builder`), or digest-pinned `nixos/nix` when `nixpkgs.ref` or `toolsImage.nix2containerRev` is overridden (slow: it compiles the nix2container tooling on every build) |
| `toolsImage.nix2containerRev` | string | `""` | nix2container commit the generated flake pins; empty uses the operator's built-in pin. Changing it changes every tools image tag |
| `knight.browserImage` | string | `""` | Browser sidecar image. Empty uses the operator's pinned default (`docker.io/chromedp/headless-shell:159.0.8154.13@sha256:381353d3530ab9d579d2fe8b9c0f4519c995617f5a71daf6bb649e2d12005bd9`). Changing it changes the pod template of knights that enable the browser, so they restart once |
| `knightSecurity.runAsUser` / `.runAsGroup` / `.fsGroup` | int | `1000` | Pod-level security context applied to every knight pod |
| `knightSecurity.fsGroupChangePolicy` | string | `OnRootMismatch` | Stops the kubelet from recursively chowning a knight's volumes on every pod mount |

**Looking up a newer browser image.** The default is pinned as `tag@sha256:digest` (Chrome 159.0.8154.13, published 2026-11-23). To move to a newer one, pick a tag from <https://hub.docker.com/r/chromedp/headless-shell/tags> and resolve its digest with `crane digest docker.io/chromedp/headless-shell:<tag>` (or `docker buildx imagetools inspect`). Then either set `knight.browserImage` to `docker.io/chromedp/headless-shell:<tag>@<digest>`, or update `DefaultBrowserImage` in `internal/knight/pod_builder.go` and the default quoted above. Nothing tracks this automatically and CI never pulls the image.

### Pod Security

Every container the chart and operator create (operator, gateway, dashboard and all knight containers) meets the Pod Security Standard `restricted` profile: `allowPrivilegeEscalation: false`, `capabilities.drop: [ALL]`, `runAsNonRoot: true` and `seccompProfile: RuntimeDefault`. The namespace can be labelled `pod-security.kubernetes.io/enforce: restricted`.

The one exception is the tools-image builder Job (`toolsImage.*`), which runs a Nix build with `allowPrivilegeEscalation: true` and as root. It needs `baseline`/`privileged`: run it in a separate namespace or exempt it from enforcement.

### Dashboard

See [dashboard.md](dashboard.md) for the pages, access and authentication.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `dashboard.enabled` | bool | `true` | Deploy the Round Table dashboard |
| `dashboard.replicas` | int | `1` | Dashboard replicas |
| `dashboard.nodeSelector` | map | `{}` | Dashboard pod `nodeSelector` (replaces the global value when set) |
| `dashboard.affinity` | object | `{}` | Dashboard pod `affinity` (replaces the global value when set) |
| `dashboard.tolerations` | list | `[]` | Dashboard pod `tolerations` (replaces the global value when set) |
| `dashboard.resources.requests.cpu` / `.memory` | string | `50m` / `64Mi` | Dashboard CPU and memory requests |
| `dashboard.resources.limits.memory` | string | `128Mi` | Dashboard memory limit (no CPU limit) |
| `dashboard.ingress.enabled` | bool | `false` | Enable ingress for dashboard |
| `dashboard.ingress.className` | string | `""` | Ingress class name |
| `dashboard.ingress.host` | string | `""` | Dashboard hostname |
| `dashboard.ingress.annotations` | map | `{}` | Ingress annotations |
| `dashboard.env.NATS_URL` | string | `nats://nats.database.svc:4222` | NATS server URL |
| `dashboard.env.NAMESPACE` | string | `roundtable` | Namespace to watch |
| `dashboard.env.FLEET_PREFIX` | string | `fleet-a` | NATS subject prefix of the fleet the dashboard shows |
| `dashboard.env.FLEET_STREAM` | string | `fleet_a_results` | JetStream results stream the dashboard reads |
| `dashboard.vault.enabled` | bool | `true` | Mount shared Obsidian vault |
| `dashboard.vault.claimName` | string | `roundtable-vault` | Vault PVC name |
| `dashboard.podSecurityContext` | object | `{runAsUser: 65532, runAsGroup: 65532}` | Dashboard pod `securityContext` (`runAsNonRoot` and the `RuntimeDefault` seccomp profile are always set). When `dashboard.vault.enabled` and `fsGroup` is not set, `fsGroup` defaults to `runAsGroup` so the pod can read the vault volume; set `fsGroup` explicitly to override. The chart-defaulted `fsGroup` also sets `fsGroupChangePolicy: OnRootMismatch` so the shared vault is not recursively chgrped on every start. Many CSI drivers skip `fsGroup` on RWX volumes; the reliable way to read mode-600 vault files is `podSecurityContext.runAsUser: 1000` and `runAsGroup: 1000` (the owner of the files). An empty `{}` keeps the always-set defaults, and `runAsNonRoot`/`seccompProfile` set here override them |
| `dashboard.gateway.tokenSecret` / `.tokenKey` | string | `""` | Secret and key of the token the dashboard sends to the gateway; empty = the gateway's own token Secret and key |
| `dashboard.gateway.serviceAccountToken` | bool | `false` | Send a projected ServiceAccount token (audience `gateway.serviceAccountAuth.audience`) instead of the static token Secret; requires `gateway.serviceAccountAuth.enabled`, and `tokenSecret`/`tokenKey` are then ignored |

### Invoke gateway

`dashboard.gateway.tokenSecret` / `tokenKey` (default empty = the gateway's Secret and key) give the dashboard its own token. `dashboard.gateway.serviceAccountToken` (bool, default `false`) mounts a projected ServiceAccount token (audience `gateway.serviceAccountAuth.audience`, `expirationSeconds` 3600) at the same path instead of the Secret; `tokenSecret`/`tokenKey` are then ignored, and the chart fails to render unless `gateway.serviceAccountAuth.enabled` is true.

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
| `gateway.nodeSelector` / `.affinity` / `.tolerations` | map / object / list | `{}` / `{}` / `[]` | Gateway pod scheduling; each replaces the global value when set |
| `gateway.image.repository` / `.tag` | string | `""` | Image repository and tag; defaults to the operator image (`/gateway` binary) |
| `gateway.natsURL` | string | `nats://nats.database.svc:4222` | NATS server URL |
| `gateway.namespace` | string | `""` | Default namespace for knights (empty = release namespace) |
| `gateway.allowedNamespaces` | list | `[]` | Extra namespaces callers may target (`"*"` = any) |
| `gateway.defaultTimeout` | duration | `60s` | Wait when the caller passes no `timeoutSeconds` |
| `gateway.maxTimeout` | duration | `30m` | Upper bound on a caller's wait |
| `gateway.maxInFlight` | int | `64` | Concurrently waiting invocations per replica (429 beyond) |
| `gateway.maxStreams` | int | `64` | Concurrently open event streams (`GET .../events`) per replica (429 beyond); `0` = unlimited |
| `gateway.taskBucket` | string | `roundtable_gateway_tasks` | NATS KV bucket for task records (also of Triggers with a knight target) |
| `gateway.taskTTL` | duration | `168h` | How long task records are kept |
| `gateway.scheduleRuns.enabled` | bool | `true` | Serve a schedule's run history and results (`GET /v1/schedules/{namespace}/{name}/runs`, `.../runs/{run}`) and let the gateway `get` Triggers ([gateway.md](gateway.md#schedule-run-history)) |
| `gateway.scheduleRuns.resultRetention` | duration | `720h` | How long the results of a schedule's runs stay readable: raises the task bucket TTL to at least this and keeps finished chain runs of schedule Triggers at least this long ([gateway.md](gateway.md#how-long-results-stay-readable)) |
| `gateway.knightSnapshots.enabled` | bool | `false` | Serve `POST /v1/knights/{name}/snapshot` and the `snapshot_knight` tool, and let the gateway `get` and `patch` Knights. When off the route answers `403` and the tool is not listed; listing snapshots is unaffected. When on, a ValidatingAdmissionPolicy lets the gateway change only the snapshot annotations (needs Kubernetes 1.30+; [gateway.md](gateway.md#knight-snapshots)) |
| `gateway.auth.existingSecret` | string | `""` | Secret holding the bearer token(s); empty = chart generates one |
| `gateway.auth.secretKey` | string | `token` | Key in that Secret; one token per line (two during a rotation), optionally `<name> <token>` to name the caller |
| `gateway.serviceAccountAuth.enabled` | bool | `true` | Also accept in-cluster callers' own ServiceAccount tokens, checked with a TokenReview; grants the gateway `create` on `tokenreviews.authentication.k8s.io` (a ClusterRole, only when enabled) ([gateway.md](gateway.md#calling-from-a-workload)) |
| `gateway.serviceAccountAuth.audience` | string | `roundtable-gateway` | Audience the projected ServiceAccount token must be issued for |
| `gateway.serviceAccountAuth.allowedNamespaces` | list | `["*"]` | Namespaces whose ServiceAccounts may call (`"*"` or an empty list = any). If you narrow it while `dashboard.gateway.serviceAccountToken=true`, include the release namespace or the dashboard's token is rejected |
| `gateway.serviceAccountAuth.reviewRate` | number | `50` | TokenReviews per second each gateway replica sends to the API server on average (cache hits and static tokens excluded); beyond the burst a caller gets `429` with `Retry-After` |
| `gateway.serviceAccountAuth.reviewBurst` | int | `100` | TokenReviews each gateway replica may send at once (0 = default; the limit cannot be turned off) |
| `gateway.service.type` / `.port` | string / int | `ClusterIP` / `80` | Service |
| `gateway.networkPolicy.enabled` | bool | `true` | Restrict ingress to the gateway |
| `gateway.networkPolicy.from` | list | `[{podSelector: {}}]` | NetworkPolicyPeers allowed in (default: release namespace). An empty list allows any source. |
| `gateway.resources` | object | 20m/32Mi req, 128Mi limit | Resources |
| `gateway.extraArgs` | list | `[]` | Extra gateway flags |
| `gateway.missions.enabled` | bool | `true` | Serve the mission endpoints and grant the gateway create/delete on Missions in its namespaces ([gateway.md](gateway.md#missions)) |
| `gateway.missions.maxBudgetUSD` | number | `10` | Upper bound on a mission's `costBudgetUSD` |
| `gateway.missions.maxTTLSeconds` | int | `86400` | Upper bound on a mission's `ttlSeconds` |
| `gateway.missions.maxTimeoutSeconds` | int | `21600` | Upper bound on a mission's `timeoutSeconds` |
| `gateway.missions.allowSkillGeneration` | bool | `false` | Let callers of the gateway mission API set `planner.allowSkillGeneration` (not applicable to Trigger mission targets) |
| `gateway.schedules.enabled` | bool | `true` | Serve the `/v1/schedules` routes (list, get, manifest, create, replace, delete) and grant `get`, `list`, `watch`, `create`, `update`, `patch` and `delete` on Triggers in the gateway's namespaces ([gateway.md](gateway.md#schedules)) |
| `gateway.hooks.enabled` | bool | `true` | Serve `POST /v1/hooks/{namespace}/{trigger}` for webhook Triggers; grants `get` on Triggers and `get` on Secrets in the gateway's namespaces (only Secrets labelled `ai.roundtable.io/webhook-secret: "true"` are used) ([gateway.md](gateway.md#webhooks)) |
| `gateway.hooks.maxBodyBytes` | int | `1048576` | Largest webhook body (413 beyond) |
| `gateway.hooks.rate` / `.burst` | number / int | `2` / `20` | Per-Trigger rate limit: deliveries per second and at once (429 beyond) |
| `gateway.runs.enabled` | bool | `true` | Let callers run a copy of a Chain with their input; grants the gateway `create`/`delete` on Chains in its namespace and `gateway.allowedNamespaces` (a ClusterRole if that holds `"*"`) |
| `gateway.runTTL` | duration | `24h` | How long a finished run copy is kept before the gateway deletes it |

### Scheduled Triggers

A Trigger with `source.schedule` runs as a CronJob `trigger-<name>` the operator creates in the Trigger's namespace (with a ServiceAccount of the same name); each run starts a pod from the operator image (`image.*`) that calls the gateway's fire endpoint with a projected token for `gateway.serviceAccountAuth.audience`. The operator gets `cronjobs` (batch) and `serviceaccounts` in its ClusterRole. Pods in the Triggers' namespaces must reach the gateway (`gateway.networkPolicy.from`). See [triggers.md](triggers.md#schedule-source).

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `triggers.gatewayURL` | string | `""` | Gateway base URL the firing pod calls. Empty = the chart's gateway Service (`http://<fullname>-gateway.<namespace>.svc:<port>`) when `gateway.enabled`; with the gateway disabled and no URL, scheduled Triggers are not Ready |
| `triggers.imagePullSecrets` | list of strings | `[]` | Names of imagePullSecrets on the firing pod. Secrets are namespaced: they must exist in the Trigger's namespace (the chart does not create or copy them), or the pod stays `ImagePullBackOff` |
| `triggers.nodeSelector` | object | `{}` | nodeSelector of the firing pod |
| `triggers.tolerations` | list | `[]` | Tolerations of the firing pod |
| `triggers.priorityClassName` | string | `""` | priorityClassName of the firing pod |

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

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `knights` | map | `{}` | Chart-managed Knights, keyed by name (see the commented examples in `values.yaml`) |
| `codingKnightDefaults.resources.requests` | map | `{cpu: "1", memory: 2Gi, ephemeral-storage: 4Gi}` | Requests for knights with `coder: true` that set no `resources` |
| `codingKnightDefaults.resources.limits` | map | `{cpu: "4", memory: 8Gi, ephemeral-storage: 20Gi}` | Limits for those knights |
| `codingKnightDefaults.workspace.size` | string | `20Gi` | Workspace volume size for those knights that set no `workspace` |

Set `knights.<name>.runtimeClassName` (and `planner.runtimeClassName`; both
default to empty, meaning not set on the Knight) to run a knight under a
specific RuntimeClass. Knights in a RoundTable inherit
`RoundTable.spec.defaults.runtimeClassName` when unset, and otherwise get the
operator default `isolation.runtimeClassName` (`gvisor`). Set
`knights.<name>.isolation` (rendered verbatim into `Knight.spec.isolation`) to
opt a chart knight out, for example `{runtime: none, reason: "needs ptrace for
the debugger skill"}`. See [runtime-isolation.md](runtime-isolation.md).

Set `knights.<name>.memory` to give a knight a persistent memory volume,
separate from its `/data` workspace, that every replica mounts read-write at
`/memory/<name>` (`KNIGHT_MEMORY_DIR`; `KNIGHT_MEMORY_ROOT=/memory`). Without
`existingClaim` the operator creates the ReadWriteMany PVC `<name>-memory`
(`size`, default `1Gi`, and `storageClassName`), owned by the Knight and
deleted with it. With `existingClaim` the shared claim is mounted read-only at
`/memory` and its `<name>` sub-directory read-write at `/memory/<name>`; the
operator never creates or deletes that claim. Knights in a RoundTable inherit
`spec.defaults.memory` (including `existingClaim`) when unset.

```yaml
knights:
  galahad:
    memory:
      size: 1Gi
      storageClassName: cephfs     # must support ReadWriteMany for replicas > 1
      # existingClaim: table-memory  # shared RWX claim instead of <name>-memory
```

Define knights inline in values to deploy them with the operator:

```yaml
knights:
  galahad:
    domain: security
    model: claude-sonnet-4-20250514
    skills: [security, threat-intel]
    tools:
      nix: [nmap, whois]
    nats:
      subjects: ["fleet-a.tasks.security.>"]
    taskTimeoutSeconds: 300
    # Platform skills from roundtable-k8s/arsenal, domain packs from your own repo.
    # Merged per skill into /skills; later arsenals win on a clash.
    arsenals:
      - name: platform
        repo: https://github.com/roundtable-k8s/arsenal
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
| `prPipeline.postRun` | string | `builtin` | `chain` adds the `verify`, `close_parent` and `handback` steps that do the post-run checks; pair it with the table's `spec.issuePickup.postRun: chain` ([docs](pr-pipeline.md#post-run-checks-builtin-or-chain)) |
| `prPipeline.postRunScripts` | string | `/skills/project/pr-postrun/scripts` | Directory of the arsenal's `rt-verify`, `rt-close-parent` and `rt-handback` in the lead's pod (`postRun: chain` only) |
| `prPipeline.timeoutSeconds` | int | `86400` | Chain timeout (seconds) |
| `prPipeline.maxQueueWaitSeconds` | int | `3600` | Max seconds a task may wait for its knight |
| `prPipeline.input` | string | `""` | Initial input, `{"repo": "<owner>/<repo>", "issue": <n>}` |
| `prPipeline.feedback.enabled` | bool | `false` | Create the feedback Chain the operator's PR feedback watcher feeds ([pr-feedback.md](pr-feedback.md)); the watcher itself is switched on per RoundTable (`spec.prFeedback`) |
| `prPipeline.feedback.name` | string | `pr-pipeline-feedback` | Feedback Chain name (the RoundTable's `spec.prFeedback.chainRef`) |
| `prPipeline.feedback.timeoutSeconds` | int | `7200` | Feedback Chain timeout (seconds) |

### Planner

Every RoundTable owns its planner knight, `<table>-planner`, on the table's NATS
names, credentials and defaults, scaled to zero when idle; RoundTable
`spec.planner` configures or disables it (see
[crd-design.md](crd-design.md#mission-controller), "Planner"). `planner.model`,
`planner.skills`, `planner.arsenals`, `planner.prompt`, `planner.concurrency` and
`planner.taskTimeoutSeconds` are the install-wide defaults those table planners
inherit (passed to the operator as `TABLE_PLANNER_DEFAULTS`). The table's
`spec.planner`, then its `spec.defaults.model` / `spec.defaults.arsenals`, win
over `planner.model` / `planner.arsenals`. The remaining keys only configure the
old install-wide planner Knight `<release>-roundtable-operator-planner`, which
is rendered only with `planner.enabled: true`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `planner.enabled` | bool | `false` | Deploy the old install-wide planner knight (no table, `fleet-a.tasks.operator.>`; the prefix can be overridden with `planner.natsPrefix`). Deprecated: missions fall back to it only when their table has no planner |
| `planner.domain` | string | `operator` | Install-wide planner domain (table planners use `planner`) |
| `planner.model` | string | `claude-sonnet-4-20250514` | Planner model; default for table planners |
| `planner.skills` | list | `[operator, shared]` | Planner skills; default for table planners |
| `planner.prompt` | object | `{}` | Prompt overrides (`identity`, `instructions`, `configMapRef`) for table planners |
| `planner.natsPrefix` | string | `fleet-a` | NATS subject prefix of the install-wide planner |
| `planner.envFrom` | list | `[]` | `envFrom` sources (ConfigMap/Secret refs) for the planner container |
| `planner.concurrency` | int | `1` | Max concurrent tasks; default for table planners |
| `planner.taskTimeoutSeconds` | int | `600` | Task timeout in seconds; default for table planners |
| `planner.runtimeClassName` | string | `""` | Install-wide planner `spec.runtimeClassName` (table planners: `spec.planner.runtimeClassName`, then the table's defaults) |
| `planner.nodeSelector` | map | `{}` | Install-wide planner knight `spec.nodeSelector` |
| `planner.affinity` | object | `{}` | Install-wide planner knight `spec.affinity` |
| `planner.tolerations` | list | `[]` | Install-wide planner knight `spec.tolerations` |
| `planner.topologySpreadConstraints` | list | `[]` | Install-wide planner knight `spec.topologySpreadConstraints` |
| `planner.priorityClassName` | string | `""` | Install-wide planner knight `spec.priorityClassName` |
| `planner.arsenals` | list | `[{name: platform, repo: https://github.com/roundtable-k8s/arsenal.git, ref: main, periodSeconds: 300}]` | Ordered skill sources (`name`, `repo`, `ref`, `periodSeconds`, `image`, `oci`); first = base, later entries override per skill. Default for table planners. A table's own `arsenals` (planner, defaults) replaces this list, it is not merged with it, so repeat `platform` there if the planner still needs it |
| `planner.github` | object | `{}` | Install-wide planner GitHub token: `tokenSecretRef.name` (Secret holding the token, mounted at `/var/run/secrets/roundtable/github/token`, `GITHUB_TOKEN_FILE`; unset by default) and `tokenSecretRef.key` (required; key within that Secret, a `corev1.SecretKeySelector` so `optional` is honoured too). Table planners use their table's `spec.github` instead. See [GitHub token file](#github-token-file) |

#### Upgrade note: table planners (#566)

`planner.enabled` now defaults to `false`. After upgrading:

- Every non-ephemeral RoundTable gets a Knight `<table>-planner` (scaled to zero
  when idle). A meta-mission that names no planner is planned by its table's
  planner.
- The install-wide `<release>-roundtable-operator-planner` Knight is removed
  unless you set `planner.enabled: true`. Missions that set
  `spec.planner.knightRef: {name: roundtable-operator-planner}` must drop it
  (to use the table's planner) or keep `planner.enabled: true`.
- If a Knight named `<table>-planner` already exists and is not owned by the
  table, the table reports a `PlannerFailed` warning event and leaves it alone:
  rename it, or point the table at it with `spec.planner.knightRef`.
- To keep a table without a planner, set `spec.planner.enabled: false` on it.

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
older `ai.roundtable.io/table` label). Mission knights get the
table's value copied into their spec, and a mission's ephemeral RoundTable
inherits it from `spec.roundTableRef`.

### Grafana dashboards

Five dashboards ship in `charts/roundtable-operator/dashboards/`: **Fleet overview** (knights by phase, tasks/min, error rate, active tasks, NATS connectivity, KEDA desired vs actual replicas), **Cost and tokens** (spend and tokens per model and knight, 24h spend, spend per table), **Chains and platform** (chain runs by status, step p95, provider retries, missions by phase, dead letters, consumers, isolation, wake latency), **Triggers and gateway** (trigger firings per minute by outcome, time since each Trigger last fired, trigger error and skipped share, gateway requests by route and code, gateway p95 latency and 5xx rate, dead letters by reason and open count) and **Knight activity** (task duration p50/p95/p99, tool calls per minute by tool and status, tool call p95 duration, blocked tool calls and NATS reconnects over 1h, prompt-cache hit ratio by knight and model). They query the `roundtable_*` metric names and need Prometheus scraping the operator and the knights, plus kube-state-metrics for the replicas panel. Every dashboard has a `datasource`, a `namespace` and a multi-select `table` variable; the `table` filter is applied to every `roundtable_*` query. Two exceptions: the KEDA panel is filtered by namespace only, since HPAs carry no table label, and the gateway p95 latency panel is not filtered by table, since the duration histogram carries only `route`. The Triggers and gateway dashboard's `table` variable also lists `none` (gateway requests that name no table). Per-chain cost is not shown: knight metrics carry no chain label.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `dashboards.enabled` | bool | `false` | Render the dashboards (one object per file). Off renders nothing. |
| `dashboards.label` | string | `grafana_dashboard` | Label the Grafana sidecar selects ConfigMaps by |
| `dashboards.labelValue` | string | `"1"` | Value of that label |
| `dashboards.folder` | string | `""` | Grafana folder; set as the `dashboards.folderAnnotation` annotation (or `spec.folder` of the CRs) when non-empty |
| `dashboards.folderAnnotation` | string | `grafana_folder` | Annotation key the sidecar reads the folder from |
| `dashboards.grafanaOperator.enabled` | bool | `false` | Render `GrafanaDashboard` CRs (`grafana.integreatly.org/v1beta1`) instead of ConfigMaps. Only when that API is in the cluster; otherwise ConfigMaps are rendered. |
| `dashboards.grafanaOperator.instanceSelector` | object | `{dashboards: grafana}` | `matchLabels` selecting the Grafana instance(s) |

`make helm-check` runs `scripts/check-grafana-dashboards.sh`, which verifies the rendering, that the JSON parses, and that every query uses only known metric names and filters on `$table`.

### Telemetry

Sets the standard OpenTelemetry `OTEL_*` environment variables on the
operator (`roundtable-operator`), gateway (`roundtable-gateway`) and dashboard
(`roundtable-ui`) Deployments. With `telemetry.enabled=false` only
`OTEL_SDK_DISABLED=true` is set. Knight pods get the same values from the
operator (see "Per-table override" below).

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `telemetry.enabled` | bool | `false` | Turn OTel export on |
| `telemetry.otlp.endpoint` | string | `""` | `OTEL_EXPORTER_OTLP_ENDPOINT`, e.g. `http://otel-collector.observability:4317`; omitted when empty |
| `telemetry.otlp.protocol` | string | `grpc` | `OTEL_EXPORTER_OTLP_PROTOCOL`: `grpc` or `http/protobuf` |
| `telemetry.sampling.ratio` | number | `1.0` | `OTEL_TRACES_SAMPLER_ARG` (sampler `parentbased_traceidratio`) |
| `telemetry.captureContent` | bool | `true` | `ROUNDTABLE_TELEMETRY_CAPTURE_CONTENT`: prompt/response text on gen_ai spans, redacted before export |
| `telemetry.resourceAttributes` | object | `{}` | Extra attributes, rendered sorted as `OTEL_RESOURCE_ATTRIBUTES=k=v,...` |
| `telemetry.traceUrlTemplate` | string | `""` | Dashboard `TRACE_URL_TEMPLATE` deep link containing `{traceId}` |

#### Per-table override

A RoundTable can override these values for its own knights with
`spec.telemetry`, so tables (tenants) can use different collectors. When the
chart sets no `telemetry.otlp.endpoint`, a table needs its own `otlpEndpoint`
to export traces:

| Field | Effect on the table's knights |
|-------|-------------------------------|
| `otlpEndpoint` | replaces `OTEL_EXPORTER_OTLP_ENDPOINT` |
| `otlpProtocol` (`grpc`, `http/protobuf`) | replaces `OTEL_EXPORTER_OTLP_PROTOCOL` |
| `samplingRatio` (string, 0 to 1) | sets `OTEL_TRACES_SAMPLER=parentbased_traceidratio` and `OTEL_TRACES_SAMPLER_ARG` |
| `disabled` | `OTEL_SDK_DISABLED=true` (only that var is set) |
| `captureContent` | `false` sets `ROUNDTABLE_TELEMETRY_CAPTURE_CONTENT=false` and `ACTIVITY_CONTENT=none`; unset inherits `telemetry.captureContent` |

Precedence: knight `spec.env` > table `spec.telemetry` > chart values. Unset
table fields inherit the chart values. OTLP auth headers are not modelled.

#### OpenTelemetry Collector

An optional collector (`telemetry.collector.*`, off by default) gives the stack
a telemetry endpoint without choosing a backend. It runs as one non-root,
read-only-root-filesystem Pod without a service account token, and a Service
`<release>-otel-collector` exposing OTLP gRPC (4317) and HTTP (4318). Every
pipeline runs `memory_limiter`, `redaction`, `batch`, so secrets are redacted
for every source, including ones whose own spans cannot be redacted in-process.
The `roundtable.table` attribute is not dropped (`allow_all_keys: true`). When
enabled and `telemetry.otlp.endpoint` is empty, `OTEL_EXPORTER_OTLP_ENDPOINT`
defaults to `http://<release>-otel-collector.<namespace>:4317` (`:4318` when
`telemetry.otlp.protocol=http/protobuf`). The standard backend is being chosen
in #385.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `telemetry.collector.enabled` | bool | `false` | Render the collector Deployment, Service and ConfigMap |
| `telemetry.collector.nodeSelector` / `.affinity` / `.tolerations` | map / object / list | `{}` / `{}` / `[]` | Collector pod scheduling; each replaces the global value when set |
| `telemetry.collector.image.repository` / `.tag` | string | `otel/opentelemetry-collector-contrib` / pinned release | Must be a contrib build (`redactionprocessor`) |
| `telemetry.collector.resources` | object | `{}` | Container resources |
| `telemetry.collector.redaction.enabled` | bool | `true` | Add the `redaction` processor to every pipeline |
| `telemetry.collector.redaction.blockedValues` | list | secret patterns | RE2 regexes; the default is ported from the knights' `redactSecrets` list (pi-knight `src/hooks.ts`). Setting it replaces the list |
| `telemetry.collector.exporters` | object | `{debug: {}}` | Passed into the collector `exporters`; every exporter a pipeline names must be defined here (or in `extraConfig`), otherwise rendering fails. Helm merges maps, so remove the default with `--set telemetry.collector.exporters.debug=null` |
| `telemetry.collector.pipelines` | object | `traces/metrics/logs: [debug]` | Exporter names per signal; a signal set to an empty list or `null` gets no pipeline |
| `telemetry.collector.extraConfig` | object | `{}` | Deep-merged last into the collector config |

Exporter examples (no auth headers or secret refs are modelled yet):

```yaml
telemetry:
  enabled: true
  collector:
    enabled: true
    exporters:
      debug: null
      otlp/traces:                       # OTLP to a trace store
        endpoint: tempo.observability:4317
        tls: {insecure: true}
      otlphttp/llm:                      # otlphttp to an LLM viewer
        endpoint: http://langfuse.observability:3000/api/public/otel
    pipelines:
      traces: [otlp/traces, otlphttp/llm]
      metrics: []
      logs: []
```

A try-it-out setup with Jaeger as the trace viewer is in
[`examples/observability/`](../examples/observability/) (`values-telemetry.yaml`).

### Scheduling

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `nodeSelector` | object | `{}` | Node selector for operator pod |
| `tolerations` | list | `[]` | Tolerations for operator pod |
| `affinity` | object | `{}` | Affinity rules for operator pod |

### Helm test

`helm test <release>` runs a Pod (`templates/tests/connection.yaml`) that calls the gateway `/readyz` (when `gateway.enabled`) and the dashboard `/api/health` (when `dashboard.enabled`) and fails if either does not answer. Nothing is rendered when both are disabled. The Pod runs in the release namespace, which the default gateway NetworkPolicy admits; the dashboard NetworkPolicy admits it by its `app.kubernetes.io/component: test` label.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `tests.image.repository` | string | `busybox` | Image of the test Pod; it needs `wget` (the operator image is distroless) |
| `tests.image.tag` | string | `1.37.0` | Tag of the test image; pin a version, not `latest` |

### Alerts

Opt-in `PrometheusRule` (`templates/prometheusrule.yaml`). Rendered only when
`alerts.enabled=true` and the cluster serves `monitoring.coreos.com/v1`.
Knight rules use the `roundtable_knight_*` metrics (they only match once
knights emit those names) and keep `table` in every aggregation. Pod and
scaling rules are scoped to the release namespace.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `alerts.enabled` | bool | `false` | Render the `PrometheusRule` |
| `alerts.costSpikeUSDPer24h` | number | `15` | `KnightLLMCostSpike` threshold: USD per table over 24h |
| `alerts.taskFailuresPerHour` | number | `2` | `KnightTaskFailures` threshold: failed tasks per knight per hour |
| `alerts.chainStepSlowSeconds` | number | `1800` | `RoundtableChainStepsSlow` threshold: p95 step duration over 1h, from `roundtable_chain_step_duration_seconds` |
| `alerts.chainFailureRatio` | number | `0.5` | `RoundtableChainRunsFailing` threshold: failed share (0-1) of `roundtable_chain_runs_total` over 1h |
| `alerts.chainMinRuns` | number | `3` | `RoundtableChainRunsFailing`: tables with fewer chain runs than this in 1h are ignored |
| `alerts.deadLettersOpenMinutes` | number | `15` | `RoundtableDeadLettersOpen`: minutes `roundtable_dead_letters_open` stays above 0 |
| `alerts.knightNotReadyMinutes` | number | `15` | `KnightNotReady`: minutes a knight may stay `Pending`, `Provisioning` or `Degraded` |
| `alerts.scheduleMissedSeconds` | number | `600` | `RoundtableScheduleMissed` threshold: seconds a scheduled Trigger's CronJob (`trigger-*`, not suspended) may stay past its next scheduled time |
| `alerts.gatewayErrorRatio` | number | `0.05` | `RoundtableGatewayErrors` threshold: 5xx share (0-1) of a gateway route's `roundtable_gateway_requests_total` over 15m; routes with fewer than `alerts.gatewayMinRequests` requests are ignored |
| `alerts.gatewayMinRequests` | int | `20` | `RoundtableGatewayErrors`: minimum requests in 15m a gateway route needs before the alert can fire |
| `alerts.operatorJob` | string | `""` | Prometheus `job` label `RoundtableReconcileErrors` matches. Empty uses `<fullname>-metrics`, the default job of the operator metrics Service; set it when the ServiceMonitor uses `jobLabel` or relabels `job` |
| `alerts.natsReconnectsPer30m` | number | `3` | `KnightNatsFlapping` threshold: knight NATS reconnects (`roundtable_knight_nats_reconnects_total`) over 30m, per knight |
| `alerts.repositoryHeldMinutes` | number | `30` | `RoundtableRepositoryHeld` threshold: minutes a github-source Trigger may hold a repository back because of an open pull request with its hold label |
| `alerts.labels` | object | `{}` | Extra labels on every rule; a `severity` here overrides the default `warning` |
| `alerts.scaling.enabled` | bool | `true` | Render the `roundtable.scaling` group (KEDA scaler errors, HPA `ScalingActive=false`) |
| `alerts.queueBacklog.enabled` | bool | `false` | Render `RoundtableQueueBacklog` in the `roundtable.scaling` group (needs `alerts.scaling.enabled`); covers only knights with a KEDA ScaledObject |
| `alerts.queueBacklog.pending` | number | `10` | `RoundtableQueueBacklog` threshold: pending tasks (`keda_scaler_metrics_value`, nats-jetstream scaler) per knight |
| `alerts.queueBacklog.forMinutes` | number | `15` | Minutes the backlog must stay above `pending` |

Groups: `roundtable.knights` (task failures, NATS disconnected, metrics absent,
lost tasks, cost spike, slow chain steps), `roundtable.pods` (restarts,
OOMKilled, Evicted, PVC almost full) and `roundtable.scaling`. A chain that
never finishes a step produces no step observation, so detecting a wholly
stuck run needs a started-runs or in-flight metric (follow-up).

`scripts/check-prometheusrule.sh` (part of `make helm-check`) renders the rules
and runs `promtool check rules` on them.
