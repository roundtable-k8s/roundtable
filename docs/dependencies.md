# Dependencies

Round Table needs **Kubernetes and NATS with JetStream enabled**. Nothing else is
required. Every other add-on is optional: the operator detects it when it starts and,
when it is missing, turns the feature off and says so in a status condition or an event.

The chart does not install NATS. Bring your own (the [quickstart](getting-started.md)
installs one on kind).

## Minimal install

[`values-minimal.yaml`](../charts/roundtable-operator/values-minimal.yaml) switches every
optional add-on off:

```bash
helm install roundtable-operator charts/roundtable-operator \
  -n roundtable --create-namespace \
  -f charts/roundtable-operator/values-minimal.yaml
```

It sets `keda: "false"`, `sandboxBackend: "false"`, no `toolsImage.repository`,
`isolation.runtimeClassName: ""`, the gateway off and the dashboard without its vault
volume. The built-in planner stays on, and no knights are created. Edit the NATS URLs
(`missionDefaultNatsURL`, `dashboard.env.NATS_URL`) to match your server. `make helm-check`
lints and renders the chart with this file and checks that no KEDA or agent-sandbox objects
appear.

## Optional dependencies

| Dependency | Chart value | Detection | When absent | Minimum |
|---|---|---|---|---|
| [KEDA](https://keda.sh) (knight autoscaling, scale-to-zero) | `keda`: `auto` (default), `"true"`, `"false"` | `auto` enables it when the API server serves `keda.sh/v1alpha1` `ScaledObject` (`pkg/runtime/keda_detect.go`) | A knight that asks for autoscaling (`spec.maxReplicas` > 1 or `lifecycle.suspendPolicy: auto`) runs `spec.replicas` pods. Condition `Autoscaled=False`, reason `KEDAUnavailable` | KEDA serving `keda.sh/v1alpha1` |
| [agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox) (`runtime: sandbox`) | `sandboxBackend`: `auto` (default), `"true"`, `"false"` | `auto` enables it when the API server serves `agents.x-k8s.io/v1beta1` `Sandbox` (`pkg/runtime/sandbox_detect.go`) | A knight with `spec.runtime: sandbox` is parked. Condition `RuntimeAvailable=False`, reason `SandboxUnavailable`. Knights with `runtime: deployment` are unaffected | agent-sandbox v1.0.0 (developed against v1.0.12) |
| Tools-image registry and Kubernetes image volumes (Nix packages from `spec.tools.nix` / `spec.tools.flakes`) | `toolsImage.repository` (empty disables), `toolsImage.pushRepository`, `toolsImage.pushInsecure` | Configured, not detected: set when `toolsImage.repository` is non-empty | The knight runs without its Nix tools. Warning event `ToolsImageNotConfigured` on the Knight | Kubernetes image volumes (on by default from 1.35) and containerd 2.1+ on the nodes; a registry that nodes pull from and the build Job can push to |
| gVisor (or another RuntimeClass) | `isolation.runtimeClassName` (default `gvisor`; empty turns the default off) | Checked per knight against the cluster's RuntimeClasses | A knight whose RuntimeClass does not exist is parked. Condition `Isolated=False`, reason `RuntimeClassNotFound`. See [runtime-isolation.md](runtime-isolation.md) | The RuntimeClass named in the value |
| Invoke gateway | `gateway.enabled` (default `true`) | On or off by value | Off: no HTTP/MCP invoke API, no webhook Triggers; the dashboard runs without it. See [gateway.md](gateway.md) | None (a Deployment from the operator image) |
| Dashboard | `dashboard.enabled` (default `true`) | On or off by value | Off: no UI, knights and chains still work through `kubectl` | None. With `dashboard.vault.enabled` it mounts the PVC `dashboard.vault.claimName` |

The `Autoscaled`, `RuntimeAvailable` and `Isolated` conditions are on the Knight's status
(`kubectl describe knight <name>`); the event is shown by the same command.

The chart also renders Prometheus Operator and Grafana Operator objects, but only for the
values `metrics.*`, `alerts.enabled` and `dashboards.grafanaOperator.enabled`, which are off by
default (see [observability.md](observability.md)).

## Not covered yet

Tiers for further add-ons (NACK, an OpenTelemetry collector, Envoy AI Gateway, CSI
snapshots) are added here when each feature lands.
