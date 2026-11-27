# First-install troubleshooting

Find what you see in the left column, read the cause, and run the fix. Every
condition type, reason and event below is set by the operator
(`api/v1alpha1/conditions.go` and the controller named in the section).

Start with these commands (replace `<ns>` with your namespace, `roundtable` in
the quickstart):

```bash
kubectl -n <ns> get knights                      # PHASE, READY, ISOLATED
kubectl -n <ns> describe knight <name>           # Conditions and Events
kubectl -n <ns> get events --sort-by=.lastTimestamp
kubectl -n <ns> get pods
kubectl -n <ns> describe pod <pod>               # the pod-level cause
kubectl -n <ns> logs deploy/<knight> -c app      # the runtime container
```

The runtime container is named `app`; the pod's other containers are sidecars
(`skill-filter`, one git-sync sidecar per arsenal, and `browser` when enabled).
Add `-o wide` to `get knights` or `-o yaml` to read the full condition messages.

## A knight is not Ready

| You see | Because | Do this |
| --- | --- | --- |
| `Available=False`, reason `Provisioning`, message "Knight deployment is being provisioned" | The Deployment has no ready replica yet. The condition does not name the cause; it is in the pod | `kubectl -n <ns> get pods`, then `describe pod <pod>` and look at the pod rows below |
| Pod status `CreateContainerConfigError` | A Secret or ConfigMap the pod references (for example the model API key in `spec.envFrom` or `spec.env`) does not exist or lacks the key | `kubectl -n <ns> describe pod <pod>` names the Secret; create it (quickstart step 2: [getting started](getting-started.md#2-store-your-api-key)) |
| Pod status `ImagePullBackOff` / `ErrImagePull` | The image cannot be pulled: wrong tag, private registry without a pull secret | `kubectl -n <ns> describe pod <pod>` shows the pull error; fix the image or add an image pull secret |
| Pod `Pending` with "unbound immediate PersistentVolumeClaims" | A PVC the pod mounts (workspace, memory) has no bound volume, usually no default StorageClass | `kubectl -n <ns> get pvc` and `describe pvc <name>`; provide a StorageClass or an existing claim |
| Dashboard pod `Pending` | The dashboard mounts the vault claim `roundtable-vault` by default and the chart does not create it ([install](install.md#helm-helm-install)) | Create the PVC or install with `dashboard.vault.enabled=false` |
| `Available=False`, reason `ReconcileError`; Warning event `ReconcileFailed` | The operator could not reconcile the Knight; the condition message is the error | `kubectl -n <ns> describe knight <name>`, read the message, fix the spec, or read the operator logs |
| `Available=False`, reason `Suspended`, phase Suspended | `spec.suspend` is true on the Knight; the operator sets this reason only for that | Resume it: `kubectl -n <ns> patch knight <name> --type merge -p '{"spec":{"suspend":false}}'`. An idle autoscaled knight has phase `Suspended` too but reason `ScaledToZero` (next row) |
| `Available=False`, reason `ScaledToZero` | KEDA scaled an autoscaled knight to zero replicas; this is normal when idle | Send it a task; it moves to `Waking` |
| `Available=False`, reason `Waking` | An autoscaled knight at zero is starting a replica | Wait; if it never becomes Ready look at the pod rows above |
| `RuntimeAvailable=False` and `Isolated=False`, reason `RuntimeClassNotFound` | The RuntimeClass the knight needs (gVisor by default) does not exist; the knight stays parked | Create the RuntimeClass, or opt out explicitly ([runtime isolation](runtime-isolation.md)); it clears on the next reconcile |
| `Isolated=False`, reason `FQDNPolicyUnavailable` | There is no Cilium, so the fallback NetworkPolicy cannot enforce hostnames | Install Cilium or change the egress settings ([runtime isolation](runtime-isolation.md#the-isolated-condition), [egress](egress.md)) |
| `Isolated=False`, reason `EgressPolicyFailed` | The egress policy failed to apply | `describe knight`, read the message and the Warning events |
| `Isolated=False`, reason `IsolationDisabled` | No RuntimeClass or no egress policy applies: the install turned a default off, or the table sets `runtimeClassName: ""` | Intended on a cluster without gVisor; otherwise re-enable the chart default ([runtime isolation](runtime-isolation.md#the-isolated-condition)) |
| `ToolsImageReady=False`, reason `ToolsImageBuilding` | The Job that builds the knight's Nix tools image is running | Wait; `kubectl -n <ns> get jobs,pods` shows the builder |
| `ToolsImageReady=False`, reason `ToolsImageBuildFailed` | The tools image build Job failed | `kubectl -n <ns> logs job/<job>` for the build error (bad flake ref or package name); fix `spec.tools` |
| `ToolsImageReady=False`, reason `InvalidFlakeRef` | A `spec.tools.flakes` ref is invalid (it bypassed admission) | Fix the ref in `spec.tools.flakes`; the message names the problem |
| Warning event `ToolsImageNotConfigured` | `spec.tools.nix` or `spec.tools.flakes` is set but the operator has no tools image repository (chart value `toolsImage.repository`); the Nix tools are not installed | Set `toolsImage.repository` in the chart, or drop `spec.tools` |

## A knight did not roll after a change

A changed pod template replaces a Recreate knight's pod, unless the rollout is
held. The old pod keeps running until the change applies. Look for the
`RolloutPending` condition (`kubectl -n <ns> get knight <name> -o jsonpath='{.status.conditions}'`):

| You see | Because | Do this |
| --- | --- | --- |
| `RolloutPending=True`, reason `WaitingForIdle` | The knight has `spec.lifecycle.rollout: whenIdle` and its consumer still has queued or in-flight tasks. The change rolls when the knight is idle, or after `lifecycle.rolloutMaxWaitSeconds` (default 3600). The operator rechecks every 30 s | Wait for the tasks to finish. The pending count is the consumer's `num_pending + num_ack_pending` (`nats consumer info <stream> <consumer>`), or the queued and in-flight counts in the gateway's `list_knights`. To roll now, set `spec.lifecycle.rollout` to `immediate`; that deletes the running pod and the tasks in it. See the **Rollout when idle** bullet in the [CRD design](crd-design.md) |
| `RolloutPending=True`, reason `WaitingForOtherRollouts` | The operator's `maxConcurrentRollouts` limit, applied per namespace, is reached by other knights rolling at once (`0` is unlimited; [helm values](helm-values.md)) | Wait; the knight rolls when a slot frees. Raise the limit if rollouts should overlap more |
| No `RolloutPending` condition, but the pod is old | Nothing is holding the rollout, so the change has not reached the Deployment yet | `kubectl -n <ns> describe knight <name>` and read the Events; a `ReconcileFailed` event or condition carries the error |

The condition is removed once the knight rolls.

## Tasks do not reach a knight

| You see | Because | Do this |
| --- | --- | --- |
| `ConsumerReady=False`, reason `NATSUnavailable`, message "Cannot reach NATS to reconcile consumer ..." | The operator cannot reach NATS, or JetStream is not enabled | Check the NATS URL (`missionDefaultNatsURL`, `gateway.natsURL`, `dashboard.env.NATS_URL`) and that JetStream is on ([install](install.md#pointing-round-table-at-your-nats)) |
| `ConsumerReady=False`, reason `StreamNotFound`, message "Consumer <name> on stream <stream>: ..." | `spec.nats.stream` does not exist | Check the RoundTable's `NATSReady` condition, or create the stream |
| `ConsumerReady=False`, reason `ConsumerFailed` | JetStream rejected the consumer, or `spec.nats.stream` / `spec.nats.subjects` is empty | Read the message in `describe knight` and fix `spec.nats` |
| RoundTable `NATSReady=False`, reason `StreamError` | The operator could not create or update the NATS streams (`spec.nats.createStreams`) | `kubectl -n <ns> describe roundtable <name>`; the message is the NATS error |

## Nothing happens (chains and tasks)

Work through this in order:

1. **Was the Chain started?** Applying a Chain does not run it. Annotate it; any
   new value starts a new run:
   `kubectl -n <ns> annotate chain <name> ai.roundtable.io/run="$(date +%s)" --overwrite`
2. **Is it valid?** `kubectl -n <ns> get chain <name> -o yaml` and look at the
   `Valid` condition. `Valid=False` carries one of `MissingRoundTableRef` (no
   `roundTableRef`; the chain is Failed), `InvalidKnightRef` (a step names a
   knight that does not exist), `CyclicDependency` (steps depend on each other
   in a loop) or `InvalidTemplate` (a step's Go template does not parse). The
   message says which step.
3. **Is the knight Ready?** `kubectl -n <ns> get knights`: READY must be `true`.
   If not, use the first table.
4. **Does its consumer exist?** `ConsumerReady` on the knight must be `True`
   (second table).
5. **Was the task dead-lettered?** Ask the gateway
   (`GET /v1/dead-letters?knight=<name>`, see [gateway](gateway.md)); each entry has
   a reason and a `state`. Retrying and the entry format are in the
   [task contract](knight-task-contract.md#delivery-and-failure).
6. **Still nothing?** `kubectl -n <ns> logs deploy/<knight> -c app` and
   `kubectl -n <ns> get events --sort-by=.lastTimestamp`.

Warning events by reason (what each one means and what to do) are listed in
[events](events.md).
