# Runtime isolation (RuntimeClass / gVisor)

Every knight runs under a sandboxing Kubernetes
[RuntimeClass](https://kubernetes.io/docs/concepts/containers/runtime-class/)
**by default**: `gvisor` (`runsc`), which puts a userspace kernel between the
knight's processes and the node kernel. Together with default-deny egress
([egress.md](egress.md)) this is the "isolated by default" rule (whiteboard
decision 8, #377). Opting a knight out is explicit on the Knight, with a reason.

If a prompt-injected knight is talked into running something hostile, gVisor
shrinks what that code can reach on the node. It does not replace egress
control or least-privilege RBAC.

## Configuration

The RuntimeClass is resolved in this order; the first one set wins:

| Order | Where | Field |
| --- | --- | --- |
| 1 | `Knight.spec.runtimeClassName` | RuntimeClass for this knight (`""` = the cluster's default runtime) |
| 2 | `RoundTable.spec.defaults.runtimeClassName` | Inherited by the table's standing knights when the knight leaves it unset |
| 3 | Operator default `KNIGHT_DEFAULT_RUNTIME_CLASS` | Helm `isolation.runtimeClassName`, default `gvisor`; `""` turns the default off for the whole install |

`Knight.spec.isolation.runtime: none` clears the result, whatever the three
say. Helm `knights.<name>.runtimeClassName` and `planner.runtimeClassName` are
rendered onto the Knight (order 1) and are empty by default.

The field is set on the pod spec shared by both backends, so it applies to
`runtime: deployment` (Deployment pod template) and `runtime: sandbox`
(`Sandbox.spec.podTemplate.spec`) alike. Mission knights are Knights too and
follow the same rules (only their template or inline spec can opt out). The
tools-image builder Job is not a knight pod and is unaffected.

If the resolved RuntimeClass does not exist, the knight is parked (fail
closed: a missing `gvisor` never silently runs a knight unsandboxed): its
`RuntimeAvailable` and `Isolated` conditions are `False` with reason
`RuntimeClassNotFound`, and the message names the class and the ways out.
Creating the RuntimeClass clears it on the next reconcile (the controller
requeues slowly while parked). On install, the chart's `NOTES.txt` warns when
the default RuntimeClass is missing.

## Opting a knight out

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Knight
spec:
  isolation:
    runtime: none          # default | none: run without a sandboxing RuntimeClass
    egress: default        # default | unrestricted: no egress policy (see egress.md)
    reason: "needs ptrace for the debugger skill"
```

`reason` is required (at least 10 characters) when `runtime` or `egress` is not
`default`; the API server rejects the Knight otherwise. There is deliberately
**no table-level opt-out**: the opt-out is always visible on the Knight. The
known incompatibilities below are the usual reasons.

## The Isolated condition

Every knight reports how isolated it actually is in the `Isolated` condition,
shown in the `ISOLATED` column of `kubectl get knights`:

| Status | Reason | Meaning |
| --- | --- | --- |
| `True` | `Isolated` | A RuntimeClass applies and a CiliumNetworkPolicy with hostname rules limits egress |
| `False` | `RuntimeClassNotFound` | The RuntimeClass does not exist; the knight stays parked |
| `False` | `OptedOut` | `spec.isolation` opts out; the message carries the reason |
| `False` | `FQDNPolicyUnavailable` | No Cilium: the fallback NetworkPolicy can't enforce hostnames |
| `False` | `EgressPolicyFailed` | The egress policy failed to apply |
| `False` | `IsolationDisabled` | No RuntimeClass or no egress policy applies (the install turned a default off, or the table sets `runtimeClassName: ""`) |

The gauge `roundtable_knight_isolated{table,knight,reason}` is `1` for each
knight's current reason, so a dashboard or alert can list every knight that is
not isolated:

```promql
roundtable_knight_isolated{reason!="Isolated"} == 1
```

## Tool installs need egress

Isolation also turns on default-deny egress. `tools.nix` and `tools.flakes`
need no egress at pod start: the tools image is built by the operator's builder
Job and mounted read-only at `/nix`, not downloaded when the pod starts; see
[Tool installs need egress](egress.md#tool-installs-need-egress). The deprecated
`tools.apt` and `tools.mise` fields are on their way out, so move them to
`tools.nix` or `tools.flakes`.

## Upgrading a cluster without gVisor

With the chart defaults, a cluster without the `gvisor` RuntimeClass parks every
knight that does not opt out. Before upgrading such a cluster, either install
gVisor (below) or set, in the operator's Helm values:

```yaml
isolation:
  runtimeClassName: ""   # no default RuntimeClass
  egress:
    mode: "off"          # optional: no default egress policy either
```

Knights that set their own `runtimeClassName` or `spec.egress` are still
honoured, so isolation can then be switched on knight by knight.

## Enabling gVisor

Install gVisor on the nodes, then create the RuntimeClass:

```yaml
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: gvisor
handler: runsc
```

- **Talos**: add the `siderolabs/gvisor` system extension to the node image and
  set the `user.max_user_namespaces` sysctl its README requires; then add the
  `runsc` runtime to containerd's CRI config per that README.
- **k3s**: install `runsc` and `containerd-shim-runsc-v1` on the node, add a
  `[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runsc]` entry
  (`runtime_type = "io.containerd.runsc.v1"`) to the containerd config template,
  and restart k3s.
- **kind**: build the node image with `runsc` installed and the same containerd
  runtime entry, or use `extraMounts` with a prepared node image.

Pin pods to prepared nodes with the RuntimeClass's own `scheduling` block
(`nodeSelector` / `tolerations`); Kubernetes' RuntimeClass admission copies them
onto every pod using the class, so Knights need no extra placement fields.

## Known incompatibilities to test

Verify these before relying on gVisor for a fleet; a knight that needs one of
them opts out with `spec.isolation.runtime: none` and a reason:

- `ptrace`-based tools (debuggers, `strace`) are limited or unsupported.
- FUSE filesystems do not work.
- Nested containers (docker-in-docker, podman) do not work.
- The Nix image volume mount used for knight tools.
- git-sync and anything leaning on inotify or unusual `/proc` and `/sys` access.
- File-heavy workloads (large builds) may be slower on the workspace volume.
