# Runtime isolation (RuntimeClass / gVisor)

Knights can run under a named Kubernetes
[RuntimeClass](https://kubernetes.io/docs/concepts/containers/runtime-class/).
gVisor (`runsc`) is the standard choice: it puts a userspace kernel between the
knight's processes and the node kernel.

## Who should turn it on

Any fleet whose knights read untrusted content or run code they were handed:
research, pentest, feed and web triage. If a prompt-injected knight is talked
into running something hostile, gVisor shrinks what that code can reach on the
node. It does not replace egress control or least-privilege RBAC.

## Configuration

| Where | Field |
| --- | --- |
| `Knight.spec.runtimeClassName` | RuntimeClass for this knight |
| `RoundTable.spec.defaults.runtimeClassName` | Inherited by the table's standing knights when the knight leaves it unset |
| Helm `knights.<name>.runtimeClassName`, `planner.runtimeClassName` | Rendered onto the Knight; empty by default |

The field is set on the pod spec shared by both backends, so it applies to
`runtime: deployment` (Deployment pod template) and `runtime: sandbox`
(`Sandbox.spec.podTemplate.spec`) alike. Unset means the cluster's default
runtime. The tools-image builder Job is not a knight pod and is unaffected.

If the named RuntimeClass does not exist, the knight is parked: its
`RuntimeAvailable` condition is `False` with reason `RuntimeClassNotFound` and a
message naming the class. Creating the RuntimeClass clears it on the next
reconcile (the controller requeues slowly while parked).

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

Verify these before enabling gVisor for a fleet:

- `ptrace`-based tools (debuggers, `strace`) are limited or unsupported.
- FUSE filesystems do not work.
- Nested containers (docker-in-docker, podman) do not work.
- The Nix image volume mount used for knight tools.
- git-sync and anything leaning on inotify or unusual `/proc` and `/sys` access.
- File-heavy workloads (large builds) may be slower on the workspace volume.
