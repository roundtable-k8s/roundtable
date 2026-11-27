# Egress allowlists

A knight that reads web pages, feeds or issues can be prompt-injected: hostile
text tells it to send data somewhere or to fetch and run something. An egress
allowlist limits where a hijacked knight can send anything. Together with
keeping credentials out of pods (#169, #173) it caps the damage.

## End state and what is live today

The model (whiteboard decision 8, #377) is **isolated by default**: every
knight is behind a default-deny egress policy and reaches only the baseline
below plus its allow-listed hosts. Opting out is explicit on the Knight.

| Step | Status |
| --- | --- |
| `Knight.spec.egress` renders a per-knight policy (#348) | live |
| `RoundTable.spec.defaults.egress` gives every knight of a table an allowlist (#349) | live |
| Default-deny for every knight, with an explicit per-knight opt-out (#387) | live (chart `isolation.egress.mode: deny`, the default) |
| `egress.allowedServices` reaches named in-cluster Services on their ports (#517) | live |

The allowlist is resolved in this order: the knight's `egress`, then the
table's `defaults.egress`, then an empty allowlist (the baseline only). With
the install-wide `isolation.egress.mode: "off"` the last step is skipped, so
knights without an allowlist get no policy.

There is no table-level opt-out: a table can only supply an allowlist. Opt-outs
stay on the Knight:

```yaml
spec:
  isolation:
    egress: unrestricted   # no egress policy at all
    reason: "crawls arbitrary research sources"
```

`reason` is required (at least 10 characters); the knight then reports
`Isolated=False`, reason `OptedOut`. See
[runtime-isolation.md](runtime-isolation.md) for the `Isolated` condition and
the `roundtable_knight_isolated` metric.

## The baseline

Every knight's policy allows, even with an empty allowlist:

| What | Why | Rendered as (Cilium) |
| --- | --- | --- |
| Cluster DNS | name resolution, and Cilium's DNS proxy for `toFQDNs` | `toEndpoints` on 53 with a DNS rule |
| The knight's NATS server(s) | tasks and results | `toEndpoints` (in-cluster) or the port to any destination |
| The telemetry collector (the OTLP endpoint, `OTEL_EXPORTER_OTLP_ENDPOINT` or the table's `telemetry.otlpEndpoint`; only when telemetry is on) | trace and metric export | `toEndpoints` on the collector's namespace (in-cluster Service), or `toFQDNs` / `toCIDR` (external); the port from the URL, else 4317 and 4318 |
| The Kubernetes API | knights use it on purpose (`automountServiceAccountToken: true`) | `toEntities: [kube-apiserver]` |
| `isolation.egress.baseHosts` (`KNIGHT_EGRESS_BASE_HOSTS`, default `openrouter.ai`, `api.anthropic.com`) | the model providers | `toFQDNs` on 443 |
| The hosts of the knight's arsenal repos (`spec.arsenals[].repo`; the default platform arsenal when empty) | the git-sync sidecars share the pod's network | `toFQDNs` on the URL's port (443 for https, 22 for ssh / `git@host:`) |

The knight's own `allowedHosts` are added on top, each on its own `ports` or,
for a plain string, on `allowedPorts`, and each of its `allowedServices` on that
Service's ports.

## The fields

```yaml
# Knight.spec.egress, or RoundTable.spec.defaults.egress (same type)
egress:
  allowedHosts:            # exact hosts or "*." wildcards; empty = the baseline only
    - api.anthropic.com
    - "*.githubusercontent.com"   # subdomains only, not githubusercontent.com itself
    - host: api.example.com       # an object limits the host to its own ports
      ports: [8443]
  allowedPorts: [443]      # TCP, default 443; applies to the plain string hosts
  allowedServices:         # in-cluster Services, each on its own ports (at most 32)
    - name: searxng
      namespace: selfhosted  # empty = the knight's namespace
      ports: [8080]          # required, 1 to 8 Service ports
```

- An `allowedHosts` entry is a string (the host, on `allowedPorts`) or an
  object `{host, ports}` (the host on those ports only, at most 16, TCP
  1-65535). The string form is unchanged. The operator renders one
  `toFQDNs` rule per distinct port set, so an object entry's ports never open
  for the other hosts. A host listed twice gets the union of its ports. On
  the plain `NetworkPolicy` fallback (no Cilium) hostnames are not enforced
  anyway, so the union of all host ports is allowed to any destination, and the
  `EgressRestricted` condition message says so.
- **The operator checks the hosts, not `kubectl apply`.** The CRD schema only
  requires `allowedHosts` to be an array of at most 64 items, because a schema
  can't express a string-or-object entry; a bad entry (a wrong shape such as
  `5` or `{host, port: 1}`, an upper-case or malformed host name, a port outside
  1-65535, more than 16 ports) is accepted by the API server. The operator
  reports it on the `EgressRestricted` condition as `False` with reason
  `EgressPolicyFailed`, naming each rejected entry, and the reconcile
  keeps failing until it is fixed. It **fails closed**: the policy is still
  rendered, from the valid entries only, so the rejected hosts are *not*
  allowed and the pod never runs without a policy (under `deny` or default-deny
  too). Entries past the 64th are dropped the same way.
- A knight's own `egress` wins over the table's. Allowlists are **not merged**
  (same rule as `resources`); an explicit `egress: {}` is its own value and
  means the baseline only.
- `allowedServices` is part of the allowlist, so it follows the same rule:
  a knight without `egress` gets the table's services, a knight with its own
  `egress` gets only its own.
- The table default applies to the table's standing knights (those labelled
  `ai.roundtable.io/round-table`; the legacy `ai.roundtable.io/table` label is
  still honoured when the newer one is absent).
- The operator renders `CiliumNetworkPolicy knight-<name>-egress` allowing the
  baseline and each allowed host on the allowed ports. Everything else is
  denied.
- Mission knights are Knights too and get the same policy and the same
  opt-out rules (only their template or inline spec can opt out). Under
  default-deny the operator no longer creates the per-mission
  `mission-<name>-isolation` NetworkPolicy, which allowed 443 to anywhere and
  would have widened the knight's policy (NetworkPolicies are additive).

## In-cluster Services

Listing `searxng.selfhosted.svc.cluster.local` under `allowedHosts` does **not**
open the Service. `toFQDNs` rules match names resolved through Cilium's DNS
proxy to addresses outside the cluster; an in-cluster name resolves to a
ClusterIP, which Cilium treats as a cluster identity, so the FQDN rule never
matches it. One shared `allowedPorts` list would also open every in-cluster
port to every allowed host. The operator warns about such entries: an
`allowedHosts` entry ending in `.svc` or `.cluster.local` adds a note to the
`EgressRestricted` message and a Warning event (`InClusterAllowedHost`). The check is on the
suffix, so a short in-cluster name (`svc.ns` or a bare `svc`) is not
recognised as in-cluster: it is not warned about, not derived as a Service from
an MCP URL, and, as the OTLP endpoint or NATS URL, is treated as an external
host: it gets a `toFQDNs` rule, which cannot match the DNS query the pod sends
(the resolver expands a short name with the search domains, so the query is for
`<name>.<ns>.svc.cluster.local`). Use the full
`<service>.<namespace>.svc[.cluster.local]` form for `allowedHosts`, the OTLP
endpoint and the NATS URL.

Use `allowedServices` instead:

```yaml
spec:
  egress:
    allowedServices:
      - name: searxng
        namespace: selfhosted
        ports: [8080]
      - name: home-assistant
        namespace: home
        ports: [8123]
```

- The operator reads each Service and renders one rule that selects the
  Service's pods: `toEndpoints` with the namespace label and the Service's pod
  selector (Cilium), or `namespaceSelector` + `podSelector` (fallback
  `NetworkPolicy`).
- `ports` are the **Service** ports the knight dials. Network policies apply
  after the ClusterIP is translated to a pod, so the rule allows each listed
  port's `targetPort` (a named `targetPort` stays a named port) with the
  port's protocol. A listed port the Service does not expose is allowed as a
  pod port as-is. Only the listed ports are opened.
- A Service that does not exist, or has no pod selector (`ExternalName`, or
  hand-managed Endpoints), is left out of the policy. On Cilium the knight
  reports `EgressRestricted=True` with reason `ServiceNotFound` and names the
  Service in the message, and a Warning event `ServiceNotFound` is recorded.
- The operator watches Services: creating, deleting or changing the selector
  or ports of an allowed Service re-renders the policy of every knight that
  uses it (directly or through its table's `defaults.egress`).
- Mission knights get `allowedServices` from their template or inline spec,
  like the rest of `egress`.

### Services on the host network

Some Services are backed by pods with `hostNetwork: true` (home-automation
software that needs mDNS or device discovery, for example). Such a pod has no
Cilium endpoint identity: traffic to it is traffic to its node (`host` on the
same node, `remote-node` on the others), so a `toEndpoints` selector never
matches it. The operator reads the Service's ready EndpointSlice endpoints and,
when every ready endpoint is a pod with `hostNetwork: true`, renders the
allowance differently:

- Cilium: `toEntities: [host, remote-node]` limited to the Service's listed
  ports, resolved to the port numbers in the EndpointSlices.
- Fallback `NetworkPolicy`: the endpoint addresses as `/32` (`/128`) `ipBlock`
  peers on those ports, refreshed when the EndpointSlice changes.

**This is wider than one Service**: on Cilium the knight can reach *every node*
on those ports, not only the node running the pod. The `EgressRestricted`
message says so (`Service ns/name uses the host network: reaches every node on
port N`). The operator watches EndpointSlices, so the rule changes back to
`toEndpoints` when the Service's pods move to the pod network, and the other
way round. A Service with a mix of host-network and ordinary pods, or with no
ready endpoint, keeps the pod-selector rule. A *named* `targetPort` is
resolved from the EndpointSlice ports; if it cannot be resolved (no ready
endpoint carries the name), that port is skipped and allows nothing, so check
the `EgressRestricted` message after a rename. A numeric `targetPort` (or, if
unset, the Service `port`) is the fallback. Node addresses or CIDRs listed
directly and selector-less Services with manual endpoints are not covered.

See [`examples/egress/in-cluster-services.yaml`](../examples/egress/in-cluster-services.yaml).

## MCP servers: listing the server is enough

A knight's `spec.mcpServers[].url` is reachable without repeating the
destination under `spec.egress`. The operator adds a rule for each URL:

- `http(s)://<service>.<namespace>.svc[.cluster.local][:port]/…` is treated as
  an `allowedServices` entry for that Service and port (default 80 for `http`,
  443 for `https`).
- Any other host is treated as an `allowedHosts` entry on that port.

```yaml
spec:
  mcpServers:
    - name: miniflux
      url: http://miniflux-mcp.selfhosted.svc:8080/mcp   # Service rule, port 8080
    - name: web-fetch
      url: https://fetch.example.com/mcp                 # host rule, port 443
  # no egress block needed for either
```

- A destination already covered by the knight's own `egress` (same Service and
  port, or a host listed or wildcarded on that port) is not added twice.
- Nothing is derived for a knight with `isolation.egress: unrestricted` (it has
  no policy), or when the install's egress mode is `off` and the knight has no
  `egress`.
- The derived entries are shown in `status.derivedEgress`
  (`allowedServices`, `allowedHosts`) so they can be told apart from the listed
  ones.
- A URL that is not `http(s)` or has no valid host sets `EgressRestricted=False`
  with reason `EgressPolicyFailed`, naming the server and URL. The policy is
  still rendered from the valid entries (fail closed).
- Mission knights are Knights, so the same derivation applies to the
  `mcpServers` of their template or inline spec.

## Cilium vs fallback

| Cluster | Result | `EgressRestricted` condition |
| --- | --- | --- |
| `cilium.io/v2` `CiliumNetworkPolicy` is served | Hostname allowlist enforced with `toFQDNs` | `True`, reason `FQDNPolicyApplied` (`ServiceNotFound` when an allowed Service is missing or has no selector) |
| Not served | A plain `NetworkPolicy` allows DNS, NATS, an in-cluster collector's namespace and, to **any** destination, the Kubernetes API ports (443, 6443), an external collector's port, the baseline hosts' ports and the allowed **ports**; hostnames cannot be enforced. Allowed Services are enforced by namespace and pod selector on their ports | `False`, reason `FQDNPolicyUnavailable` |
| Applying the policy failed | See the condition message | `False`, reason `EgressPolicyFailed` |

Do not treat a fallback cluster as isolated: check `EgressRestricted` before
relying on the allowlist.

```sh
kubectl get knight galahad -o jsonpath='{.status.conditions[?(@.type=="EgressRestricted")]}'
```

## By runtime backend

- **`runtime: deployment`** and **per-knight Sandboxes** (`runtime: sandbox`
  without a template): the operator's policy above. It selects pods by the
  labels `app.kubernetes.io/name=knight` and `app.kubernetes.io/instance=<name>`
  only, so it covers both.
- **Sandboxes created from an agent-sandbox `SandboxTemplate`** get the
  template's `networkPolicy`, not the operator's policy. Upstream's default
  blocks private ranges and cluster DNS, so NATS and DNS must be allowed
  explicitly in the template, and a `networkPolicy` cannot express hostnames:
  the template can only filter by IP/CIDR and port. Use the template for
  coarse limits and expect no per-host allowlist there.

## Tool installs need egress

`tools.nix` and `tools.flakes` need no egress at pod start. The operator's
builder Job builds the packages into a tools image, and the knight mounts that
image read-only at `/nix` as a Kubernetes image volume. Nothing is downloaded
when the pod starts, so the knight needs no package hosts in `allowedHosts`.
Changing the tool set builds a new image; the builder Job pushes it to the
registry, which is not part of the knight's allowlist. The kubelet, not the
knight pod, pulls the tools image, so the registry needs no `allowedHosts`
entry either.

When enabling isolation on an existing knight, check its `tools` first; see
`examples/tristan.yaml`.

## Rules the allowlist cannot express

`spec.egress` stays hosts, ports and services. It will not gain L7 rules,
CIDRs, entities or ICMP rules. For those, write your own `CiliumNetworkPolicy`
in the knight's namespace. The operator does not manage it: it never creates,
updates or deletes it, and it is not removed when the Knight is.

Select the knight's pods with the same two labels the operator's policy uses,
`app.kubernetes.io/name: knight` and `app.kubernetes.io/instance: <name>`. Both
runtime backends set them. Cilium allow rules are additive, so a second policy
adds allowances on top of `knight-<name>-egress` and cannot remove one. This is
standard Cilium behaviour; it has not been tested on a live cluster for this
operator.

```yaml
apiVersion: cilium.io/v2
kind: CiliumNetworkPolicy
metadata:
  name: galahad-extra-egress
  namespace: roundtable
spec:
  endpointSelector:
    matchLabels:
      app.kubernetes.io/name: knight
      app.kubernetes.io/instance: galahad
  egress:
    - toCIDR:
        - 10.20.0.0/16        # a LAN range that has no DNS name
      toPorts:
        - ports:
            - port: "8443"
              protocol: TCP
```

To filter HTTP requests as well, add a `rules.http` list under the `toPorts`
entry, as in Cilium's own documentation. Keep the policy's selector to the two
labels above so it covers exactly one knight.

On a cluster without Cilium, a plain `NetworkPolicy` with the same selector is
also additive, but it cannot express L7 rules, and the example above is a
Cilium policy, so it applies only where `cilium.io/v2` is served.

## Finding the hosts a knight needs

1. Start the knight with a generous allowlist (or temporarily with
   `isolation.egress: unrestricted` and a reason) on a Cilium cluster with
   Hubble.
2. Run its normal tasks, then list what it contacted:

   ```sh
   hubble observe --namespace roundtable --pod galahad --protocol dns --since 1h -o jsonpb \
     | jq -r '.flow.l7.dns.query' | sort -u
   hubble observe --namespace roundtable --pod galahad --verdict DROPPED --since 1h
   ```

3. Add the hosts the task really needs to `allowedHosts`; re-run and watch for
   `DROPPED` verdicts. A host that appears only in a dropped flow is either a
   missing entry or something the knight should not reach.
4. Remember CDN and redirect hosts (for example `*.githubusercontent.com`).

## Starter allowlists

[`examples/egress/`](../examples/egress/) has starter allowlists for GitHub
read access, package documentation, model providers, a research knight and a
feed-reading knight. They are not bound to anything by default: copy what you
need into a knight or a table's `defaults.egress`.

`package-docs.yaml` is for documentation and registry metadata only: it
allows the doc sites and registry APIs, not the hosts that serve package
downloads (for example `files.pythonhosted.org` or `static.crates.io`), so it
is not enough to install dependencies.

## Out of scope

Mission-level allowlists chosen by the planner, and egress-denied metrics or
span events (#153).
