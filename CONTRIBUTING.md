# Contributing to Round Table

Thanks for your interest in contributing! 🏰

## Getting Started

1. **Fork** the repository
2. **Clone** your fork: `git clone https://github.com/YOUR_USER/roundtable.git`
3. **Create a branch**: `git checkout -b feat/your-feature`
4. **Make changes** and write tests
5. **Submit a PR** against `main`

## Development Setup

### Prerequisites

- Go 1.26+
- Docker
- kubectl + a Kubernetes cluster (kind, minikube, etc.)
- [kubebuilder](https://book.kubebuilder.io/) (for CRD generation)
- NATS CLI (`nats`) — optional, for debugging

### Nix dev shell (recommended)

`flake.nix` pins the whole toolchain: Go, golangci-lint v2.8.0, helm v3.19.x,
controller-gen v0.20.x, kustomize, kubectl, kind, setup-envtest and the envtest
assets (kube-apiserver/etcd, exported as `KUBEBUILDER_ASSETS`). With
[Nix flakes](https://nixos.org/download) enabled:

```bash
nix develop        # or `direnv allow` with `use flake` in .envrc
make test lint helm-check
```

The Makefile prefers tools found on `PATH` and an existing `KUBEBUILDER_ASSETS`,
and falls back to downloading/building them into `./bin` when they are missing,
so Nix is optional. When changing a `*_VERSION` in the Makefile, update the
matching version and hashes in `flake.nix` (`nix flake check` verifies them).
Humans and knights should use the same shell.

The same toolchain is also available as an installable environment,
`packages.<system>.knight-tools` (a `buildEnv` sharing the tool list with the
dev shell, so versions cannot drift), with the envtest assets linked at
`share/envtest`:

```bash
out=$(nix build .#knight-tools --no-link --print-out-paths)
PATH=$out/bin:$PATH KUBEBUILDER_ASSETS=$out/share/envtest make test
```

### Build & Test

```bash
# Build the operator
make build

# Run tests
make test

# End-to-end tests (needs Docker, kind, helm and kubectl; no LLM)
make e2e TIER=core

# Generate CRDs after type changes
make manifests generate

# Run locally against a cluster
make run

# Build Docker image
make docker-build IMG=roundtable-operator:dev
```

`make e2e TIER=core` builds the operator image, creates a kind cluster
(`roundtable-operator-test-e2e`), loads the image, installs NATS with JetStream
(official `nats` chart, pinned) into `database` and `charts/roundtable-operator`
with the local image (dashboard, gateway and planner off), then checks that the
operator pod is Ready, logs no errors in the first minute and that a minimal
Knight (`test/e2e/testdata/knight-core.yaml`) gets a Deployment with one
available replica. The cluster is deleted afterwards, also on failure.
`make test-e2e` is an alias.

Scenarios are gated by Ginkgo labels, one per tier; `TIER` picks the tier
(`core` by default). `make e2e-tiers` lists every tier and what it proves, and
`make e2e TIER=<typo>` fails at once with the valid tiers, before any cluster is
created. `all` runs everything. The `restart` tier needs the fix from #700
(chain steps stranded by an operator restart); without it the spec times out
waiting for the step's result to be read. It is not part of the default
`TIER=core` run. After the run, `make e2e` prints a table of passed, failed and
skipped scenarios per tier with the time each tier took. A new tier adds its own
label and files without touching core. Plain `go test ./...` does not build the
e2e package (it needs `-tags=e2e`).


### Helm Chart

```bash
# Install from local chart
helm install roundtable charts/roundtable-operator \
  --namespace roundtable --create-namespace

# Upgrade
helm upgrade roundtable charts/roundtable-operator \
  --namespace roundtable
```

## What to Work On

- Check [open issues](https://github.com/roundtable-k8s/roundtable/issues) — look for `good first issue` labels
- Bug fixes are always welcome
- Feature ideas? Open an issue first to discuss
- Proposing a new built-in capability? Say in the issue which existing component you considered and why it does not fit (see [principles](docs/principles.md#9-dont-rebuild-the-wheel))
- Cite the [decision register](docs/decisions.md) by number ("decision 8") when an issue or PR rests on a product decision. To record a new one, add a dated entry to the register in a pull request

## Code Style

- Follow standard Go conventions (`gofmt`, `go vet`)
- Write tests for new functionality
- Keep CRD types well-documented with `// +kubebuilder` markers
- Use structured logging (`logf.FromContext(ctx)`)

## CRD Changes

When modifying types in `api/v1alpha1/`:

1. Update the Go types
2. Run `make manifests generate` to regenerate CRDs and DeepCopy
3. Update Helm chart CRDs: `cp config/crd/bases/* charts/roundtable-operator/crds/`
4. Bump chart version in `Chart.yaml`

## Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add warm pool support for instant mission assembly
fix: prevent race condition in warm knight claiming
docs: add architecture overview
test: add chain controller integration tests
chore: bump Go to 1.26
```

## Pull Requests

- Keep PRs focused — one feature or fix per PR
- Include tests
- Update documentation if behavior changes
- Link related issues
- A PR that adds a write verb to any ServiceAccount the chart creates must default it off and say so in its description

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
