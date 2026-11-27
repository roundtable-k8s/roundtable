# Image URL to use all building/pushing image targets
IMG ?= controller:latest

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

# agent-sandbox release whose CRDs are vendored under test/crds/agent-sandbox/
# for the sandbox envtest. This is the only place the version is pinned; bump
# it and run `make update-agent-sandbox-crds`.
AGENT_SANDBOX_VERSION ?= v1.0.12
AGENT_SANDBOX_CRD_FILES = agents.x-k8s.io_sandboxes.yaml \
	extensions.agents.x-k8s.io_sandboxclaims.yaml \
	extensions.agents.x-k8s.io_sandboxtemplates.yaml \
	extensions.agents.x-k8s.io_sandboxwarmpools.yaml

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	"$(CONTROLLER_GEN)" rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	"$(CONTROLLER_GEN)" object:headerFile="hack/boilerplate.go.txt" paths="./..."

# Knight task contract (pkg/nats/contract): schemas and fixtures copied from
# roundtable-k8s/knights at a pinned commit, plus Go types generated from them.
KNIGHTS_REPO ?= https://github.com/roundtable-k8s/knights.git
CONTRACT_DIR := pkg/nats/contract

.PHONY: contract-sync
contract-sync: ## Copy the knight task contract schemas and fixtures from roundtable-k8s/knights at KNIGHTS_REF (a commit), then regenerate the Go types.
	@test -n "$(KNIGHTS_REF)" || { echo "usage: make contract-sync KNIGHTS_REF=<knights commit>" >&2; exit 1; }
	@set -e; tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; \
	git -C "$$tmp" init -q; \
	git -C "$$tmp" fetch -q --depth 1 "$(KNIGHTS_REPO)" "$(KNIGHTS_REF)"; \
	git -C "$$tmp" checkout -q FETCH_HEAD; \
	rm -f $(CONTRACT_DIR)/schema/*.schema.json $(CONTRACT_DIR)/testdata/*.json; \
	mkdir -p $(CONTRACT_DIR)/schema $(CONTRACT_DIR)/testdata; \
	cp "$$tmp"/packages/core/contract/*.schema.json $(CONTRACT_DIR)/schema/; \
	cp "$$tmp"/packages/core/contract/fixtures/*.json $(CONTRACT_DIR)/testdata/; \
	git -C "$$tmp" rev-parse HEAD > $(CONTRACT_DIR)/VERSION; \
	echo "contract synced to knights@$$(cat $(CONTRACT_DIR)/VERSION)"
	$(MAKE) generate-contract

.PHONY: generate-contract
generate-contract: ## Regenerate pkg/nats/contract/zz_generated.go from the vendored schemas (go-jsonschema, pinned as a go.mod tool).
	CGO_ENABLED=0 go generate ./$(CONTRACT_DIR)

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: update-agent-sandbox-crds
update-agent-sandbox-crds: ## Re-download the agent-sandbox CRDs ($(AGENT_SANDBOX_VERSION)) used by the sandbox envtest.
	@mkdir -p test/crds/agent-sandbox
	@for f in $(AGENT_SANDBOX_CRD_FILES); do \
		echo "Downloading $$f ($(AGENT_SANDBOX_VERSION))"; \
		curl -fsSL -o "test/crds/agent-sandbox/$$f" \
			"https://raw.githubusercontent.com/kubernetes-sigs/agent-sandbox/$(AGENT_SANDBOX_VERSION)/k8s/crds/$$f" || exit 1; \
	done

.PHONY: test
test: manifests generate fmt vet $(if $(KUBEBUILDER_ASSETS),,setup-envtest) ## Run tests (uses KUBEBUILDER_ASSETS when set, e.g. by `nix develop`).
	KUBEBUILDER_ASSETS="$(or $(KUBEBUILDER_ASSETS),$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path))" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# TODO(user): To use a different vendor for e2e tests, modify the setup under 'tests/e2e'.
# The default setup assumes Kind is pre-installed and builds/loads the Manager Docker image locally.
# CertManager is installed by default; skip with:
# - CERT_MANAGER_INSTALL_SKIP=true
KIND_CLUSTER ?= roundtable-operator-test-e2e
TILT_KIND_CLUSTER ?= roundtable-dev
TILT_ARGS ?= up

.PHONY: tilt-up
tilt-up: ## Create the dev kind cluster if missing and run tilt up (docs/local-development.md)
	@command -v $(KIND) >/dev/null 2>&1 || { echo "kind is not installed."; exit 1; }
	@command -v tilt >/dev/null 2>&1 || { echo "tilt is not installed (https://docs.tilt.dev/install.html)."; exit 1; }
	@if $(KIND) get clusters | grep -qx '$(TILT_KIND_CLUSTER)'; then \
		echo "Kind cluster '$(TILT_KIND_CLUSTER)' already exists. Skipping creation."; \
	else \
		$(KIND) create cluster --name $(TILT_KIND_CLUSTER) --wait 120s; \
	fi
	kubectl config use-context kind-$(TILT_KIND_CLUSTER)
	tilt $(TILT_ARGS)

.PHONY: tilt-down
tilt-down: ## Tear down the dev kind cluster created by tilt-up
	@$(KIND) delete cluster --name $(TILT_KIND_CLUSTER)

.PHONY: setup-test-e2e
setup-test-e2e: ## Set up a Kind cluster for e2e tests if it does not exist
	@command -v $(KIND) >/dev/null 2>&1 || { \
		echo "Kind is not installed. Please install Kind manually."; \
		exit 1; \
	}
	@case "$$($(KIND) get clusters)" in \
		*"$(KIND_CLUSTER)"*) \
			echo "Kind cluster '$(KIND_CLUSTER)' already exists. Skipping creation." ;; \
		*) \
			echo "Creating Kind cluster '$(KIND_CLUSTER)'..."; \
			$(KIND) create cluster --name $(KIND_CLUSTER) ;; \
	esac

# TIER selects the e2e scenarios (Ginkgo labels in test/e2e): core (default),
# trace, trigger, or all. Needs docker, kind, helm and kubectl; see CONTRIBUTING.md.
TIER ?= core

.PHONY: e2e
e2e: setup-test-e2e manifests generate fmt vet ## Run the e2e tests of TIER (default core) on a Kind cluster with NATS and the Helm chart, then delete the cluster.
	@status=0; \
	TIER=$(TIER) KIND=$(KIND) KIND_CLUSTER=$(KIND_CLUSTER) go test -tags=e2e -timeout 30m ./test/e2e/ -v -ginkgo.v || status=$$?; \
	$(MAKE) cleanup-test-e2e; \
	exit $$status

.PHONY: test-e2e
test-e2e: e2e ## Alias for e2e.

# Image of the e2e stub LLM (test/e2e/stubllm): an OpenAI-compatible server with
# scripted replies. The binary is built into bin/ on the host, then packaged.
.PHONY: stubllm-image
stubllm-image: ## Build the e2e stub LLM image as stubllm:e2e.
	mkdir -p bin/stubllm-image
	CGO_ENABLED=0 go build -o bin/stubllm-image/stubllm ./test/e2e/stubllm
	$(CONTAINER_TOOL) build -t stubllm:e2e -f test/e2e/stubllm/Dockerfile bin/stubllm-image

.PHONY: cleanup-test-e2e
cleanup-test-e2e: ## Tear down the Kind cluster used for e2e tests
	@$(KIND) delete cluster --name $(KIND_CLUSTER)

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	"$(GOLANGCI_LINT)" run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	"$(GOLANGCI_LINT)" run --fix

.PHONY: helm-check
helm-check: helm ## Lint the Helm chart (also with values-minimal.yaml) and check examples/chain-pr-pipeline.yaml equals its rendered PR pipeline.
	HELM="$(HELM)" scripts/check-pr-pipeline-example.sh
	HELM="$(HELM)" scripts/check-dashboard-rbac.sh
	HELM="$(HELM)" scripts/check-gateway-default.sh
	HELM="$(HELM)" scripts/check-leader-election.sh
	HELM="$(HELM)" scripts/check-pod-scheduling.sh
	HELM="$(HELM)" scripts/check-image-tag.sh
	HELM="$(HELM)" scripts/check-networkpolicy.sh
	HELM="$(HELM)" scripts/check-metrics.sh
	HELM="$(HELM)" scripts/check-trigger-scheduling.sh
	HELM="$(HELM)" scripts/check-prometheusrule.sh
	HELM="$(HELM)" scripts/check-grafana-dashboards.sh
	HELM="$(HELM)" scripts/check-telemetry-env.sh
	HELM="$(HELM)" scripts/check-observability-example.sh
	scripts/check-observability-doc.sh
	scripts/check-metrics-doc.sh
	scripts/check-doc-links.sh
	scripts/check-install-versions.sh
	scripts/check-helm-values-doc.sh
	HELM="$(HELM)" scripts/check-isolation-env.sh
	HELM="$(HELM)" scripts/check-minimal-values.sh
	HELM="$(HELM)" scripts/check-quickstart-values.sh
	HELM="$(HELM)" scripts/check-planner.sh

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	"$(GOLANGCI_LINT)" config verify

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager and gateway binaries.
	go build -o bin/manager cmd/main.go
	go build -o bin/gateway ./cmd/gateway

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd/main.go

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# VERSION is reported as service.version in traces.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build --build-arg VERSION=$(VERSION) -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name roundtable-operator-builder
	$(CONTAINER_TOOL) buildx use roundtable-operator-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm roundtable-operator-builder
	rm Dockerfile.cross

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default > dist/install.yaml

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" apply -f -; else echo "No CRDs to install; skipping."; fi

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -; else echo "No CRDs to delete; skipping."; fi

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

## Tool Binaries
# Each tool is taken from PATH when present (the `nix develop` shell in flake.nix
# provides the pinned versions, see the *_VERSION variables below) and otherwise
# installed into $(LOCALBIN). Override any of them on the command line or in the
# environment, e.g. `make HELM=/usr/local/bin/helm helm-check`.
tool-path = $(or $(shell command -v $(1) 2>/dev/null),$(LOCALBIN)/$(1))
KUBECTL ?= kubectl
KIND ?= kind
KUSTOMIZE ?= $(call tool-path,kustomize)
CONTROLLER_GEN ?= $(call tool-path,controller-gen)
ENVTEST ?= $(call tool-path,setup-envtest)
# `make lint` needs a golangci-lint built with the module plugins from
# .custom-gcl.yml (logcheck), so GOLANGCI_LINT defaults to the custom binary in
# $(LOCALBIN). A stock golangci-lint on PATH is only used to build it (below).
GOLANGCI_LINT ?= $(LOCALBIN)/golangci-lint
GOLANGCI_LINT_STOCK ?= $(shell command -v golangci-lint 2>/dev/null)
HELM ?= $(call tool-path,helm)

## Tool Versions
KUSTOMIZE_VERSION ?= v5.8.1
CONTROLLER_TOOLS_VERSION ?= v0.20.1

#ENVTEST_VERSION is the version of controller-runtime release branch to fetch the envtest setup script (i.e. release-0.20)
ENVTEST_VERSION ?= $(shell v='$(call gomodver,sigs.k8s.io/controller-runtime)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_VERSION manually (controller-runtime replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?([0-9]+)\.([0-9]+).*/release-\1.\2/')

#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell v='$(call gomodver,k8s.io/api)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_K8S_VERSION manually (k8s.io/api replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?[0-9]+\.([0-9]+).*/1.\1/')

GOLANGCI_LINT_VERSION ?= v2.8.0
HELM_VERSION ?= v3.19.0
# Go toolchain pinned by the go.mod `toolchain` directive (also used by CI via setup-go).
# `+auto` keeps a newer local Go but never accepts an older one.
GO_TOOLCHAIN ?= $(shell sed -n "s/^toolchain //p" go.mod)+auto
.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): | $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): | $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@$(call retry,"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path) || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: print-envtest-assets
print-envtest-assets: ## Print the absolute envtest binaries path (run setup-envtest first), for CI to export as KUBEBUILDER_ASSETS.
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -i -p path

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): | $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: helm
helm: $(HELM) ## Download helm locally if necessary.
$(HELM): export CGO_ENABLED = 0
$(HELM): | $(LOCALBIN)
	$(call go-install-tool,$(HELM),helm.sh/helm/v3/cmd/helm,$(HELM_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): | $(LOCALBIN)
	@if [ -n "$(GOLANGCI_LINT_STOCK)" ] && [ -f .custom-gcl.yml ] && [ "$(GOLANGCI_LINT_STOCK)" != "$(GOLANGCI_LINT)" ]; then \
		echo "Building custom golangci-lint with plugins using $(GOLANGCI_LINT_STOCK)..." && \
		GOTOOLCHAIN=$(GO_TOOLCHAIN) "$(GOLANGCI_LINT_STOCK)" custom --destination "$(LOCALBIN)" --name golangci-lint; \
	else \
		$(MAKE) --no-print-directory install-golangci-lint-from-source; \
	fi

.PHONY: install-golangci-lint-from-source
install-golangci-lint-from-source:
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))
	@test -f .custom-gcl.yml && { \
		echo "Building custom golangci-lint with plugins..." && \
		GOTOOLCHAIN=$(GO_TOOLCHAIN) $(GOLANGCI_LINT) custom --destination $(LOCALBIN) --name golangci-lint-custom && \
		mv -f $(LOCALBIN)/golangci-lint-custom $(GOLANGCI_LINT); \
	} || true

# retry runs a command up to 3 times, 5 seconds apart, retrying only on a
# non-zero exit and printing the attempt number. Guards against transient
# network errors (sum.golang.org stream errors, 503s from download hosts).
# $1 - command (shell snippet)
define retry
n=1; until $(1); do \
	rc=$$?; \
	[ $$n -lt 3 ] || { echo "Attempt $$n/3 failed (exit $$rc); giving up" >&2; exit $$rc; }; \
	echo "Attempt $$n/3 failed (exit $$rc); retrying in 5s..." >&2; \
	n=$$((n+1)); sleep 5; \
	echo "Attempt $$n/3..."; \
done
endef

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f "$(1)" ;\
$(call retry,GOBIN="$(LOCALBIN)" go install $${package}) ;\
mv "$(LOCALBIN)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef

define gomodver
$(shell go list -m -f '{{if .Replace}}{{.Replace.Version}}{{else}}{{.Version}}{{end}}' $(1) 2>/dev/null)
endef
