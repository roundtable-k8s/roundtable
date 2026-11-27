//go:build e2e
// +build e2e

/*
Copyright 2026 dapperdivers.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/roundtable-k8s/roundtable/test/utils"
)

const (
	// managerImage is the manager image to be built and loaded for testing.
	managerImage = "example.com/roundtable-operator:v0.0.1"
	// stubKnightImage is the no-LLM knight runtime (test/e2e/stubknight) the
	// Chain trace case runs its steps on.
	stubKnightImage = "example.com/roundtable-e2e-stub-knight:v0.0.1"

	// stubLLMImage is the stub LLM (test/e2e/stubllm), built by `make stubllm-image`.
	stubLLMImage = "stubllm:e2e"

	// stubGitHubImage is the stub GitHub API (test/e2e/stubgithub), built by `make stubgithub-image`.
	stubGitHubImage = "stubgithub:e2e"

	// NATS is installed once for the whole suite, by every tier, at the
	// operator's default URL (nats://nats.database.svc:4222).
	natsNamespace = "database"
	natsRelease   = "nats"
	natsValues    = "test/e2e/testdata/nats-values.yaml"
	// natsChartVersion pins the official nats chart (https://nats-io.github.io/k8s/helm/charts/).
	natsChartVersion = "1.3.6"
	natsChartRepo    = "https://nats-io.github.io/k8s/helm/charts/"
)

// Tier labels. Every scenario carries exactly one; `make e2e TIER=<tier>`
// runs the scenarios of that tier (TIER=all runs every tier). A later tier
// adds its own label and files without touching core.
const (
	tierCore    = "core"
	tierTrace   = "trace"
	tierTrigger = "trigger"
	tierLost    = "lost"
	tierRestart = "restart"
	tierGateway = "gateway"
	tierChain   = "chain"
	tierGitHub  = "github"
)

// tier is the selected tier (TIER env, default core).
func tier() string {
	if t := os.Getenv("TIER"); t != "" {
		return t
	}
	return tierCore
}

// tierSelected reports whether scenarios labelled with t will run.
func tierSelected(t string) bool {
	return tier() == "all" || tier() == t
}

// TestE2E runs the e2e test suite in an isolated Kind cluster, restricted to
// the scenarios of the selected tier.
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting roundtable-operator e2e test suite (tier %q)\n", tier())
	suiteConfig, reporterConfig := GinkgoConfiguration()
	if tier() != "all" && suiteConfig.LabelFilter == "" {
		suiteConfig.LabelFilter = tier()
	}
	RunSpecs(t, "e2e suite", suiteConfig, reporterConfig)
}

var _ = BeforeSuite(func() {
	By("building the manager image")
	cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", managerImage))
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the manager image")

	By("loading the manager image on Kind")
	err = utils.LoadImageToKindClusterWithName(managerImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the manager image into Kind")

	if tierSelected(tierCore) {
		By("building and loading the stub LLM image")
		_, err = utils.Run(exec.Command("make", "stubllm-image"))
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the stub LLM image")
		err = utils.LoadImageToKindClusterWithName(stubLLMImage)
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the stub LLM image into Kind")
	}

	if tierSelected(tierTrace) || tierSelected(tierTrigger) || tierSelected(tierLost) || tierSelected(tierRestart) ||
		tierSelected(tierGateway) || tierSelected(tierChain) ||
		tierSelected(tierGitHub) {
		buildStubKnightImage()
	}

	if tierSelected(tierGitHub) {
		By("building and loading the stub GitHub image")
		_, err = utils.Run(exec.Command("make", "stubgithub-image"))
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the stub GitHub image")
		err = utils.LoadImageToKindClusterWithName(stubGitHubImage)
		ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the stub GitHub image into Kind")
	}

	By("installing NATS with JetStream")
	_, err = utils.Run(exec.Command("helm", "upgrade", "--install", natsRelease, "nats",
		"--repo", natsChartRepo, "--version", natsChartVersion,
		"-n", natsNamespace, "--create-namespace", "-f", natsValues, "--wait", "--timeout", "5m"))
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to install NATS")
})

var _ = AfterSuite(func() {
	_, _ = utils.Run(exec.Command("helm", "uninstall", natsRelease, "-n", natsNamespace, "--wait"))
	_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", natsNamespace, "--ignore-not-found", "--wait=false"))
})

// buildStubKnightImage builds the stub knight binary on the host, packs it into
// an image and loads it into Kind.
func buildStubKnightImage() {
	By("building the stub knight image")
	dir, err := os.MkdirTemp("", "stubknight")
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	defer func() { _ = os.RemoveAll(dir) }()
	// Run replaces cmd.Env, so the env for a static Linux binary is set in sh.
	cmd := exec.Command("sh", "-c", `CGO_ENABLED=0 GOOS=linux go build -o "$0/stubknight" ./test/e2e/stubknight`, dir)
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the stub knight")
	cmd = exec.Command("docker", "build", "-t", stubKnightImage, "-f", "test/e2e/stubknight/Dockerfile", dir)
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the stub knight image")

	By("loading the stub knight image on Kind")
	err = utils.LoadImageToKindClusterWithName(stubKnightImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the stub knight image into Kind")
}
