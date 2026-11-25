//go:build e2e

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
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/roundtable-k8s/roundtable/test/utils"
)

// The core tier (#351): NATS with JetStream from the official Helm chart, the
// operator from charts/roundtable-operator with the locally built image, and
// one minimal Knight that must reach an available Deployment.
const (
	coreNamespace  = "roundtable-core"
	coreRelease    = "rt-core"
	coreOperator   = "rt-core" // fullnameOverride in core-values.yaml
	coreValues     = "test/e2e/testdata/core-values.yaml"
	coreKnight     = "core-knight"
	coreKnightYAML = "test/e2e/testdata/knight-core.yaml"
	natsNamespace  = "database"
	natsRelease    = "nats"
	natsValues     = "test/e2e/testdata/nats-values.yaml"
	// natsChartVersion pins the official nats chart (https://nats-io.github.io/k8s/helm/charts/).
	natsChartVersion = "1.3.6"
	natsChartRepo    = "https://nats-io.github.io/k8s/helm/charts/"
)

var _ = Describe("Core install", Ordered, Label(tierCore), func() {
	BeforeAll(func() {
		SetDefaultEventuallyTimeout(3 * time.Minute)
		SetDefaultEventuallyPollingInterval(2 * time.Second)

		By("installing NATS with JetStream")
		_, err := utils.Run(exec.Command("helm", "upgrade", "--install", natsRelease, "nats",
			"--repo", natsChartRepo, "--version", natsChartVersion,
			"-n", natsNamespace, "--create-namespace", "-f", natsValues, "--wait", "--timeout", "5m"))
		Expect(err).NotTo(HaveOccurred(), "Failed to install NATS")

		By("installing the operator chart with the local image")
		_, err = utils.Run(exec.Command("helm", "upgrade", "--install", coreRelease, "charts/roundtable-operator",
			"-n", coreNamespace, "--create-namespace", "-f", coreValues, "--wait", "--timeout", "5m"))
		Expect(err).NotTo(HaveOccurred(), "Failed to install the operator chart")
	})

	AfterAll(func() {
		// Delete the Knight while the operator still runs its finalizers.
		_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", coreKnightYAML,
			"--ignore-not-found", "--timeout=2m"))
		_, _ = utils.Run(exec.Command("helm", "uninstall", coreRelease, "-n", coreNamespace, "--wait"))
		_, _ = utils.Run(exec.Command("helm", "uninstall", natsRelease, "-n", natsNamespace, "--wait"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", coreNamespace, natsNamespace,
			"--ignore-not-found", "--wait=false"))
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		for _, args := range [][]string{
			{"get", "pods", "-A", "-o", "wide"},
			{"logs", "deployment/" + coreOperator, "-n", coreNamespace, "--tail=200"},
			{"get", "knight", coreKnight, "-n", coreNamespace, "-o", "yaml"},
			{"describe", "deployment", coreKnight, "-n", coreNamespace},
			{"get", "events", "-A", "--sort-by=.lastTimestamp"},
		} {
			out, err := utils.Run(exec.Command("kubectl", args...))
			_, _ = fmt.Fprintf(GinkgoWriter, "kubectl %s:\n%s\n%v\n", strings.Join(args, " "), out, err)
		}
	})

	It("runs a Ready operator pod", func() {
		_, err := utils.Run(exec.Command("kubectl", "rollout", "status", "deployment/"+coreOperator,
			"-n", coreNamespace, "--timeout=3m"))
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "pods", "-n", coreNamespace,
				"-l", "app.kubernetes.io/instance="+coreRelease, "-o",
				"jsonpath={range .items[*]}{.status.conditions[?(@.type=='Ready')].status}{\"\\n\"}{end}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(utils.GetNonEmptyLines(out)).To(ContainElement("True"))
		}).Should(Succeed())
	})

	It("logs no errors in the first minute", func() {
		// Let the operator reconcile for a minute, then scan its log (JSON
		// lines from zap; error-level lines carry "level":"error").
		time.Sleep(time.Minute)
		out, err := utils.Run(exec.Command("kubectl", "logs", "deployment/"+coreOperator,
			"-n", coreNamespace, "--since=2m"))
		Expect(err).NotTo(HaveOccurred())
		var errs []string
		for _, line := range utils.GetNonEmptyLines(out) {
			if strings.Contains(line, `"level":"error"`) || strings.Contains(line, "\tERROR\t") {
				errs = append(errs, line)
			}
		}
		Expect(errs).To(BeEmpty(), "operator logged errors:\n%s", strings.Join(errs, "\n"))
	})

	It("gives a minimal Knight a Deployment with an available replica", func() {
		_, err := utils.Run(exec.Command("kubectl", "apply", "-f", coreKnightYAML))
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "deployment", coreKnight,
				"-n", coreNamespace, "-o", "jsonpath={.status.availableReplicas}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("1"))
		}, 5*time.Minute).Should(Succeed())
	})
})
