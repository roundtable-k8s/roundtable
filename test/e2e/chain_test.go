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

// The chain tier (#872): the Chain engine itself on the stub knight, with no
// LLM. Step output feeds the next step, a false `when` skips a step without
// dispatching a task, and a forEach step fans out once per list item.
const (
	chainNamespace = "roundtable-chain"
	chainRelease   = "rt-chain"
	chainOperator  = "rt-chain" // fullnameOverride in chain-values.yaml
	chainValues    = "test/e2e/testdata/chain-values.yaml"
	chainManifests = "test/e2e/testdata/chain.yaml"
	chainName      = "engine"
	chainKnight    = "stub"
)

var _ = Describe("Chain engine", Ordered, Label(tierChain), func() {
	kget := func(args ...string) (string, error) {
		out, err := utils.Run(exec.Command("kubectl", append([]string{"get", "-n", chainNamespace}, args...)...))
		return strings.TrimSpace(out), err
	}
	// stepField reads one jsonpath field of the named entry in status.steps.
	stepField := func(step, field string) string {
		out, err := kget("chain", chainName, "-o",
			fmt.Sprintf("jsonpath={.status.steps[?(@.name==%q)].%s}", step, field))
		Expect(err).NotTo(HaveOccurred())
		return out
	}

	BeforeAll(func() {
		SetDefaultEventuallyTimeout(3 * time.Minute)
		SetDefaultEventuallyPollingInterval(2 * time.Second)

		By("installing the operator chart")
		_, err := utils.Run(exec.Command("helm", "upgrade", "--install", chainRelease, "charts/roundtable-operator",
			"-n", chainNamespace, "--create-namespace", "-f", chainValues, "--wait", "--timeout", "5m"))
		Expect(err).NotTo(HaveOccurred(), "Failed to install the operator chart")

		By("creating the table, the stub knight and the Chain")
		_, err = utils.Run(exec.Command("kubectl", "apply", "-f", chainManifests))
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		_, _ = utils.Run(exec.Command("kubectl", "delete", "chain,knight,roundtable", "--all",
			"-n", chainNamespace, "--ignore-not-found", "--timeout=2m"))
		_, _ = utils.Run(exec.Command("helm", "uninstall", chainRelease, "-n", chainNamespace, "--wait"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", chainNamespace, "--ignore-not-found", "--wait=false"))
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		for _, args := range [][]string{
			{"get", "chain", "-n", chainNamespace, "-o", "yaml"},
			{"get", "knight,pods", "-n", chainNamespace, "-o", "wide"},
			{"logs", "deployment/" + chainOperator, "-n", chainNamespace, "--tail=200"},
			{"logs", "-l", "app.kubernetes.io/name=knight", "-n", chainNamespace, "--tail=100"},
		} {
			out, err := utils.Run(exec.Command("kubectl", args...))
			_, _ = fmt.Fprintf(GinkgoWriter, "kubectl %s:\n%s\n%v\n", strings.Join(args, " "), out, err)
		}
	})

	It("runs the Chain to Succeeded", func() {
		By("waiting for the stub knight to be Ready")
		Eventually(func(g Gomega) {
			out, err := kget("knight", chainKnight, "-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Ready"))
		}, 5*time.Minute).Should(Succeed())

		By("waiting for the run to succeed")
		Eventually(func(g Gomega) {
			out, err := kget("chain", chainName, "-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(Equal("Failed"), "chain run failed")
			g.Expect(out).To(Equal("Succeeded"))
		}, 5*time.Minute).Should(Succeed())
	})

	It("passes step a's output to step b", func() {
		Expect(stepField("a", "phase")).To(Equal("Succeeded"))
		aOut := stepField("a", "output")
		Expect(aOut).To(ContainSubstring("ALPHA-OUT"))
		Expect(stepField("b", "phase")).To(Equal("Succeeded"))
		// The stub echoes the rendered prompt, which quotes a's whole output.
		Expect(stepField("b", "output")).To(ContainSubstring(aOut))
	})

	It("skips step c, whose when is false, without a task", func() {
		Expect(stepField("c", "phase")).To(Equal("Skipped"))
		Expect(stepField("c", "reason")).To(Equal("WhenFalse"))
		Expect(stepField("c", "taskId")).To(BeEmpty(), "a skipped step must not be dispatched")
		Expect(stepField("c", "output")).To(BeEmpty())
	})

	It("fans the forEach step out over exactly two items", func() {
		Expect(stepField("fan", "phase")).To(Equal("Succeeded"))
		Expect(stepField("fan", "fanOut.total")).To(Equal("2"))
		Expect(stepField("fan", "fanOut.succeeded")).To(Equal("2"))
		Expect(stepField("fan", "fanOut.failed")).To(Equal("0"))
		out := stepField("fan", "output")
		Expect(out).To(ContainSubstring("Fan-out item: alpha"))
		Expect(out).To(ContainSubstring("Fan-out item: beta"))
		Expect(strings.Count(out, "Fan-out item:")).To(Equal(2))
	})
})
