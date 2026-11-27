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

// The restart tier (#739): a Chain step in flight when the operator pod is
// replaced by a rollout still completes, with no change to the Chain. It needs
// the fix from #700; without it the first spec times out waiting for step one's
// result to be read.
const (
	restartNamespace = "roundtable-restart"
	restartRelease   = "rt-restart"
	restartOperator  = "rt-restart" // fullnameOverride in restart-values.yaml
	restartValues    = "test/e2e/testdata/restart-values.yaml"
	restartManifests = "test/e2e/testdata/restart.yaml"
	restartKnight    = "stub"
	restartChain     = "restart-e2e"
)

var _ = Describe("Operator restart", Ordered, Label(tierRestart), func() {
	kget := func(kind, name, jsonpath string) (string, error) {
		out, err := utils.Run(exec.Command("kubectl", "get", kind, name, "-n", restartNamespace,
			"-o", "jsonpath="+jsonpath))
		return strings.TrimSpace(out), err
	}

	BeforeAll(func() {
		By("installing the operator chart")
		_, err := utils.Run(exec.Command("helm", "upgrade", "--install", restartRelease, "charts/roundtable-operator",
			"-n", restartNamespace, "--create-namespace", "-f", restartValues, "--wait", "--timeout", "5m"))
		Expect(err).NotTo(HaveOccurred(), "Failed to install the operator chart")

		By("creating the table, the stub knight and the two-step Chain")
		_, err = utils.Run(exec.Command("kubectl", "apply", "-f", restartManifests))
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the stub knight to be Ready")
		Eventually(func(g Gomega) {
			out, err := kget("knight", restartKnight, "{.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Ready"))
		}, 5*time.Minute, 2*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		_, _ = utils.Run(exec.Command("kubectl", "delete", "chain,knight,roundtable", "--all",
			"-n", restartNamespace, "--ignore-not-found", "--timeout=2m"))
		_, _ = utils.Run(exec.Command("helm", "uninstall", restartRelease, "-n", restartNamespace, "--wait"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", restartNamespace, "--ignore-not-found", "--wait=false"))
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		for _, args := range [][]string{
			{"get", "chain", restartChain, "-n", restartNamespace, "-o", "yaml"},
			{"get", "knight,pods", "-n", restartNamespace, "-o", "wide"},
			{"logs", "deployment/" + restartOperator, "-n", restartNamespace, "--tail=200"},
			{"logs", "-l", "app.kubernetes.io/name=knight", "-n", restartNamespace, "--tail=100", "--timestamps"},
			{"get", "events", "-n", restartNamespace, "--sort-by=.lastTimestamp"},
		} {
			out, err := utils.Run(exec.Command("kubectl", args...))
			_, _ = fmt.Fprintf(GinkgoWriter, "kubectl %s:\n%s\n%v\n", strings.Join(args, " "), out, err)
		}
	})

	It("completes a chain step that was in flight when the operator was rolled out", func() {
		By("starting the Chain run")
		_, err := utils.Run(exec.Command("kubectl", "annotate", "chain", restartChain, "-n", restartNamespace,
			"--overwrite", "ai.roundtable.io/run=run-1"))
		Expect(err).NotTo(HaveOccurred())

		By("waiting until step one is Running with a taskId")
		var taskID string
		Eventually(func(g Gomega) {
			out, err := kget("chain", restartChain,
				`{.status.stepStatuses[?(@.name=="slow")].phase}|{.status.stepStatuses[?(@.name=="slow")].taskId}`)
			g.Expect(err).NotTo(HaveOccurred())
			f := strings.Split(out, "|")
			g.Expect(f).To(HaveLen(2))
			g.Expect(f[0]).To(Equal("Running"))
			g.Expect(f[1]).NotTo(BeEmpty())
			taskID = f[1]
		}, 2*time.Minute, time.Second).Should(Succeed())

		By("rolling out the operator while the step sleeps")
		_, err = utils.Run(exec.Command("kubectl", "rollout", "restart", "deployment/"+restartOperator,
			"-n", restartNamespace))
		Expect(err).NotTo(HaveOccurred())
		_, err = utils.Run(exec.Command("kubectl", "rollout", "status", "deployment/"+restartOperator,
			"-n", restartNamespace, "--timeout=2m"))
		Expect(err).NotTo(HaveOccurred())

		// The stub logs "handled task <id>" once it has published the result.
		By("waiting until the stub knight has published step one's result")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "logs", "-n", restartNamespace,
				"-l", "app.kubernetes.io/name=knight", "--tail=-1"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("handled task " + taskID))
		}, 2*time.Minute, 2*time.Second).Should(Succeed())

		By("expecting the Chain to succeed within 60 s of the result, with no change to it")
		Eventually(func(g Gomega) {
			phase, err := kget("chain", restartChain, "{.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).NotTo(Equal("Failed"), "chain run failed")
			g.Expect(phase).To(Equal("Succeeded"))
		}, 60*time.Second, 2*time.Second).Should(Succeed(),
			"the operator did not read step one's result after the rollout (#700)")

		By("checking that step two ran")
		out, err := kget("chain", restartChain, `{range .status.stepStatuses[*]}{.name}={.phase}{"\n"}{end}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(utils.GetNonEmptyLines(out)).To(Equal([]string{"slow=Succeeded", "after=Succeeded"}))
		output, err := kget("chain", restartChain, `{.status.stepStatuses[?(@.name=="after")].output}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(output).NotTo(BeEmpty())
	})
})
