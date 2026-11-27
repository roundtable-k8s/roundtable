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

// The mission tier (#977): a meta-Mission is planned by the stub knight (it
// answers the planning task from STUB_PLAN_JSON), the plan's Chain runs on the
// same stub knight and the Mission ends Succeeded, with no LLM.
const (
	missionNamespace = "roundtable-mission"
	missionRelease   = "rt-mission"
	missionOperator  = "rt-mission" // fullnameOverride in mission-values.yaml
	missionValues    = "test/e2e/testdata/mission-values.yaml"
	missionManifests = "test/e2e/testdata/mission.yaml"
	missionName      = "planned"
	missionKnight    = "stub"
)

var _ = Describe("Meta-Mission", Ordered, Label(tierMission), func() {
	kget := func(args ...string) (string, error) {
		out, err := utils.Run(exec.Command("kubectl", append([]string{"get", "-n", missionNamespace}, args...)...))
		return strings.TrimSpace(out), err
	}

	BeforeAll(func() {
		SetDefaultEventuallyTimeout(3 * time.Minute)
		SetDefaultEventuallyPollingInterval(2 * time.Second)

		By("installing the operator chart")
		_, err := utils.Run(exec.Command("helm", "upgrade", "--install", missionRelease, "charts/roundtable-operator",
			"-n", missionNamespace, "--create-namespace", "-f", missionValues, "--wait", "--timeout", "5m"))
		Expect(err).NotTo(HaveOccurred(), "Failed to install the operator chart")

		By("creating the table, the stub knight and the Mission")
		_, err = utils.Run(exec.Command("kubectl", "apply", "-f", missionManifests))
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		_, _ = utils.Run(exec.Command("kubectl", "delete", "mission,chain,knight,roundtable", "--all",
			"-n", missionNamespace, "--ignore-not-found", "--timeout=2m"))
		_, _ = utils.Run(exec.Command("helm", "uninstall", missionRelease, "-n", missionNamespace, "--wait"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", missionNamespace, "--ignore-not-found", "--wait=false"))
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		for _, args := range [][]string{
			{"get", "mission,chain", "-n", missionNamespace, "-o", "yaml"},
			{"get", "knight,pods", "-n", missionNamespace, "-o", "wide"},
			{"logs", "deployment/" + missionOperator, "-n", missionNamespace, "--tail=200"},
			{"logs", "-l", "app.kubernetes.io/name=knight", "-n", missionNamespace, "--tail=100"},
		} {
			out, err := utils.Run(exec.Command("kubectl", args...))
			_, _ = fmt.Fprintf(GinkgoWriter, "kubectl %s:\n%s\n%v\n", strings.Join(args, " "), out, err)
		}
	})

	It("plans the Mission and runs it to Succeeded", func() {
		By("waiting for the stub knight to be Ready")
		Eventually(func(g Gomega) {
			out, err := kget("knight", missionKnight, "-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Ready"))
		}, 5*time.Minute).Should(Succeed())

		By("waiting for the Mission to succeed")
		Eventually(func(g Gomega) {
			out, err := kget("mission", missionName, "-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(Equal("Failed"), "mission failed")
			g.Expect(out).To(Equal("Succeeded"))
		}, 8*time.Minute).Should(Succeed())
	})

	It("generated exactly one Chain from the plan", func() {
		out, err := kget("mission", missionName, "-o", "jsonpath={.status.planningResult.chainsGenerated}")
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(Equal("1"))
	})

	It("ran the generated Chain's step to Succeeded", func() {
		chain, err := kget("mission", missionName, "-o", "jsonpath={.status.chainStatuses[0].chainCRName}")
		Expect(err).NotTo(HaveOccurred())
		Expect(chain).NotTo(BeEmpty())
		out, err := kget("chain", chain, "-o", "jsonpath={.status.steps[*].phase}")
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.Fields(out)).To(ContainElement("Succeeded"))
	})
})
