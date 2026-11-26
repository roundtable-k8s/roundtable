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
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
	"github.com/roundtable-k8s/roundtable/test/utils"
)

// The core tier (#351): the operator from charts/roundtable-operator with the locally built image, and
// one minimal Knight that must reach an available Deployment.
const (
	coreNamespace  = "roundtable-core"
	coreRelease    = "rt-core"
	coreOperator   = "rt-core" // fullnameOverride in core-values.yaml
	coreValues     = "test/e2e/testdata/core-values.yaml"
	coreKnight     = "core-knight"
	coreKnightYAML = "test/e2e/testdata/knight-core.yaml"

	// The round trip: a task on NATS, a knight, the stub LLM, a result.
	stubLLMYAML      = "test/e2e/testdata/stubllm.yaml"
	roundtripKnight  = "rt-knight"
	roundtripYAML    = "test/e2e/testdata/knight-roundtrip.yaml"
	roundtripTasks   = "core_tasks"
	roundtripResults = "core_results"
	// STUB:greeting makes the stub LLM reply with stubGreeting.
	stubGreeting = "Hello from the stub LLM."
	// natsLocalPort is where the in-cluster NATS is port-forwarded to.
	natsLocalPort = "14222"
)

// natsForward, natsConn and natsJS are the port-forward to NATS and the
// JetStream handle the core scenarios publish and read results with.
var (
	natsForward *exec.Cmd
	natsConn    *nats.Conn
	natsJS      jetstream.JetStream
)

var _ = Describe("Core install", Ordered, Label(tierCore), func() {
	BeforeAll(func() {
		SetDefaultEventuallyTimeout(3 * time.Minute)
		SetDefaultEventuallyPollingInterval(2 * time.Second)

		By("installing the operator chart with the local image")
		_, err := utils.Run(exec.Command("helm", "upgrade", "--install", coreRelease, "charts/roundtable-operator",
			"-n", coreNamespace, "--create-namespace", "-f", coreValues, "--wait", "--timeout", "5m"))
		Expect(err).NotTo(HaveOccurred(), "Failed to install the operator chart")

		// The knights consume from these streams; create them up front so the
		// runtime can bind its consumers as soon as the pods start.
		By("port-forwarding NATS and creating the core streams")
		natsForward = exec.Command("kubectl", "port-forward", "-n", natsNamespace, "svc/nats", natsLocalPort+":4222")
		Expect(natsForward.Start()).To(Succeed())
		Eventually(func() error {
			var cerr error
			natsConn, cerr = nats.Connect("nats://127.0.0.1:" + natsLocalPort)
			return cerr
		}, time.Minute, time.Second).Should(Succeed())
		natsJS, err = jetstream.New(natsConn)
		Expect(err).NotTo(HaveOccurred())
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		for name, subjects := range map[string][]string{
			roundtripTasks:   {"core.tasks.>"},
			roundtripResults: {"core.results.>"},
		} {
			_, err = natsJS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: subjects})
			Expect(err).NotTo(HaveOccurred(), "stream %s", name)
		}
	})

	AfterAll(func() {
		if natsConn != nil {
			natsConn.Close()
		}
		if natsForward != nil && natsForward.Process != nil {
			_ = natsForward.Process.Kill()
			_ = natsForward.Wait()
		}
		// Delete the Knight while the operator still runs its finalizers.
		_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", coreKnightYAML,
			"--ignore-not-found", "--timeout=2m"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", roundtripYAML,
			"--ignore-not-found", "--timeout=2m"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", stubLLMYAML, "--ignore-not-found"))
		_, _ = utils.Run(exec.Command("helm", "uninstall", coreRelease, "-n", coreNamespace, "--wait"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", coreNamespace,
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
			{"logs", "-l", "app.kubernetes.io/instance=" + coreKnight, "-n", coreNamespace,
				"--all-containers", "--tail=100"},
			{"describe", "deployment", coreKnight, "-n", coreNamespace},
			{"logs", "-l", "app.kubernetes.io/instance=" + coreKnight, "-n", coreNamespace,
				"--all-containers", "--tail=200"},
			{"get", "knight", roundtripKnight, "-n", coreNamespace, "-o", "yaml"},
			{"logs", "-l", "app.kubernetes.io/instance=" + roundtripKnight, "-n", coreNamespace,
				"--all-containers", "--tail=200"},
			{"logs", "deployment/stubllm", "-n", coreNamespace, "--tail=200"},
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
			"-n", coreNamespace, "--since=5m"))
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
		// pi-knight preflights a model it cannot find in its catalog against an
		// OpenAI-compatible endpoint and stays unready (503) until that answers,
		// so the minimal Knight's model is routed to the stub LLM too.
		By("deploying the stub LLM")
		_, err := utils.Run(exec.Command("kubectl", "apply", "-f", stubLLMYAML))
		Expect(err).NotTo(HaveOccurred())
		_, err = utils.Run(exec.Command("kubectl", "rollout", "status", "deployment/stubllm",
			"-n", coreNamespace, "--timeout=3m"))
		Expect(err).NotTo(HaveOccurred())

		By("creating the minimal Knight")
		_, err = utils.Run(exec.Command("kubectl", "apply", "-f", coreKnightYAML))
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "deployment", coreKnight,
				"-n", coreNamespace, "-o", "jsonpath={.status.availableReplicas}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("1"))
		}, 5*time.Minute).Should(Succeed())
	})

	// The round trip (#353): a task published on NATS reaches a pi-knight, which
	// calls the stub LLM, and the result lands on the results stream.
	It("round-trips a task through a knight and the stub LLM", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		By("creating the Knight that routes its model to the stub LLM")
		_, err := utils.Run(exec.Command("kubectl", "apply", "-f", roundtripYAML))
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "knight", roundtripKnight,
				"-n", coreNamespace, "-o", "jsonpath={.status.phase}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Ready"))
		}, 5*time.Minute).Should(Succeed())

		By("publishing a STUB:greeting task")
		taskID := fmt.Sprintf("e2e-core-%d", time.Now().UnixNano())
		payload, err := json.Marshal(natspkg.TaskPayload{TaskID: taskID, Task: "STUB:greeting"})
		Expect(err).NotTo(HaveOccurred())
		_, err = natsJS.Publish(ctx, "core.tasks.rt."+roundtripKnight, payload)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the result on the results stream")
		cons, err := natsJS.OrderedConsumer(ctx, roundtripResults, jetstream.OrderedConsumerConfig{
			FilterSubjects: []string{"core.results." + taskID},
		})
		Expect(err).NotTo(HaveOccurred())
		var result natspkg.TaskResult
		Eventually(func(g Gomega) {
			msg, err := cons.Next(jetstream.FetchMaxWait(5 * time.Second))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(json.Unmarshal(msg.Data(), &result)).To(Succeed())
		}, 3*time.Minute, time.Second).Should(Succeed())

		Expect(result.GetTaskID()).To(Equal(taskID))
		Expect(result.GetError()).To(BeEmpty(), "task failed")
		Expect(result.GetOutput()).To(ContainSubstring(stubGreeting))
	})
})
