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
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/roundtable-k8s/roundtable/test/utils"
)

// The trigger tier (#737): a NATS-source Trigger on a real NATS server and a
// real operator pod starts a Chain run from an event, the run completes on the
// stub knight, and a duplicate event starts no second run.
const (
	triggerNamespace = "roundtable-trigger"
	triggerRelease   = "rt-trigger"
	triggerOperator  = "rt-trigger" // --set fullnameOverride (shared core-values.yaml)
	triggerValues    = "test/e2e/testdata/core-values.yaml"
	triggerManifests = "test/e2e/testdata/trigger.yaml"
	triggerName      = "on-event"
	triggerKnight    = "stub"
	triggerSubject   = "e2e.events.deploys.prod"
	triggerNATSPort  = "14223"
	// triggerEvent is the event payload; its dedupe key is api@1.2.3.
	triggerEvent = `{"service":"api","version":"1.2.3"}`
)

var _ = Describe("Trigger", Ordered, Label(tierTrigger), func() {
	var (
		forward *exec.Cmd
		conn    *nats.Conn
		js      jetstream.JetStream
	)

	kget := func(args ...string) (string, error) {
		out, err := utils.Run(exec.Command("kubectl", append([]string{"get", "-n", triggerNamespace}, args...)...))
		return strings.TrimSpace(out), err
	}

	BeforeAll(func() {
		By("installing the operator chart")
		_, err := utils.Run(exec.Command("helm", "upgrade", "--install", triggerRelease, "charts/roundtable-operator",
			"-n", triggerNamespace, "--create-namespace", "-f", triggerValues, "--set", "fullnameOverride="+triggerOperator, "--wait", "--timeout", "5m"))
		Expect(err).NotTo(HaveOccurred(), "Failed to install the operator chart")

		By("creating the table, the stub knight, the template Chain and the Trigger")
		_, err = utils.Run(exec.Command("kubectl", "apply", "-f", triggerManifests))
		Expect(err).NotTo(HaveOccurred())

		By("port-forwarding NATS")
		forward = exec.Command("kubectl", "port-forward", "-n", natsNamespace, "svc/nats", triggerNATSPort+":4222")
		Expect(forward.Start()).To(Succeed())
		Eventually(func() error {
			var cerr error
			conn, cerr = nats.Connect("nats://127.0.0.1:" + triggerNATSPort)
			return cerr
		}, time.Minute, time.Second).Should(Succeed())
		js, err = jetstream.New(conn)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		if conn != nil {
			conn.Close()
		}
		if forward != nil && forward.Process != nil {
			_ = forward.Process.Kill()
			_ = forward.Wait()
		}
		// Delete the resources while the operator still runs their finalizers
		// (the Trigger's finalizer deletes its JetStream consumer).
		_, _ = utils.Run(exec.Command("kubectl", "delete", "trigger,chain,knight,roundtable", "--all",
			"-n", triggerNamespace, "--ignore-not-found", "--timeout=2m"))
		_, _ = utils.Run(exec.Command("helm", "uninstall", triggerRelease, "-n", triggerNamespace, "--wait"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", triggerNamespace, "--ignore-not-found", "--wait=false"))
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		for _, args := range [][]string{
			{"get", "trigger,chain", "-n", triggerNamespace, "-o", "yaml"},
			{"get", "knight,pods", "-n", triggerNamespace, "-o", "wide"},
			{"logs", "deployment/" + triggerOperator, "-n", triggerNamespace, "--tail=200"},
			{"logs", "-l", "app.kubernetes.io/name=knight", "-n", triggerNamespace, "--tail=100"},
			{"get", "events", "-n", triggerNamespace, "--sort-by=.lastTimestamp"},
		} {
			out, err := utils.Run(exec.Command("kubectl", args...))
			_, _ = fmt.Fprintf(GinkgoWriter, "kubectl %s:\n%s\n%v\n", strings.Join(args, " "), out, err)
		}
	})

	publish := func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, err := js.Publish(ctx, triggerSubject, []byte(triggerEvent))
		Expect(err).NotTo(HaveOccurred(), "publish on %s", triggerSubject)
	}

	runs := func() []string {
		out, err := kget("chains", "-l", "ai.roundtable.io/trigger="+triggerName,
			"-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
		Expect(err).NotTo(HaveOccurred())
		return utils.GetNonEmptyLines(out)
	}

	It("starts a Chain run from an event and it succeeds on the stub knight", func() {
		By("waiting for the stub knight and the Trigger to be Ready")
		Eventually(func(g Gomega) {
			out, err := kget("knight", triggerKnight, "-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Ready"))
		}, 5*time.Minute).Should(Succeed())
		Eventually(func(g Gomega) {
			out, err := kget("trigger", triggerName, "-o",
				"jsonpath={.status.conditions[?(@.type=='Ready')].status}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}).Should(Succeed())

		By("publishing an event")
		publish()

		By("waiting for one run owned by the Trigger")
		var run string
		Eventually(func(g Gomega) {
			names := runs()
			g.Expect(names).To(HaveLen(1))
			run = names[0]
			out, err := kget("chain", run, "-o", "jsonpath={.metadata.ownerReferences[0].kind}/"+
				"{.metadata.ownerReferences[0].name}|{.metadata.annotations.ai\\.roundtable\\.io/trigger-subject}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Trigger/" + triggerName + "|" + triggerSubject))
		}).Should(Succeed())

		By("waiting for the run to succeed")
		Eventually(func(g Gomega) {
			out, err := kget("chain", run, "-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(Equal("Failed"), "chain run failed")
			g.Expect(out).To(Equal("Succeeded"))
		}, 5*time.Minute).Should(Succeed())
	})

	It("starts no second run for a duplicate event", func() {
		before := runs()
		Expect(before).To(HaveLen(1))

		By("publishing the same event again")
		publish()

		By("waiting for the Trigger to record the duplicate")
		Eventually(func(g Gomega) {
			out, err := kget("trigger", triggerName, "-o",
				"jsonpath={range .status.recentFirings[*]}{.skipReason}{\"\\n\"}{end}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("duplicate dedupe key"))
		}).Should(Succeed())

		Consistently(runs, 10*time.Second, 2*time.Second).Should(Equal(before))
	})

	It("logs no Trigger errors", func() {
		out, err := utils.Run(exec.Command("kubectl", "logs", "deployment/"+triggerOperator,
			"-n", triggerNamespace))
		Expect(err).NotTo(HaveOccurred())
		var errs []string
		for _, line := range utils.GetNonEmptyLines(out) {
			if (strings.Contains(line, `"level":"error"`) || strings.Contains(line, "\tERROR\t")) &&
				strings.Contains(strings.ToLower(line), "trigger") {
				errs = append(errs, line)
			}
		}
		Expect(errs).To(BeEmpty(), "operator logged Trigger errors:\n%s", strings.Join(errs, "\n"))
	})
})
