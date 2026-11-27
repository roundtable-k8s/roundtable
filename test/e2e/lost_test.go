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
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/roundtable-k8s/roundtable/internal/deadletter"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
	"github.com/roundtable-k8s/roundtable/test/utils"
)

// The lost-task case (#738): a stub knight that dies mid-task, on the
// consumer config the operator owns (max_deliver 1, 30 s ack wait), ends the
// task as a dead-letter entry.
const (
	lostNamespace   = "roundtable-lost"
	lostRelease     = "rt-lost" // fullnameOverride in lost-values.yaml
	lostValues      = "test/e2e/testdata/lost-values.yaml"
	lostManifests   = "test/e2e/testdata/lost.yaml"
	lostTable       = "lost-table"
	lostKnight      = "stub"
	lostPrefix      = "lost"
	lostDomain      = "lost"
	lostTasksStream = "lost_tasks"
	// lostNATSPort is where NATS is port-forwarded to for this tier.
	lostNATSPort = "14223"
)

var _ = Describe("Lost task", Ordered, Label(tierLost), func() {
	var (
		forward *exec.Cmd
		nc      *nats.Conn
		js      jetstream.JetStream
	)

	BeforeAll(func() {
		SetDefaultEventuallyTimeout(3 * time.Minute)
		SetDefaultEventuallyPollingInterval(2 * time.Second)

		By("installing the operator chart with the local image")
		_, err := utils.Run(exec.Command("helm", "upgrade", "--install", lostRelease, "charts/roundtable-operator",
			"-n", lostNamespace, "--create-namespace", "-f", lostValues, "--wait", "--timeout", "5m"))
		Expect(err).NotTo(HaveOccurred())

		By("creating the table and the stub knight")
		_, err = utils.Run(exec.Command("kubectl", "apply", "-f", lostManifests))
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the stub knight to be Ready")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "knight", lostKnight, "-n", lostNamespace,
				"-o", "jsonpath={.status.phase}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(Equal("Ready"))
		}, 5*time.Minute, 2*time.Second).Should(Succeed())

		By("port-forwarding NATS")
		forward = exec.Command("kubectl", "port-forward", "-n", natsNamespace, "svc/nats", lostNATSPort+":4222")
		Expect(forward.Start()).To(Succeed())
		Eventually(func() error {
			var cerr error
			nc, cerr = nats.Connect("nats://127.0.0.1:" + lostNATSPort)
			return cerr
		}, time.Minute, time.Second).Should(Succeed())
		js, err = jetstream.New(nc)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		if nc != nil {
			nc.Close()
		}
		if forward != nil && forward.Process != nil {
			_ = forward.Process.Kill()
			_ = forward.Wait()
		}
		// Delete the resources while the operator still runs their finalizers.
		_, _ = utils.Run(exec.Command("kubectl", "delete", "knight,roundtable", "--all",
			"-n", lostNamespace, "--ignore-not-found", "--timeout=2m"))
		_, _ = utils.Run(exec.Command("helm", "uninstall", lostRelease, "-n", lostNamespace, "--wait"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", lostNamespace, "--ignore-not-found", "--wait=false"))
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		for _, args := range [][]string{
			{"get", "knight,pods", "-n", lostNamespace, "-o", "wide"},
			{"get", "knight", lostKnight, "-n", lostNamespace, "-o", "yaml"},
			{"logs", "deployment/" + lostRelease, "-n", lostNamespace, "--tail=300"},
			{"logs", "-l", "app.kubernetes.io/name=knight", "-n", lostNamespace, "--tail=100", "--previous"},
			{"logs", "-l", "app.kubernetes.io/name=knight", "-n", lostNamespace, "--tail=100"},
			{"get", "events", "-n", lostNamespace, "--sort-by=.lastTimestamp"},
		} {
			out, err := utils.Run(exec.Command("kubectl", args...))
			_, _ = fmt.Fprintf(GinkgoWriter, "kubectl %s:\n%s\n%v\n", strings.Join(args, " "), out, err)
		}
	})

	It("binds the operator's consumer config", func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		cons, err := js.Consumer(ctx, lostTasksStream, natspkg.KnightTaskConsumerName(lostKnight))
		Expect(err).NotTo(HaveOccurred())
		info, err := cons.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Config.MaxDeliver).To(Equal(natspkg.KnightTaskMaxDeliver))
		Expect(info.Config.AckWait).To(Equal(natspkg.KnightTaskAckWait))
	})

	It("dead-letters a task whose knight dies without acking it", func() {
		subject := natspkg.TaskSubject(lostPrefix, lostDomain, lostKnight)
		okID := fmt.Sprintf("lost-ok-%d", time.Now().Unix())
		crashID := fmt.Sprintf("lost-crash-%d", time.Now().Unix())

		By("a normal task is answered")
		sub, err := nc.SubscribeSync(natspkg.ResultSubject(lostPrefix, okID))
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = sub.Unsubscribe() }()
		publishLostTask(js, subject, okID, "STUB:greeting")
		msg, err := sub.NextMsg(2 * time.Minute)
		Expect(err).NotTo(HaveOccurred(), "no result for the normal task")
		var res natspkg.TaskResult
		Expect(json.Unmarshal(msg.Data, &res)).To(Succeed())
		Expect(res.TaskID).To(Equal(okID))

		By("a STUB:crash task kills the knight, which the Deployment restarts")
		publishLostTask(js, subject, crashID, "STUB:crash")

		By("the crashed task gets a KV entry and a lost entry in the DLQ stream")
		var entry deadletter.Entry
		Eventually(func(g Gomega) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			kv, err := js.KeyValue(ctx, deadletter.KVBucket)
			g.Expect(err).NotTo(HaveOccurred())
			key := deadletter.Key(lostTable, crashID)
			kve, err := kv.Get(ctx, key)
			g.Expect(err).NotTo(HaveOccurred(), "KV key %s", key)
			var rec deadletter.Record
			g.Expect(json.Unmarshal(kve.Value(), &rec)).To(Succeed())
			g.Expect(rec.State).To(Equal(deadletter.StateOpen))
			g.Expect(rec.Reason).To(Equal(deadletter.ReasonLost))

			dlq, err := js.Stream(ctx, deadletter.StreamName(lostTasksStream))
			g.Expect(err).NotTo(HaveOccurred())
			raw, err := dlq.GetLastMsgForSubject(ctx, deadletter.Subject(lostPrefix, lostKnight, crashID))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(json.Unmarshal(raw.Data, &entry)).To(Succeed())
			g.Expect(entry.Reason).To(Equal(deadletter.ReasonLost))
			g.Expect(entry.TaskID).To(Equal(crashID))
			g.Expect(entry.Table).To(Equal(lostTable))
		}, 90*time.Second, 3*time.Second).Should(Succeed())

		By("the never-acked original is deleted from the tasks stream after the grace period")
		Eventually(func(g Gomega) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			st, err := js.Stream(ctx, lostTasksStream)
			g.Expect(err).NotTo(HaveOccurred())
			_, err = st.GetMsg(ctx, entry.StreamSeq)
			g.Expect(errors.Is(err, jetstream.ErrMsgNotFound)).To(BeTrue(),
				"original task at seq %d still in %s: %v", entry.StreamSeq, lostTasksStream, err)
		}, 2*time.Minute, 3*time.Second).Should(Succeed())
	})
})

func publishLostTask(js jetstream.JetStream, subject, taskID, text string) {
	payload, err := json.Marshal(natspkg.TaskPayload{TaskID: taskID, Task: text})
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = js.Publish(ctx, subject, payload)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "publish %s", taskID)
}
