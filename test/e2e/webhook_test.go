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
	"io"
	"net/http"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/roundtable-k8s/roundtable/test/utils"
)

// The webhook tier (#904): an HTTP POST to a webhook Trigger's URL on the
// gateway, with the Trigger's own credential, starts exactly one Chain run on
// the stub knight; a repeated delivery id starts none and a wrong credential
// is refused.
const (
	webhookNamespace = "roundtable-webhook"
	webhookRelease   = "rt-webhook"
	webhookOperator  = "rt-webhook" // fullnameOverride in webhook-values.yaml
	webhookService   = "rt-webhook-gateway"
	webhookValues    = "test/e2e/testdata/webhook-values.yaml"
	webhookManifests = "test/e2e/testdata/webhook.yaml"
	webhookKnight    = "stub"
	webhookTrigger   = "on-hook"
	webhookToken     = "e2e-webhook-token-0123456789" // the e2e-hook Secret
	webhookPort      = "18081"
	webhookMsgID     = "e2e-delivery-1"
	// webhookBody is the delivery; its dedupe key is api@2.0.0.
	webhookBody = `{"service":"api","version":"2.0.0"}`
)

var _ = Describe("Webhook", Ordered, Label(tierWebhook), func() {
	var (
		forward *exec.Cmd
		base    = "http://127.0.0.1:" + webhookPort
		hookURL = base + "/v1/hooks/" + webhookNamespace + "/" + webhookTrigger
	)

	kget := func(args ...string) (string, error) {
		out, err := utils.Run(exec.Command("kubectl", append([]string{"get", "-n", webhookNamespace}, args...)...))
		return strings.TrimSpace(out), err
	}

	// post sends one delivery and returns the status and body.
	post := func(token, msgID string) (int, string) {
		req, err := http.NewRequest(http.MethodPost, hookURL, strings.NewReader(webhookBody))
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if msgID != "" {
			req.Header.Set("Idempotency-Key", msgID)
		}
		resp, err := (&http.Client{Timeout: time.Minute}).Do(req)
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = resp.Body.Close() }()
		out, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		return resp.StatusCode, string(out)
	}

	runs := func() []string {
		out, err := kget("chains", "-l", "ai.roundtable.io/trigger="+webhookTrigger,
			"-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
		Expect(err).NotTo(HaveOccurred())
		return utils.GetNonEmptyLines(out)
	}

	BeforeAll(func() {
		SetDefaultEventuallyTimeout(3 * time.Minute)
		SetDefaultEventuallyPollingInterval(2 * time.Second)

		By("installing the operator chart with the gateway on")
		_, err := utils.Run(exec.Command("helm", "upgrade", "--install", webhookRelease, "charts/roundtable-operator",
			"-n", webhookNamespace, "--create-namespace", "-f", webhookValues, "--wait", "--timeout", "5m"))
		Expect(err).NotTo(HaveOccurred(), "Failed to install the operator chart")

		By("creating the table, the stub knight, the template Chain, the credential and the Trigger")
		_, err = utils.Run(exec.Command("kubectl", "apply", "-f", webhookManifests))
		Expect(err).NotTo(HaveOccurred())

		By("port-forwarding the gateway Service")
		forward = exec.Command("kubectl", "port-forward", "-n", webhookNamespace, "svc/"+webhookService, webhookPort+":80")
		Expect(forward.Start()).To(Succeed())
		Eventually(func() error {
			resp, err := http.Get(base + "/healthz")
			if err != nil {
				return err
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("healthz: %s", resp.Status)
			}
			return nil
		}, time.Minute, time.Second).Should(Succeed())

		By("waiting for the stub knight and the Trigger to be Ready")
		Eventually(func(g Gomega) {
			out, err := kget("knight", webhookKnight, "-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Ready"))
		}, 5*time.Minute).Should(Succeed())
		Eventually(func(g Gomega) {
			out, err := kget("trigger", webhookTrigger, "-o",
				"jsonpath={.status.conditions[?(@.type=='Ready')].status}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}).Should(Succeed())
	})

	AfterAll(func() {
		if forward != nil && forward.Process != nil {
			_ = forward.Process.Kill()
			_ = forward.Wait()
		}
		// Delete the resources while the operator still runs their finalizers.
		_, _ = utils.Run(exec.Command("kubectl", "delete", "trigger,chain,knight,roundtable", "--all",
			"-n", webhookNamespace, "--ignore-not-found", "--timeout=2m"))
		_, _ = utils.Run(exec.Command("helm", "uninstall", webhookRelease, "-n", webhookNamespace, "--wait"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", webhookNamespace, "--ignore-not-found", "--wait=false"))
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		for _, args := range [][]string{
			{"get", "trigger,chain", "-n", webhookNamespace, "-o", "yaml"},
			{"get", "knight,pods", "-n", webhookNamespace, "-o", "wide"},
			{"logs", "deployment/" + webhookService, "-n", webhookNamespace, "--tail=200"},
			{"logs", "deployment/" + webhookOperator, "-n", webhookNamespace, "--tail=200"},
			{"get", "events", "-n", webhookNamespace, "--sort-by=.lastTimestamp"},
		} {
			out, err := utils.Run(exec.Command("kubectl", args...))
			_, _ = fmt.Fprintf(GinkgoWriter, "kubectl %s:\n%s\n%v\n", strings.Join(args, " "), out, err)
		}
	})

	It("refuses a missing or wrong credential and starts no run", func() {
		code, body := post("", webhookMsgID)
		Expect(code).To(Equal(http.StatusUnauthorized), body)

		code, body = post("wrong-token-0123456789", webhookMsgID)
		Expect(code).To(Equal(http.StatusUnauthorized), body)

		Consistently(runs, 10*time.Second, 2*time.Second).Should(BeEmpty())
	})

	It("starts one Chain run from a delivery and it succeeds on the stub knight", func() {
		code, body := post(webhookToken, webhookMsgID)
		Expect(code).To(Equal(http.StatusAccepted), body)
		Expect(body).To(ContainSubstring(".events.webhook." + webhookNamespace + "." + webhookTrigger))

		var run string
		Eventually(func(g Gomega) {
			names := runs()
			g.Expect(names).To(HaveLen(1))
			run = names[0]
			out, err := kget("chain", run, "-o", "jsonpath={.metadata.ownerReferences[0].kind}/"+
				"{.metadata.ownerReferences[0].name}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Trigger/" + webhookTrigger))
		}).Should(Succeed())

		By("waiting for the run to succeed")
		Eventually(func(g Gomega) {
			out, err := kget("chain", run, "-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(Equal("Failed"), "chain run failed")
			g.Expect(out).To(Equal("Succeeded"))
		}, 5*time.Minute).Should(Succeed())
	})

	It("starts no second run for a repeated delivery id", func() {
		before := runs()
		Expect(before).To(HaveLen(1))

		code, body := post(webhookToken, webhookMsgID)
		Expect(code).To(Equal(http.StatusAccepted), body)

		Consistently(runs, 10*time.Second, 2*time.Second).Should(Equal(before))
	})
})
