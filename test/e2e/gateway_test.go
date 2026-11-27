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
	"bytes"
	"encoding/json"
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

// The gateway tier (#873): the invoke gateway against a real operator, NATS
// and the stub knight. A caller authenticates with a ServiceAccount token
// (audience roundtable-gateway), asks the knight, and runs an opted-in Chain
// by name through the HTTP API.
const (
	gatewayNamespace = "roundtable-gateway"
	gatewayRelease   = "rt-gateway"
	gatewayOperator  = "rt-gateway" // fullnameOverride in gateway-values.yaml
	gatewayService   = "rt-gateway-gateway"
	gatewayValues    = "test/e2e/testdata/gateway-values.yaml"
	gatewayManifests = "test/e2e/testdata/gateway.yaml"
	gatewayKnight    = "stub"
	gatewayChain     = "echo"
	gatewayCaller    = "e2e-caller"
	gatewayAudience  = "roundtable-gateway"
	gatewayPort      = "18080"
)

var _ = Describe("Gateway", Ordered, Label(tierGateway), func() {
	var (
		forward *exec.Cmd
		token   string
		base    = "http://127.0.0.1:" + gatewayPort
	)

	// call sends one request with the bearer token (none when empty) and
	// returns the status and body.
	call := func(method, path, tok string, body any) (int, []byte) {
		var rd io.Reader
		if body != nil {
			b, err := json.Marshal(body)
			Expect(err).NotTo(HaveOccurred())
			rd = bytes.NewReader(b)
		}
		req, err := http.NewRequest(method, base+path, rd)
		Expect(err).NotTo(HaveOccurred())
		if tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := (&http.Client{Timeout: 2 * time.Minute}).Do(req)
		Expect(err).NotTo(HaveOccurred(), "%s %s", method, path)
		defer func() { _ = resp.Body.Close() }()
		out, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		return resp.StatusCode, out
	}

	kget := func(args ...string) (string, error) {
		out, err := utils.Run(exec.Command("kubectl", append([]string{"get", "-n", gatewayNamespace}, args...)...))
		return strings.TrimSpace(out), err
	}

	BeforeAll(func() {
		SetDefaultEventuallyTimeout(3 * time.Minute)
		SetDefaultEventuallyPollingInterval(2 * time.Second)

		By("installing the operator chart with the gateway on")
		_, err := utils.Run(exec.Command("helm", "upgrade", "--install", gatewayRelease, "charts/roundtable-operator",
			"-n", gatewayNamespace, "--create-namespace", "-f", gatewayValues, "--wait", "--timeout", "5m"))
		Expect(err).NotTo(HaveOccurred(), "Failed to install the operator chart")

		By("creating the table, the stub knight and the opted-in Chain")
		_, err = utils.Run(exec.Command("kubectl", "apply", "-f", gatewayManifests))
		Expect(err).NotTo(HaveOccurred())

		By("creating a caller ServiceAccount and its token")
		_, err = utils.Run(exec.Command("kubectl", "create", "serviceaccount", gatewayCaller, "-n", gatewayNamespace))
		Expect(err).NotTo(HaveOccurred())
		out, err := utils.Run(exec.Command("kubectl", "create", "token", gatewayCaller,
			"-n", gatewayNamespace, "--audience", gatewayAudience, "--duration", "1h"))
		Expect(err).NotTo(HaveOccurred())
		token = strings.TrimSpace(out)
		Expect(token).NotTo(BeEmpty())

		By("port-forwarding the gateway Service")
		forward = exec.Command("kubectl", "port-forward", "-n", gatewayNamespace, "svc/"+gatewayService, gatewayPort+":80")
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

		By("waiting for the stub knight to be Ready")
		Eventually(func(g Gomega) {
			out, err := kget("knight", gatewayKnight, "-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Ready"))
		}, 5*time.Minute).Should(Succeed())
	})

	AfterAll(func() {
		if forward != nil && forward.Process != nil {
			_ = forward.Process.Kill()
			_ = forward.Wait()
		}
		_, _ = utils.Run(exec.Command("kubectl", "delete", "chain,knight,roundtable", "--all",
			"-n", gatewayNamespace, "--ignore-not-found", "--timeout=2m"))
		_, _ = utils.Run(exec.Command("helm", "uninstall", gatewayRelease, "-n", gatewayNamespace, "--wait"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", gatewayNamespace, "--ignore-not-found", "--wait=false"))
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		for _, args := range [][]string{
			{"get", "chain,knight,pods", "-n", gatewayNamespace, "-o", "wide"},
			{"logs", "deployment/" + gatewayService, "-n", gatewayNamespace, "--tail=200"},
			{"logs", "deployment/" + gatewayOperator, "-n", gatewayNamespace, "--tail=100"},
			{"get", "events", "-n", gatewayNamespace, "--sort-by=.lastTimestamp"},
		} {
			out, err := utils.Run(exec.Command("kubectl", args...))
			_, _ = fmt.Fprintf(GinkgoWriter, "kubectl %s:\n%s\n%v\n", strings.Join(args, " "), out, err)
		}
	})

	It("rejects a missing or invalid token", func() {
		code, _ := call(http.MethodGet, "/v1/dead-letters", "", nil)
		Expect(code).To(Equal(http.StatusUnauthorized))

		code, _ = call(http.MethodGet, "/v1/dead-letters", "not-a-valid-token-0123456789", nil)
		Expect(code).To(Equal(http.StatusUnauthorized))

		code, _ = call(http.MethodPost, "/v1/knights/"+gatewayKnight+"/invoke", "",
			map[string]any{"task": "hello"})
		Expect(code).To(Equal(http.StatusUnauthorized))
	})

	It("asks the stub knight and reads the task back", func() {
		code, body := call(http.MethodPost, "/v1/knights/"+gatewayKnight+"/invoke", token,
			map[string]any{"task": "ping gateway e2e", "timeoutSeconds": 90})
		Expect(code).To(Equal(http.StatusOK), string(body))
		var rec struct {
			TaskID string `json:"taskId"`
			Status string `json:"status"`
			Output string `json:"output"`
			Caller string `json:"caller"`
		}
		Expect(json.Unmarshal(body, &rec)).To(Succeed())
		Expect(rec.Status).To(Equal("succeeded"), string(body))
		Expect(rec.Output).To(ContainSubstring("ping gateway e2e"))
		Expect(rec.TaskID).NotTo(BeEmpty())
		Expect(rec.Caller).To(Equal(gatewayNamespace + "/" + gatewayCaller))

		By("fetching the task by ID")
		code, body = call(http.MethodGet, "/v1/tasks/"+rec.TaskID, token, nil)
		Expect(code).To(Equal(http.StatusOK), string(body))
		var got struct {
			Status string `json:"status"`
			Output string `json:"output"`
		}
		Expect(json.Unmarshal(body, &got)).To(Succeed())
		Expect(got.Status).To(Equal("succeeded"))
		Expect(got.Output).To(ContainSubstring("ping gateway e2e"))
	})

	It("runs the opted-in Chain by name and reads the run back", func() {
		code, body := call(http.MethodPost, "/v1/chains/"+gatewayChain+"/runs", token,
			map[string]any{"input": "run gateway e2e", "waitSeconds": 120})
		Expect(code).To(SatisfyAny(Equal(http.StatusOK), Equal(http.StatusAccepted)), string(body))
		var run struct {
			Run       string `json:"run"`
			Namespace string `json:"namespace"`
		}
		Expect(json.Unmarshal(body, &run)).To(Succeed())
		Expect(run.Run).NotTo(BeEmpty())
		Expect(run.Namespace).To(Equal(gatewayNamespace))

		By("waiting for the run to succeed")
		Eventually(func(g Gomega) {
			code, body := call(http.MethodGet, "/v1/runs/"+run.Namespace+"/"+run.Run+"?waitSeconds=20", token, nil)
			g.Expect(code).To(SatisfyAny(Equal(http.StatusOK), Equal(http.StatusAccepted)), string(body))
			var rec struct {
				Phase  string `json:"phase"`
				Done   bool   `json:"done"`
				Output string `json:"output"`
			}
			g.Expect(json.Unmarshal(body, &rec)).To(Succeed())
			g.Expect(rec.Phase).NotTo(Equal("Failed"), string(body))
			g.Expect(rec.Done).To(BeTrue(), string(body))
			g.Expect(rec.Phase).To(Equal("Succeeded"))
			g.Expect(rec.Output).To(ContainSubstring("run gateway e2e"))
		}, 5*time.Minute).Should(Succeed())
	})

	It("lists no dead letters for this tier's table", func() {
		code, body := call(http.MethodGet, "/v1/dead-letters", token, nil)
		Expect(code).To(Equal(http.StatusOK), string(body))
		// The dead-letter bucket is shared by every e2e tier (the lost tier
		// leaves an entry on purpose), so only assert on this tier's table.
		var list struct {
			DeadLetters []struct {
				Table string `json:"table"`
			} `json:"deadLetters"`
		}
		Expect(json.Unmarshal(body, &list)).To(Succeed())
		for _, dl := range list.DeadLetters {
			Expect(dl.Table).NotTo(Equal("e2e-table"), string(body))
		}
	})
})
