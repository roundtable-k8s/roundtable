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

// The schedule tier (#998): a scheduled Trigger with a knight target is
// created through the gateway, fired with run now (no wait for a cron tick),
// suspended and deleted. It runs on the stub knight; no KEDA, no extra image.
const (
	scheduleNamespace = "roundtable-schedule"
	scheduleRelease   = "rt-schedule"
	scheduleService   = "rt-schedule-gateway"
	scheduleManifests = "test/e2e/testdata/schedule.yaml"
	scheduleKnight    = "stub"
	scheduleName      = "e2e-nightly"
	scheduleCaller    = "e2e-caller"
	schedulePort      = "18081"
)

var _ = Describe("Schedule", Ordered, Label(tierSchedule), func() {
	var (
		forward *exec.Cmd
		token   string
		base    = "http://127.0.0.1:" + schedulePort
		path    = "/v1/schedules/" + scheduleNamespace + "/" + scheduleName
	)

	call := func(method, p string, body any) (int, []byte) {
		var rd io.Reader
		if body != nil {
			b, err := json.Marshal(body)
			Expect(err).NotTo(HaveOccurred())
			rd = bytes.NewReader(b)
		}
		req, err := http.NewRequest(method, base+p, rd)
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Authorization", "Bearer "+token)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := (&http.Client{Timeout: 2 * time.Minute}).Do(req)
		Expect(err).NotTo(HaveOccurred(), "%s %s", method, p)
		defer func() { _ = resp.Body.Close() }()
		out, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		return resp.StatusCode, out
	}

	kget := func(args ...string) (string, error) {
		out, err := utils.Run(exec.Command("kubectl", append([]string{"get", "-n", scheduleNamespace}, args...)...))
		return strings.TrimSpace(out), err
	}

	type scheduleView struct {
		Name      string     `json:"name"`
		ManagedBy string     `json:"managedBy"`
		Suspended bool       `json:"suspended"`
		NextRunAt *time.Time `json:"nextRunAt"`
	}
	listSchedules := func(g Gomega) []scheduleView {
		code, body := call(http.MethodGet, "/v1/schedules?namespace="+scheduleNamespace, nil)
		g.Expect(code).To(Equal(http.StatusOK), string(body))
		var list struct {
			Schedules []scheduleView `json:"schedules"`
		}
		g.Expect(json.Unmarshal(body, &list)).To(Succeed())
		return list.Schedules
	}
	find := func(g Gomega) *scheduleView {
		for _, s := range listSchedules(g) {
			if s.Name == scheduleName {
				return &s
			}
		}
		return nil
	}

	BeforeAll(func() {
		SetDefaultEventuallyTimeout(3 * time.Minute)
		SetDefaultEventuallyPollingInterval(2 * time.Second)

		By("installing the operator chart with the gateway on")
		_, err := utils.Run(exec.Command("helm", "upgrade", "--install", scheduleRelease, "charts/roundtable-operator",
			"-n", scheduleNamespace, "--create-namespace", "-f", gatewayValues,
			"--set", "fullnameOverride="+scheduleRelease, "--wait", "--timeout", "5m"))
		Expect(err).NotTo(HaveOccurred(), "Failed to install the operator chart")

		By("creating the table and the stub knight")
		_, err = utils.Run(exec.Command("kubectl", "apply", "-f", scheduleManifests))
		Expect(err).NotTo(HaveOccurred())

		By("creating a caller ServiceAccount and its token")
		_, err = utils.Run(exec.Command("kubectl", "create", "serviceaccount", scheduleCaller, "-n", scheduleNamespace))
		Expect(err).NotTo(HaveOccurred())
		out, err := utils.Run(exec.Command("kubectl", "create", "token", scheduleCaller,
			"-n", scheduleNamespace, "--audience", gatewayAudience, "--duration", "1h"))
		Expect(err).NotTo(HaveOccurred())
		token = strings.TrimSpace(out)
		Expect(token).NotTo(BeEmpty())

		By("port-forwarding the gateway Service")
		forward = exec.Command("kubectl", "port-forward", "-n", scheduleNamespace, "svc/"+scheduleService, schedulePort+":80")
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
			out, err := kget("knight", scheduleKnight, "-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Ready"))
		}, 5*time.Minute).Should(Succeed())
	})

	AfterAll(func() {
		if forward != nil && forward.Process != nil {
			_ = forward.Process.Kill()
			_ = forward.Wait()
		}
		_, _ = utils.Run(exec.Command("kubectl", "delete", "trigger,knight,roundtable", "--all",
			"-n", scheduleNamespace, "--ignore-not-found", "--timeout=2m"))
		_, _ = utils.Run(exec.Command("helm", "uninstall", scheduleRelease, "-n", scheduleNamespace, "--wait"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", scheduleNamespace, "--ignore-not-found", "--wait=false"))
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		for _, args := range [][]string{
			{"get", "trigger,cronjob,serviceaccount,knight,pods", "-n", scheduleNamespace, "-o", "wide"},
			{"describe", "trigger", scheduleName, "-n", scheduleNamespace},
			{"logs", "deployment/" + scheduleService, "-n", scheduleNamespace, "--tail=200"},
			{"logs", "deployment/" + scheduleRelease, "-n", scheduleNamespace, "--tail=100"},
			{"get", "events", "-n", scheduleNamespace, "--sort-by=.lastTimestamp"},
		} {
			out, err := utils.Run(exec.Command("kubectl", args...))
			_, _ = fmt.Fprintf(GinkgoWriter, "kubectl %s:\n%s\n%v\n", strings.Join(args, " "), out, err)
		}
	})

	It("creates a schedule with a knight target and lists it", func() {
		code, body := call(http.MethodPost, "/v1/schedules", map[string]any{
			"name":      scheduleName,
			"namespace": scheduleNamespace,
			"spec": map[string]any{
				"roundTableRef": map[string]any{"name": "e2e-table"},
				"source": map[string]any{
					// Far from firing: the run is started with run now.
					"schedule": map[string]any{"cron": "0 3 * * *"},
				},
				"suspend": false,
				"target": map[string]any{
					"knight": map[string]any{"name": scheduleKnight, "task": "scheduled ping e2e", "timeoutSeconds": 90},
				},
			},
		})
		Expect(code).To(Equal(http.StatusCreated), string(body))

		Eventually(func(g Gomega) {
			s := find(g)
			g.Expect(s).NotTo(BeNil(), "schedule not listed")
			g.Expect(s.ManagedBy).To(Equal("api"))
			g.Expect(s.NextRunAt).NotTo(BeNil(), "nextRunAt not set yet")
			g.Expect(s.Suspended).To(BeFalse())
		}).Should(Succeed())

		By("waiting for the operator-owned CronJob")
		Eventually(func(g Gomega) {
			_, err := kget("cronjob", "trigger-"+scheduleName)
			g.Expect(err).NotTo(HaveOccurred())
		}).Should(Succeed())
	})

	It("runs it now and records a succeeded run", func() {
		code, body := call(http.MethodPost, path+"/run", nil)
		Expect(code).To(Equal(http.StatusCreated), string(body))
		var run struct {
			State string `json:"state"`
			Key   string `json:"key"`
		}
		Expect(json.Unmarshal(body, &run)).To(Succeed())
		Expect(run.State).To(Equal("started"), string(body))
		Expect(run.Key).To(HavePrefix("manual:"))

		Eventually(func(g Gomega) {
			code, body := call(http.MethodGet, path+"/runs", nil)
			g.Expect(code).To(Equal(http.StatusOK), string(body))
			var list struct {
				Runs []struct {
					Run     string `json:"run"`
					Outcome string `json:"outcome"`
				} `json:"runs"`
			}
			g.Expect(json.Unmarshal(body, &list)).To(Succeed())
			g.Expect(list.Runs).NotTo(BeEmpty(), string(body))
			// Newest first.
			g.Expect(list.Runs[0].Outcome).NotTo(SatisfyAny(Equal("failed"), Equal("lost")), string(body))
			g.Expect(list.Runs[0].Outcome).To(Equal("succeeded"), string(body))
		}, 5*time.Minute).Should(Succeed())
	})

	It("suspends it and deletes it with its CronJob", func() {
		code, body := call(http.MethodPost, path+"/suspend", nil)
		Expect(code).To(Equal(http.StatusOK), string(body))
		Eventually(func(g Gomega) {
			s := find(g)
			g.Expect(s).NotTo(BeNil())
			g.Expect(s.Suspended).To(BeTrue())
		}).Should(Succeed())

		code, body = call(http.MethodDelete, path, nil)
		Expect(code).To(Equal(http.StatusOK), string(body))

		Eventually(func(g Gomega) {
			g.Expect(find(g)).To(BeNil())
			for _, kind := range []string{"trigger/" + scheduleName, "cronjob/trigger-" + scheduleName,
				"serviceaccount/trigger-" + scheduleName} {
				_, err := kget(kind)
				g.Expect(err).To(HaveOccurred(), "%s still exists", kind)
			}
		}).Should(Succeed())
	})
})
