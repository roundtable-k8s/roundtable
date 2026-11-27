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

// The github tier (#870): issue pickup on the trigger engine against the stub
// GitHub API (test/e2e/stubgithub, #866). A labelled issue starts a Chain run
// on the stub knight, the claim shows in the stub's state, and a later poll
// of the same issue starts no second run.
const (
	githubNamespace = "roundtable-github"
	githubRelease   = "rt-github"
	githubOperator  = "rt-github" // fullnameOverride in github-values.yaml
	githubValues    = "test/e2e/testdata/github-values.yaml"
	githubManifests = "test/e2e/testdata/github.yaml"
	githubTable     = "gh-table"
	githubKnight    = "stub"
	githubRepo      = "acme/widgets"
	githubStubPort  = "18080"
)

// stubIssue is an issue in the stub's /_stub/state answer.
type stubIssue struct {
	Number   int      `json:"number"`
	Title    string   `json:"title"`
	State    string   `json:"state"`
	Labels   []string `json:"labels"`
	Comments []string `json:"comments"`
}

var _ = Describe("GitHub", Ordered, Label(tierGitHub), func() {
	var forward *exec.Cmd

	kget := func(args ...string) (string, error) {
		out, err := utils.Run(exec.Command("kubectl", append([]string{"get", "-n", githubNamespace}, args...)...))
		return strings.TrimSpace(out), err
	}

	stubURL := "http://127.0.0.1:" + githubStubPort

	seed := func(title string) {
		body := fmt.Sprintf(`{"repo":%q,"title":%q,"body":"plain work","labels":["rt:ready"]}`, githubRepo, title)
		resp, err := http.Post(stubURL+"/_stub/issues", "application/json", strings.NewReader(body))
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = resp.Body.Close() }()
		Expect(resp.StatusCode).To(Equal(http.StatusCreated))
	}

	state := func() ([]stubIssue, error) {
		resp, err := http.Get(stubURL + "/_stub/state?repo=" + githubRepo)
		if err != nil {
			return nil, err
		}
		defer func() { _ = resp.Body.Close() }()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("stub state: %s: %s", resp.Status, raw)
		}
		var st struct {
			Issues []stubIssue `json:"issues"`
		}
		if err := json.Unmarshal(raw, &st); err != nil {
			return nil, err
		}
		return st.Issues, nil
	}

	runs := func() []string {
		out, err := kget("chains", "-l", "ai.roundtable.io/pickup-roundtable="+githubTable,
			"-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
		Expect(err).NotTo(HaveOccurred())
		return utils.GetNonEmptyLines(out)
	}

	BeforeAll(func() {
		By("installing the operator chart, pointed at the stub GitHub API")
		_, err := utils.Run(exec.Command("helm", "upgrade", "--install", githubRelease, "charts/roundtable-operator",
			"-n", githubNamespace, "--create-namespace", "-f", githubValues, "--wait", "--timeout", "5m"))
		Expect(err).NotTo(HaveOccurred(), "Failed to install the operator chart")

		By("creating the stub GitHub, the table, the stub knight and the template Chain")
		_, err = utils.Run(exec.Command("kubectl", "apply", "-f", githubManifests))
		Expect(err).NotTo(HaveOccurred())
		_, err = utils.Run(exec.Command("kubectl", "rollout", "status", "deployment/stubgithub",
			"-n", githubNamespace, "--timeout=3m"))
		Expect(err).NotTo(HaveOccurred())

		By("port-forwarding the stub GitHub")
		forward = exec.Command("kubectl", "port-forward", "-n", githubNamespace, "svc/stubgithub", githubStubPort+":8080")
		Expect(forward.Start()).To(Succeed())
		Eventually(func() error {
			resp, gerr := http.Get(stubURL + "/_stub/state?repo=none/none")
			if gerr != nil {
				return gerr
			}
			return resp.Body.Close()
		}, time.Minute, time.Second).Should(Succeed())
	})

	AfterAll(func() {
		if forward != nil && forward.Process != nil {
			_ = forward.Process.Kill()
			_ = forward.Wait()
		}
		// Delete the resources while the operator still runs their finalizers.
		_, _ = utils.Run(exec.Command("kubectl", "delete", "trigger,chain,knight,roundtable", "--all",
			"-n", githubNamespace, "--ignore-not-found", "--timeout=2m"))
		_, _ = utils.Run(exec.Command("helm", "uninstall", githubRelease, "-n", githubNamespace, "--wait"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", githubNamespace, "--ignore-not-found", "--wait=false"))
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		for _, args := range [][]string{
			{"get", "roundtable,trigger,chain", "-n", githubNamespace, "-o", "yaml"},
			{"get", "knight,pods", "-n", githubNamespace, "-o", "wide"},
			{"logs", "deployment/" + githubOperator, "-n", githubNamespace, "--tail=200"},
			{"logs", "deployment/stubgithub", "-n", githubNamespace, "--tail=100"},
			{"get", "events", "-n", githubNamespace, "--sort-by=.lastTimestamp"},
		} {
			out, err := utils.Run(exec.Command("kubectl", args...))
			_, _ = fmt.Fprintf(GinkgoWriter, "kubectl %s:\n%s\n%v\n", strings.Join(args, " "), out, err)
		}
	})

	It("picks up a ready issue, runs it on the stub knight and claims it", func() {
		By("waiting for the stub knight and the generated Trigger to be Ready")
		Eventually(func(g Gomega) {
			out, err := kget("knight", githubKnight, "-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Ready"))
		}, 5*time.Minute).Should(Succeed())
		Eventually(func(g Gomega) {
			out, err := kget("trigger", githubTable+"-issue-pickup", "-o",
				"jsonpath={.status.conditions[?(@.type=='Ready')].status}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("True"))
		}, 4*time.Minute).Should(Succeed())

		By("seeding an rt:ready issue in the stub")
		seed("add a widget")

		By("waiting for a run of issue 1 owned by the table")
		var run string
		Eventually(func(g Gomega) {
			names := runs()
			g.Expect(names).To(HaveLen(1))
			run = names[0]
			out, err := kget("chain", run, "-o", "jsonpath={.metadata.annotations.ai\\.roundtable\\.io/pickup-repo}|"+
				"{.metadata.annotations.ai\\.roundtable\\.io/pickup-issue}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal(githubRepo + "|1"))
		}, 4*time.Minute).Should(Succeed())

		By("waiting for the run to succeed")
		Eventually(func(g Gomega) {
			out, err := kget("chain", run, "-o", "jsonpath={.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).NotTo(Equal("Failed"), "chain run failed")
			g.Expect(out).To(Equal("Succeeded"))
		}, 5*time.Minute).Should(Succeed())

		By("reading the claim from the stub: label swap and comment")
		Eventually(func(g Gomega) {
			issues, err := state()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(issues).NotTo(BeEmpty())
			g.Expect(issues[0].Number).To(Equal(1), "the claimed issue must be issue 1")
			g.Expect(issues[0].Labels).To(ContainElement("rt:in-progress"))
			g.Expect(issues[0].Labels).NotTo(ContainElement("rt:ready"))
			g.Expect(issues[0].Comments).NotTo(BeEmpty())
		}, 4*time.Minute).Should(Succeed())
	})

	It("starts no second run for the same issue on a later poll", func() {
		before := runs()
		Expect(before).To(HaveLen(1))

		By("seeding a second issue so that another poll cycle visibly happened")
		seed("add another widget")
		Eventually(func(g Gomega) {
			g.Expect(runs()).To(HaveLen(2))
		}, 5*time.Minute).Should(Succeed())

		By("checking the first issue still has exactly one run")
		count := 0
		for _, name := range runs() {
			out, err := kget("chain", name, "-o", "jsonpath={.metadata.annotations.ai\\.roundtable\\.io/pickup-issue}")
			Expect(err).NotTo(HaveOccurred())
			if out == "1" {
				count++
			}
		}
		Expect(count).To(Equal(1))
		Consistently(runs, 10*time.Second, 2*time.Second).Should(HaveLen(2))
	})
})
