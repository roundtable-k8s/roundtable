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
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/roundtable-k8s/roundtable/test/utils"
)

// The Chain trace case (#500): the chart with telemetry and its collector on,
// a two-step Chain run against the stub knight, and the exported spans checked
// against docs/design/telemetry-conventions.md.
const (
	traceNamespace   = "roundtable-e2e"
	traceRelease     = "rt-e2e"
	traceCollector   = "rt-e2e-otel-collector" // fullnameOverride + "-otel-collector"
	traceValues      = "test/e2e/testdata/otel-values.yaml"
	traceNATS        = "test/e2e/testdata/nats.yaml"
	traceManifests   = "test/e2e/testdata/chain-trace.yaml"
	traceTable       = "e2e-table"
	traceKnight      = "stub"
	traceChain       = "trace-e2e"
	traceTasksPrefix = "e2e.tasks."
	traceResultsPfx  = "e2e.results."
)

// traceSteps are the Chain's steps, in order.
var traceSteps = []string{"gather", "report"}

// fakeCredential is a GitHub token shape the knights' redactSecrets list
// matches (ghp_ + 36 chars). It is put in the first step's prompt and must
// never be exported as is.
var fakeCredential = "ghp_" + strings.Repeat("e2eFAKE0", 4) + "abcd"

// contentAttrs are the content attributes of docs/design/telemetry-conventions.md
// "Content capture" (chat spans) plus the GenAI tool content attributes.
var contentAttrs = []string{
	"gen_ai.input.messages",
	"gen_ai.output.messages",
	"gen_ai.system_instructions",
	"gen_ai.tool.call.arguments",
	"gen_ai.tool.call.result",
}

// redactionMarkers are what a redacted secret is replaced with: the knights'
// redactSecrets, or the collector's redaction processor.
var redactionMarkers = []string{"[REDACTED]", "****"}

var _ = Describe("Chain trace", Ordered, Label(tierTrace), func() {
	BeforeAll(func() {
		By("deploying NATS with JetStream")
		_, err := utils.Run(exec.Command("kubectl", "apply", "-f", traceNATS))
		Expect(err).NotTo(HaveOccurred())
		_, err = utils.Run(exec.Command("kubectl", "rollout", "status", "deployment/nats",
			"-n", "database", "--timeout=3m"))
		Expect(err).NotTo(HaveOccurred())

		By("installing the chart with telemetry and the collector (file exporter)")
		_, err = utils.Run(exec.Command("helm", "upgrade", "--install", traceRelease, "charts/roundtable-operator",
			"-n", traceNamespace, "--create-namespace", "-f", traceValues, "--wait", "--timeout", "5m"))
		Expect(err).NotTo(HaveOccurred())

		By("creating the table, the stub knight and the two-step Chain")
		manifests, err := readProjectFile(traceManifests)
		Expect(err).NotTo(HaveOccurred())
		apply := exec.Command("kubectl", "apply", "-f", "-")
		apply.Stdin = strings.NewReader(strings.ReplaceAll(manifests, "FAKE_CREDENTIAL", fakeCredential))
		_, err = utils.Run(apply)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the stub knight to be Ready")
		Eventually(func(g Gomega) {
			out, err := kubectlGet("knight", traceKnight, "{.status.phase}")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("Ready"))
		}, 5*time.Minute, 2*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		// Delete the resources while the operator still runs their finalizers.
		_, _ = utils.Run(exec.Command("kubectl", "delete", "chain,knight,roundtable", "--all",
			"-n", traceNamespace, "--ignore-not-found", "--timeout=2m"))
		_, _ = utils.Run(exec.Command("helm", "uninstall", traceRelease, "-n", traceNamespace, "--wait"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", traceNamespace, "--ignore-not-found", "--wait=false"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "-f", traceNATS, "--ignore-not-found", "--wait=false"))
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		for _, args := range [][]string{
			{"get", "chain", traceChain, "-n", traceNamespace, "-o", "yaml"},
			{"get", "knight,pods", "-n", traceNamespace, "-o", "wide"},
			{"logs", "deployment/" + traceRelease, "-n", traceNamespace, "--tail=200"},
			{"logs", "-l", "app.kubernetes.io/name=knight", "-n", traceNamespace, "--tail=100"},
			{"logs", "deployment/" + traceCollector, "-n", traceNamespace, "--tail=50"},
			{"get", "events", "-n", traceNamespace, "--sort-by=.lastTimestamp"},
		} {
			out, err := utils.Run(exec.Command("kubectl", args...))
			_, _ = fmt.Fprintf(GinkgoWriter, "kubectl %s:\n%s\n%v\n", strings.Join(args, " "), out, err)
		}
	})

	It("exports one connected, redacted trace with the table on every span", func() {
		traceID := runChain("run-1")
		Eventually(func(g Gomega) {
			checkChainTrace(g, collectorOutput(g), traceID, true)
		}, 2*time.Minute, 3*time.Second).Should(Succeed())
	})

	It("exports no content attributes with captureContent=false", func() {
		By("turning content capture off in the chart")
		_, err := utils.Run(exec.Command("helm", "upgrade", traceRelease, "charts/roundtable-operator",
			"-n", traceNamespace, "-f", traceValues, "--set", "telemetry.captureContent=false",
			"--wait", "--timeout", "5m"))
		Expect(err).NotTo(HaveOccurred())

		By("waiting until only knight pods with capture off are left")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "pods", "-n", traceNamespace,
				"-l", "app.kubernetes.io/name=knight,app.kubernetes.io/instance="+traceKnight,
				"-o", `jsonpath={range .items[*]}{.metadata.name}|{.metadata.deletionTimestamp}|`+
					`{.status.containerStatuses[?(@.name=="app")].ready}|`+
					`{.spec.containers[?(@.name=="app")].env[?(@.name=="ROUNDTABLE_TELEMETRY_CAPTURE_CONTENT")].value}{"\n"}{end}`))
			g.Expect(err).NotTo(HaveOccurred())
			pods := utils.GetNonEmptyLines(out)
			g.Expect(pods).To(HaveLen(1), "knight pods: %v", pods)
			f := strings.Split(pods[0], "|")
			g.Expect(f).To(HaveLen(4))
			g.Expect(f[1]).To(BeEmpty(), "pod is terminating")
			g.Expect(f[2]).To(Equal("true"), "pod is not ready")
			g.Expect(f[3]).To(Equal("false"), "capture content env")
		}, 5*time.Minute, 2*time.Second).Should(Succeed())

		traceID := runChain("run-2")
		Eventually(func(g Gomega) {
			checkChainTrace(g, collectorOutput(g), traceID, false)
		}, 2*time.Minute, 3*time.Second).Should(Succeed())
	})
})

// runChain starts a new run of the Chain with the run annotation, waits for it
// to succeed and returns its trace ID.
func runChain(trigger string) string {
	By("starting Chain run " + trigger)
	_, err := utils.Run(exec.Command("kubectl", "annotate", "chain", traceChain, "-n", traceNamespace,
		"--overwrite", "ai.roundtable.io/run="+trigger))
	Expect(err).NotTo(HaveOccurred())

	var traceID string
	Eventually(func(g Gomega) {
		out, err := kubectlGet("chain", traceChain, "{.status.lastRunTrigger}|{.status.phase}|{.status.traceID}")
		g.Expect(err).NotTo(HaveOccurred())
		f := strings.Split(out, "|")
		g.Expect(f).To(HaveLen(3))
		g.Expect(f[0]).To(Equal(trigger))
		g.Expect(f[1]).NotTo(Equal("Failed"), "chain run failed")
		g.Expect(f[1]).To(Equal("Succeeded"))
		g.Expect(f[2]).To(MatchRegexp(`^[0-9a-f]{32}$`))
		traceID = f[2]
	}, 5*time.Minute, 2*time.Second).Should(Succeed())
	return traceID
}

// collectorOutput returns what the collector wrote: OTLP JSON lines from the
// file exporter, mixed with the collector's own log lines.
func collectorOutput(g Gomega) string {
	cmd := exec.Command("kubectl", "logs", "deployment/"+traceCollector, "-n", traceNamespace)
	out, err := cmd.Output()
	g.Expect(err).NotTo(HaveOccurred())
	return string(out)
}

// checkChainTrace asserts the conventions on the exported spans of one run.
func checkChainTrace(g Gomega, out, traceID string, captureContent bool) {
	g.Expect(out).NotTo(ContainSubstring(fakeCredential), "the fake credential was exported unredacted")

	all, err := utils.ParseOTLPJSONLines(out)
	g.Expect(err).NotTo(HaveOccurred())
	checkTraceSpans(g, all, traceID, captureContent)
}

// checkTraceSpans asserts the conventions on the spans of trace traceID; all
// are every exported span.
func checkTraceSpans(g Gomega, all []utils.Span, traceID string, captureContent bool) {
	spans := utils.SpansInTrace(all, traceID)
	g.Expect(spans).NotTo(BeEmpty(), "no spans in trace %s", traceID)

	// One trace: one root (the run), everything else reachable from it.
	g.Expect(utils.CheckConnected(spans)).To(Succeed())
	roots := utils.SpansNamed(spans, "chain "+traceChain)
	g.Expect(roots).To(HaveLen(1), "chain root span")
	root := roots[0]
	g.Expect(root.ParentSpanID).To(BeEmpty())

	// The table on every span.
	for _, s := range spans {
		table, ok := s.Attr("roundtable.table")
		g.Expect(ok).To(BeTrue(), "span %q has no roundtable.table", s.Name)
		g.Expect(table).To(Equal(traceTable), "span %q", s.Name)
	}

	taskIDs := map[string]bool{}
	for _, step := range traceSteps {
		stepSpans := utils.SpansNamed(spans, "step "+step)
		g.Expect(stepSpans).To(HaveLen(1), "step %s spans", step)
		stepSpan := stepSpans[0]
		g.Expect(utils.IsDescendant(spans, stepSpan, root)).To(BeTrue())
		taskID := stepSpan.Attrs["roundtable.task.id"]
		g.Expect(taskID).NotTo(BeEmpty(), "step %s span has no roundtable.task.id", step)
		taskIDs[taskID] = true

		task := spansForTask(spans, taskID)

		// operator -> NATS: the task publish under the step.
		publish := one(g, utils.SpansWithPrefix(task, "publish "+traceTasksPrefix), "task publish of "+step)
		g.Expect(publish.Attrs).To(HaveKeyWithValue("messaging.system", "nats"))
		g.Expect(utils.IsDescendant(spans, publish, stepSpan)).To(BeTrue(), "task publish under step %s", step)

		// NATS -> knight: the knight task span continues the publish.
		agent := one(g, utils.SpansNamed(task, "invoke_agent "+traceKnight), "invoke_agent of "+step)
		g.Expect(agent.Attrs).To(HaveKeyWithValue("gen_ai.operation.name", "invoke_agent"))
		g.Expect(agent.Attrs).To(HaveKeyWithValue("gen_ai.agent.name", traceKnight))
		g.Expect(utils.IsDescendant(spans, agent, publish)).To(BeTrue(), "invoke_agent under the task publish of %s", step)

		// The LLM turn: tokens, cost and (only when captured) content.
		var chats []utils.Span
		for _, s := range utils.SpansWithPrefix(spans, "chat ") {
			if utils.IsDescendant(spans, s, agent) {
				chats = append(chats, s)
			}
		}
		g.Expect(chats).NotTo(BeEmpty(), "chat spans under invoke_agent of %s", step)
		for _, chat := range chats {
			g.Expect(chat.Attrs).To(HaveKeyWithValue("gen_ai.operation.name", "chat"))
			g.Expect(chat.Attrs).To(HaveKey("gen_ai.request.model"))
			for _, k := range []string{"gen_ai.usage.input_tokens", "gen_ai.usage.output_tokens"} {
				n, err := strconv.ParseInt(chat.Attrs[k], 10, 64)
				g.Expect(err).NotTo(HaveOccurred(), "%s on %q", k, chat.Name)
				g.Expect(n).To(BeNumerically(">", 0), k)
			}
			cost, err := strconv.ParseFloat(chat.Attrs["roundtable.cost.usd"], 64)
			g.Expect(err).NotTo(HaveOccurred(), "roundtable.cost.usd on %q", chat.Name)
			g.Expect(cost).To(BeNumerically(">", 0))
			if captureContent {
				g.Expect(chat.Attrs["gen_ai.input.messages"]).NotTo(BeEmpty(), "prompt content on %q", chat.Name)
				g.Expect(chat.Attrs["gen_ai.output.messages"]).NotTo(BeEmpty(), "response content on %q", chat.Name)
			}
		}
		if captureContent && step == traceSteps[0] {
			// The prompt with the fake credential is captured, redacted.
			g.Expect(chats[0].Attrs["gen_ai.input.messages"]).To(SatisfyAny(
				ContainSubstring(redactionMarkers[0]), ContainSubstring(redactionMarkers[1])),
				"the prompt content is not redacted")
		}

		// knight -> NATS -> operator: the result is consumed in the operator
		// under the knight's span.
		result := one(g, utils.SpansNamed(spans, "process "+traceResultsPfx+taskID), "result consume of "+step)
		g.Expect(result.Attrs).To(HaveKeyWithValue("messaging.operation.type", "process"))
		g.Expect(utils.IsDescendant(spans, result, agent)).To(BeTrue(), "result consume under invoke_agent of %s", step)
	}

	// No span of these tasks landed in another trace.
	for _, s := range all {
		if s.TraceID != traceID && taskIDs[s.Attrs["roundtable.task.id"]] {
			g.Expect(s.TraceID).To(Equal(traceID),
				"span %q of task %s is in another trace", s.Name, s.Attrs["roundtable.task.id"])
		}
	}

	if !captureContent {
		for _, s := range spans {
			for _, k := range contentAttrs {
				g.Expect(s.Attrs).NotTo(HaveKey(k), "content attribute on %q with captureContent=false", s.Name)
			}
		}
	}
}

func spansForTask(spans []utils.Span, taskID string) []utils.Span {
	var out []utils.Span
	for _, s := range spans {
		if s.Attrs["roundtable.task.id"] == taskID {
			out = append(out, s)
		}
	}
	return out
}

func one(g Gomega, spans []utils.Span, what string) utils.Span {
	g.Expect(spans).To(HaveLen(1), what)
	return spans[0]
}

// readProjectFile reads a file given relative to the project root.
func readProjectFile(path string) (string, error) {
	dir, err := utils.GetProjectDir()
	if err != nil {
		return "", err
	}
	b, err := os.ReadFile(filepath.Join(dir, path))
	return string(b), err
}

func kubectlGet(kind, name, jsonpath string) (string, error) {
	out, err := utils.Run(exec.Command("kubectl", "get", kind, name, "-n", traceNamespace,
		"-o", "jsonpath="+jsonpath))
	return strings.TrimSpace(out), err
}
