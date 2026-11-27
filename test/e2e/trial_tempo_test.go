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
	"net/url"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/roundtable-k8s/roundtable/test/utils"
)

// The Tempo trial (#768): the Chain trace run exported to a Tempo in the kind
// cluster, read back through Tempo's HTTP API. Results go into
// docs/design/observability-standard.md ("Trial results: Tempo").
const (
	tempoManifests = "test/e2e/testdata/tempo.yaml"
	trialValues    = "test/e2e/testdata/otel-trial-values.yaml"
	tempoImage     = "grafana/tempo:3.1.1"
	// trialNoticePrefix marks the measurement line; the e2e-trial job turns it
	// into a ::notice annotation.
	trialNoticePrefix = "TRIAL_NOTICE: "
)

var _ = Describe("Trial: Grafana Tempo", Ordered, Label(tierTrial), func() {
	var atRest tempoStats

	BeforeAll(func() {
		By("creating the namespace and Tempo")
		// The trace tier shares this namespace; wait out a previous deletion.
		_, err := utils.Run(exec.Command("kubectl", "delete", "namespace", traceNamespace,
			"--ignore-not-found", "--wait=true", "--timeout=5m"))
		Expect(err).NotTo(HaveOccurred())
		_, err = utils.Run(exec.Command("kubectl", "create", "namespace", traceNamespace))
		Expect(err).NotTo(HaveOccurred())
		manifests, err := readProjectFile(tempoManifests)
		Expect(err).NotTo(HaveOccurred())
		apply := exec.Command("kubectl", "apply", "-f", "-")
		apply.Stdin = strings.NewReader(manifests)
		_, err = utils.Run(apply)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for Tempo to be ready")
		Eventually(func(g Gomega) {
			out, err := tempoGet("/ready")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring("ready"))
		}, 5*time.Minute, 3*time.Second).Should(Succeed())
		time.Sleep(20 * time.Second) // settle: "at rest" is a quiet Tempo
		atRest = tempoMeasure()

		By("installing the chart with telemetry, the collector, file and Tempo exporters")
		_, err = utils.Run(exec.Command("helm", "upgrade", "--install", traceRelease, "charts/roundtable-operator",
			"-n", traceNamespace, "-f", trialValues, "--wait", "--timeout", "5m"))
		Expect(err).NotTo(HaveOccurred())

		By("creating the table, the stub knight and the two-step Chain")
		chainManifests, err := readProjectFile(traceManifests)
		Expect(err).NotTo(HaveOccurred())
		apply = exec.Command("kubectl", "apply", "-f", "-")
		apply.Stdin = strings.NewReader(strings.ReplaceAll(chainManifests, "FAKE_CREDENTIAL", fakeCredential))
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
		_, _ = utils.Run(exec.Command("kubectl", "delete", "chain,knight,roundtable", "--all",
			"-n", traceNamespace, "--ignore-not-found", "--timeout=2m"))
		_, _ = utils.Run(exec.Command("helm", "uninstall", traceRelease, "-n", traceNamespace, "--wait"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", traceNamespace, "--ignore-not-found", "--wait=true", "--timeout=5m"))
	})

	AfterEach(func() {
		if !CurrentSpecReport().Failed() {
			return
		}
		for _, args := range [][]string{
			{"get", "chain", traceChain, "-n", traceNamespace, "-o", "yaml"},
			{"get", "knight,pods", "-n", traceNamespace, "-o", "wide"},
			{"logs", "deployment/tempo", "-c", "tempo", "-n", traceNamespace, "--tail=100"},
			{"logs", "deployment/" + traceCollector, "-n", traceNamespace, "--tail=50"},
			{"get", "events", "-n", traceNamespace, "--sort-by=.lastTimestamp"},
		} {
			out, err := utils.Run(exec.Command("kubectl", args...))
			_, _ = fmt.Fprintf(GinkgoWriter, "kubectl %s:\n%s\n%v\n", strings.Join(args, " "), out, err)
		}
	})

	It("stores the run as one tree, finds it by table, keeps content readable and the credential out", func() {
		traceID := runChain("run-1")

		By("(a) the whole trace by id: root, step spans and knight spans in one tree")
		var body string
		Eventually(func(g Gomega) {
			out, err := tempoGet("/api/v2/traces/" + traceID)
			g.Expect(err).NotTo(HaveOccurred())
			spans, err := utils.ParseTempoTrace(out)
			g.Expect(err).NotTo(HaveOccurred())
			checkTraceSpans(g, spans, traceID, true)
			body = out
		}, 3*time.Minute, 3*time.Second).Should(Succeed())

		By("(b) a TraceQL search on roundtable.table finds the run")
		query := fmt.Sprintf(`{ .roundtable.table = %q }`, traceTable)
		Eventually(func(g Gomega) {
			out, err := tempoGet("/api/search?limit=20&q=" + url.QueryEscape(query))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(ContainSubstring(traceID), "TraceQL %s", query)
		}, 3*time.Minute, 3*time.Second).Should(Succeed())

		By("(c) content attributes are readable, the fake credential is not returned")
		spans, err := utils.ParseTempoTrace(body)
		Expect(err).NotTo(HaveOccurred())
		var readable bool
		for _, s := range utils.SpansWithPrefix(spans, "chat ") {
			if len(s.Attrs["gen_ai.input.messages"]) > 0 && len(s.Attrs["gen_ai.output.messages"]) > 0 {
				readable = true
			}
		}
		Expect(readable).To(BeTrue(), "no chat span with readable prompt and response")
		Expect(body).NotTo(ContainSubstring(fakeCredential), "Tempo returned the fake credential")

		By("measuring Tempo after the run")
		after := tempoMeasure()
		line := fmt.Sprintf("%stempo=%s memory_rss_at_rest=%s memory_rss_after=%s cpu_seconds_at_rest=%.1f cpu_seconds_after=%.1f "+
			"storage_at_rest=%dKiB storage_after=%dKiB", trialNoticePrefix, tempoVersion(),
			mib(atRest.rssBytes), mib(after.rssBytes), atRest.cpuSeconds, after.cpuSeconds, atRest.storageKiB, after.storageKiB)
		_, _ = fmt.Fprintln(GinkgoWriter, line)
	})
})

// tempoGet GETs a path of Tempo's HTTP port through the API server's service proxy.
func tempoGet(path string) (string, error) {
	cmd := exec.Command("kubectl", "get", "--raw", "/api/v1/namespaces/"+traceNamespace+"/services/tempo:http/proxy"+path)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		err = fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return string(out), err
}

// tempoVersion is the version Tempo reports (buildinfo), or the image if it cannot say.
func tempoVersion() string {
	out, err := tempoGet("/api/status/buildinfo")
	if err == nil {
		if m := regexp.MustCompile(`"version":\s*"([^"]+)"`).FindStringSubmatch(out); m != nil {
			return m[1] + " (" + tempoImage + ")"
		}
	}
	return tempoImage
}

// tempoStats is what the trial measures. Memory and CPU come from Tempo's own
// process metrics (kind has no metrics-server, so `kubectl top` is not an
// option); storage is `du` of /var/tempo from a sidecar sharing the volume.
type tempoStats struct {
	rssBytes   float64
	cpuSeconds float64
	storageKiB int64
}

func tempoMeasure() tempoStats {
	var st tempoStats
	metrics, err := tempoGet("/metrics")
	Expect(err).NotTo(HaveOccurred())
	st.rssBytes = metricValue(metrics, "process_resident_memory_bytes")
	st.cpuSeconds = metricValue(metrics, "process_cpu_seconds_total")

	out, err := utils.Run(exec.Command("kubectl", "exec", "deployment/tempo", "-c", "data-probe",
		"-n", traceNamespace, "--", "du", "-sk", "/var/tempo"))
	Expect(err).NotTo(HaveOccurred())
	fields := strings.Fields(out)
	Expect(fields).NotTo(BeEmpty())
	st.storageKiB, err = strconv.ParseInt(fields[0], 10, 64)
	Expect(err).NotTo(HaveOccurred())
	return st
}

func metricValue(metrics, name string) float64 {
	m := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(name) + `\s+(\S+)`).FindStringSubmatch(metrics)
	Expect(m).NotTo(BeNil(), "metric %s", name)
	v, err := strconv.ParseFloat(m[1], 64)
	Expect(err).NotTo(HaveOccurred())
	return v
}

func mib(b float64) string { return fmt.Sprintf("%.0fMiB", b/(1<<20)) }
