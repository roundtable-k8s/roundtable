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

package controller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func smokeKnight(name, image string, phase aiv1alpha1.KnightPhase, labels map[string]string) aiv1alpha1.Knight {
	return aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Spec:       aiv1alpha1.KnightSpec{Image: image},
		Status:     aiv1alpha1.KnightStatus{Phase: phase},
	}
}

func TestSmokeVersion(t *testing.T) {
	ready := aiv1alpha1.KnightPhaseReady
	ks := []aiv1alpha1.Knight{
		smokeKnight("b", "img:2", ready, nil),
		smokeKnight("a", "", ready, nil),
		smokeKnight("eph", "img:eph", ready, map[string]string{aiv1alpha1.LabelEphemeral: "true"}),
		smokeKnight("warm", "img:warm", ready, map[string]string{aiv1alpha1.LabelWarmPool: "true"}),
	}
	v1, comps := smokeVersion("op:1", "def:1", ks)
	want := []string{"knight/a=def:1", "knight/b=img:2", "operator=op:1"}
	if strings.Join(comps, ",") != strings.Join(want, ",") {
		t.Fatalf("components = %v, want %v", comps, want)
	}
	// Order of the list does not matter.
	v1b, _ := smokeVersion("op:1", "def:1", []aiv1alpha1.Knight{ks[1], ks[0], ks[2], ks[3]})
	if v1 != v1b {
		t.Errorf("version depends on knight order: %s vs %s", v1, v1b)
	}
	for name, v := range map[string]string{
		"operator image": first(smokeVersion("op:2", "def:1", ks)),
		"default image":  first(smokeVersion("op:1", "def:2", ks)),
		"knight image":   first(smokeVersion("op:1", "def:1", []aiv1alpha1.Knight{ks[0], smokeKnight("a", "img:3", ready, nil)})),
	} {
		if v == v1 {
			t.Errorf("changing the %s did not change the version", name)
		}
	}
}

func first(v string, _ []string) string { return v }

func TestKnightsSettled(t *testing.T) {
	ready, prov := aiv1alpha1.KnightPhaseReady, aiv1alpha1.KnightPhaseProvisioning
	if ok, _ := knightsSettled([]aiv1alpha1.Knight{smokeKnight("a", "", ready, nil), smokeKnight("s", "", aiv1alpha1.KnightPhaseSuspended, nil)}); !ok {
		t.Error("ready and suspended knights should be settled")
	}
	ok, why := knightsSettled([]aiv1alpha1.Knight{smokeKnight("a", "", ready, nil), smokeKnight("b", "", prov, nil)})
	if ok || !strings.Contains(why, "knight b") {
		t.Errorf("provisioning knight: ok=%v why=%q", ok, why)
	}
	if ok, _ := knightsSettled([]aiv1alpha1.Knight{smokeKnight("e", "", prov, map[string]string{aiv1alpha1.LabelEphemeral: "x"})}); !ok {
		t.Error("ephemeral knights must be ignored")
	}
}

func smokeChain(steps ...aiv1alpha1.ChainStepStatus) *aiv1alpha1.Chain {
	return &aiv1alpha1.Chain{Status: aiv1alpha1.ChainStatus{Phase: aiv1alpha1.ChainPhaseSucceeded, StepStatuses: steps}}
}

func smokeStep(name string, phase aiv1alpha1.ChainStepPhase, output string) aiv1alpha1.ChainStepStatus {
	return aiv1alpha1.ChainStepStatus{Name: name, Phase: phase, Output: output}
}

func TestEvaluateSmoke(t *testing.T) {
	succ, fail, skip := aiv1alpha1.ChainStepPhaseSucceeded, aiv1alpha1.ChainStepPhaseFailed, aiv1alpha1.ChainStepPhaseSkipped
	withModel := smokeStep("m", succ, "Model: claude-sonnet\nResult: pass")
	withModel.Model = "claude-sonnet"
	wrongModel := smokeStep("m", succ, "Model: claude-opus\nResult: pass")
	wrongModel.Model = "claude-sonnet"
	noModel := smokeStep("m", succ, "Result: pass")
	noModel.Model = "claude-sonnet"
	errored := smokeStep("e", fail, "partial")
	errored.Error = "timed out"

	cases := []struct {
		name     string
		chain    *aiv1alpha1.Chain
		pass     bool
		failStep string
		reason   string
	}{
		{"all pass", smokeChain(smokeStep("a", succ, "ok\n**Result: pass**"), smokeStep("b", skip, "")), true, "", ""},
		{"explicit fail", smokeChain(smokeStep("a", succ, "Result: fail (no gh)")), false, "a", "no \"Result: pass\" line"},
		{"quoted pass is not a verdict", smokeChain(smokeStep("a", succ, `I will say "Result: pass" if ok. Result: fail (x)`)), false, "a", "no \"Result: pass\""},
		{"no verdict", smokeChain(smokeStep("a", succ, "done")), false, "a", "no \"Result: pass\""},
		{"failed step", smokeChain(errored), false, "e", "timed out"},
		{"nothing ran", smokeChain(smokeStep("a", skip, "")), false, "chain", "no smoke step ran"},
		{"model honoured", smokeChain(withModel), true, "", ""},
		{"model not honoured", smokeChain(wrongModel), false, "m", "model override not honoured"},
		{"model line missing", smokeChain(noModel), false, "m", "no Model line"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pass, failures := evaluateSmoke(c.chain)
			if pass != c.pass {
				t.Fatalf("pass = %v (%v), want %v", pass, failures, c.pass)
			}
			if pass {
				return
			}
			if failures[0].Step != c.failStep || !strings.Contains(failures[0].Reason, c.reason) {
				t.Errorf("failure = %+v, want step %q reason ~%q", failures[0], c.failStep, c.reason)
			}
		})
	}
}

func TestSmokeFailureBodySanitizes(t *testing.T) {
	rt := &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: "rt", Namespace: "ns"}}
	st := &aiv1alpha1.SmokeStatus{Components: []string{"operator=op:2"}, KnownGoodVersion: "old", KnownGoodComponents: []string{"operator=op:1"}}
	body := failureBody(rt, st, "abc", []smokeFailure{{Step: "smoke_x`\n# injected", Reason: "bad\x1b[31m", Output: "```\n@everyone\x00" + strings.Repeat("x", 5000)}})
	for _, want := range []string{"version `abc`", "`operator=op:2`", "Last known-good version: `old`", "operator=op:1"} {
		if !strings.Contains(body, want) {
			t.Errorf("body lacks %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "\x1b") || strings.Contains(body, "\x00") {
		t.Error("control characters survived")
	}
	if strings.Count(body, "```") != 2 {
		t.Errorf("output must not close its own fence:\n%s", body)
	}
	if len(body) > 4000 {
		t.Errorf("output not truncated: %d bytes", len(body))
	}
}

func TestSmokeExample(t *testing.T) {
	data, err := os.ReadFile("../../examples/chain-smoke.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var chain aiv1alpha1.Chain
	if err := yaml.UnmarshalStrict(data, &chain); err != nil {
		t.Fatal(err)
	}
	if err := (&ChainReconciler{}).validateDAG(&chain); err != nil {
		t.Fatal(err)
	}
	if reason, err := validateConditionsAndLoops(&chain); err != nil {
		t.Fatalf("%s: %v", reason, err)
	}
	if err := (&ChainReconciler{}).validateTemplates(&chain); err != nil {
		t.Fatal(err)
	}
	// Per knight the two context steps must run in different sessions.
	runIDs := map[string]string{}
	for _, s := range chain.Spec.Steps {
		if strings.HasPrefix(s.Name, "ctx_") && strings.HasSuffix(s.Name, "rt_lead") {
			runIDs[s.Name] = s.ResumeRunID
		}
	}
	if len(runIDs) != 2 || runIDs["ctx_set_rt_lead"] == runIDs["ctx_probe_rt_lead"] || runIDs["ctx_set_rt_lead"] == "" {
		t.Errorf("context steps must use different resumeRunIds: %v", runIDs)
	}
}

// fakeSmokeGitHub serves the issue and pull request endpoints of two
// repositories: o/issues (failure issues) and o/cluster (automation PRs).
type fakeSmokeGitHub struct {
	mu     sync.Mutex
	issues []map[string]any
	pulls  []map[string]any
	labels map[string][]string
	denied bool // the hold repo answers 403
}

func (f *fakeSmokeGitHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Header.Get("Authorization") == "" {
		http.Error(w, "no token", http.StatusUnauthorized)
		return
	}
	write := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/repos/o/issues/issues":
		b := map[string]any{}
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &b)
		f.issues = append(f.issues, b)
		write(map[string]any{"number": len(f.issues), "html_url": "https://github.com/o/issues/issues/1"})
	case strings.HasPrefix(r.URL.Path, "/repos/o/cluster/") && f.denied:
		http.Error(w, "forbidden", http.StatusForbidden)
	case r.Method == http.MethodGet && r.URL.Path == "/repos/o/cluster/pulls":
		write(f.pulls)
	case r.Method == http.MethodPost && r.URL.Path == "/repos/o/cluster/issues/7/labels":
		b := struct{ Labels []string }{}
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &b)
		f.labels["7"] = append(f.labels["7"], b.Labels...)
		write([]any{})
	default:
		http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusTeapot)
	}
}

var _ = Describe("Smoke watcher", func() {
	const (
		ns     = "smoke-ns"
		rtName = "smoke-table"
		chain  = "smoke-chain"
	)
	ctx := context.Background()
	rtKey := types.NamespacedName{Name: rtName, Namespace: ns}

	var (
		gh       *fakeSmokeGitHub
		srv      *httptest.Server
		now      time.Time
		r        *SmokeReconciler
		recorder *record.FakeRecorder
	)

	reconcileOnce := func() {
		now = now.Add(2 * time.Minute)
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: rtKey})
		Expect(err).NotTo(HaveOccurred())
	}
	status := func() *aiv1alpha1.SmokeStatus {
		rt := &aiv1alpha1.RoundTable{}
		Expect(k8sClient.Get(ctx, rtKey, rt)).To(Succeed())
		return rt.Status.Smoke
	}
	getChain := func() *aiv1alpha1.Chain {
		c := &aiv1alpha1.Chain{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: chain, Namespace: ns}, c)).To(Succeed())
		return c
	}
	// finishChain plays the Chain controller: the run the watcher started
	// ends with the given step outputs.
	finishChain := func(phase aiv1alpha1.ChainPhase, steps ...aiv1alpha1.ChainStepStatus) {
		c := getChain()
		c.Status = aiv1alpha1.ChainStatus{Phase: phase, LastRunTrigger: c.Annotations[aiv1alpha1.AnnotationChainRun], StepStatuses: steps}
		Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
	}
	setImage := func(image string) {
		k := &aiv1alpha1.Knight{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "smoke-knight", Namespace: ns}, k)).To(Succeed())
		k.Spec.Image = image
		Expect(k8sClient.Update(ctx, k)).To(Succeed())
	}
	configMap := func() map[string]string {
		cm := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rtName + "-smoke", Namespace: ns}, cm)).To(Succeed())
		return cm.Data
	}
	holdPR := func(branch string) map[string]any {
		return map[string]any{"number": 7, "html_url": "https://github.com/o/cluster/pull/7", "state": "open", "labels": []any{}, "head": map[string]any{"ref": branch}}
	}

	BeforeEach(func() {
		gh = &fakeSmokeGitHub{labels: map[string][]string{}, pulls: []map[string]any{holdPR("autodeploy/roundtable")}}
		srv = httptest.NewServer(gh)
		now = time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
		recorder = record.NewFakeRecorder(100)
		r = &SmokeReconciler{Client: k8sClient, Recorder: recorder, GitHubBaseURL: srv.URL, OperatorImage: "op:1", Now: func() time.Time { return now }}

		// Namespaces are never really deleted in envtest: create once.
		Expect(client.IgnoreAlreadyExists(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))).To(Succeed())
		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "smoke-gh", Namespace: ns},
			StringData: map[string]string{"token": "gh-test-token"},
		})).To(Succeed())
		k := &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: "smoke-knight", Namespace: ns},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "dev", Image: "knight:1", Skills: []string{"shared"},
				NATS: aiv1alpha1.KnightNATS{Subjects: []string{"smoke.tasks.dev.>"}},
			},
		}
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		k.Status.Phase = aiv1alpha1.KnightPhaseReady
		Expect(k8sClient.Status().Update(ctx, k)).To(Succeed())
		Expect(k8sClient.Create(ctx, &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: ns},
			Spec: aiv1alpha1.RoundTableSpec{
				NATS:   aiv1alpha1.RoundTableNATS{URL: "nats://localhost:4222", SubjectPrefix: "smoke", TasksStream: "smoke_tasks", ResultsStream: "smoke_results"},
				GitHub: &aiv1alpha1.GitHubConfig{TokenSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "smoke-gh"}, Key: "token"}},
				Smoke: &aiv1alpha1.SmokeSpec{
					Enabled:   true,
					ChainRef:  &corev1.LocalObjectReference{Name: chain},
					IssueRepo: "o/issues",
					Hold:      &aiv1alpha1.SmokeHold{Repo: "o/cluster"},
				},
			},
		})).To(Succeed())
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: chain, Namespace: ns},
			Spec: aiv1alpha1.ChainSpec{RoundTableRef: aiv1alpha1.LocalRef(rtName), Steps: []aiv1alpha1.ChainStep{
				{Name: "smoke", KnightRef: corev1.LocalObjectReference{Name: "smoke-knight"}, Task: "smoke {{ .Input }}"},
			}},
		})).To(Succeed())
	})

	AfterEach(func() {
		srv.Close()
		_ = k8sClient.Delete(ctx, &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: chain, Namespace: ns}})
		_ = k8sClient.Delete(ctx, &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: ns}})
		_ = k8sClient.Delete(ctx, &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: "smoke-knight", Namespace: ns}})
		_ = k8sClient.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: rtName + "-smoke", Namespace: ns}})
		_ = k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "smoke-gh", Namespace: ns}})
	})

	pass := aiv1alpha1.ChainStepStatus{Name: "smoke", Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: "Result: pass"}

	It("tests a settled version once, records it known-good, and retests after the next rollout", func() {
		reconcileOnce()
		Expect(status().ObservedVersion).NotTo(BeEmpty())
		Expect(status().ActiveRun).To(BeEmpty(), "a version is tested only after two polls saw it")
		Expect(getChain().Annotations).NotTo(HaveKey(aiv1alpha1.AnnotationChainRun))

		reconcileOnce()
		v1 := status().ObservedVersion
		Expect(status().ActiveRun).To(HavePrefix(v1 + "@smoke-"))
		c := getChain()
		Expect(c.Annotations).To(HaveKey(aiv1alpha1.AnnotationChainRun))
		Expect(c.Spec.Input).To(ContainSubstring(`"version":"` + v1 + `"`))
		Expect(c.Spec.Input).To(ContainSubstring("knight/smoke-knight=knight:1"))

		finishChain(aiv1alpha1.ChainPhaseRunning)
		reconcileOnce()
		Expect(status().ActiveRun).NotTo(BeEmpty(), "a running chain is waited for")

		finishChain(aiv1alpha1.ChainPhaseSucceeded, pass)
		reconcileOnce()
		st := status()
		Expect(st.ActiveRun).To(BeEmpty())
		Expect(st.LastResult).To(Equal(aiv1alpha1.SmokePassed))
		Expect(st.KnownGoodVersion).To(Equal(v1))
		Expect(st.KnownGoodComponents).To(ContainElement("knight/smoke-knight=knight:1"))
		Expect(st.Held).To(BeFalse())
		Expect(configMap()).To(HaveKeyWithValue("known-good-version", v1))
		Expect(configMap()).To(HaveKeyWithValue("held", "false"))

		// The known-good version is not tested again.
		trigger := getChain().Annotations[aiv1alpha1.AnnotationChainRun]
		reconcileOnce()
		reconcileOnce()
		Expect(getChain().Annotations[aiv1alpha1.AnnotationChainRun]).To(Equal(trigger))

		// A new image starts a new smoke.
		setImage("knight:2")
		reconcileOnce()
		reconcileOnce()
		Expect(status().ActiveRun).NotTo(BeEmpty())
		Expect(status().ActiveRun).NotTo(HavePrefix(v1 + "@"))
	})

	It("waits for the rollout to settle", func() {
		k := &aiv1alpha1.Knight{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "smoke-knight", Namespace: ns}, k)).To(Succeed())
		k.Status.Phase = aiv1alpha1.KnightPhaseProvisioning
		Expect(k8sClient.Status().Update(ctx, k)).To(Succeed())
		reconcileOnce()
		reconcileOnce()
		Expect(status().ActiveRun).To(BeEmpty())
		Expect(status().Message).To(ContainSubstring("smoke-knight is Provisioning"))
	})

	It("opens a needs-human issue and holds the automation PR when the smoke fails", func() {
		reconcileOnce()
		reconcileOnce()
		v1 := status().ObservedVersion
		finishChain(aiv1alpha1.ChainPhasePartiallySucceeded,
			aiv1alpha1.ChainStepStatus{Name: "smoke", Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: "guardrail: rm ran\nResult: fail (guardrail missing)"})
		reconcileOnce()

		st := status()
		Expect(st.LastResult).To(Equal(aiv1alpha1.SmokeFailed))
		Expect(st.LastTestedVersion).To(Equal(v1))
		Expect(st.KnownGoodVersion).To(BeEmpty())
		Expect(st.Held).To(BeTrue())
		Expect(st.IssueURL).To(Equal("https://github.com/o/issues/issues/1"))
		Expect(st.HoldPullRequest).To(Equal("https://github.com/o/cluster/pull/7"))
		Expect(st.ActiveRun).To(BeEmpty())
		Expect(gh.issues).To(HaveLen(1))
		Expect(gh.issues[0]["labels"]).To(ConsistOf("needs-human"))
		Expect(gh.issues[0]["body"]).To(ContainSubstring("guardrail missing"))
		Expect(gh.labels["7"]).To(ConsistOf("hold"))
		Expect(configMap()).To(HaveKeyWithValue("held", "true"))
		Expect(configMap()).To(HaveKeyWithValue("last-result", "Failed"))

		// The failed version is not retried and no second issue is opened.
		reconcileOnce()
		reconcileOnce()
		Expect(gh.issues).To(HaveLen(1))
		Expect(status().ActiveRun).To(BeEmpty())

		// A later pass releases the status (the label stays for a human).
		setImage("knight:fixed")
		reconcileOnce()
		reconcileOnce()
		finishChain(aiv1alpha1.ChainPhaseSucceeded, pass)
		reconcileOnce()
		Expect(status().Held).To(BeFalse())
		Expect(status().HoldPullRequest).To(BeEmpty())
		Expect(configMap()).To(HaveKeyWithValue("held", "false"))
	})

	It("keeps holding and retries when the automation PR is missing or unreachable", func() {
		gh.pulls = nil
		reconcileOnce()
		reconcileOnce()
		finishChain(aiv1alpha1.ChainPhaseSucceeded, aiv1alpha1.ChainStepStatus{Name: "smoke", Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "timed out"})
		reconcileOnce()
		Expect(status().Held).To(BeTrue())
		Expect(status().HoldPullRequest).To(BeEmpty())
		Expect(status().Message).To(ContainSubstring("no open pull request"))

		gh.denied = true
		reconcileOnce()
		Expect(status().LastError).To(ContainSubstring("hold o/cluster"))
		Expect(status().Held).To(BeTrue())

		gh.denied = false
		gh.pulls = []map[string]any{holdPR("autodeploy/roundtable")}
		reconcileOnce()
		Expect(status().HoldPullRequest).To(Equal("https://github.com/o/cluster/pull/7"))
		Expect(status().LastError).To(BeEmpty())
		Expect(gh.labels["7"]).To(ConsistOf("hold"))
		Expect(gh.issues).To(HaveLen(1))
	})

	It("does not label a pull request from another branch", func() {
		gh.pulls = []map[string]any{holdPR("renovate/something")}
		reconcileOnce()
		reconcileOnce()
		finishChain(aiv1alpha1.ChainPhaseFailed)
		reconcileOnce()
		Expect(status().Held).To(BeTrue())
		Expect(gh.labels).To(BeEmpty())
	})

	It("forgets a run whose chain never started", func() {
		reconcileOnce()
		reconcileOnce()
		old := status().ActiveRun
		Expect(old).NotTo(BeEmpty())
		now = now.Add(10 * time.Minute)
		reconcileOnce()
		// The stale run was dropped and the same version dispatched again.
		Expect(status().ActiveRun).NotTo(BeEmpty())
		Expect(status().ActiveRun).NotTo(Equal(old))
	})
})
