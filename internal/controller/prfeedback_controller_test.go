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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/github"
)

// fakePRGitHub serves the REST endpoints the PR feedback watcher uses for one
// repository, o/r, from mutable state.
type fakePRGitHub struct {
	mu       sync.Mutex
	pulls    []map[string]any
	closed   []map[string]any
	checks   map[string][]map[string]any // head sha -> check runs
	mgRuns   []map[string]any
	logs     map[string]string // job id -> log
	labels   map[string][]string
	comments map[string][]string
	issues   []map[string]any
}

func newFakePRGitHub() *fakePRGitHub {
	return &fakePRGitHub{checks: map[string][]map[string]any{}, logs: map[string]string{}, labels: map[string][]string{}, comments: map[string][]string{}}
}

func (f *fakePRGitHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer gh-test-token" {
		http.Error(w, "bad credentials", http.StatusUnauthorized)
		return
	}
	p := strings.TrimPrefix(r.URL.Path, "/repos/o/r")
	parts := strings.Split(strings.Trim(p, "/"), "/")
	write := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	body := func() map[string]any {
		m := map[string]any{}
		b, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(b, &m); err != nil {
			var arr []any // add-labels is a bare JSON array
			if json.Unmarshal(b, &arr) == nil {
				m["labels"] = arr
			}
		}
		return m
	}
	switch {
	case r.Method == http.MethodGet && p == "/pulls":
		write(f.pulls)
	case r.Method == http.MethodGet && len(parts) == 2 && parts[0] == "pulls":
		for _, pr := range append(f.pulls, f.closed...) {
			if fmt.Sprint(pr["number"]) == parts[1] {
				write(pr)
				return
			}
		}
		http.Error(w, "not found", http.StatusNotFound)
	case r.Method == http.MethodGet && len(parts) == 3 && parts[0] == "commits" && parts[2] == "check-runs":
		write(map[string]any{"check_runs": f.checks[parts[1]]})
	case r.Method == http.MethodGet && p == "/actions/runs":
		write(map[string]any{"workflow_runs": f.mgRuns})
	case r.Method == http.MethodGet && len(parts) == 4 && parts[1] == "jobs" && parts[3] == "logs":
		http.Redirect(w, r, "http://"+r.Host+"/blob/"+parts[2], http.StatusFound)
	case r.Method == http.MethodGet && len(parts) == 0 || strings.HasPrefix(r.URL.Path, "/blob/"):
		_, _ = io.WriteString(w, f.logs[strings.TrimPrefix(r.URL.Path, "/blob/")])
	case r.Method == http.MethodGet && len(parts) == 4 && parts[1] == "runs" && parts[3] == "jobs":
		write(map[string]any{"jobs": []map[string]any{{"id": 900, "name": "build", "conclusion": "failure"}}})
	case r.Method == http.MethodPost && len(parts) == 3 && parts[0] == "issues" && parts[2] == "labels":
		for _, l := range body()["labels"].([]any) {
			f.labels[parts[1]] = append(f.labels[parts[1]], l.(string))
		}
		write([]any{})
	case r.Method == http.MethodPost && len(parts) == 3 && parts[0] == "issues" && parts[2] == "comments":
		f.comments[parts[1]] = append(f.comments[parts[1]], body()["body"].(string))
		write(map[string]any{})
	case r.Method == http.MethodGet && p == "/issues":
		write(f.issues)
	case r.Method == http.MethodPost && p == "/issues":
		b := body()
		n := 100 + len(f.issues)
		is := map[string]any{"number": n, "title": b["title"], "html_url": fmt.Sprintf("https://github.com/o/r/issues/%d", n), "labels": labelObjects(b["labels"])}
		f.issues = append(f.issues, is)
		write(is)
	default:
		http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), http.StatusTeapot)
	}
}

// labelObjects turns the label names of a create-issue request into the label
// objects GitHub returns.
func labelObjects(v any) []map[string]any {
	names, _ := v.([]any)
	out := make([]map[string]any, 0, len(names))
	for _, n := range names {
		out = append(out, map[string]any{"name": n})
	}
	return out
}

func fakePull(number int, sha string) map[string]any {
	return map[string]any{
		"number": number, "html_url": fmt.Sprintf("https://github.com/o/r/pull/%d", number), "state": "open",
		"user": map[string]any{"login": "dunce-bot[bot]"}, "labels": []any{}, "head": map[string]any{"sha": sha},
	}
}

func fakeCheck(id int, name, conclusion, started string) map[string]any {
	return map[string]any{
		"id": id, "name": name, "status": "completed", "conclusion": conclusion, "started_at": started,
		"html_url": fmt.Sprintf("https://github.com/o/r/actions/runs/1/job/%d", id), "app": map[string]any{"slug": "github-actions"},
	}
}

var _ = Describe("PR feedback watcher", func() {
	const (
		ns           = "default"
		rtName       = "fb-table"
		feedbackName = "fb-feedback"
		pipelineName = "fb-pipeline"
	)
	ctx := context.Background()
	rtKey := types.NamespacedName{Name: rtName, Namespace: ns}

	var (
		gh  *fakePRGitHub
		srv *httptest.Server
		now time.Time
		r   *PRFeedbackReconciler
	)

	reconcileOnce := func() {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: rtKey})
		Expect(err).NotTo(HaveOccurred())
	}
	status := func() *aiv1alpha1.PRFeedbackStatus {
		rt := &aiv1alpha1.RoundTable{}
		Expect(k8sClient.Get(ctx, rtKey, rt)).To(Succeed())
		return rt.Status.PRFeedback
	}
	prRecord := func(n int64) *aiv1alpha1.PRFeedbackPullRequest {
		for _, p := range status().PullRequests {
			if p.Number == n {
				return &p
			}
		}
		return nil
	}
	feedbackChain := func() *aiv1alpha1.Chain {
		c := &aiv1alpha1.Chain{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: feedbackName, Namespace: ns}, c)).To(Succeed())
		return c
	}

	BeforeEach(func() {
		gh = newFakePRGitHub()
		srv = httptest.NewServer(gh)
		now = time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
		r = &PRFeedbackReconciler{Client: k8sClient, Recorder: record.NewFakeRecorder(100), GitHub: github.NewClientFactory(srv.URL), Now: func() time.Time { return now }}

		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "fb-gh", Namespace: ns},
			StringData: map[string]string{"token": "gh-test-token\n"},
		})).To(Succeed())
		Expect(k8sClient.Create(ctx, &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: ns},
			Spec: aiv1alpha1.RoundTableSpec{
				NATS:   aiv1alpha1.RoundTableNATS{URL: "nats://localhost:4222", SubjectPrefix: "fb", TasksStream: "fb_tasks", ResultsStream: "fb_results"},
				GitHub: &aiv1alpha1.GitHubConfig{TokenSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "fb-gh"}, Key: "token"}},
				PRFeedback: &aiv1alpha1.PRFeedbackSpec{
					Enabled:           true,
					Repos:             []string{"o/r"},
					ChainRef:          &corev1.LocalObjectReference{Name: feedbackName},
					MaxAttempts:       1,
					FallbackKnightRef: &corev1.LocalObjectReference{Name: "rt-ui"},
				},
			},
		})).To(Succeed())
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: feedbackName, Namespace: ns},
			Spec: aiv1alpha1.ChainSpec{RoundTableRef: aiv1alpha1.LocalRef(rtName), Steps: []aiv1alpha1.ChainStep{
				{Name: "fix", KnightRef: corev1.LocalObjectReference{Name: "rt-operator"}, Task: "fix {{ .Input }}", ResumeRunID: "json(input).runId"},
			}},
		})).To(Succeed())
		pipeline := &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: pipelineName, Namespace: ns},
			Spec: aiv1alpha1.ChainSpec{RoundTableRef: aiv1alpha1.LocalRef(rtName), Input: `{"repo": "o/r", "issue": 42}`, Steps: []aiv1alpha1.ChainStep{
				{Name: "plan", KnightRef: corev1.LocalObjectReference{Name: "rt-lead"}, Task: "plan"},
				{Name: "implement", KnightRef: corev1.LocalObjectReference{Name: "rt-operator"}, Task: "implement", DependsOn: []string{"plan"}},
			}},
		}
		Expect(k8sClient.Create(ctx, pipeline)).To(Succeed())
		pipeline.Status = aiv1alpha1.ChainStatus{Phase: aiv1alpha1.ChainPhaseSucceeded, RunID: "run-1", StepStatuses: []aiv1alpha1.ChainStepStatus{
			{Name: "plan", Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: `{"coder": "rt-operator"}`},
			{Name: "implement", Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: "https://github.com/o/r/pull/5\nImplemented."},
		}}
		Expect(k8sClient.Status().Update(ctx, pipeline)).To(Succeed())
	})

	AfterEach(func() {
		srv.Close()
		_ = k8sClient.Delete(ctx, &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: feedbackName, Namespace: ns}})
		_ = k8sClient.Delete(ctx, &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: pipelineName, Namespace: ns}})
		_ = k8sClient.Delete(ctx, &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: ns}})
		_ = k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "fb-gh", Namespace: ns}})
	})

	It("feeds a red CI run back to the author's session, then escalates when rounds run out", func() {
		gh.pulls = []map[string]any{fakePull(5, "h1")}
		gh.checks["h1"] = []map[string]any{fakeCheck(101, "lint", "failure", "2026-11-23T11:00:00Z")}
		gh.logs["101"] = "2026-11-23T11:00:01.0000000Z go vet\n2026-11-23T11:00:02.0000000Z ##[error]undefined: agentRuntimeForImage\n"
		reconcileOnce()

		st := status()
		Expect(st.LastError).To(BeEmpty())
		rec := prRecord(5)
		Expect(rec).NotTo(BeNil())
		Expect(rec.Phase).To(Equal(aiv1alpha1.PRFeedbackFixing))
		Expect(rec.Knight).To(Equal("rt-operator"))
		Expect(rec.RunID).To(Equal("run-1"))
		Expect(rec.Issue).To(BeEquivalentTo(42))
		Expect(rec.Attempts).To(BeEquivalentTo(1))
		Expect(rec.FailureKind).To(Equal(failureKindCI))
		Expect(rec.FailedChecks).To(Equal([]string{"lint"}))

		c := feedbackChain()
		trigger := c.Annotations[aiv1alpha1.AnnotationChainRun]
		Expect(trigger).To(HavePrefix("prfeedback-5-"))
		Expect(st.ActiveRun).To(Equal("o/r#5@" + trigger))
		in := map[string]any{}
		Expect(json.Unmarshal([]byte(c.Spec.Input), &in)).To(Succeed())
		Expect(in).To(HaveKeyWithValue("knight", "rt-operator"))
		Expect(in).To(HaveKeyWithValue("runId", "run-1"))
		Expect(in).To(HaveKeyWithValue("kind", "ci"))
		Expect(in).To(HaveKeyWithValue("headSha", "h1"))
		Expect(in["log"]).To(ContainSubstring("##[error]undefined: agentRuntimeForImage"))
		Expect(in["log"]).NotTo(ContainSubstring("2026-11-23T11"))

		// Not due yet: nothing changes.
		now = now.Add(time.Minute)
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: rtKey})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(BeNumerically("~", 4*time.Minute, time.Second))

		// The feedback run finishes; the author pushed h2, which fails again.
		c.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
		c.Status.LastRunTrigger = trigger
		c.Status.RunID = "fb-run"
		Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
		gh.pulls = []map[string]any{fakePull(5, "h2")}
		gh.checks["h2"] = []map[string]any{fakeCheck(102, "test", "failure", "2026-11-23T12:01:00Z")}
		now = now.Add(5 * time.Minute)
		reconcileOnce()

		rec = prRecord(5)
		Expect(rec.Phase).To(Equal(aiv1alpha1.PRFeedbackEscalated))
		Expect(status().ActiveRun).To(BeEmpty())
		Expect(gh.labels["5"]).To(ConsistOf("needs-human"))
		Expect(gh.comments["5"]).To(ConsistOf(ContainSubstring("after 1 fix round(s)")))
		Expect(feedbackChain().Annotations[aiv1alpha1.AnnotationChainRun]).To(Equal(trigger), "no second run")

		// Once labelled, the PR is left to the human.
		gh.pulls[0]["labels"] = []any{map[string]any{"name": "needs-human"}}
		now = now.Add(5 * time.Minute)
		reconcileOnce()
		Expect(gh.comments["5"]).To(HaveLen(1))

		// Merged after a new push: the record is dropped, no flake reported.
		merged := fakePull(5, "h3")
		merged["state"], merged["merged"] = "closed", true
		gh.closed = []map[string]any{merged}
		gh.pulls = nil
		now = now.Add(5 * time.Minute)
		reconcileOnce()
		Expect(prRecord(5)).To(BeNil())
		Expect(gh.issues).To(BeEmpty())
	})

	It("opens a flake issue instead of a fix round for a check that passed and failed on one commit", func() {
		gh.pulls = []map[string]any{fakePull(6, "h6")}
		gh.checks["h6"] = []map[string]any{
			fakeCheck(201, "test", "success", "2026-11-23T11:00:00Z"),
			fakeCheck(202, "test", "failure", "2026-11-23T11:10:00Z"),
		}
		reconcileOnce()

		rec := prRecord(6)
		Expect(rec).NotTo(BeNil())
		Expect(rec.Knight).To(Equal("rt-ui"), "untraced PRs go to the fallback knight")
		Expect(rec.Phase).To(Equal(aiv1alpha1.PRFeedbackWatching))
		Expect(rec.Attempts).To(BeZero())
		Expect(gh.issues).To(HaveLen(1))
		Expect(gh.issues[0]["title"]).To(Equal("Flaky check: test"))
		Expect(gh.issues[0]["labels"]).To(ConsistOf(HaveKeyWithValue("name", "flaky-test")))
		Expect(gh.comments["6"]).To(ConsistOf(ContainSubstring("https://github.com/o/r/issues/100")))
		Expect(feedbackChain().Annotations).NotTo(HaveKey(aiv1alpha1.AnnotationChainRun))

		// Reported once.
		now = now.Add(5 * time.Minute)
		reconcileOnce()
		Expect(gh.issues).To(HaveLen(1))
		Expect(gh.comments["6"]).To(HaveLen(1))
	})

	It("ignores merge-guard, a policy check that passes then fails on one commit: no flake issue, no fix round", func() {
		gh.pulls = []map[string]any{fakePull(7, "h7")}
		gh.checks["h7"] = []map[string]any{
			fakeCheck(301, "merge-guard", "success", "2026-11-23T11:00:00Z"),
			fakeCheck(302, "merge-guard", "failure", "2026-11-23T11:10:00Z"),
		}
		reconcileOnce()
		now = now.Add(5 * time.Minute)
		reconcileOnce()

		Expect(prRecord(7)).To(BeNil())
		Expect(gh.issues).To(BeEmpty())
		Expect(gh.comments).NotTo(HaveKey("7"))
		Expect(feedbackChain().Annotations).NotTo(HaveKey(aiv1alpha1.AnnotationChainRun))
	})

	It("feeds a merge-queue rejection and reports a flake when the same commit later passes the queue", func() {
		gh.pulls = []map[string]any{fakePull(5, "h1")}
		gh.checks["h1"] = []map[string]any{fakeCheck(101, "lint", "success", "2026-11-23T11:00:00Z")}
		gh.mgRuns = []map[string]any{{
			"id": 77, "name": "CI", "head_branch": "gh-readonly-queue/main/pr-5-0123abcd", "head_sha": "m1",
			"status": "completed", "conclusion": "failure", "created_at": "2026-11-23T11:30:00Z", "html_url": "https://github.com/o/r/actions/runs/77",
		}}
		gh.logs["900"] = "##[error]vet: undefined: agentRuntimeForImage\n"
		reconcileOnce()

		rec := prRecord(5)
		Expect(rec.Phase).To(Equal(aiv1alpha1.PRFeedbackFixing))
		Expect(rec.FailureKind).To(Equal(failureKindMergeQueue))
		Expect(rec.LastFailure).To(Equal("mq:m1"))
		in := map[string]any{}
		Expect(json.Unmarshal([]byte(feedbackChain().Spec.Input), &in)).To(Succeed())
		Expect(in["log"]).To(ContainSubstring("=== job build ==="))
		Expect(in["log"]).To(ContainSubstring("undefined: agentRuntimeForImage"))

		// The author pushed nothing (flaky); the lead re-queued and it passed.
		c := feedbackChain()
		c.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
		c.Status.LastRunTrigger = c.Annotations[aiv1alpha1.AnnotationChainRun]
		Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
		gh.mgRuns = append([]map[string]any{{
			"id": 78, "name": "CI", "head_branch": "gh-readonly-queue/main/pr-5-4567abcd", "head_sha": "m2",
			"status": "completed", "conclusion": "success", "created_at": "2026-11-23T12:30:00Z",
		}}, gh.mgRuns...)
		now = now.Add(5 * time.Minute)
		reconcileOnce()

		rec = prRecord(5)
		Expect(rec.Phase).To(Equal(aiv1alpha1.PRFeedbackWatching))
		Expect(rec.Message).To(ContainSubstring("flaky"))
		Expect(gh.issues).To(HaveLen(1))
		Expect(gh.issues[0]["title"]).To(Equal("Flaky check: CI"))
	})

	It("sends a PR with merge conflicts back to its author for a rebase, then escalates when rounds run out", func() {
		pr := fakePull(5, "h1")
		pr["mergeable_state"] = "dirty"
		gh.pulls = []map[string]any{pr}
		gh.checks["h1"] = []map[string]any{fakeCheck(101, "lint", "success", "2026-11-23T11:00:00Z")}
		reconcileOnce()

		Expect(status().LastError).To(BeEmpty())
		rec := prRecord(5)
		Expect(rec).NotTo(BeNil())
		Expect(rec.Phase).To(Equal(aiv1alpha1.PRFeedbackFixing))
		Expect(rec.Knight).To(Equal("rt-operator"))
		Expect(rec.RunID).To(Equal("run-1"))
		Expect(rec.Attempts).To(BeEquivalentTo(1))
		Expect(rec.FailureKind).To(Equal(failureKindConflict))
		Expect(rec.LastFailure).To(Equal("conflict:h1"))
		Expect(rec.FailedChecks).To(BeEmpty())

		c := feedbackChain()
		trigger := c.Annotations[aiv1alpha1.AnnotationChainRun]
		Expect(trigger).To(HavePrefix("prfeedback-5-"))
		in := map[string]any{}
		Expect(json.Unmarshal([]byte(c.Spec.Input), &in)).To(Succeed())
		Expect(in).To(HaveKeyWithValue("kind", "conflict"))
		Expect(in).To(HaveKeyWithValue("knight", "rt-operator"))
		Expect(in).To(HaveKeyWithValue("runId", "run-1"))
		Expect(in).To(HaveKeyWithValue("headSha", "h1"))
		Expect(in).To(HaveKeyWithValue("url", "https://github.com/o/r/pull/5"))
		Expect(in).NotTo(HaveKey("log"), "a conflict has no CI log")

		// The run finishes without a push: the same conflict is not fed again.
		c.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
		c.Status.LastRunTrigger = trigger
		Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
		now = now.Add(5 * time.Minute)
		reconcileOnce()
		rec = prRecord(5)
		Expect(rec.Phase).To(Equal(aiv1alpha1.PRFeedbackWatching))
		Expect(feedbackChain().Annotations[aiv1alpha1.AnnotationChainRun]).To(Equal(trigger))
		Expect(gh.issues).To(BeEmpty(), "a conflict is never reported as a flake")

		// Rebased to h2 but still conflicting: the rounds (1) are used up.
		pr = fakePull(5, "h2")
		pr["mergeable_state"] = "dirty"
		gh.pulls = []map[string]any{pr}
		now = now.Add(5 * time.Minute)
		reconcileOnce()
		rec = prRecord(5)
		Expect(rec.Phase).To(Equal(aiv1alpha1.PRFeedbackEscalated))
		Expect(gh.labels["5"]).To(ConsistOf("needs-human"))
		Expect(gh.comments["5"]).To(ConsistOf(ContainSubstring("Still conflicting with the base branch after 1 fix round(s)")))
		Expect(feedbackChain().Annotations[aiv1alpha1.AnnotationChainRun]).To(Equal(trigger), "no second run")
	})

	// busyWithPR5 starts a conflict fix round for PR 5 so that PR 6 stays
	// queued behind it, and returns the trigger of the running round.
	busyWithPR5 := func() string {
		pr5 := fakePull(5, "h5")
		pr5["mergeable_state"] = "dirty"
		gh.pulls = []map[string]any{pr5}
		reconcileOnce()
		Expect(prRecord(5).Phase).To(Equal(aiv1alpha1.PRFeedbackFixing))
		return feedbackChain().Annotations[aiv1alpha1.AnnotationChainRun]
	}
	pollPR6 := func(state string) {
		pr5 := fakePull(5, "h5")
		pr5["mergeable_state"] = "dirty"
		pr6 := fakePull(6, "h6")
		pr6["mergeable_state"] = state
		gh.pulls = []map[string]any{pr5, pr6}
		now = now.Add(5 * time.Minute)
		reconcileOnce()
	}

	It("keeps a queued conflict when the merge state is unknown for one poll", func() {
		trigger := busyWithPR5()
		pollPR6("dirty")
		Expect(prRecord(6).Phase).To(Equal(aiv1alpha1.PRFeedbackQueued))

		pollPR6("unknown")
		Expect(prRecord(6).Phase).To(Equal(aiv1alpha1.PRFeedbackQueued))
		Expect(prRecord(6).LastFailure).To(Equal("conflict:h6"))

		pollPR6("dirty")
		Expect(prRecord(6).Phase).To(Equal(aiv1alpha1.PRFeedbackQueued))

		// The busy round ends: the queued conflict gets its fix round.
		c := feedbackChain()
		c.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
		c.Status.LastRunTrigger = trigger
		Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
		pollPR6("dirty")
		Expect(prRecord(6).Phase).To(Equal(aiv1alpha1.PRFeedbackFixing))
		Expect(feedbackChain().Annotations[aiv1alpha1.AnnotationChainRun]).To(HavePrefix("prfeedback-6-"))
	})

	It("detects a conflict again on the same head after a queued one cleared", func() {
		busyWithPR5()
		pollPR6("dirty")
		Expect(prRecord(6).Phase).To(Equal(aiv1alpha1.PRFeedbackQueued))

		pollPR6("clean")
		rec := prRecord(6)
		Expect(rec.Phase).To(Equal(aiv1alpha1.PRFeedbackWatching))
		Expect(rec.LastFailure).To(BeEmpty())

		pollPR6("dirty")
		Expect(prRecord(6).Phase).To(Equal(aiv1alpha1.PRFeedbackQueued))
		Expect(prRecord(6).LastFailure).To(Equal("conflict:h6"))
	})

	It("leaves a clean PR alone", func() {
		pr := fakePull(5, "h1")
		pr["mergeable_state"] = "clean"
		gh.pulls = []map[string]any{pr}
		gh.checks["h1"] = []map[string]any{fakeCheck(101, "lint", "success", "2026-11-23T11:00:00Z")}
		reconcileOnce()
		Expect(status().LastError).To(BeEmpty())
		Expect(status().PullRequests).To(BeEmpty())
		Expect(feedbackChain().Annotations).NotTo(HaveKey(aiv1alpha1.AnnotationChainRun))
	})

	It("waits while the pipeline that opened the PR is still running", func() {
		p := &aiv1alpha1.Chain{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: pipelineName, Namespace: ns}, p)).To(Succeed())
		p.Status.Phase = aiv1alpha1.ChainPhaseRunning
		Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
		gh.pulls = []map[string]any{fakePull(5, "h1")}
		gh.checks["h1"] = []map[string]any{fakeCheck(101, "lint", "failure", "2026-11-23T11:00:00Z")}
		reconcileOnce()
		Expect(status().PullRequests).To(BeEmpty())
		Expect(feedbackChain().Annotations).NotTo(HaveKey(aiv1alpha1.AnnotationChainRun))
	})

	It("records a missing token as the poll error", func() {
		Expect(k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "fb-gh", Namespace: ns}})).To(Succeed())
		reconcileOnce()
		Expect(status().LastError).To(ContainSubstring("fb-gh"))
		Expect(status().LastPollTime).NotTo(BeNil())
	})
})
