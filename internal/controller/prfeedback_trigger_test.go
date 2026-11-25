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
	"net/http/httptest"
	"time"

	"github.com/go-logr/logr"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/chainrun"
	"github.com/roundtable-k8s/roundtable/internal/github"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// PR feedback on the trigger engine (#528): the PR feedback controller
// generates a github-source Trigger per author; the Trigger controller polls
// pull request state and starts the feedback runs.
var _ = Describe("PR feedback trigger engine", func() {
	const (
		ns           = "default"
		rtName       = "fbt-table"
		feedbackName = "fbt-feedback"
		pipelineName = "fbt-pipeline"
		trgName      = rtName + aiv1alpha1.PRFeedbackTriggerInfix + "dunce-bot-bot"
	)
	ctx := context.Background()
	rtKey := types.NamespacedName{Name: rtName, Namespace: ns}
	trgKey := types.NamespacedName{Name: trgName, Namespace: ns}
	runName := func(n string) string { return chainrun.Name(feedbackName, trgName, dedupeHash("o/r#"+n)) }

	var (
		natsSrv  *natsserver.Server
		cli      natspkg.Client
		gh       *fakePRGitHub
		ghSrv    *httptest.Server
		feedback *PRFeedbackReconciler
		triggers *TriggerReconciler
		clock    time.Time
	)

	reconcileFeedback := func() {
		_, err := feedback.Reconcile(ctx, reconcile.Request{NamespacedName: rtKey})
		Expect(err).NotTo(HaveOccurred())
	}
	reconcileTrigger := func() {
		_, err := triggers.Reconcile(ctx, reconcile.Request{NamespacedName: trgKey})
		Expect(err).NotTo(HaveOccurred())
	}
	poll := func() { triggers.pollGitHub(ctx, trgKey) }
	getRT := func() *aiv1alpha1.RoundTable {
		rt := &aiv1alpha1.RoundTable{}
		Expect(k8sClient.Get(ctx, rtKey, rt)).To(Succeed())
		return rt
	}
	setEngine := func(e aiv1alpha1.PRFeedbackEngine) {
		rt := getRT()
		rt.Spec.PRFeedback.Engine = e
		Expect(k8sClient.Update(ctx, rt)).To(Succeed())
	}
	getTrigger := func() (*aiv1alpha1.Trigger, error) {
		t := &aiv1alpha1.Trigger{}
		return t, k8sClient.Get(ctx, trgKey, t)
	}
	generated := func() []string {
		l := &aiv1alpha1.TriggerList{}
		Expect(k8sClient.List(ctx, l, client.InNamespace(ns), client.MatchingLabels{aiv1alpha1.LabelPRFeedbackRoundTable: rtName})).To(Succeed())
		var names []string
		for _, t := range l.Items {
			if t.DeletionTimestamp.IsZero() {
				names = append(names, t.Name)
			}
		}
		return names
	}
	getChain := func(name string) *aiv1alpha1.Chain {
		c := &aiv1alpha1.Chain{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, c); err != nil {
			return nil
		}
		return c
	}
	runs := func() []string {
		l := &aiv1alpha1.ChainList{}
		Expect(k8sClient.List(ctx, l, client.InNamespace(ns), client.MatchingLabels{aiv1alpha1.LabelTrigger: trgName})).To(Succeed())
		names := make([]string, 0, len(l.Items))
		for _, c := range l.Items {
			names = append(names, c.Name)
		}
		return names
	}
	runID := func(name string) string {
		c := getChain(name)
		if c == nil {
			return ""
		}
		return c.Annotations[aiv1alpha1.AnnotationChainRun]
	}
	input := func(name string) map[string]any {
		in := map[string]any{}
		Expect(json.Unmarshal([]byte(getChain(name).Spec.Input), &in)).To(Succeed())
		return in
	}
	finish := func(name string) {
		c := getChain(name)
		Expect(c).NotTo(BeNil())
		c.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
		c.Status.LastRunTrigger = c.Annotations[aiv1alpha1.AnnotationChainRun]
		c.Status.CompletedAt = &metav1.Time{Time: clock}
		Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
		clock = clock.Add(time.Minute) // a run started again gets a new run ID
	}
	githubReady := func() string {
		t, err := getTrigger()
		Expect(err).NotTo(HaveOccurred())
		c := meta.FindStatusCondition(t.Status.Conditions, aiv1alpha1.ConditionTriggerGitHubReady)
		Expect(c).NotTo(BeNil())
		return c.Message
	}
	comments := func(n string) func() []string {
		return func() []string {
			gh.mu.Lock()
			defer gh.mu.Unlock()
			return append([]string(nil), gh.comments[n]...)
		}
	}
	setPulls := func(pulls ...map[string]any) {
		gh.mu.Lock()
		defer gh.mu.Unlock()
		gh.pulls = pulls
	}
	failing := func(sha string, id int, names ...string) {
		gh.mu.Lock()
		defer gh.mu.Unlock()
		for i, n := range names {
			gh.checks[sha] = append(gh.checks[sha], fakeCheck(id+i, n, "failure", "2026-11-23T11:00:00Z"))
		}
	}
	const wait, every = 10 * time.Second, 100 * time.Millisecond

	BeforeEach(func() {
		opts := natstest.DefaultTestOptions
		opts.Port = -1
		opts.JetStream = true
		opts.StoreDir = GinkgoT().TempDir()
		natsSrv = natstest.RunServer(&opts)
		cli = natspkg.NewClient(natspkg.Config{URL: natsSrv.ClientURL()}, logr.Discard())
		Expect(cli.Connect()).To(Succeed())
		provider := natspkg.NewProviderWithClient(cli, logr.Discard())

		gh = newFakePRGitHub()
		ghSrv = httptest.NewServer(gh)
		clock = time.Now()
		factory := github.NewClientFactory(ghSrv.URL)
		feedback = &PRFeedbackReconciler{
			Client: k8sClient, Recorder: record.NewFakeRecorder(200), GitHub: factory, Now: func() time.Time { return clock },
		}
		triggers = &TriggerReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(200),
			NATS: provider, GitHub: factory, Now: func() time.Time { return clock },
			manualGitHubPolls: true,
		}

		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "fbt-gh", Namespace: ns},
			StringData: map[string]string{"token": "gh-test-token"},
		})).To(Succeed())
		rt := &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: ns},
			Spec: aiv1alpha1.RoundTableSpec{
				NATS: aiv1alpha1.RoundTableNATS{
					SubjectPrefix: "fbt", TasksStream: "fbt_tasks", ResultsStream: "fbt_results", CreateStreams: true,
				},
				GitHub: &aiv1alpha1.GitHubConfig{TokenSecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "fbt-gh"}, Key: "token"}},
				PRFeedback: &aiv1alpha1.PRFeedbackSpec{
					Engine:      aiv1alpha1.PRFeedbackEngineTrigger,
					Enabled:     true,
					Repos:       []string{"o/r"},
					ChainRef:    &corev1.LocalObjectReference{Name: feedbackName},
					MaxAttempts: 2,
				},
			},
		}
		Expect(k8sClient.Create(ctx, rt)).To(Succeed())
		Expect((&RoundTableReconciler{NATS: provider}).ensureStreams(ctx, rt)).To(Succeed())
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: feedbackName, Namespace: ns},
			Spec: aiv1alpha1.ChainSpec{RoundTableRef: aiv1alpha1.LocalRef(rtName), Steps: []aiv1alpha1.ChainStep{
				{Name: "fix", KnightRef: corev1.LocalObjectReference{Name: "rt-operator"}, Task: "fix {{ .Input }}", ResumeRunID: "json(input).runId"},
			}},
		})).To(Succeed())
		pipeline := &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: pipelineName, Namespace: ns},
			Spec: aiv1alpha1.ChainSpec{RoundTableRef: aiv1alpha1.LocalRef(rtName), Input: `{"repo": "o/r", "issue": 42}`, Steps: []aiv1alpha1.ChainStep{
				{Name: "implement", KnightRef: corev1.LocalObjectReference{Name: "rt-operator"}, Task: "implement"},
			}},
		}
		Expect(k8sClient.Create(ctx, pipeline)).To(Succeed())
		pipeline.Status = aiv1alpha1.ChainStatus{Phase: aiv1alpha1.ChainPhaseSucceeded, RunID: "run-1", StepStatuses: []aiv1alpha1.ChainStepStatus{
			{Name: "implement", Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: "https://github.com/o/r/pull/5\nImplemented."},
		}}
		Expect(k8sClient.Status().Update(ctx, pipeline)).To(Succeed())
	})

	AfterEach(func() {
		triggers.init()
		triggers.pollers.stopAll()
		triggers.consumers.stopAll()
		l := &aiv1alpha1.TriggerList{}
		Expect(k8sClient.List(ctx, l, client.InNamespace(ns), client.MatchingLabels{aiv1alpha1.LabelPRFeedbackRoundTable: rtName})).To(Succeed())
		for i := range l.Items {
			t := &l.Items[i]
			t.Finalizers = nil
			Expect(client.IgnoreNotFound(k8sClient.Update(ctx, t))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, t))).To(Succeed())
		}
		Expect(k8sClient.DeleteAllOf(ctx, &aiv1alpha1.Chain{}, client.InNamespace(ns),
			client.MatchingLabels{aiv1alpha1.LabelTrigger: trgName})).To(Succeed())
		for _, name := range []string{feedbackName, pipelineName} {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}))).To(Succeed())
		}
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: ns}}))).To(Succeed())
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "fbt-gh", Namespace: ns}}))).To(Succeed())
		ghSrv.Close()
		_ = cli.Close()
		natsSrv.Shutdown()
	})

	It("creates no Trigger with engine unset and runs the builtin poller", func() {
		setEngine("")
		Expect(getRT().Spec.PRFeedback.Engine).To(Equal(aiv1alpha1.PRFeedbackEngineBuiltin), "CRD default")
		setPulls(fakePull(5, "h1"))
		failing("h1", 101, "lint")

		reconcileFeedback()
		Expect(generated()).To(BeEmpty())
		st := getRT().Status.PRFeedback
		Expect(st.Engine).To(Equal(aiv1alpha1.PRFeedbackEngineBuiltin))
		Expect(st.Triggers).To(BeEmpty())
		Expect(st.Conditions).To(BeEmpty())
		Expect(st.ActiveRun).NotTo(BeEmpty(), "the builtin poller started a fix round")
	})

	It("generates an owned Trigger per author from the prFeedback fields and stops the builtin poller", func() {
		setPulls(fakePull(5, "h1"))
		failing("h1", 101, "lint")
		reconcileFeedback()

		trg, err := getTrigger()
		Expect(err).NotTo(HaveOccurred())
		Expect(metav1.GetControllerOf(trg).Name).To(Equal(rtName))
		Expect(prFeedbackTable(trg)).To(Equal(rtName))
		src := trg.Spec.Source.GitHub
		Expect(src.Repositories).To(Equal([]string{"o/r"}))
		Expect(src.Kind).To(Equal(aiv1alpha1.TriggerGitHubKindPullRequest))
		Expect(src.Author).To(Equal("dunce-bot[bot]"))
		Expect(src.Checks).To(Equal(aiv1alpha1.TriggerGitHubChecksFailing))
		Expect(src.IgnoreChecks).To(Equal([]string{"merge-guard"}))
		Expect(src.Mergeable).To(Equal(aiv1alpha1.TriggerGitHubMergeableConflicting))
		Expect(src.MergeQueue).To(Equal(aiv1alpha1.TriggerGitHubMergeQueueRejected))
		Expect(src.MaxRunsPerItem).To(Equal(int32(2)))
		Expect(src.OnMaxRuns.AddLabels).To(Equal([]string{"needs-human"}))
		Expect(src.PollInterval.Duration).To(Equal(300 * time.Second))
		Expect(trg.Spec.Target.ChainRef.Name).To(Equal(feedbackName))
		Expect(trg.Spec.Concurrency.MaxActive).To(Equal(int32(1)))
		Expect(validateGitHubSource(src)).To(Succeed())

		st := getRT().Status.PRFeedback
		Expect(st.Engine).To(Equal(aiv1alpha1.PRFeedbackEngineTrigger))
		Expect(st.Triggers).To(Equal([]string{trgName}))
		Expect(st.ActiveRun).To(BeEmpty(), "the builtin poller starts nothing")
		Expect(st.PullRequests).To(BeEmpty())
		cond := meta.FindStatusCondition(st.Conditions, aiv1alpha1.ConditionPRFeedbackFlakeReportingSkipped)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonPRFeedbackTriggerEngine))
		Expect(getChain(feedbackName).Annotations).NotTo(HaveKey(aiv1alpha1.AnnotationChainRun))

		By("following spec changes: one Trigger per author")
		rt := getRT()
		rt.Spec.PRFeedback.Authors = []string{"dunce-bot[bot]", "Octo-Cat"}
		rt.Spec.PRFeedback.IgnoreChecks = []string{"lint"}
		Expect(k8sClient.Update(ctx, rt)).To(Succeed())
		reconcileFeedback()
		Expect(generated()).To(ConsistOf(trgName, rtName+"-pr-feedback-octo-cat"))
		trg, _ = getTrigger()
		Expect(trg.Spec.Source.GitHub.IgnoreChecks).To(Equal([]string{"lint"}))

		By("dropping the Trigger of an author no longer watched")
		rt = getRT()
		rt.Spec.PRFeedback.Authors = []string{"dunce-bot[bot]"}
		Expect(k8sClient.Update(ctx, rt)).To(Succeed())
		reconcileFeedback()
		Expect(generated()).To(ConsistOf(trgName))
	})

	It("feeds each failure of a bot pull request back once, up to the attempt cap, then escalates", func() {
		other := fakePull(6, "h6")
		other["user"] = map[string]any{"login": "alice"}
		setPulls(fakePull(5, "h1"), other, fakePull(7, "h7"))
		failing("h1", 101, "lint", "merge-guard")
		failing("h6", 601, "lint")
		failing("h7", 701, "merge-guard")
		gh.logs["101"] = "2026-11-23T11:00:01.0000000Z go vet\n2026-11-23T11:00:02.0000000Z ##[error]undefined: agentRuntimeForImage\n"
		reconcileFeedback()
		reconcileTrigger()
		poll()

		Eventually(runs, wait, every).Should(ConsistOf(runName("5")))
		in := input(runName("5"))
		Expect(in).To(HaveKeyWithValue("repo", "o/r"))
		Expect(in).To(HaveKeyWithValue("pr", BeEquivalentTo(5)))
		Expect(in).To(HaveKeyWithValue("url", "https://github.com/o/r/pull/5"))
		Expect(in).To(HaveKeyWithValue("knight", "rt-operator"))
		Expect(in).To(HaveKeyWithValue("runId", "run-1"))
		Expect(in).To(HaveKeyWithValue("issue", BeEquivalentTo(42)))
		Expect(in).To(HaveKeyWithValue("attempt", BeEquivalentTo(1)))
		Expect(in).To(HaveKeyWithValue("maxAttempts", BeEquivalentTo(2)))
		Expect(in).To(HaveKeyWithValue("kind", "ci"))
		Expect(in).To(HaveKeyWithValue("headSha", "h1"))
		Expect(in).To(HaveKeyWithValue("failedChecks", ConsistOf("lint")), "merge-guard is ignored")
		Expect(in).To(HaveKeyWithValue("failureUrl", "https://github.com/o/r/actions/runs/1/job/101"))
		Expect(in["log"]).To(ContainSubstring("UNTRUSTED CI OUTPUT"))
		Expect(in["log"]).To(ContainSubstring("##[error]undefined: agentRuntimeForImage"))
		first := runID(runName("5"))

		By("not firing the same failure twice, nor for other authors or ignored checks")
		poll()
		finish(runName("5"))
		poll()
		Consistently(runs, time.Second, 200*time.Millisecond).Should(ConsistOf(runName("5")))
		Expect(runID(runName("5"))).To(Equal(first))
		Expect(githubReady()).To(ContainSubstring("1 matching pull request(s)"))

		By("firing again for a new head commit")
		setPulls(fakePull(5, "h2"))
		failing("h2", 102, "test")
		poll()
		Eventually(func() string { return runID(runName("5")) }, wait, every).ShouldNot(Equal(first))
		Expect(input(runName("5"))).To(HaveKeyWithValue("attempt", BeEquivalentTo(2)))
		Expect(input(runName("5"))).To(HaveKeyWithValue("failedChecks", ConsistOf("test")))
		second := runID(runName("5"))

		By("escalating the next failure once the attempts ran out")
		finish(runName("5"))
		setPulls(fakePull(5, "h3"))
		failing("h3", 103, "test")
		poll()
		Eventually(comments("5"), wait, every).Should(ConsistOf(
			"CI is still failing after 2 fix round(s) (ci: test, https://github.com/o/r/actions/runs/1/job/103). Escalating to a human."))
		Expect(gh.labels["5"]).To(ConsistOf("needs-human"))
		Expect(runID(runName("5"))).To(Equal(second))
		Expect(getChain(runName("5")).Annotations).To(HaveKeyWithValue(aiv1alpha1.AnnotationTriggerHandedBack, ContainSubstring("o/r#5@ci:h3")))

		By("leaving a labelled pull request to the human")
		labelled := fakePull(5, "h4")
		labelled["labels"] = []any{map[string]any{"name": "needs-human"}}
		setPulls(labelled)
		failing("h4", 104, "test")
		poll()
		Consistently(func() string { return runID(runName("5")) }, time.Second, 200*time.Millisecond).Should(Equal(second))
		Expect(comments("5")()).To(HaveLen(1))

		By("starting over when the human removes the label")
		setPulls(fakePull(5, "h4"))
		poll()
		Eventually(func() string { return runID(runName("5")) }, wait, every).ShouldNot(Equal(second))
		Expect(input(runName("5"))).To(HaveKeyWithValue("attempt", BeEquivalentTo(1)))
	})

	It("waits for the opening pipeline and needs a known author knight", func() {
		pipeline := getChain(pipelineName)
		pipeline.Status.Phase = aiv1alpha1.ChainPhaseRunning
		Expect(k8sClient.Status().Update(ctx, pipeline)).To(Succeed())
		setPulls(fakePull(5, "h1"), fakePull(8, "h8"))
		failing("h1", 101, "lint")
		failing("h8", 801, "lint")
		reconcileFeedback()
		reconcileTrigger()
		poll()
		Expect(githubReady()).To(ContainSubstring("2 without a known author knight or with its pipeline still running"))
		Consistently(runs, time.Second, 200*time.Millisecond).Should(BeEmpty())

		By("handing an untraced pull request to the fallback knight")
		rt := getRT()
		rt.Spec.PRFeedback.FallbackKnightRef = &corev1.LocalObjectReference{Name: "rt-ui"}
		Expect(k8sClient.Update(ctx, rt)).To(Succeed())
		poll()
		Eventually(runs, wait, every).Should(ConsistOf(runName("8")))
		Expect(input(runName("8"))).To(HaveKeyWithValue("knight", "rt-ui"))
		Expect(input(runName("8"))).NotTo(HaveKey("issue"))
	})

	It("removes the generated Trigger when switching back to builtin", func() {
		reconcileFeedback()
		Expect(generated()).To(ConsistOf(trgName))
		reconcileTrigger()

		setEngine(aiv1alpha1.PRFeedbackEngineBuiltin)
		reconcileFeedback()
		trg, err := getTrigger()
		Expect(err).NotTo(HaveOccurred())
		Expect(trg.DeletionTimestamp).NotTo(BeNil(), "deleted; the Trigger finalizer releases its consumer")
		reconcileTrigger()
		_, err = getTrigger()
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
		st := getRT().Status.PRFeedback
		Expect(st.Engine).To(Equal(aiv1alpha1.PRFeedbackEngineBuiltin))
		Expect(st.Triggers).To(BeEmpty())
		Expect(meta.FindStatusCondition(st.Conditions, aiv1alpha1.ConditionPRFeedbackFlakeReportingSkipped)).To(BeNil())
	})

	It("does not touch a Trigger it does not own", func() {
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Trigger{
			ObjectMeta: metav1.ObjectMeta{Name: trgName, Namespace: ns},
			Spec: aiv1alpha1.TriggerSpec{
				Source: aiv1alpha1.TriggerSource{Schedule: &aiv1alpha1.TriggerScheduleSource{Cron: "0 * * * *"}},
				Target: aiv1alpha1.TriggerTarget{ChainRef: corev1.LocalObjectReference{Name: feedbackName}},
			},
		})).To(Succeed())
		_, err := feedback.Reconcile(ctx, reconcile.Request{NamespacedName: rtKey})
		Expect(err).To(MatchError(ContainSubstring("not owned by this RoundTable")))
		Expect(getRT().Status.PRFeedback.LastError).To(ContainSubstring("not owned by this RoundTable"))

		setEngine(aiv1alpha1.PRFeedbackEngineBuiltin)
		reconcileFeedback()
		_, err = getTrigger()
		Expect(err).NotTo(HaveOccurred(), "a Trigger the table does not control is never deleted")
		Expect(k8sClient.Delete(ctx, &aiv1alpha1.Trigger{ObjectMeta: metav1.ObjectMeta{Name: trgName, Namespace: ns}})).To(Succeed())
	})
})

var _ = Describe("GitHub source state selectors", func() {
	var (
		gh  *fakePRGitHub
		srv *httptest.Server
		cl  *github.Client
	)
	BeforeEach(func() {
		gh = newFakePRGitHub()
		srv = httptest.NewServer(gh)
		cl = github.NewClientFactory(srv.URL).New("gh-test-token")
	})
	AfterEach(func() { srv.Close() })

	src := func() *aiv1alpha1.TriggerGitHubSource {
		return &aiv1alpha1.TriggerGitHubSource{
			Repositories: []string{"o/r"}, Kind: aiv1alpha1.TriggerGitHubKindPullRequest,
			Checks: aiv1alpha1.TriggerGitHubChecksFailing, Mergeable: aiv1alpha1.TriggerGitHubMergeableConflicting,
			MergeQueue: aiv1alpha1.TriggerGitHubMergeQueueRejected, IgnoreChecks: []string{"merge-guard"},
		}
	}
	list := func(s *aiv1alpha1.TriggerGitHubSource) map[int]githubItem {
		cands, _, problems, err := listGitHubCandidates(context.Background(), cl, s, "o/r")
		Expect(err).NotTo(HaveOccurred())
		Expect(problems).To(BeEmpty())
		out := map[int]githubItem{}
		for _, c := range cands {
			out[c.item.Number] = c.item
		}
		return out
	}

	It("selects conflicts first, then failed checks, then merge queue rejections, with a key per failure", func() {
		conflict := fakePull(1, "c1")
		conflict["mergeable_state"] = "dirty"
		gh.pulls = []map[string]any{conflict, fakePull(2, "c2"), fakePull(3, "c3"), fakePull(4, "c4")}
		gh.checks["c1"] = []map[string]any{fakeCheck(11, "lint", "failure", "2026-11-23T11:00:00Z")}
		gh.checks["c2"] = []map[string]any{
			fakeCheck(21, "test", "failure", "2026-11-23T11:00:00Z"),
			fakeCheck(22, "merge-guard", "failure", "2026-11-23T11:00:00Z"),
		}
		gh.checks["c2"][0]["output"] = map[string]any{"title": "Tests", "summary": "1 failed\x1b[31m\u200b"}
		gh.checks["c4"] = []map[string]any{fakeCheck(41, "merge-guard", "failure", "2026-11-23T11:00:00Z")}
		gh.mgRuns = []map[string]any{{
			"id": 31, "name": "CI", "head_branch": "gh-readonly-queue/main/pr-3-abc123", "head_sha": "mg3",
			"event": "merge_group", "status": "completed", "conclusion": "failure",
			"html_url": "https://github.com/o/r/actions/runs/31", "created_at": "2026-11-23T11:00:00Z",
		}}

		items := list(src())
		Expect(items).To(HaveLen(3), "#4 only fails an ignored check")
		Expect(items[1].FailureKind).To(Equal(failureKindConflict))
		Expect(items[1].MergeState).To(Equal("dirty"))
		Expect(items[1].eventKey()).To(Equal("o/r#1@conflict:c1"))
		Expect(items[2].FailureKind).To(Equal(failureKindCI))
		Expect(items[2].FailedChecks).To(Equal([]githubFailedCheck{{Name: "test", URL: "https://github.com/o/r/actions/runs/1/job/21", Summary: "1 failed"}}))
		Expect(items[2].eventKey()).To(Equal("o/r#2@ci:c2"))
		Expect(items[3].FailureKind).To(Equal(failureKindMergeQueue))
		Expect(items[3].eventKey()).To(Equal("o/r#3@merge-queue:mg3"))
		Expect(items[3].Author).To(Equal("dunce-bot[bot]"))

		By("keeping the key stable when the head changes but the failed merge group stays newest")
		gh.pulls[2] = fakePull(3, "c3-new")
		again := list(src())
		Expect(again).To(HaveLen(3))
		Expect(again[3].HeadSHA).To(Equal("c3-new"))
		Expect(again[3].eventKey()).To(Equal(items[3].eventKey()))

		By("round-tripping the payload through an event")
		ev, err := githubEvent(items[2], "s", time.Now())
		Expect(err).NotTo(HaveOccurred())
		Expect(ev.Headers).To(HaveKeyWithValue(natspkg.EventKeyHeader, "o/r#2@ci:c2"))
		back, err := githubEventItem(ev)
		Expect(err).NotTo(HaveOccurred())
		Expect(back.eventKey()).To(Equal("o/r#2@ci:c2"))
		Expect(ev.Data).To(HaveKeyWithValue("headSha", "c2"))
		Expect(ev.Data).To(HaveKeyWithValue("mergeState", ""))
		Expect(ev.Data).To(HaveKeyWithValue("failedChecks", HaveLen(1)))

		By("selecting only what is asked for, by author and labels")
		s := src()
		s.Mergeable, s.MergeQueue = "", ""
		Expect(list(s)).To(HaveKey(1), "#1's checks fail too")
		Expect(list(s)[1].FailureKind).To(Equal(failureKindCI))
		Expect(list(s)).NotTo(HaveKey(3))
		s.Author = "alice"
		Expect(list(s)).To(BeEmpty())
		s = src()
		s.Query.Labels = []string{"watch"}
		Expect(list(s)).To(BeEmpty())
		s.Query.Labels = nil
		s.OnMaxRuns = &aiv1alpha1.TriggerGitHubHandBack{AddLabels: []string{"needs-human"}}
		gh.pulls[1]["labels"] = []any{map[string]any{"name": "needs-human"}}
		Expect(list(s)).NotTo(HaveKey(2), "an item carrying an onMaxRuns label is not selected")
	})

	It("decides per item run whether a failure starts, repeats, waits or is handed back", func() {
		trg := &aiv1alpha1.Trigger{Spec: aiv1alpha1.TriggerSpec{Source: aiv1alpha1.TriggerSource{GitHub: src()}}}
		trg.Spec.Source.GitHub.MaxRunsPerItem = 2
		state, _ := githubItemRun(trg, nil, "k1")
		Expect(state).To(Equal(githubItemRunNew))

		run := &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			aiv1alpha1.AnnotationChainRun: "1", aiv1alpha1.AnnotationTriggerKey: "k1", aiv1alpha1.AnnotationTriggerItemRuns: "1",
		}}}
		state, _ = githubItemRun(trg, run, "k1")
		Expect(state).To(Equal(githubItemRunBusy))
		run.Status.LastRunTrigger = "1"
		state, _ = githubItemRun(trg, run, "k1")
		Expect(state).To(Equal(githubItemRunDuplicate))
		state, n := githubItemRun(trg, run, "k2")
		Expect(state).To(Equal(githubItemRunAgain))
		Expect(n).To(Equal(int32(1)))
		run.Annotations[aiv1alpha1.AnnotationTriggerItemRuns] = "2"
		state, n = githubItemRun(trg, run, "k2")
		Expect(state).To(Equal(githubItemRunExhausted))
		Expect(n).To(Equal(int32(2)))
		run.Annotations[aiv1alpha1.AnnotationTriggerHandedBack] = "k2"
		state, n = githubItemRun(trg, run, "k2")
		Expect(state).To(Equal(githubItemRunAgain))
		Expect(n).To(BeZero())
	})

	It("names generated Triggers per author without collisions", func() {
		rt := &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: "t"}}
		names := prFeedbackTriggerNames(rt, &aiv1alpha1.PRFeedbackSpec{})
		Expect(names).To(Equal(map[string]string{"t-pr-feedback-dunce-bot-bot": "dunce-bot[bot]"}))
		names = prFeedbackTriggerNames(rt, &aiv1alpha1.PRFeedbackSpec{Authors: []string{"a.b", "a-b", "x"}})
		Expect(names).To(HaveLen(3))
		Expect(names).To(HaveKeyWithValue("t-pr-feedback-x", "x"))
		for name, a := range names {
			if a != "x" {
				Expect(name).To(MatchRegexp(`^t-pr-feedback-a-b-[0-9a-f]{6}$`))
			}
		}
	})

	It("renders the builtin input shape and escalation comments", func() {
		rt := &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: "t", Namespace: "ns"}}
		spec := prFeedbackTriggerSpec(rt, &aiv1alpha1.PRFeedbackSpec{Repos: []string{"o/r"}}, "dunce-bot[bot]")
		Expect(validateGitHubSource(spec.Source.GitHub)).To(Succeed())
		item := githubItem{Repo: "o/r", Number: 9, URL: "https://github.com/o/r/pull/9", GitHubPRState: &GitHubPRState{
			HeadSHA: "s9", FailureKind: failureKindConflict, FailureURL: "https://github.com/o/r/pull/9", FailedChecks: []githubFailedCheck{},
		}}
		ev, err := githubEvent(item, "s", time.Now())
		Expect(err).NotTo(HaveOccurred())
		ev.Data = withEventFields(ev.Data, map[string]any{"knight": "k", "runId": "r"})
		data := triggerTemplateData{Event: ev, Run: triggerRunRef{Attempt: 3}}
		in, err := renderTriggerTemplate("input", spec.Input, data)
		Expect(err).NotTo(HaveOccurred())
		Expect(in).To(MatchJSON(`{"repo": "o/r", "pr": 9, "url": "https://github.com/o/r/pull/9", "knight": "k", "runId": "r",
			"attempt": 3, "maxAttempts": 3, "kind": "conflict", "headSha": "s9", "failedChecks": null, "failureUrl": "https://github.com/o/r/pull/9"}`))
		comment, err := renderTriggerTemplate("c", spec.Source.GitHub.OnMaxRuns.Comment, data)
		Expect(err).NotTo(HaveOccurred())
		Expect(comment).To(Equal("Still conflicting with the base branch after 3 fix round(s) (head s9). Escalating to a human."))
	})
})
