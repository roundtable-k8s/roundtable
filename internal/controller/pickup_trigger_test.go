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

// Issue pickup on the trigger engine (#520): the pickup controller generates
// a github-source Trigger; the Trigger controller polls and starts the runs.
var _ = Describe("IssuePickup trigger engine", func() {
	const (
		ns       = "default"
		rtName   = "ptrg-table"
		tmplName = "ptrg-pipeline"
		repo     = pickupTestRepo
		token    = "ghs_ptrg"
		trgName  = rtName + aiv1alpha1.IssuePickupTriggerSuffix
	)
	rtKey := types.NamespacedName{Name: rtName, Namespace: ns}
	trgKey := types.NamespacedName{Name: trgName, Namespace: ns}
	run := func(n string) string { return tmplName + "-widgets-" + n }

	var (
		natsSrv  *natsserver.Server
		cli      natspkg.Client
		gh       *fakeGitHub
		ghSrv    *httptest.Server
		pickup   *IssuePickupReconciler
		triggers *TriggerReconciler
		recorder *record.FakeRecorder
		clock    time.Time
	)

	reconcilePickup := func() {
		_, err := pickup.Reconcile(ctx, reconcile.Request{NamespacedName: rtKey})
		Expect(err).NotTo(HaveOccurred())
	}
	tick := func() {
		clock = clock.Add(10 * time.Minute)
		reconcilePickup()
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
	setEngine := func(e aiv1alpha1.IssuePickupEngine) {
		rt := getRT()
		rt.Spec.IssuePickup.Engine = e
		Expect(k8sClient.Update(ctx, rt)).To(Succeed())
	}
	getTrigger := func() (*aiv1alpha1.Trigger, error) {
		t := &aiv1alpha1.Trigger{}
		return t, k8sClient.Get(ctx, trgKey, t)
	}
	getChain := func(name string) *aiv1alpha1.Chain {
		c := &aiv1alpha1.Chain{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, c); err != nil {
			return nil
		}
		return c
	}
	pickupChains := func() []string {
		l := &aiv1alpha1.ChainList{}
		Expect(k8sClient.List(ctx, l, client.InNamespace(ns), client.MatchingLabels{aiv1alpha1.LabelPickupRoundTable: rtName})).To(Succeed())
		names := make([]string, 0, len(l.Items))
		for _, c := range l.Items {
			names = append(names, c.Name)
		}
		return names
	}
	finish := func(name string) {
		c := getChain(name)
		Expect(c).NotTo(BeNil())
		c.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
		c.Status.LastRunTrigger = c.Annotations[aiv1alpha1.AnnotationChainRun]
		c.Status.CompletedAt = &metav1.Time{Time: clock}
		Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
	}
	labelsOf := func(n int) func() []string { return func() []string { return gh.labels(repo, n) } }
	githubReady := func() string {
		t, err := getTrigger()
		Expect(err).NotTo(HaveOccurred())
		c := meta.FindStatusCondition(t.Status.Conditions, aiv1alpha1.ConditionTriggerGitHubReady)
		Expect(c).NotTo(BeNil())
		return c.Message
	}
	// startEngine generates the Trigger and starts its consumer.
	startEngine := func() {
		reconcilePickup()
		reconcileTrigger()
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

		gh = newFakeGitHub()
		gh.token = token
		ghSrv = httptest.NewServer(gh)
		clock = time.Now()
		recorder = record.NewFakeRecorder(200)
		factory := github.NewClientFactory(ghSrv.URL)
		pickup = &IssuePickupReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: recorder,
			GitHub: factory, Now: func() time.Time { return clock },
		}
		triggers = &TriggerReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(200),
			NATS: provider, GitHub: factory, Now: func() time.Time { return clock },
			manualGitHubPolls: true,
		}

		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "ptrg-gh", Namespace: ns},
			Data:       map[string][]byte{"token": []byte(token)},
		})).To(Succeed())
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: tmplName, Namespace: ns},
			Spec: aiv1alpha1.ChainSpec{
				RoundTableRef: &corev1.LocalObjectReference{Name: rtName},
				Steps:         []aiv1alpha1.ChainStep{{Name: "plan", KnightRef: corev1.LocalObjectReference{Name: "lead"}, Task: "plan {{ .Input }}", TimeoutSeconds: 120}},
			},
		})).To(Succeed())
		rt := &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: ns},
			Spec: aiv1alpha1.RoundTableSpec{
				NATS: aiv1alpha1.RoundTableNATS{
					SubjectPrefix: "ptrg", TasksStream: "ptrg_tasks", ResultsStream: "ptrg_results", CreateStreams: true,
				},
				GitHub: &aiv1alpha1.GitHubConfig{TokenSecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "ptrg-gh"}, Key: "token"}},
				IssuePickup: &aiv1alpha1.IssuePickupSpec{
					Engine:    aiv1alpha1.IssuePickupEngineTrigger,
					Repos:     []string{repo},
					ChainRef:  corev1.LocalObjectReference{Name: tmplName},
					Assignees: []string{"octo"},
				},
			},
		}
		Expect(k8sClient.Create(ctx, rt)).To(Succeed())
		Expect((&RoundTableReconciler{NATS: provider}).ensureStreams(ctx, rt)).To(Succeed())
	})

	AfterEach(func() {
		triggers.init()
		triggers.pollers.stopAll()
		triggers.consumers.stopAll()
		if t, err := getTrigger(); err == nil {
			t.Finalizers = nil
			Expect(client.IgnoreNotFound(k8sClient.Update(ctx, t))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, t))).To(Succeed())
		}
		Expect(k8sClient.DeleteAllOf(ctx, &aiv1alpha1.Chain{}, client.InNamespace(ns),
			client.MatchingLabels{aiv1alpha1.LabelPickupRoundTable: rtName})).To(Succeed())
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: tmplName, Namespace: ns}}))).To(Succeed())
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: ns}}))).To(Succeed())
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "ptrg-gh", Namespace: ns}}))).To(Succeed())
		ghSrv.Close()
		_ = cli.Close()
		natsSrv.Shutdown()
	})

	It("creates no Trigger with engine unset", func() {
		rt := getRT()
		rt.Spec.IssuePickup.Engine = ""
		Expect(k8sClient.Update(ctx, rt)).To(Succeed())
		Expect(getRT().Spec.IssuePickup.Engine).To(Equal(aiv1alpha1.IssuePickupEngineBuiltin), "CRD default")
		gh.add(repo, 7, "Add a widget", "please", "rt:ready")

		reconcilePickup()
		_, err := getTrigger()
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
		Expect(pickupChains()).To(ConsistOf(run("7")))
		st := getRT().Status.IssuePickup
		Expect(st.Engine).To(Equal(aiv1alpha1.IssuePickupEngineBuiltin))
		Expect(st.Trigger).To(BeEmpty())
		Expect(st.Conditions).To(BeEmpty())
	})

	It("generates an owned Trigger from the issuePickup fields and stops the builtin poller", func() {
		gh.add(repo, 7, "Add a widget", "please", "rt:ready")
		reconcilePickup()

		trg, err := getTrigger()
		Expect(err).NotTo(HaveOccurred())
		Expect(metav1.GetControllerOf(trg).Name).To(Equal(rtName))
		Expect(pickupTable(trg)).To(Equal(rtName))
		src := trg.Spec.Source.GitHub
		Expect(src).NotTo(BeNil())
		Expect(src.Repositories).To(Equal([]string{repo}))
		Expect(src.Query.Labels).To(Equal([]string{"rt:ready"}))
		Expect(src.PollInterval.Duration).To(Equal(300 * time.Second))
		Expect(src.Claim.AddLabels).To(Equal([]string{"rt:in-progress"}))
		Expect(src.Claim.RemoveLabels).To(Equal([]string{"rt:ready"}))
		Expect(src.Claim.Assignees).To(Equal([]string{"octo"}))
		Expect(src.MaxActivePerRepository).To(Equal(int32(1)))
		Expect(src.HoldWhilePullRequestLabeled).To(Equal("breaking"))
		Expect(src.SkipIfBodyMatches.Patterns).To(HaveLen(3))
		Expect(src.SkipIfBodyMatches.HandBack.AddLabels).To(Equal([]string{"needs-human"}))
		Expect(trg.Spec.Target.ChainRef.Name).To(Equal(tmplName))
		Expect(trg.Spec.Concurrency.MaxActive).To(Equal(int32(2)))
		Expect(*trg.Spec.TTLSecondsAfterFinished).To(BeZero())
		Expect(trg.Spec.RoundTableRef.Name).To(Equal(rtName))

		Expect(pickupChains()).To(BeEmpty(), "the builtin poller starts nothing")
		st := getRT().Status.IssuePickup
		Expect(st.Engine).To(Equal(aiv1alpha1.IssuePickupEngineTrigger))
		Expect(st.Trigger).To(Equal(trgName))
		Expect(st.Conditions).To(BeEmpty(), "not deprecated before the trigger engine succeeded")

		By("following spec changes")
		rt := getRT()
		rt.Spec.IssuePickup.MaxActive = 5
		Expect(k8sClient.Update(ctx, rt)).To(Succeed())
		reconcilePickup()
		trg, _ = getTrigger()
		Expect(trg.Spec.Concurrency.MaxActive).To(Equal(int32(5)))
	})

	It("starts one run per ready issue with the pickup input and labels, within the per-repo cap", func() {
		gh.add(repo, 7, "Add a widget", "please", "rt:ready")
		gh.add(repo, 8, "Another widget", "later", "rt:ready")
		startEngine()
		poll()

		Eventually(func() *aiv1alpha1.Chain { return getChain(run("7")) }, wait, every).ShouldNot(BeNil())
		c := getChain(run("7"))
		Expect(c.Spec.Input).To(MatchJSON(`{"repo": "acme/widgets", "issue": 7}`))
		Expect(c.Labels).To(HaveKeyWithValue(aiv1alpha1.LabelPickupRoundTable, rtName))
		Expect(c.Labels).To(HaveKeyWithValue(aiv1alpha1.LabelTrigger, trgName))
		Expect(c.Labels).To(HaveKeyWithValue(chainrun.LabelRunSource, chainrun.SourceTrigger))
		Expect(c.Annotations).To(HaveKeyWithValue(aiv1alpha1.AnnotationPickupRepo, repo))
		Expect(c.Annotations).To(HaveKeyWithValue(aiv1alpha1.AnnotationPickupIssue, "7"))
		Expect(metav1.GetControllerOf(c).Kind).To(Equal("RoundTable"))
		runID := c.Annotations[aiv1alpha1.AnnotationChainRun]

		Eventually(labelsOf(7), wait, every).Should(ConsistOf("rt:in-progress"))
		Eventually(func() []string { return gh.commentsOn(repo, 7) }, wait, every).Should(HaveLen(1))
		Expect(gh.commentsOn(repo, 7)[0]).To(ContainSubstring("started Chain `" + run("7") + "` (run `" + runID + "`)"))
		Expect(gh.commentsOn(repo, 7)[0]).To(ContainSubstring("Add `rt:ready` again"))
		Eventually(func() *aiv1alpha1.Chain { return getChain(run("7")) }, wait, every).Should(
			WithTransform(func(c *aiv1alpha1.Chain) string { return c.Annotations[aiv1alpha1.AnnotationPickupClaimed] }, Equal(runID)))
		Expect(gh.assigned[repo+"#7"]).To(Equal([]string{"octo"}))
		Consistently(pickupChains, time.Second, 200*time.Millisecond).Should(ConsistOf(run("7")), "per-repo cap of 1 holds #8")
		Expect(gh.labels(repo, 8)).To(ConsistOf("rt:ready"))

		By("a ready label re-added mid-run is dropped without a second run or comment")
		gh.setLabels(7, "rt:in-progress", "rt:ready")
		poll()
		Eventually(labelsOf(7), wait, every).Should(ConsistOf("rt:in-progress"))
		Expect(gh.commentsOn(repo, 7)).To(HaveLen(1))
		Expect(pickupChains()).To(ConsistOf(run("7")))

		By("taking the next issue once the first run finished, and marking the builtin engine deprecated")
		finish(run("7"))
		poll()
		Eventually(pickupChains, wait, every).Should(ConsistOf(run("7"), run("8")))
		Eventually(labelsOf(8), wait, every).Should(ConsistOf("rt:in-progress"))
		tick()
		st := getRT().Status.IssuePickup
		Expect(st.TriggerEngineSucceededAt).NotTo(BeNil())
		cond := meta.FindStatusCondition(st.Conditions, aiv1alpha1.ConditionIssuePickupBuiltinDeprecated)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonIssuePickupTriggerEngineProven))
		Eventually(recorder.Events).Should(Receive(ContainSubstring("BuiltinEngineDeprecated")))

		By("re-running a finished run when the ready label comes back")
		gh.setLabels(7, "rt:ready")
		finish(run("8"))
		poll()
		Eventually(func() bool { return pickupChainActive(getChain(run("7"))) }, wait, every).Should(BeTrue())
		Expect(getChain(run("7")).Annotations[aiv1alpha1.AnnotationChainRun]).NotTo(Equal(runID))
		Eventually(func() []string { return gh.commentsOn(repo, 7) }, wait, every).Should(HaveLen(2))
		Eventually(labelsOf(7), wait, every).Should(ConsistOf("rt:in-progress"))
	})

	It("holds a repo while a breaking PR is open and hands back issues that need a human", func() {
		gh.add(repo, 3, "Bump CI", "edit .github/workflows/ci.yml", "rt:ready")
		gh.add(repo, 4, "Tricky", "", "rt:ready", "needs-human")
		gh.add(repo, 7, "Add a widget", "please", "rt:ready")
		breaking := github.PullRequest{Number: 241, Labels: []github.Label{{Name: "breaking"}}}
		breaking.Head.Ref = "rt/241-crd-change"
		gh.pulls[repo] = append(gh.pulls[repo], breaking)
		startEngine()
		poll()

		Expect(gh.labels(repo, 3)).To(ConsistOf("needs-human"))
		Expect(gh.commentsOn(repo, 3)).To(ConsistOf(And(ContainSubstring(".github/workflows"), ContainSubstring("Removed `rt:ready`"))))
		Expect(gh.labels(repo, 4)).To(ConsistOf("needs-human"))
		Expect(gh.labels(repo, 7)).To(ConsistOf("rt:ready"))
		Expect(githubReady()).To(ContainSubstring("2 handed back, 1 held by open breaking pull requests"))
		Consistently(pickupChains, time.Second, 200*time.Millisecond).Should(BeEmpty())

		By("handing back once, and picking up after the breaking PR closed")
		gh.closePRs(repo)
		poll()
		Eventually(pickupChains, wait, every).Should(ConsistOf(run("7")))
		Expect(gh.commentsOn(repo, 3)).To(HaveLen(1))

		By("holding when the PR lookup fails")
		finish(run("7"))
		gh.add(repo, 9, "More", "", "rt:ready")
		gh.failPRs = true
		poll()
		Expect(githubReady()).To(ContainSubstring("list breaking pull requests in acme/widgets"))
		Consistently(pickupChains, time.Second, 200*time.Millisecond).Should(ConsistOf(run("7")))
	})

	It("switches back to builtin without picking an in-progress issue twice, and forth again", func() {
		gh.add(repo, 7, "Add a widget", "please", "rt:ready")
		startEngine()
		poll()
		Eventually(func() []string { return gh.commentsOn(repo, 7) }, wait, every).Should(HaveLen(1))
		Eventually(func() *aiv1alpha1.Chain { return getChain(run("7")) }, wait, every).Should(
			WithTransform(func(c *aiv1alpha1.Chain) string { return c.Annotations[aiv1alpha1.AnnotationPickupClaimed] }, Not(BeEmpty())))

		By("deleting the generated Trigger; the run keeps going")
		setEngine(aiv1alpha1.IssuePickupEngineBuiltin)
		reconcilePickup()
		trg, err := getTrigger()
		Expect(err).NotTo(HaveOccurred())
		Expect(trg.DeletionTimestamp).NotTo(BeNil(), "deleted; the Trigger finalizer releases its consumer")
		reconcileTrigger()
		_, err = getTrigger()
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
		Expect(pickupChains()).To(ConsistOf(run("7")))
		Expect(getRT().Status.IssuePickup.Engine).To(Equal(aiv1alpha1.IssuePickupEngineBuiltin))

		By("the builtin poller drops a ready label re-added mid-run without a second run or comment")
		gh.setLabels(7, "rt:in-progress", "rt:ready")
		tick()
		Expect(pickupChains()).To(ConsistOf(run("7")))
		Expect(gh.labels(repo, 7)).To(ConsistOf("rt:in-progress"))
		Expect(gh.commentsOn(repo, 7)).To(HaveLen(1))

		By("the builtin poller picks new issues, not the in-progress one")
		finish(run("7"))
		gh.add(repo, 8, "Next", "", "rt:ready")
		tick()
		Expect(pickupChains()).To(ConsistOf(run("7"), run("8")))
		Expect(gh.commentsOn(repo, 7)).To(HaveLen(1))
		Expect(getRT().Status.IssuePickup.LastError).To(BeEmpty())

		By("switching to the trigger engine again: a builtin run is not claimed twice")
		setEngine(aiv1alpha1.IssuePickupEngineTrigger)
		startEngine()
		gh.setLabels(8, "rt:in-progress", "rt:ready")
		poll()
		Eventually(labelsOf(8), wait, every).Should(ConsistOf("rt:in-progress"))
		Expect(gh.commentsOn(repo, 8)).To(HaveLen(1))
		Expect(pickupChains()).To(ConsistOf(run("7"), run("8")))
	})

	It("does not touch a Trigger it does not own", func() {
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Trigger{
			ObjectMeta: metav1.ObjectMeta{Name: trgName, Namespace: ns},
			Spec: aiv1alpha1.TriggerSpec{
				Source: aiv1alpha1.TriggerSource{Schedule: &aiv1alpha1.TriggerScheduleSource{Cron: "0 * * * *"}},
				Target: aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: tmplName}},
			},
		})).To(Succeed())
		reconcilePickup()
		Expect(getRT().Status.IssuePickup.LastError).To(ContainSubstring("not owned by this RoundTable"))

		setEngine(aiv1alpha1.IssuePickupEngineBuiltin)
		tick()
		_, err := getTrigger()
		Expect(err).NotTo(HaveOccurred(), "a Trigger the table does not control is never deleted")
	})
})

var _ = Describe("IssuePickup trigger engine helpers", func() {
	It("quotes template delimiters in literal text", func() {
		Expect(templateLiteral("rt:ready")).To(Equal("rt:ready"))
		out, err := renderTriggerTemplate("t", "x "+templateLiteral("a{{b}}")+" y", triggerTemplateData{})
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(Equal("x a{{b}} y"))
	})

	It("renders the pickup input and comments from a github event", func() {
		rt := &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: "t", Namespace: "ns"}}
		spec := pickupTriggerSpec(rt, &aiv1alpha1.IssuePickupSpec{Repos: []string{"a/b"}, ChainRef: corev1.LocalObjectReference{Name: "pr"}})
		Expect(validateGitHubSource(spec.Source.GitHub)).To(Succeed())
		ev, err := githubEvent(githubItem{Repo: "a/b", Number: 1234567}, "s", time.Now())
		Expect(err).NotTo(HaveOccurred())
		data := triggerTemplateData{Event: ev, Run: triggerRunRef{Name: "pr-b-1234567", ID: "42"}, Reason: "labelled `needs-human`"}
		in, err := renderTriggerTemplate("input", spec.Input, data)
		Expect(err).NotTo(HaveOccurred())
		Expect(in).To(MatchJSON(`{"repo": "a/b", "issue": 1234567}`))
		claim, err := renderTriggerTemplate("c", spec.Source.GitHub.Claim.Comment, data)
		Expect(err).NotTo(HaveOccurred())
		Expect(claim).To(ContainSubstring("RoundTable `ns/t`): started Chain `pr-b-1234567` (run `42`)"))
		hb, err := renderTriggerTemplate("h", spec.Source.GitHub.SkipIfBodyMatches.HandBack.Comment, data)
		Expect(err).NotTo(HaveOccurred())
		Expect(hb).To(HavePrefix("Not picked up by the Round Table: this issue needs a human (labelled `needs-human`)."))
	})

	It("only treats Triggers controlled by their labelled RoundTable as pickup Triggers", func() {
		trg := &aiv1alpha1.Trigger{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{aiv1alpha1.LabelPickupRoundTable: "t"}}}
		Expect(pickupTable(trg)).To(BeEmpty())
		yes := true
		trg.OwnerReferences = []metav1.OwnerReference{{APIVersion: aiv1alpha1.GroupVersion.String(), Kind: "RoundTable", Name: "t", Controller: &yes}}
		Expect(pickupTable(trg)).To(Equal("t"))
		trg.OwnerReferences[0].Name = "other"
		Expect(pickupTable(trg)).To(BeEmpty())
	})
})
