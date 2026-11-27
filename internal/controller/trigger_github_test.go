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
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/chainrun"
	"github.com/roundtable-k8s/roundtable/internal/github"
	"github.com/roundtable-k8s/roundtable/pkg/metrics"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// triggerGitHubItem is an issue or pull request served by triggerFakeGitHub.
type triggerGitHubItem struct {
	number int
	title  string
	author string
	pull   bool
	closed bool
	branch string
	labels []string
}

// triggerFakeGitHub serves the issue list and label endpoints a github-source
// Trigger uses from memory.
type triggerFakeGitHub struct {
	mu    sync.Mutex
	token string
	items map[string][]*triggerGitHubItem // repo -> items
	// lists counts list requests; labelCalls label writes.
	lists, labelCalls int
	// failLabels makes label writes fail with 500.
	failLabels bool
	// rateLimitReset, when set, answers list requests with a primary rate
	// limit error resetting then.
	rateLimitReset time.Time
	// onLabel runs on every label write (before it is applied).
	onLabel func()
}

// triggerTestRepo is the repository the github-source tests poll.
const triggerTestRepo = "acme/widgets"

var triggerPullsRoute = regexp.MustCompile(`^/repos/([^/]+/[^/]+)/pulls$`)

var triggerIssuesRoute = regexp.MustCompile(`^/repos/([^/]+/[^/]+)/issues(?:/(\d+)/labels(?:/(.+))?)?$`)

// add serves item from the test repository.
func (f *triggerFakeGitHub) add(item *triggerGitHubItem) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.items[triggerTestRepo] = append(f.items[triggerTestRepo], item)
}

func (f *triggerFakeGitHub) labelsOf(repo string, n int) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, it := range f.items[repo] {
		if it.number == n {
			return slices.Clone(it.labels)
		}
	}
	return nil
}

func (f *triggerFakeGitHub) counts() (lists, labelCalls int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lists, f.labelCalls
}

func (f *triggerFakeGitHub) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	f.mu.Lock()
	onLabel := f.onLabel
	f.mu.Unlock()
	if req.Method != http.MethodGet && onLabel != nil {
		onLabel()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if req.Header.Get("Authorization") != "Bearer "+f.token {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"message":"Bad credentials"}`))
		return
	}
	if pm := triggerPullsRoute.FindStringSubmatch(req.URL.EscapedPath()); pm != nil && req.Method == http.MethodGet {
		repo, _ := url.PathUnescape(pm[1])
		out := []map[string]any{}
		for _, it := range f.items[repo] {
			if !it.pull || it.closed {
				continue
			}
			labels := []map[string]string{}
			for _, l := range it.labels {
				labels = append(labels, map[string]string{"name": l})
			}
			out = append(out, map[string]any{
				"number": it.number, "title": it.title, "state": "open", "labels": labels,
				"head": map[string]string{"ref": it.branch}, "user": map[string]string{"login": it.author},
			})
		}
		_ = json.NewEncoder(w).Encode(out)
		return
	}
	m := triggerIssuesRoute.FindStringSubmatch(req.URL.EscapedPath())
	if m == nil {
		http.NotFound(w, req)
		return
	}
	repo, _ := url.PathUnescape(m[1])
	if m[2] == "" {
		f.lists++
		if !f.rateLimitReset.IsZero() {
			w.Header().Set("X-RateLimit-Limit", "5000")
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(f.rateLimitReset.Unix(), 10))
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"message":"API rate limit exceeded"}`))
			return
		}
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Remaining", "4999")
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
		want := strings.Split(req.URL.Query().Get("labels"), ",")
		state := req.URL.Query().Get("state")
		out := []map[string]any{}
		for _, it := range f.items[repo] {
			open := string(aiv1alpha1.TriggerGitHubStateOpen)
			if (state == open && it.closed) || (state == string(aiv1alpha1.TriggerGitHubStateClosed) && !it.closed) {
				continue
			}
			if slices.ContainsFunc(want, func(l string) bool { return l != "" && !slices.Contains(it.labels, l) }) {
				continue
			}
			labels := []map[string]string{}
			for _, l := range it.labels {
				labels = append(labels, map[string]string{"name": l})
			}
			st := string(aiv1alpha1.TriggerGitHubStateOpen)
			if it.closed {
				st = string(aiv1alpha1.TriggerGitHubStateClosed)
			}
			o := map[string]any{
				"number": it.number, "title": it.title, "state": st, "labels": labels,
				"html_url": "https://github.com/" + repo + "/issues/" + strconv.Itoa(it.number),
				"user":     map[string]string{"login": it.author},
			}
			if it.pull {
				o["pull_request"] = map[string]string{"url": "x"}
			}
			out = append(out, o)
		}
		_ = json.NewEncoder(w).Encode(out)
		return
	}

	f.labelCalls++
	if f.failLabels {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"boom"}`))
		return
	}
	n, _ := strconv.Atoi(m[2])
	var item *triggerGitHubItem
	for _, it := range f.items[repo] {
		if it.number == n {
			item = it
		}
	}
	if item == nil {
		http.NotFound(w, req)
		return
	}
	switch req.Method {
	case http.MethodPost:
		var add []string
		_ = json.NewDecoder(req.Body).Decode(&add)
		for _, l := range add {
			if !slices.Contains(item.labels, l) {
				item.labels = append(item.labels, l)
			}
		}
	case http.MethodDelete:
		name, _ := url.PathUnescape(m[3])
		i := slices.Index(item.labels, name)
		if i < 0 {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"Label does not exist"}`))
			return
		}
		item.labels = slices.Delete(item.labels, i, i+1)
	}
	_, _ = w.Write([]byte(`[]`))
}

// conflictStatusClient answers every Trigger status patch with a conflict.
type conflictStatusClient struct {
	client.Client
	conflicts int
}

func (c *conflictStatusClient) Status() client.SubResourceWriter {
	return conflictStatusWriter{SubResourceWriter: c.Client.Status(), c: c}
}

type conflictStatusWriter struct {
	client.SubResourceWriter
	c *conflictStatusClient
}

func (w conflictStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if _, ok := obj.(*aiv1alpha1.Trigger); ok {
		w.c.conflicts++
		return apierrors.NewConflict(schema.GroupResource{Group: aiv1alpha1.GroupVersion.Group, Resource: "triggers"}, obj.GetName(), errors.New("the object has been modified"))
	}
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

var _ = Describe("Trigger GitHub source", func() {
	const (
		ns       = "default"
		rtName   = "trgh-rt"
		prefix   = "trgh"
		tmplName = "trgh-target"
		repo     = triggerTestRepo
		token    = "gh-trigger-token"
	)

	var (
		srv   *natsserver.Server
		cli   natspkg.Client
		gh    *triggerFakeGitHub
		ghSrv *httptest.Server
		r     *TriggerReconciler
		clock time.Time
	)

	runsOf := func(trigger string) []aiv1alpha1.Chain {
		list := &aiv1alpha1.ChainList{}
		Expect(k8sClient.List(ctx, list, client.InNamespace(ns),
			client.MatchingLabels{aiv1alpha1.LabelTrigger: trigger})).To(Succeed())
		return list.Items
	}
	getTrigger := func(name string) *aiv1alpha1.Trigger {
		t := &aiv1alpha1.Trigger{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, t)).To(Succeed())
		return t
	}
	reconcileTrigger := func(name string) {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
		Expect(err).NotTo(HaveOccurred())
	}
	poll := func(name string) time.Duration {
		return r.pollGitHub(ctx, types.NamespacedName{Namespace: ns, Name: name})
	}
	githubReady := func(name string) *metav1.Condition {
		return meta.FindStatusCondition(getTrigger(name).Status.Conditions, aiv1alpha1.ConditionTriggerGitHubReady)
	}
	finish := func(name string) {
		c := &aiv1alpha1.Chain{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, c)).To(Succeed())
		c.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
		c.Status.LastRunTrigger = c.Annotations[aiv1alpha1.AnnotationChainRun]
		c.Status.CompletedAt = &metav1.Time{Time: clock}
		Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
	}
	githubTrigger := func(name string, mutate func(*aiv1alpha1.TriggerSpec)) *aiv1alpha1.Trigger {
		t := &aiv1alpha1.Trigger{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: aiv1alpha1.TriggerSpec{
				Source: aiv1alpha1.TriggerSource{GitHub: &aiv1alpha1.TriggerGitHubSource{
					Repositories: []string{repo},
					Query:        aiv1alpha1.TriggerGitHubQuery{Labels: []string{"rt:ready"}},
				}},
				Target:        aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: tmplName}},
				Input:         `{{ toJson .Event.Data }}`,
				RoundTableRef: &corev1.LocalObjectReference{Name: rtName},
			},
		}
		if mutate != nil {
			mutate(&t.Spec)
		}
		Expect(k8sClient.Create(ctx, t)).To(Succeed())
		reconcileTrigger(name)
		return getTrigger(name)
	}

	BeforeEach(func() {
		opts := natstest.DefaultTestOptions
		opts.Port = -1
		opts.JetStream = true
		opts.StoreDir = GinkgoT().TempDir()
		srv = natstest.RunServer(&opts)
		cli = natspkg.NewClient(natspkg.Config{URL: srv.ClientURL()}, logr.Discard())
		Expect(cli.Connect()).To(Succeed())

		gh = &triggerFakeGitHub{token: token, items: map[string][]*triggerGitHubItem{}}
		ghSrv = httptest.NewServer(gh)

		clock = time.Now()
		provider := natspkg.NewProviderWithClient(cli, logr.Discard())
		r = &TriggerReconciler{
			Client:            k8sClient,
			Scheme:            k8sClient.Scheme(),
			Recorder:          record.NewFakeRecorder(100),
			NATS:              provider,
			GitHub:            github.NewClientFactory(ghSrv.URL),
			Now:               func() time.Time { return clock },
			manualGitHubPolls: true,
		}

		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "trgh-gh", Namespace: ns},
			Data:       map[string][]byte{"token": []byte(token + "\n")},
		})).To(Succeed())
		rt := &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: ns},
			Spec: aiv1alpha1.RoundTableSpec{
				NATS: aiv1alpha1.RoundTableNATS{
					SubjectPrefix: prefix, TasksStream: "trgh_tasks", ResultsStream: "trgh_results", CreateStreams: true,
				},
				GitHub: &aiv1alpha1.GitHubConfig{TokenSecretRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "trgh-gh"}, Key: "token"}},
			},
		}
		Expect(k8sClient.Create(ctx, rt)).To(Succeed())
		Expect((&RoundTableReconciler{NATS: provider}).ensureStreams(ctx, rt)).To(Succeed())
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: tmplName, Namespace: ns},
			Spec: aiv1alpha1.ChainSpec{
				RoundTableRef: &corev1.LocalObjectReference{Name: rtName},
				Steps:         []aiv1alpha1.ChainStep{{Name: "do", KnightRef: corev1.LocalObjectReference{Name: "lead"}, Task: "do {{ .Input }}", TimeoutSeconds: 120}},
			},
		})).To(Succeed())
	})

	AfterEach(func() {
		r.init()
		r.pollers.stopAll()
		r.consumers.stopAll()
		triggers := &aiv1alpha1.TriggerList{}
		Expect(k8sClient.List(ctx, triggers, client.InNamespace(ns))).To(Succeed())
		for i := range triggers.Items {
			t := &triggers.Items[i]
			t.Finalizers = nil
			Expect(client.IgnoreNotFound(k8sClient.Update(ctx, t))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, t))).To(Succeed())
		}
		Expect(k8sClient.DeleteAllOf(ctx, &aiv1alpha1.Chain{}, client.InNamespace(ns),
			client.MatchingLabels{chainrun.LabelRunSource: chainrun.SourceTrigger})).To(Succeed())
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: tmplName, Namespace: ns}}))).To(Succeed())
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: ns}}))).To(Succeed())
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "trgh-gh", Namespace: ns}}))).To(Succeed())
		ghSrv.Close()
		_ = cli.Close()
		srv.Shutdown()
	})

	It("fires once per matching issue and claims it only after its run exists", func() {
		gh.add(&triggerGitHubItem{number: 1, title: "Fix the widget", author: "alice", labels: []string{"rt:ready", "bug"}})
		gh.add(&triggerGitHubItem{number: 2, title: "Not ready", author: "bob", labels: []string{"bug"}})
		gh.add(&triggerGitHubItem{number: 3, title: "A ready pull", author: "carol", pull: true, labels: []string{"rt:ready"}})
		gh.add(&triggerGitHubItem{number: 4, title: "Closed", author: "dan", closed: true, labels: []string{"rt:ready"}})
		var runsAtClaim []int
		gh.onLabel = func() {
			list := &aiv1alpha1.ChainList{}
			_ = k8sClient.List(ctx, list, client.InNamespace(ns), client.MatchingLabels{aiv1alpha1.LabelTrigger: "ready"})
			gh.mu.Lock()
			runsAtClaim = append(runsAtClaim, len(list.Items))
			gh.mu.Unlock()
		}

		t := githubTrigger("ready", func(s *aiv1alpha1.TriggerSpec) {
			s.Source.GitHub.Claim = &aiv1alpha1.TriggerGitHubClaim{AddLabels: []string{"rt:in-progress"}, RemoveLabels: []string{"rt:ready"}}
		})
		subject := natspkg.GitHubEventSubject(prefix, ns, "ready")
		Expect(t.Finalizers).To(ContainElement(aiv1alpha1.FinalizerTrigger))
		Expect(t.Status.Consumer).To(Equal(natspkg.TriggerConsumerName(ns, "ready")))
		ready := meta.FindStatusCondition(t.Status.Conditions, aiv1alpha1.ConditionTriggerReady)
		Expect(ready.Status).To(Equal(metav1.ConditionTrue))
		Expect(ready.Message).To(ContainSubstring(subject))
		Expect(ready.Message).To(ContainSubstring("polling 1 repositories every 1m0s"))

		Expect(poll("ready")).To(Equal(time.Minute))
		Eventually(func() int { return len(runsOf("ready")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
		run := runsOf("ready")[0]
		Expect(run.Annotations[aiv1alpha1.AnnotationTriggerKey]).To(Equal("acme/widgets#1"))
		Expect(run.Annotations[aiv1alpha1.AnnotationTriggerSubject]).To(Equal(subject))
		var input map[string]any
		Expect(json.Unmarshal([]byte(run.Spec.Input), &input)).To(Succeed())
		Expect(input).To(Equal(map[string]any{
			"repo": repo, "number": float64(1), "title": "Fix the widget", "labels": []any{"rt:ready", "bug"},
			"url": "https://github.com/acme/widgets/issues/1", "author": "alice", "kind": "issue", "state": "open",
		}))

		Eventually(func() []string { return gh.labelsOf(repo, 1) }, 10*time.Second, 100*time.Millisecond).
			Should(ConsistOf("bug", "rt:in-progress"))
		// Every label write happened after the run existed.
		gh.mu.Lock()
		claimed := slices.Clone(runsAtClaim)
		gh.mu.Unlock()
		Expect(claimed).NotTo(BeEmpty())
		Expect(slices.Contains(claimed, 0)).To(BeFalse())
		Eventually(func(g Gomega) {
			st := getTrigger("ready").Status
			g.Expect(st.RecentFirings).To(HaveLen(1))
			g.Expect(st.RecentFirings[0].Run).To(Equal(run.Name))
			g.Expect(st.RecentFirings[0].SkipReason).To(BeEmpty())
		}, 5*time.Second, 100*time.Millisecond).Should(Succeed())
		cond := githubReady("ready")
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonTriggerPolling))
		Expect(cond.Message).To(ContainSubstring("rate limit remaining 4999/5000"))
		Expect(getTrigger("ready").Status.LastPolledAt).NotTo(BeNil())

		// The claim removed the selecting label: later polls start nothing.
		_, labelCalls := gh.counts()
		poll("ready")
		Consistently(func() int { return len(runsOf("ready")) }, time.Second, 200*time.Millisecond).Should(Equal(1))
		Expect(getTrigger("ready").Status.RecentFirings).To(HaveLen(1))
		_, after := gh.counts()
		Expect(after).To(Equal(labelCalls))
	})

	It("skips an event whose payload has no valid item", func() {
		githubTrigger("malformed", nil)
		subject := natspkg.GitHubEventSubject(prefix, ns, "malformed")
		Expect(cli.PublishJSON(subject, map[string]any{"title": "Forged"})).To(Succeed())
		Eventually(func(g Gomega) {
			st := getTrigger("malformed").Status
			g.Expect(st.RecentFirings).To(HaveLen(1))
			g.Expect(st.RecentFirings[0].Run).To(BeEmpty())
			g.Expect(st.RecentFirings[0].SkipReason).To(ContainSubstring("github event has no valid item"))
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
		Expect(runsOf("malformed")).To(BeEmpty())
	})

	It("fires a run-now request, which carries no github item", func() {
		githubTrigger("runnow", nil)
		t := getTrigger("runnow")
		t.Annotations = map[string]string{aiv1alpha1.AnnotationTriggerRunNow: "req-1"}
		Expect(k8sClient.Update(ctx, t)).To(Succeed())
		reconcileTrigger("runnow")
		Expect(runsOf("runnow")).To(HaveLen(1))
		st := getTrigger("runnow").Status
		Expect(st.RecentFirings).To(HaveLen(1))
		Expect(st.RecentFirings[0].SkipReason).To(BeEmpty())
		Expect(getTrigger("runnow").Annotations).NotTo(HaveKey(aiv1alpha1.AnnotationTriggerRunNow))
	})

	It("requeues instead of failing when the status patch conflicts", func() {
		githubTrigger("conflict", nil)
		cc := &conflictStatusClient{Client: k8sClient}
		r.Client = cc
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "conflict"}})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(BeNumerically(">", 0))
		Expect(cc.conflicts).To(BeNumerically(">", 0))
	})

	It("skips an event for a repository the Trigger does not list", func() {
		gh.add(&triggerGitHubItem{number: 1, title: "Fix the widget", author: "alice", labels: []string{"rt:ready"}})
		githubTrigger("listed", func(s *aiv1alpha1.TriggerSpec) {
			s.Source.GitHub.Claim = &aiv1alpha1.TriggerGitHubClaim{AddLabels: []string{"rt:in-progress"}, RemoveLabels: []string{"rt:ready"}}
		})
		subject := natspkg.GitHubEventSubject(prefix, ns, "listed")
		Expect(cli.PublishJSON(subject, map[string]any{
			"repo": "other/repo", "number": 7, "title": "Forged", "labels": []string{"rt:ready"},
		})).To(Succeed())
		Eventually(func(g Gomega) {
			st := getTrigger("listed").Status
			g.Expect(st.RecentFirings).To(HaveLen(1))
			g.Expect(st.RecentFirings[0].Run).To(BeEmpty())
			g.Expect(st.RecentFirings[0].SkipReason).To(Equal("repository other/repo is not in source.github.repositories"))
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
		Expect(runsOf("listed")).To(BeEmpty())
		_, labelCalls := gh.counts()
		Expect(labelCalls).To(BeZero())
	})

	It("starts a finished run again when a claimed item is selected again", func() {
		labels5 := func() []string {
			gh.mu.Lock()
			defer gh.mu.Unlock()
			return slices.Clone(gh.items[repo][0].labels)
		}
		gh.add(&triggerGitHubItem{number: 5, title: "Again", author: "alice", labels: []string{"rt:ready"}})
		githubTrigger("again", func(s *aiv1alpha1.TriggerSpec) {
			s.Source.GitHub.Claim = &aiv1alpha1.TriggerGitHubClaim{AddLabels: []string{"rt:in-progress"}, RemoveLabels: []string{"rt:ready"}}
		})
		poll("again")
		Eventually(func() int { return len(runsOf("again")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
		Eventually(labels5, 10*time.Second, 100*time.Millisecond).Should(ConsistOf("rt:in-progress"))
		first := runsOf("again")[0]
		firstID := first.Annotations[aiv1alpha1.AnnotationChainRun]
		Eventually(func() string { return runsOf("again")[0].Annotations[aiv1alpha1.AnnotationTriggerClaimed] },
			10*time.Second, 100*time.Millisecond).Should(Equal(firstID))
		Expect(first.Annotations).To(HaveKeyWithValue(aiv1alpha1.AnnotationTriggerGitHubRepo, repo))

		finish(first.Name)
		clock = clock.Add(time.Minute)
		gh.mu.Lock()
		gh.items[repo][0].labels = []string{"rt:ready"}
		gh.mu.Unlock()
		poll("again")
		Eventually(func() bool { return chainrun.Active(&runsOf("again")[0]) }, 10*time.Second, 100*time.Millisecond).Should(BeTrue())
		runs := runsOf("again")
		Expect(runs).To(HaveLen(1))
		Expect(runs[0].Name).To(Equal(first.Name))
		Expect(runs[0].Annotations[aiv1alpha1.AnnotationChainRun]).NotTo(Equal(firstID))
		Eventually(labels5, 10*time.Second, 100*time.Millisecond).Should(ConsistOf("rt:in-progress"))
	})

	It("does not fire again while the item's run exists", func() {
		gh.add(&triggerGitHubItem{number: 7, title: "Stay labelled", author: "alice", labels: []string{"rt:ready"}})
		githubTrigger("noclaim", nil)
		poll("noclaim")
		Eventually(func() int { return len(runsOf("noclaim")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
		Eventually(func() int { return len(getTrigger("noclaim").Status.RecentFirings) }, 5*time.Second, 100*time.Millisecond).Should(Equal(1))

		// Still labelled and its run is active: nothing is published again.
		poll("noclaim")
		finish(runsOf("noclaim")[0].Name)
		poll("noclaim")
		Consistently(func() int { return len(runsOf("noclaim")) }, time.Second, 200*time.Millisecond).Should(Equal(1))
		Expect(getTrigger("noclaim").Status.RecentFirings).To(HaveLen(1))
		_, labelCalls := gh.counts()
		Expect(labelCalls).To(BeZero())
	})

	It("records a failed claim and retries it on the next poll", func() {
		gh.add(&triggerGitHubItem{number: 5, title: "Flaky labels", author: "alice", labels: []string{"rt:ready"}})
		gh.failLabels = true
		githubTrigger("retry", func(s *aiv1alpha1.TriggerSpec) {
			s.Source.GitHub.Claim = &aiv1alpha1.TriggerGitHubClaim{RemoveLabels: []string{"rt:ready"}}
		})
		poll("retry")
		Eventually(func(g Gomega) {
			f := getTrigger("retry").Status.RecentFirings
			g.Expect(f).To(HaveLen(1))
			g.Expect(f[0].Run).NotTo(BeEmpty())
			g.Expect(f[0].SkipReason).To(ContainSubstring("claim of acme/widgets#5 failed"))
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
		Expect(runsOf("retry")).To(HaveLen(1))
		Expect(gh.labelsOf(repo, 5)).To(ConsistOf("rt:ready"))

		gh.mu.Lock()
		gh.failLabels = false
		gh.mu.Unlock()
		poll("retry")
		Eventually(func() []string { return gh.labelsOf(repo, 5) }, 10*time.Second, 100*time.Millisecond).Should(BeEmpty())
		Expect(runsOf("retry")).To(HaveLen(1))
		Expect(getTrigger("retry").Status.RecentFirings).To(HaveLen(1))
	})

	It("holds items back at maxActive", func() {
		for n := 1; n <= 3; n++ {
			gh.add(&triggerGitHubItem{number: n, title: "item", author: "alice", labels: []string{"rt:ready"}})
		}
		githubTrigger("capped", func(s *aiv1alpha1.TriggerSpec) {
			s.Concurrency.MaxActive = 1
		})
		poll("capped")
		Eventually(func() int { return len(runsOf("capped")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
		first := runsOf("capped")[0]
		Expect(first.Annotations[aiv1alpha1.AnnotationTriggerKey]).To(Equal("acme/widgets#1"))
		Expect(githubReady("capped").Message).To(ContainSubstring("1 published, 0 claim retries, 2 held back by concurrency"))

		poll("capped")
		Consistently(func() int { return len(runsOf("capped")) }, time.Second, 200*time.Millisecond).Should(Equal(1))

		// Once the run finished, the next item starts; the first does not
		// start again while its run exists.
		finish(first.Name)
		poll("capped")
		Eventually(func() int { return len(runsOf("capped")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(2))
		runs := runsOf("capped")
		keys := make([]string, 0, len(runs))
		for _, c := range runs {
			keys = append(keys, c.Annotations[aiv1alpha1.AnnotationTriggerKey])
		}
		Expect(keys).To(ConsistOf("acme/widgets#1", "acme/widgets#2"))
	})

	It("reports a repository held by a breaking pull request and clears it when the pull request closes", func() {
		gh.add(&triggerGitHubItem{number: 1, title: "item", author: "alice", labels: []string{"rt:ready"}})
		gh.add(&triggerGitHubItem{number: 7, title: "big change", author: "bob", pull: true, branch: "rt/7-big", labels: []string{"breaking"}})
		githubTrigger("held", func(s *aiv1alpha1.TriggerSpec) {
			s.Source.GitHub.HoldWhilePullRequestLabeled = "breaking"
		})
		heldCond := func() *metav1.Condition {
			return meta.FindStatusCondition(getTrigger("held").Status.Conditions, aiv1alpha1.ConditionTriggerRepositoryHeld)
		}
		labels := prometheus.Labels{"namespace": ns, "trigger": "held", "repository": repo}
		DeferCleanup(func() { r.clearRepositoryHolds(ns, "held") })

		Expect(poll("held")).To(BeNumerically(">", 0))
		Expect(runsOf("held")).To(BeEmpty())
		c := heldCond()
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionTrue))
		Expect(c.Reason).To(Equal(aiv1alpha1.ReasonTriggerBreakingPullRequest))
		Expect(c.Message).To(Equal("acme/widgets: #7 (rt/7-big) for 0s"))
		started := clock.Unix()
		Expect(testutil.ToFloat64(metrics.TriggerGitHubHeldItems.With(labels))).To(Equal(1.0))
		Expect(testutil.ToFloat64(metrics.TriggerGitHubHoldStartedTimestampSeconds.With(labels))).To(Equal(float64(started)))
		transition := c.LastTransitionTime

		// Still held later: the age grows, the transition time and start do not.
		clock = clock.Add(45 * time.Minute)
		poll("held")
		c = heldCond()
		Expect(c.Status).To(Equal(metav1.ConditionTrue))
		Expect(c.Message).To(Equal("acme/widgets: #7 (rt/7-big) for 45m0s"))
		Expect(c.LastTransitionTime).To(Equal(transition))
		Expect(testutil.ToFloat64(metrics.TriggerGitHubHoldStartedTimestampSeconds.With(labels))).To(Equal(float64(started)))

		// The pull request closes: condition False, series gone, item published.
		gh.mu.Lock()
		gh.items[repo][1].closed = true
		gh.mu.Unlock()
		poll("held")
		Eventually(func() int { return len(runsOf("held")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
		c = heldCond()
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(aiv1alpha1.ReasonTriggerNoHold))
		Expect(triggerSeries(metrics.TriggerGitHubHeldItems, "held")).To(BeZero())
		Expect(triggerSeries(metrics.TriggerGitHubHoldStartedTimestampSeconds, "held")).To(BeZero())
	})

	It("drops hold state when the label is removed, the Trigger is suspended or the repository is removed", func() {
		gh.add(&triggerGitHubItem{number: 7, title: "big change", author: "bob", pull: true, branch: "rt/7-big", labels: []string{"breaking"}})
		githubTrigger("held2", func(s *aiv1alpha1.TriggerSpec) {
			s.Source.GitHub.HoldWhilePullRequestLabeled = "breaking"
		})
		DeferCleanup(func() { r.clearRepositoryHolds(ns, "held2") })
		heldCond := func() *metav1.Condition {
			return meta.FindStatusCondition(getTrigger("held2").Status.Conditions, aiv1alpha1.ConditionTriggerRepositoryHeld)
		}
		update := func(mutate func(*aiv1alpha1.Trigger)) {
			t := getTrigger("held2")
			mutate(t)
			Expect(k8sClient.Update(ctx, t)).To(Succeed())
		}
		hold := func() {
			poll("held2")
			Expect(heldCond().Status).To(Equal(metav1.ConditionTrue))
			Expect(triggerSeries(metrics.TriggerGitHubHoldStartedTimestampSeconds, "held2")).To(Equal(1))
		}
		noSeries := func() {
			Expect(triggerSeries(metrics.TriggerGitHubHeldItems, "held2")).To(BeZero())
			Expect(triggerSeries(metrics.TriggerGitHubHoldStartedTimestampSeconds, "held2")).To(BeZero())
		}
		cleared := func() {
			Expect(heldCond().Status).To(Equal(metav1.ConditionFalse))
			noSeries()
		}

		hold()
		update(func(t *aiv1alpha1.Trigger) { t.Spec.Source.GitHub.HoldWhilePullRequestLabeled = "" })
		poll("held2")
		cleared()

		update(func(t *aiv1alpha1.Trigger) { t.Spec.Source.GitHub.HoldWhilePullRequestLabeled = "breaking" })
		hold()
		// Suspending stops the poller, so Reconcile does the clearing.
		update(func(t *aiv1alpha1.Trigger) { t.Spec.Suspend = true })
		reconcileTrigger("held2")
		Expect(heldCond()).To(BeNil())
		noSeries()

		update(func(t *aiv1alpha1.Trigger) { t.Spec.Suspend = false })
		hold()
		update(func(t *aiv1alpha1.Trigger) { t.Spec.Source.GitHub.Repositories = []string{"acme/other"} })
		poll("held2")
		cleared()
	})

	It("starts a second held repository's hold when it becomes held, not at the first one's start", func() {
		const other = "acme/gadgets"
		gh.add(&triggerGitHubItem{number: 7, title: "big change", author: "bob", pull: true, branch: "rt/7-big", labels: []string{"breaking"}})
		githubTrigger("held3", func(s *aiv1alpha1.TriggerSpec) {
			s.Source.GitHub.Repositories = []string{repo, other}
			s.Source.GitHub.HoldWhilePullRequestLabeled = "breaking"
		})
		DeferCleanup(func() { r.clearRepositoryHolds(ns, "held3") })
		series := func(name string) float64 {
			return testutil.ToFloat64(metrics.TriggerGitHubHoldStartedTimestampSeconds.With(prometheus.Labels{"namespace": ns, "trigger": "held3", "repository": name}))
		}

		poll("held3")
		first := clock.Unix()
		Expect(series(repo)).To(Equal(float64(first)))

		clock = clock.Add(2 * time.Hour)
		gh.mu.Lock()
		gh.items[other] = append(gh.items[other], &triggerGitHubItem{number: 9, title: "other", author: "bob", pull: true, branch: "rt/9-x", labels: []string{"breaking"}})
		gh.mu.Unlock()
		poll("held3")
		Expect(series(repo)).To(Equal(float64(first)))
		Expect(series(other)).To(Equal(float64(clock.Unix())))
		msg := meta.FindStatusCondition(getTrigger("held3").Status.Conditions, aiv1alpha1.ConditionTriggerRepositoryHeld).Message
		Expect(msg).To(ContainSubstring(other + ": #9 (rt/9-x) for 0s"))
	})

	It("shares the total cap across repositories with fairShare", func() {
		addTo := func(r string, n int) {
			gh.mu.Lock()
			defer gh.mu.Unlock()
			gh.items[r] = append(gh.items[r], &triggerGitHubItem{number: n, title: "item", author: "alice", labels: []string{"rt:ready"}})
		}
		for n := 1; n <= 3; n++ {
			addTo("acme/widgets", n)
		}
		addTo("acme/gadgets", 7)
		repoKeys := func(name string) []string {
			runs := runsOf(name)
			keys := make([]string, 0, len(runs))
			for _, c := range runs {
				keys = append(keys, c.Annotations[aiv1alpha1.AnnotationTriggerKey])
			}
			return keys
		}
		githubTrigger("unfair", func(s *aiv1alpha1.TriggerSpec) {
			s.Source.GitHub.Repositories = []string{"acme/widgets", "acme/gadgets"}
			s.Concurrency.MaxActive = 2
		})
		poll("unfair")
		Eventually(func() []string { return repoKeys("unfair") }, 10*time.Second, 100*time.Millisecond).
			Should(ConsistOf("acme/widgets#1", "acme/widgets#2"))

		githubTrigger("fair", func(s *aiv1alpha1.TriggerSpec) {
			// An empty repository first must not waste a pass.
			s.Source.GitHub.Repositories = []string{"acme/empty", "acme/widgets", "acme/gadgets"}
			s.Source.GitHub.FairShare = true
			s.Concurrency.MaxActive = 2
		})
		poll("fair")
		Eventually(func() []string { return repoKeys("fair") }, 10*time.Second, 100*time.Millisecond).
			Should(ConsistOf("acme/widgets#1", "acme/gadgets#7"))
	})

	It("selects pull requests with kind pullRequest", func() {
		gh.add(&triggerGitHubItem{number: 8, title: "An issue", author: "alice", labels: []string{"rt:ready"}})
		gh.add(&triggerGitHubItem{number: 9, title: "A pull", author: "bob", pull: true, labels: []string{"rt:ready"}})
		githubTrigger("pulls", func(s *aiv1alpha1.TriggerSpec) {
			s.Source.GitHub.Kind = aiv1alpha1.TriggerGitHubKindPullRequest
			s.Input = `{{ .Event.Data.kind }} {{ .Event.Data.repo }}#{{ .Event.Data.number }} by {{ .Event.Data.author }}`
		})
		poll("pulls")
		Eventually(func() int { return len(runsOf("pulls")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
		Expect(runsOf("pulls")[0].Spec.Input).To(Equal("pullRequest acme/widgets#9 by bob"))
	})

	It("backs off until a rate limit resets", func() {
		gh.add(&triggerGitHubItem{number: 1, title: "item", author: "alice", labels: []string{"rt:ready"}})
		reset := clock.Add(20 * time.Minute).Truncate(time.Second)
		gh.rateLimitReset = reset
		githubTrigger("limited", func(s *aiv1alpha1.TriggerSpec) {
			s.Source.GitHub.Repositories = []string{repo, "acme/gadgets"}
		})
		wait := poll("limited")
		Expect(wait).To(BeNumerically(">", 19*time.Minute))
		Expect(wait).To(BeNumerically("<=", 20*time.Minute))
		// The second repository is not listed once the limit is hit.
		lists, _ := gh.counts()
		Expect(lists).To(Equal(1))
		cond := githubReady("limited")
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonTriggerRateLimited))
		Consistently(func() int { return len(runsOf("limited")) }, time.Second, 200*time.Millisecond).Should(BeZero())

		gh.mu.Lock()
		gh.rateLimitReset = time.Time{}
		gh.mu.Unlock()
		Expect(poll("limited")).To(Equal(time.Minute))
		Eventually(func() int { return len(runsOf("limited")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
		Expect(githubReady("limited").Status).To(Equal(metav1.ConditionTrue))
	})

	It("reports a missing token in GitHubReady", func() {
		githubTrigger("notoken", nil)
		Expect(k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "trgh-gh", Namespace: ns}})).To(Succeed())
		Eventually(func(g Gomega) {
			poll("notoken")
			cond := githubReady("notoken")
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			g.Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonTriggerTokenError))
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())
		lists, _ := gh.counts()
		Expect(lists).To(BeZero())
	})

	It("starts its poll loop from Reconcile and stops it when suspended", func() {
		r.manualGitHubPolls = false
		gh.add(&triggerGitHubItem{number: 1, title: "item", author: "alice", labels: []string{"rt:ready"}})
		githubTrigger("looped", nil)
		key := types.NamespacedName{Namespace: ns, Name: "looped"}.String()
		Expect(r.pollers.running(key)).To(BeTrue())
		Eventually(func() int { return len(runsOf("looped")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
		Eventually(func() *metav1.Condition { return githubReady("looped") }, 5*time.Second, 100*time.Millisecond).ShouldNot(BeNil())

		t := getTrigger("looped")
		t.Spec.Suspend = true
		Expect(k8sClient.Update(ctx, t)).To(Succeed())
		reconcileTrigger("looped")
		Expect(r.pollers.running(key)).To(BeFalse())
		t = getTrigger("looped")
		Expect(meta.FindStatusCondition(t.Status.Conditions, aiv1alpha1.ConditionTriggerGitHubReady)).To(BeNil())
		Expect(t.Status.Consumer).To(BeEmpty())
	})

	It("validates the github source", func() {
		base := func(name string) *aiv1alpha1.Trigger {
			return &aiv1alpha1.Trigger{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
				Spec: aiv1alpha1.TriggerSpec{
					Source: aiv1alpha1.TriggerSource{GitHub: &aiv1alpha1.TriggerGitHubSource{
						Repositories: []string{repo},
						Query:        aiv1alpha1.TriggerGitHubQuery{Labels: []string{"rt:ready"}},
					}},
					Target:        aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: tmplName}},
					RoundTableRef: &corev1.LocalObjectReference{Name: rtName},
				},
			}
		}
		t := base("v-gh-noref")
		t.Spec.RoundTableRef = nil
		Expect(k8sClient.Create(ctx, t)).To(MatchError(ContainSubstring("roundTableRef is required for source.github")))

		t = base("v-gh-fast")
		t.Spec.Source.GitHub.PollInterval = &metav1.Duration{Duration: 10 * time.Second}
		Expect(k8sClient.Create(ctx, t)).To(MatchError(ContainSubstring("pollInterval must be at least 30s")))

		t = base("v-gh-claim")
		t.Spec.Source.GitHub.Claim = &aiv1alpha1.TriggerGitHubClaim{}
		Expect(k8sClient.Create(ctx, t)).To(MatchError(ContainSubstring("set claim.addLabels or claim.removeLabels")))

		t = base("v-gh-repo")
		t.Spec.Source.GitHub.Repositories = []string{"not-a-repo"}
		Expect(k8sClient.Create(ctx, t)).To(MatchError(ContainSubstring("spec.source.github.repositories[0]")))

		t = base("v-gh-nolabels")
		t.Spec.Source.GitHub.Query.Labels = nil
		Expect(k8sClient.Create(ctx, t)).To(MatchError(ContainSubstring("set query.labels, or a state selector")))

		t = base("v-gh-state-issue")
		t.Spec.Source.GitHub.Checks = aiv1alpha1.TriggerGitHubChecksFailing
		Expect(k8sClient.Create(ctx, t)).To(MatchError(ContainSubstring("checks, mergeable and mergeQueue need kind: pullRequest")))

		t = base("v-gh-state-closed")
		t.Spec.Source.GitHub.Kind = aiv1alpha1.TriggerGitHubKindPullRequest
		t.Spec.Source.GitHub.Mergeable = aiv1alpha1.TriggerGitHubMergeableConflicting
		t.Spec.Source.GitHub.Query.State = aiv1alpha1.TriggerGitHubStateClosed
		Expect(k8sClient.Create(ctx, t)).To(MatchError(ContainSubstring("leave query.state open")))

		t = base("v-gh-maxruns")
		t.Spec.Source.GitHub.Kind = aiv1alpha1.TriggerGitHubKindPullRequest
		t.Spec.Source.GitHub.Checks = aiv1alpha1.TriggerGitHubChecksFailing
		t.Spec.Source.GitHub.MaxRunsPerItem = 3
		Expect(k8sClient.Create(ctx, t)).To(MatchError(ContainSubstring("maxRunsPerItem needs a state selector and onMaxRuns.addLabels")))

		t = base("v-gh-ignore")
		t.Spec.Source.GitHub.IgnoreChecks = []string{"merge-guard"}
		Expect(k8sClient.Create(ctx, t)).To(MatchError(ContainSubstring("ignoreChecks needs checks or mergeQueue")))

		t = base("v-gh-state-ok")
		t.Spec.Source.GitHub.Query.Labels = nil
		t.Spec.Source.GitHub.Kind = aiv1alpha1.TriggerGitHubKindPullRequest
		t.Spec.Source.GitHub.Checks = aiv1alpha1.TriggerGitHubChecksFailing
		t.Spec.Source.GitHub.MaxRunsPerItem = 3
		t.Spec.Source.GitHub.OnMaxRuns = &aiv1alpha1.TriggerGitHubHandBack{AddLabels: []string{"needs-human"}}
		Expect(k8sClient.Create(ctx, t)).To(Succeed())

		t = base("v-gh-ok")
		Expect(k8sClient.Create(ctx, t)).To(Succeed())
		t = getTrigger("v-gh-ok")
		Expect(t.Spec.Source.GitHub.Kind).To(Equal(aiv1alpha1.TriggerGitHubKindIssue))
		Expect(t.Spec.Source.GitHub.Query.State).To(Equal(aiv1alpha1.TriggerGitHubStateOpen))
	})
})

func TestGithubRepoListed(t *testing.T) {
	src := &aiv1alpha1.TriggerGitHubSource{Repositories: []string{"acme/app", " Acme/Widgets "}}
	for _, tc := range []struct {
		src  *aiv1alpha1.TriggerGitHubSource
		repo string
		want bool
	}{
		{src, "acme/app", true},
		{src, "ACME/App", true},
		{src, "acme/widgets", true},
		{src, "other/repo", false},
		{src, "acme/app2", false},
		{src, "", false},
		{nil, "acme/app", false},
	} {
		if got := githubRepoListed(tc.src, tc.repo); got != tc.want {
			t.Errorf("githubRepoListed(%q) = %v, want %v", tc.repo, got, tc.want)
		}
	}
}

// triggerSeries counts a collector's series for one Trigger, so specs do not
// depend on series other specs left in the shared gauges.
func triggerSeries(c prometheus.Collector, trigger string) int {
	ch := make(chan prometheus.Metric, 64)
	go func() { c.Collect(ch); close(ch) }()
	n := 0
	for m := range ch {
		d := &dto.Metric{}
		_ = m.Write(d)
		var ns, tr string
		for _, l := range d.Label {
			switch l.GetName() {
			case "namespace":
				ns = l.GetValue()
			case "trigger":
				tr = l.GetValue()
			}
		}
		if ns == "default" && tr == trigger {
			n++
		}
	}
	return n
}
