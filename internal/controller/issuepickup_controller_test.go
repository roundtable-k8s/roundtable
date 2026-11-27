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
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/github"
)

// fakeGitHub serves the issue endpoints the pickup controller uses from
// memory.
type fakeGitHub struct {
	mu       sync.Mutex
	issues   map[string][]*github.Issue // repo -> issues
	pulls    map[string][]github.PullRequest
	failPRs  bool
	comments map[string][]string // "repo#n" -> bodies
	assigned map[string][]string
	requests int
	token    string
	// repoLabels: repo -> label name -> description; labelCalls counts
	// label-endpoint requests.
	repoLabels map[string]map[string]string
	labelCalls int
	// failLabel: label names whose creation fails with 500.
	failLabel map[string]bool
	// failComments makes the next n comment calls fail with 500.
	failComments int
	// prState: "repo#n" -> pull request returned by GET /pulls/{n}.
	prState map[string]*github.PullRequest
	// autoMerged: node IDs auto-merge was enabled for; failAutoMerge makes it fail.
	autoMerged    []string
	failAutoMerge bool
	// reviews: "repo#n" -> review bodies on the PR (oldest first).
	reviews map[string][]string
}

func (f *fakeGitHub) setPR(pr github.PullRequest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.prState == nil {
		f.prState = map[string]*github.PullRequest{}
	}
	pr.Number = 7
	pr.NodeID = "acme/widgets#7"
	f.prState[pr.NodeID] = &pr
}

func newFakeGitHub() *fakeGitHub {
	return &fakeGitHub{issues: map[string][]*github.Issue{}, pulls: map[string][]github.PullRequest{}, comments: map[string][]string{}, assigned: map[string][]string{}, repoLabels: map[string]map[string]string{}}
}

func (f *fakeGitHub) add(repo string, n int, title, body string, labels ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	is := &github.Issue{Number: n, Title: title, Body: body}
	for _, l := range labels {
		is.Labels = append(is.Labels, github.Label{Name: l})
	}
	f.issues[repo] = append(f.issues[repo], is)
}

func (f *fakeGitHub) addPR(repo string, n int, branch string, labels ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	pr := github.PullRequest{Number: n}
	pr.Head.Ref = branch
	for _, l := range labels {
		pr.Labels = append(pr.Labels, github.Label{Name: l})
	}
	f.pulls[repo] = append(f.pulls[repo], pr)
}

func (f *fakeGitHub) closePRs(repo string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pulls[repo] = nil
}

func (f *fakeGitHub) labels(repo string, n int) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	is := f.find(repo, n)
	out := make([]string, 0, len(is.Labels))
	for _, l := range is.Labels {
		out = append(out, l.Name)
	}
	return out
}

func (f *fakeGitHub) commentsOn(repo string, n int) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.comments[repo+"#"+strconv.Itoa(n)])
}

// pickupTestRepo is the repository the IssuePickup tests operate on.
const pickupTestRepo = "acme/widgets"

// setLabels replaces the labels of issue n in pickupTestRepo.
func (f *fakeGitHub) setLabels(n int, labels ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	is := f.find(pickupTestRepo, n)
	is.Labels = nil
	for _, l := range labels {
		is.Labels = append(is.Labels, github.Label{Name: l})
	}
}

func (f *fakeGitHub) find(repo string, n int) *github.Issue {
	for _, is := range f.issues[repo] {
		if is.Number == n {
			return is
		}
	}
	return nil
}

var pullsRoute = regexp.MustCompile(`^/repos/([^/]+/[^/]+)/pulls$`)

var reviewsRoute = regexp.MustCompile(`^/repos/([^/]+/[^/]+)/pulls/(\d+)/reviews$`)

var pullRoute = regexp.MustCompile(`^/repos/([^/]+/[^/]+)/pulls/(\d+)$`)

var labelRoute = regexp.MustCompile(`^/repos/([^/]+/[^/]+)/labels(?:/(.+))?$`)

var issueRoute = regexp.MustCompile(`^/repos/([^/]+/[^/]+)/issues(?:/(\d+)(?:(/labels|/comments|/assignees)(?:/(.+))?)?)?$`)

func (f *fakeGitHub) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests++
	if req.Header.Get("Authorization") != "Bearer "+f.token {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"message":"Bad credentials"}`))
		return
	}
	if req.URL.Path == "/graphql" {
		var body struct {
			Variables struct{ ID string } `json:"variables"`
		}
		_ = json.NewDecoder(req.Body).Decode(&body)
		if f.failAutoMerge {
			_, _ = w.Write([]byte(`{"errors":[{"message":"Pull request is in clean status"}]}`))
			return
		}
		f.autoMerged = append(f.autoMerged, body.Variables.ID)
		if pr := f.prState[body.Variables.ID]; pr != nil {
			pr.AutoMerge = &struct{}{}
		}
		_, _ = w.Write([]byte(`{"data":{}}`))
		return
	}
	if rm := reviewsRoute.FindStringSubmatch(req.URL.EscapedPath()); rm != nil {
		repo, _ := url.PathUnescape(rm[1])
		key := repo + "#" + rm[2]
		if req.Method == http.MethodPost {
			var body map[string]string
			_ = json.NewDecoder(req.Body).Decode(&body)
			if f.reviews == nil {
				f.reviews = map[string][]string{}
			}
			f.reviews[key] = append(f.reviews[key], body["body"])
			_, _ = w.Write([]byte(`{}`))
			return
		}
		out := []github.Review{}
		for i, b := range f.reviews[key] {
			out = append(out, github.Review{ID: int64(i + 1), State: "COMMENTED", Body: b})
		}
		_ = json.NewEncoder(w).Encode(out)
		return
	}
	if gm := pullRoute.FindStringSubmatch(req.URL.EscapedPath()); gm != nil {
		repo, _ := url.PathUnescape(gm[1])
		pr := f.prState[repo+"#"+gm[2]]
		if pr == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(pr)
		return
	}
	if pm := pullsRoute.FindStringSubmatch(req.URL.EscapedPath()); pm != nil {
		if f.failPRs {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		repo, _ := url.PathUnescape(pm[1])
		out := f.pulls[repo]
		if out == nil {
			out = []github.PullRequest{}
		}
		_ = json.NewEncoder(w).Encode(out)
		return
	}
	if lm := labelRoute.FindStringSubmatch(req.URL.EscapedPath()); lm != nil {
		f.labelCalls++
		repo, _ := url.PathUnescape(lm[1])
		if f.repoLabels[repo] == nil {
			f.repoLabels[repo] = map[string]string{}
		}
		if req.Method == http.MethodGet {
			name, _ := url.PathUnescape(lm[2])
			if _, ok := f.repoLabels[repo][name]; !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write([]byte(`{}`))
			return
		}
		var body map[string]string
		_ = json.NewDecoder(req.Body).Decode(&body)
		if f.failLabel[body["name"]] {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if _, ok := f.repoLabels[repo][body["name"]]; ok {
			w.WriteHeader(http.StatusUnprocessableEntity)
			return
		}
		f.repoLabels[repo][body["name"]] = body["description"] + "|" + body["color"]
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
		return
	}
	m := issueRoute.FindStringSubmatch(req.URL.EscapedPath())
	if m == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	repo, _ := url.PathUnescape(m[1])
	if m[2] == "" { // list
		label := req.URL.Query().Get("labels")
		out := []*github.Issue{}
		for _, is := range f.issues[repo] {
			if is.HasLabel(label) {
				out = append(out, is)
			}
		}
		_ = json.NewEncoder(w).Encode(out)
		return
	}
	n, _ := strconv.Atoi(m[2])
	is := f.find(repo, n)
	if is == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if m[3] == "" && req.Method == http.MethodGet { // one issue
		out := *is
		if out.State == "" {
			out.State = "open"
		}
		_ = json.NewEncoder(w).Encode(out)
		return
	}
	var body map[string]any
	if req.Body != nil {
		var raw any
		_ = json.NewDecoder(req.Body).Decode(&raw)
		switch v := raw.(type) {
		case map[string]any:
			body = v
		case []any: // add-labels is a bare JSON array
			body = map[string]any{"labels": v}
		}
	}
	switch {
	case m[3] == "" && req.Method == http.MethodPatch:
		is.State, _ = body["state"].(string)
		is.StateReason, _ = body["state_reason"].(string)
	case m[3] == "/labels" && req.Method == http.MethodPost:
		for _, l := range body["labels"].([]any) {
			if !is.HasLabel(l.(string)) {
				is.Labels = append(is.Labels, github.Label{Name: l.(string)})
			}
		}
		_, _ = w.Write([]byte(`[]`)) // the real API returns the label list
		return
	case m[3] == "/labels" && req.Method == http.MethodDelete:
		name, _ := url.PathUnescape(m[4])
		if !is.HasLabel(name) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		is.Labels = slices.DeleteFunc(is.Labels, func(l github.Label) bool { return l.Name == name })
	case m[3] == "/comments":
		if f.failComments > 0 {
			f.failComments--
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		key := repo + "#" + m[2]
		f.comments[key] = append(f.comments[key], body["body"].(string))
		w.WriteHeader(http.StatusCreated)
	case m[3] == "/assignees":
		for _, a := range body["assignees"].([]any) {
			f.assigned[repo+"#"+m[2]] = append(f.assigned[repo+"#"+m[2]], a.(string))
		}
		w.WriteHeader(http.StatusCreated)
	}
	_, _ = w.Write([]byte(`{}`))
}

var _ = Describe("IssuePickup controller", func() {
	const (
		ns       = "default"
		rtName   = "pickup-table"
		tmplName = "pickup-pipeline"
		repo     = pickupTestRepo
		token    = "ghs_test"
	)
	ctx := context.Background()
	rtKey := types.NamespacedName{Name: rtName, Namespace: ns}

	var (
		gh    *fakeGitHub
		srv   *httptest.Server
		r     *IssuePickupReconciler
		clock time.Time
	)

	reconcileOnce := func() reconcile.Result {
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: rtKey})
		Expect(err).NotTo(HaveOccurred())
		return res
	}
	tick := func() reconcile.Result {
		clock = clock.Add(10 * time.Minute)
		return reconcileOnce()
	}
	getRT := func() *aiv1alpha1.RoundTable {
		rt := &aiv1alpha1.RoundTable{}
		Expect(k8sClient.Get(ctx, rtKey, rt)).To(Succeed())
		return rt
	}
	pickupChains := func() []aiv1alpha1.Chain {
		l := &aiv1alpha1.ChainList{}
		Expect(k8sClient.List(ctx, l, client.InNamespace(ns), client.MatchingLabels{aiv1alpha1.LabelPickupRoundTable: rtName})).To(Succeed())
		return l.Items
	}
	finish := func(name string) {
		c := &aiv1alpha1.Chain{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, c)).To(Succeed())
		c.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
		c.Status.LastRunTrigger = c.Annotations[aiv1alpha1.AnnotationChainRun]
		c.Status.CompletedAt = &metav1.Time{Time: clock}
		Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
	}

	BeforeEach(func() {
		gh = newFakeGitHub()
		gh.token = token
		srv = httptest.NewServer(gh)
		clock = time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
		r = &IssuePickupReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: record.NewFakeRecorder(100),
			GitHub:   github.NewClientFactory(srv.URL),
			Now:      func() time.Time { return clock },
		}

		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "pickup-gh", Namespace: ns},
			Data:       map[string][]byte{"token": []byte(token + "\n")},
		})).To(Succeed())
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: tmplName, Namespace: ns},
			Spec: aiv1alpha1.ChainSpec{
				RoundTableRef: &corev1.LocalObjectReference{Name: rtName},
				Input:         `{"repo": "x/y", "issue": 1}`,
				Schedule:      "0 * * * *",
				Steps:         []aiv1alpha1.ChainStep{{Name: "plan", KnightRef: corev1.LocalObjectReference{Name: "lead"}, Task: "plan {{ .Input }}", TimeoutSeconds: 120}},
			},
		})).To(Succeed())
		Expect(k8sClient.Create(ctx, &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: ns},
			Spec: aiv1alpha1.RoundTableSpec{
				NATS: aiv1alpha1.RoundTableNATS{
					URL: "nats://localhost:4222", SubjectPrefix: "pick", TasksStream: "pick_tasks", ResultsStream: "pick_results",
				},
				GitHub: &aiv1alpha1.GitHubConfig{TokenSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "pickup-gh"}, Key: "token"}},
				IssuePickup: &aiv1alpha1.IssuePickupSpec{
					Repos:     []string{repo},
					ChainRef:  corev1.LocalObjectReference{Name: tmplName},
					Assignees: []string{"octo"},
				},
			},
		})).To(Succeed())
	})

	AfterEach(func() {
		srv.Close()
		for _, c := range pickupChains() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &c))).To(Succeed())
		}
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: tmplName, Namespace: ns}}))).To(Succeed())
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: ns}}))).To(Succeed())
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "pickup-gh", Namespace: ns}}))).To(Succeed())
	})

	It("applies CRD defaults to spec.issuePickup", func() {
		p := getRT().Spec.IssuePickup
		Expect(p.ReadyLabel).To(Equal("rt:ready"))
		Expect(p.InProgressLabel).To(Equal("rt:in-progress"))
		Expect(p.NeedsHumanLabel).To(Equal("needs-human"))
		Expect(p.IntervalSeconds).To(Equal(int32(300)))
		Expect(p.MaxActivePerRepo).To(Equal(int32(1)))
		Expect(p.MaxActive).To(Equal(int32(2)))
		Expect(p.TTLSecondsAfterFinished).To(Equal(new(int32(604800))))
		Expect(p.HumanPatterns).To(HaveLen(3))
		Expect(p.BreakingLabel).To(Equal("breaking"))
		Expect(p.PostRun).To(Equal(aiv1alpha1.IssuePickupPostRunBuiltin))
	})

	It("holds new work in a repo while a breaking PR is open (#260)", func() {
		gh.add(repo, 7, "Add a widget", "please", "rt:ready")
		gh.addPR(repo, 241, "rt/241-crd-change", "Breaking") // label match is case-insensitive
		gh.addPR(repo, 242, "rt/242-other", "docs")

		reconcileOnce()
		Expect(pickupChains()).To(BeEmpty())
		Expect(gh.labels(repo, 7)).To(ConsistOf("rt:ready"))
		Expect(gh.commentsOn(repo, 7)).To(BeEmpty())
		Expect(getRT().Status.IssuePickup.PickedTotal).To(Equal(int64(0)))
		Expect(getRT().Status.IssuePickup.LastError).To(BeEmpty())

		By("picking the issue up once the breaking PR is merged or closed")
		gh.closePRs(repo)
		tick()
		Expect(pickupChains()).To(HaveLen(1))
		Expect(gh.labels(repo, 7)).To(ConsistOf("rt:in-progress"))
	})

	It("does not hold for PRs without the breaking label", func() {
		gh.add(repo, 7, "Add a widget", "please", "rt:ready")
		gh.addPR(repo, 242, "rt/242-other", "docs")
		reconcileOnce()
		Expect(pickupChains()).To(HaveLen(1))
	})

	It("holds when the breaking PR lookup fails", func() {
		gh.add(repo, 7, "Add a widget", "please", "rt:ready")
		gh.failPRs = true
		reconcileOnce()
		Expect(pickupChains()).To(BeEmpty())
		Expect(getRT().Status.IssuePickup.LastError).To(ContainSubstring("list breaking PRs"))
	})

	It("claims a ready issue once, starts a Chain copy and respects the per-repo cap", func() {
		gh.add(repo, 7, "Add a widget", "please", "rt:ready")
		gh.add(repo, 8, "Another widget", "later", "rt:ready")

		res := reconcileOnce()
		Expect(res.RequeueAfter).To(Equal(300 * time.Second))

		chains := pickupChains()
		Expect(chains).To(HaveLen(1))
		c := chains[0]
		Expect(c.Name).To(Equal(tmplName + "-widgets-7"))
		Expect(c.Spec.Input).To(MatchJSON(`{"repo": "acme/widgets", "issue": 7}`))
		Expect(c.Spec.Schedule).To(BeEmpty())
		Expect(c.Spec.Steps).To(HaveLen(1))
		Expect(c.Annotations).To(HaveKeyWithValue(aiv1alpha1.AnnotationPickupRepo, repo))
		Expect(c.Annotations).To(HaveKeyWithValue(aiv1alpha1.AnnotationPickupIssue, "7"))
		trigger := c.Annotations[aiv1alpha1.AnnotationChainRun]
		Expect(trigger).NotTo(BeEmpty())
		Expect(c.Annotations).To(HaveKeyWithValue(aiv1alpha1.AnnotationPickupClaimed, trigger))
		Expect(metav1.GetControllerOf(&c).Name).To(Equal(rtName))

		Expect(gh.labels(repo, 7)).To(ConsistOf("rt:in-progress"))
		Expect(gh.commentsOn(repo, 7)).To(HaveLen(1))
		Expect(gh.commentsOn(repo, 7)[0]).To(ContainSubstring("started Chain `" + c.Name + "`"))
		Expect(gh.assigned[repo+"#7"]).To(Equal([]string{"octo"}))
		Expect(gh.labels(repo, 8)).To(ConsistOf("rt:ready"), "per-repo cap of 1 leaves #8 ready")

		st := getRT().Status.IssuePickup
		Expect(st).NotTo(BeNil())
		Expect(st.PickedTotal).To(Equal(int64(1)))
		Expect(st.Active).To(Equal(int32(1)))
		Expect(st.LastError).To(BeEmpty())

		By("not polling again before the interval")
		before := gh.requests
		res = reconcileOnce()
		Expect(gh.requests).To(Equal(before))
		Expect(res.RequeueAfter).To(BeNumerically("~", 300*time.Second, time.Second))

		By("a ready label re-added mid-run is dropped without a second chain or comment")
		gh.setLabels(7, "rt:in-progress", "rt:ready")
		tick()
		Expect(pickupChains()).To(HaveLen(1))
		Expect(gh.labels(repo, 7)).To(ConsistOf("rt:in-progress"))
		Expect(gh.commentsOn(repo, 7)).To(HaveLen(1))
		Expect(gh.labels(repo, 8)).To(ConsistOf("rt:ready"))

		By("taking the next issue once the first Chain finished")
		finish(c.Name)
		tick()
		Expect(pickupChains()).To(HaveLen(2))
		Expect(gh.labels(repo, 8)).To(ConsistOf("rt:in-progress"))
		Expect(getRT().Status.IssuePickup.PickedTotal).To(Equal(int64(2)))

		By("re-running a finished Chain when the ready label comes back")
		gh.setLabels(7, "rt:ready")
		finish(tmplName + "-widgets-8")
		tick()
		again := &aiv1alpha1.Chain{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: c.Name, Namespace: ns}, again)).To(Succeed())
		Expect(again.Annotations[aiv1alpha1.AnnotationChainRun]).NotTo(Equal(trigger))
		Expect(pickupChainActive(again)).To(BeTrue())
		Expect(gh.commentsOn(repo, 7)).To(HaveLen(2))

		By("deleting finished Chains after ttlSecondsAfterFinished")
		clock = clock.Add(8 * 24 * time.Hour)
		reconcileOnce()
		left := pickupChains()
		names := make([]string, 0, len(left))
		for _, ch := range left {
			names = append(names, ch.Name)
		}
		Expect(names).To(ConsistOf(c.Name), "#8 finished 8 days ago; #7 is running again")
	})

	It("creates missing pickup labels once, never touching existing ones", func() {
		gh.repoLabels[repo] = map[string]string{"needs-human": "custom|123456"}

		reconcileOnce()
		Expect(gh.repoLabels[repo]).To(HaveKeyWithValue("rt:ready", "Round Table: ready to be picked up|0e8a16"))
		Expect(gh.repoLabels[repo]).To(HaveKeyWithValue("rt:in-progress", "Round Table: a chain is working on this|fbca04"))
		Expect(gh.repoLabels[repo]).To(HaveKeyWithValue("needs-human", "custom|123456"))
		calls := gh.labelCalls
		Expect(calls).To(Equal(5)) // 3 GETs + 2 POSTs

		// Later polls use the cache.
		tick()
		Expect(gh.labelCalls).To(Equal(calls))

		// A changed label name is a new label set and is ensured once.
		rt := getRT()
		rt.Spec.IssuePickup.ReadyLabel = "rt:go"
		Expect(k8sClient.Update(ctx, rt)).To(Succeed())
		tick()
		Expect(gh.repoLabels[repo]).To(HaveKey("rt:go"))
		Expect(gh.repoLabels[repo]).To(HaveKey("rt:ready"))
	})

	It("creates the other pickup labels when one fails, and retries only the failed one", func() {
		gh.failLabel = map[string]bool{"rt:ready": true}

		reconcileOnce()
		Expect(gh.repoLabels[repo]).To(HaveKey("rt:in-progress"))
		Expect(gh.repoLabels[repo]).To(HaveKey("needs-human"))
		Expect(gh.repoLabels[repo]).NotTo(HaveKey("rt:ready"))
		lastErr := getRT().Status.IssuePickup.LastError
		Expect(lastErr).To(ContainSubstring("ensure labels"))
		Expect(lastErr).To(ContainSubstring("rt:ready"))
		Expect(strings.Count(lastErr, "ensure labels")).To(Equal(1))

		gh.failLabel = nil
		calls := gh.labelCalls
		tick()
		Expect(gh.repoLabels[repo]).To(HaveKey("rt:ready"))
		Expect(gh.labelCalls).To(Equal(calls+2), "only the failed label is retried (GET + POST)")
		Expect(getRT().Status.IssuePickup.LastError).To(BeEmpty())
	})

	It("hands issues that need a human back instead of starting a Chain", func() {
		gh.add(repo, 3, "Bump CI", "edit .github/workflows/ci.yml", "rt:ready")
		gh.add(repo, 4, "Tricky", "", "rt:ready", "needs-human")

		reconcileOnce()
		Expect(pickupChains()).To(BeEmpty())
		Expect(gh.labels(repo, 3)).To(ConsistOf("needs-human"))
		Expect(gh.commentsOn(repo, 3)).To(ConsistOf(ContainSubstring(".github/workflows")))
		Expect(gh.labels(repo, 4)).To(ConsistOf("needs-human"))
		Expect(getRT().Status.IssuePickup.SkippedTotal).To(Equal(int64(2)))

		tick()
		Expect(gh.commentsOn(repo, 3)).To(HaveLen(1), "handed back once")
	})

	It("finishes a half-done claim on the next poll without a second Chain", func() {
		gh.add(repo, 5, "Feature", "", "rt:ready")
		gh.failComments = 1

		reconcileOnce()
		Expect(pickupChains()).To(HaveLen(1))
		st := getRT().Status.IssuePickup
		Expect(st.LastError).To(ContainSubstring("claim"))
		Expect(st.PickedTotal).To(BeZero())
		Expect(gh.commentsOn(repo, 5)).To(BeEmpty())

		// The ready label was already removed; put it back as if the
		// removal had failed too, so the poll sees the issue again.
		gh.setLabels(5, "rt:ready", "rt:in-progress")
		tick()
		chains := pickupChains()
		Expect(chains).To(HaveLen(1))
		Expect(chains[0].Annotations[aiv1alpha1.AnnotationPickupClaimed]).To(Equal(chains[0].Annotations[aiv1alpha1.AnnotationChainRun]))
		Expect(gh.commentsOn(repo, 5)).To(HaveLen(1))
		Expect(gh.labels(repo, 5)).To(ConsistOf("rt:in-progress"))
		Expect(getRT().Status.IssuePickup.LastError).To(BeEmpty())
	})

	It("caps unfinished Chains per table across repos", func() {
		const other = "acme/gears"
		rt := getRT()
		rt.Spec.IssuePickup.Repos = []string{repo, other}
		Expect(k8sClient.Update(ctx, rt)).To(Succeed())
		gh.add(repo, 1, "a", "", "rt:ready")
		gh.add(other, 1, "b", "", "rt:ready")
		gh.add(other, 2, "c", "", "rt:ready")

		reconcileOnce()
		Expect(pickupChains()).To(HaveLen(2))
		Expect(gh.labels(repo, 1)).To(ConsistOf("rt:in-progress"))
		Expect(gh.labels(other, 1)).To(ConsistOf("rt:in-progress"))
		Expect(gh.labels(other, 2)).To(ConsistOf("rt:ready"))
		Expect(gh.commentsOn(other, 2)).To(BeEmpty())

		By("lowering maxActive below the running count starts nothing new")
		finish(tmplName + "-gears-1")
		rt = getRT()
		rt.Spec.IssuePickup.MaxActive = 1
		Expect(k8sClient.Update(ctx, rt)).To(Succeed())
		tick()
		Expect(gh.labels(other, 2)).To(ConsistOf("rt:ready"))
		Expect(getRT().Status.IssuePickup.Active).To(Equal(int32(1)))
	})

	It("does nothing when suspended and reports a bad token", func() {
		gh.add(repo, 9, "Feature", "", "rt:ready")
		rt := getRT()
		rt.Spec.IssuePickup.Suspend = true
		Expect(k8sClient.Update(ctx, rt)).To(Succeed())
		Expect(reconcileOnce().RequeueAfter).To(BeZero())
		Expect(gh.requests).To(BeZero())

		rt = getRT()
		rt.Spec.IssuePickup.Suspend = false
		Expect(k8sClient.Update(ctx, rt)).To(Succeed())
		gh.token = "rotated"
		reconcileOnce()
		Expect(pickupChains()).To(BeEmpty())
		Expect(getRT().Status.IssuePickup.LastError).To(ContainSubstring("401"))
	})

	// Verifies the merge step on GitHub (#301): its output is ignored.
	Context("merge verification", func() {
		start := func() string {
			gh.add(repo, 301, "Fix", "body", "rt:ready")
			reconcileOnce()
			chains := pickupChains()
			Expect(chains).To(HaveLen(1))
			name := chains[0].Name
			c := &aiv1alpha1.Chain{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, c)).To(Succeed())
			c.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
			c.Status.LastRunTrigger = c.Annotations[aiv1alpha1.AnnotationChainRun]
			c.Status.CompletedAt = &metav1.Time{Time: clock}
			c.Status.StepStatuses = []aiv1alpha1.ChainStepStatus{
				{Name: "review", Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: "PRs: https://github.com/acme/widgets/pull/7\nVerdict: ship it"},
				{Name: "merge", Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: "Looks good. Verdict: ship it"},
			}
			Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
			return name
		}
		verified := func(name string) bool {
			c := &aiv1alpha1.Chain{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, c)).To(Succeed())
			return c.Annotations[aiv1alpha1.AnnotationPickupVerified] == c.Status.LastRunTrigger
		}

		It("queues an open PR the merge step left alone", func() {
			gh.setPR(github.PullRequest{State: "open"})
			name := start()
			tick()
			Expect(gh.autoMerged).To(Equal([]string{"acme/widgets#7"}))
			Expect(gh.labels(repo, 301)).NotTo(ContainElement("needs-human"))
			Expect(verified(name)).To(BeTrue())
			n := len(gh.autoMerged)
			tick()
			Expect(gh.autoMerged).To(HaveLen(n)) // verified once
		})

		It("posts the missing review, then queues the PR", func() {
			gh.setPR(github.PullRequest{State: "open"})
			start()
			tick()
			Expect(gh.reviews["acme/widgets#7"]).To(HaveLen(1))
			Expect(gh.reviews["acme/widgets#7"][0]).To(ContainSubstring("Verdict: ship it"))
			Expect(gh.autoMerged).To(Equal([]string{"acme/widgets#7"}))
		})

		It("does not repost when the newest review already matches", func() {
			gh.setPR(github.PullRequest{State: "open"})
			gh.reviews = map[string][]string{"acme/widgets#7": {"Verdict: needs changes", "Verdict: ship it"}}
			start()
			tick()
			Expect(gh.reviews["acme/widgets#7"]).To(HaveLen(2))
			Expect(gh.autoMerged).To(Equal([]string{"acme/widgets#7"}))
		})

		It("posts a needs-changes review and does not queue the PR", func() {
			gh.setPR(github.PullRequest{State: "open"})
			name := start()
			c := &aiv1alpha1.Chain{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, c)).To(Succeed())
			c.Status.StepStatuses[0].Output = "PRs: https://github.com/acme/widgets/pull/7\nVerdict: needs changes"
			Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
			tick()
			Expect(gh.reviews["acme/widgets#7"]).To(HaveLen(1))
			Expect(gh.reviews["acme/widgets#7"][0]).To(ContainSubstring("Verdict: needs changes"))
			Expect(gh.autoMerged).To(BeEmpty())
			Expect(gh.labels(repo, 301)).To(ContainElement("needs-human"))
		})

		It("accepts merged and already-queued PRs untouched", func() {
			q := github.PullRequest{State: "open", AutoMerge: &struct{}{}}
			gh.setPR(q)
			name := start()
			tick()
			Expect(gh.autoMerged).To(BeEmpty())
			Expect(gh.labels(repo, 301)).NotTo(ContainElement("needs-human"))
			Expect(verified(name)).To(BeTrue())
		})

		It("labels the issue needs-human when the PR is closed unmerged", func() {
			gh.setPR(github.PullRequest{State: "closed"})
			name := start()
			tick()
			Expect(gh.labels(repo, 301)).To(ContainElement("needs-human"))
			Expect(gh.labels(repo, 301)).NotTo(ContainElement("rt:in-progress"))
			Expect(gh.commentsOn(repo, 301)[len(gh.commentsOn(repo, 301))-1]).To(ContainSubstring("closed without being merged"))
			Expect(verified(name)).To(BeTrue())
		})

		It("labels the issue needs-human when the PR cannot be queued", func() {
			gh.setPR(github.PullRequest{State: "open"})
			gh.failAutoMerge = true
			start()
			tick()
			Expect(gh.labels(repo, 301)).To(ContainElement("needs-human"))
		})

		It("labels the issue needs-human when no PR can be found", func() {
			name := start()
			c := &aiv1alpha1.Chain{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, c)).To(Succeed())
			c.Status.StepStatuses = c.Status.StepStatuses[1:]
			c.Status.StepStatuses[0].Output = "The PR is in the merge queue"
			c.Status.StepStatuses[0].Name = "merge"
			Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
			tick()
			Expect(gh.labels(repo, 301)).To(ContainElement("needs-human"))
		})
	})

	// The parent of a split run closes once every sub-issue is closed (#319).
	Context("split parent", func() {
		closeSub := func(n int, reason string) {
			gh.mu.Lock()
			defer gh.mu.Unlock()
			gh.find(repo, n).State = "closed"
			gh.find(repo, n).StateReason = reason
		}
		parentState := func() string {
			gh.mu.Lock()
			defer gh.mu.Unlock()
			return gh.find(repo, 319).State
		}
		start := func() string {
			gh.add(repo, 319, "Big", "body", "rt:ready")
			gh.add(repo, 320, "Part one", "Part of #319")
			gh.add(repo, 321, "Part two", "Part of #319")
			gh.setPR(github.PullRequest{State: "open", AutoMerge: &struct{}{}})
			reconcileOnce()
			chains := pickupChains()
			Expect(chains).To(HaveLen(1))
			c := &chains[0]
			c.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
			c.Status.LastRunTrigger = c.Annotations[aiv1alpha1.AnnotationChainRun]
			c.Status.CompletedAt = &metav1.Time{Time: clock}
			c.Status.StepStatuses = []aiv1alpha1.ChainStepStatus{
				{Name: "plan", Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: "```json\n{\"coder\":\"x\",\"split\":true,\"issues\":[{\"number\":320,\"title\":\"a\"},{\"number\":321,\"title\":\"b\"}]}\n```"},
				{Name: "review", Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: "PRs: https://github.com/acme/widgets/pull/7\nVerdict: ship it"},
				{Name: "merge", Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: "queued"},
			}
			Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
			return c.Name
		}

		It("closes the parent only after both sub-issues are closed", func() {
			name := start()
			tick() // verifies the merge step
			Expect(parentState()).NotTo(Equal("closed"))
			tick()
			Expect(parentState()).NotTo(Equal("closed"))

			closeSub(320, "completed")
			tick()
			Expect(parentState()).NotTo(Equal("closed"))
			Expect(gh.labels(repo, 319)).To(ContainElement("rt:in-progress"))

			closeSub(321, "completed")
			tick()
			Expect(parentState()).To(Equal("closed"))
			Expect(gh.labels(repo, 319)).NotTo(ContainElement("rt:in-progress"))
			comments := gh.commentsOn(repo, 319)
			Expect(comments[len(comments)-1]).To(ContainSubstring("acme/widgets#7"))

			By("settling once per run")
			n := len(comments)
			tick()
			Expect(gh.commentsOn(repo, 319)).To(HaveLen(n))
			c := &aiv1alpha1.Chain{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, c)).To(Succeed())
			Expect(c.Annotations[aiv1alpha1.AnnotationPickupParentDone]).To(Equal(c.Status.LastRunTrigger))
		})

		It("labels the parent needs-human when a sub-issue needs a human", func() {
			start()
			closeSub(320, "completed")
			gh.setLabels(321, "needs-human")
			tick()
			tick()
			Expect(parentState()).NotTo(Equal("closed"))
			Expect(gh.labels(repo, 319)).To(ConsistOf("needs-human"))
			comments := gh.commentsOn(repo, 319)
			Expect(comments[len(comments)-1]).To(ContainSubstring("#321"))
		})

		It("does not close the parent when a sub-issue was closed as not planned", func() {
			start()
			closeSub(320, "completed")
			closeSub(321, "not_planned")
			tick()
			tick()
			Expect(parentState()).NotTo(Equal("closed"))
			Expect(gh.labels(repo, 319)).To(ConsistOf("needs-human"))
		})
	})

	// A failed run hands its issue back (#314): needs-human, not a stale
	// in-progress label.
	Context("failed chain", func() {
		fail := func() string {
			gh.add(repo, 314, "Fix", "body", "rt:ready")
			reconcileOnce()
			chains := pickupChains()
			Expect(chains).To(HaveLen(1))
			c := &chains[0]
			c.Status.Phase = aiv1alpha1.ChainPhaseFailed
			c.Status.LastRunTrigger = c.Annotations[aiv1alpha1.AnnotationChainRun]
			c.Status.CompletedAt = &metav1.Time{Time: clock}
			c.Status.StepStatuses = []aiv1alpha1.ChainStepStatus{
				{Name: "plan", Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "output has no parseable JSON block after one retry: invalid character '`'"},
				{Name: "implement", Phase: aiv1alpha1.ChainStepPhaseSkipped, Reason: "ChainFailed"},
			}
			Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
			return c.Name
		}
		handled := func(name string) bool {
			c := &aiv1alpha1.Chain{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, c)).To(Succeed())
			return c.Annotations[aiv1alpha1.AnnotationPickupVerified] == c.Status.LastRunTrigger
		}

		It("labels the issue needs-human, removes in-progress and says which step failed", func() {
			name := fail()
			Expect(gh.labels(repo, 314)).To(ConsistOf("rt:in-progress"))
			tick()
			Expect(gh.labels(repo, 314)).To(ConsistOf("needs-human"))
			comments := gh.commentsOn(repo, 314)
			Expect(comments).To(HaveLen(2)) // claim + hand-back
			Expect(comments[1]).To(ContainSubstring("Chain `" + name + "`"))
			Expect(comments[1]).To(ContainSubstring("`plan`: output has no parseable JSON block"))
			Expect(comments[1]).NotTo(ContainSubstring("`implement`"))
			Expect(handled(name)).To(BeTrue())

			By("handing back once per run")
			tick()
			Expect(gh.commentsOn(repo, 314)).To(HaveLen(2))
			Expect(pickupChains()).To(HaveLen(1))
		})

		It("leaves an issue a human already took over alone", func() {
			name := fail()
			gh.mu.Lock()
			gh.find(repo, 314).Labels = []github.Label{{Name: "wontfix"}}
			gh.mu.Unlock()
			tick()
			Expect(gh.labels(repo, 314)).To(ConsistOf("wontfix"))
			Expect(gh.commentsOn(repo, 314)).To(HaveLen(1))
			Expect(handled(name)).To(BeTrue())
		})

		It("leaves a closed issue alone", func() {
			name := fail()
			gh.mu.Lock()
			gh.find(repo, 314).State = "closed"
			gh.mu.Unlock()
			tick()
			Expect(gh.labels(repo, 314)).To(ConsistOf("rt:in-progress"))
			Expect(gh.commentsOn(repo, 314)).To(HaveLen(1))
			Expect(handled(name)).To(BeTrue())
		})

		// #591: a merge step that timed out with its PR queued is done.
		mergeTimeout := func(n int) string {
			gh.add(repo, n, "Fix", "body", "rt:ready")
			reconcileOnce()
			chains := pickupChains()
			Expect(chains).To(HaveLen(1))
			c := &chains[0]
			c.Status.Phase = aiv1alpha1.ChainPhaseFailed
			c.Status.RunsFailed = 1
			c.Status.LastRunTrigger = c.Annotations[aiv1alpha1.AnnotationChainRun]
			c.Status.CompletedAt = &metav1.Time{Time: clock}
			c.Status.StepStatuses = []aiv1alpha1.ChainStepStatus{
				{Name: "review", Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: "PRs: https://github.com/acme/widgets/pull/7\nVerdict: ship it"},
				{Name: "merge", Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "step timed out after 3600s", TaskID: "merge-task"},
			}
			Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
			return c.Name
		}
		getChain := func(name string) *aiv1alpha1.Chain {
			c := &aiv1alpha1.Chain{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, c)).To(Succeed())
			return c
		}

		It("records a merge step that timed out with its PR queued as succeeded, then verifies it", func() {
			gh.setPR(github.PullRequest{State: "open", AutoMerge: &struct{}{}})
			gh.reviews = map[string][]string{"acme/widgets#7": {"Verdict: ship it"}}
			name := mergeTimeout(591)
			tick()
			c := getChain(name)
			Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseSucceeded))
			Expect(c.Status.RunsFailed).To(BeZero())
			Expect(c.Status.StepStatuses[1].Phase).To(Equal(aiv1alpha1.ChainStepPhaseSucceeded))
			Expect(c.Status.StepStatuses[1].Reason).To(Equal(stepReasonQueuedAtTimeout))
			Expect(c.Status.StepStatuses[1].Output).To(ContainSubstring("https://github.com/acme/widgets/pull/7 queued"))
			Expect(gh.labels(repo, 591)).To(ConsistOf("rt:in-progress"))
			Expect(gh.commentsOn(repo, 591)).To(HaveLen(1)) // the claim only

			By("verifying the recovered run like any succeeded one")
			tick()
			Expect(handled(name)).To(BeTrue())
			Expect(gh.autoMerged).To(BeEmpty())
			Expect(gh.labels(repo, 591)).NotTo(ContainElement("needs-human"))
		})

		It("hands back a merge step that timed out with its PR not queued", func() {
			gh.setPR(github.PullRequest{State: "open"})
			name := mergeTimeout(592)
			tick()
			c := getChain(name)
			Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseFailed))
			Expect(c.Status.StepStatuses[1].Phase).To(Equal(aiv1alpha1.ChainStepPhaseFailed))
			Expect(gh.labels(repo, 592)).To(ConsistOf("needs-human"))
			Expect(gh.commentsOn(repo, 592)[1]).To(ContainSubstring("`merge`: step timed out"))
			Expect(handled(name)).To(BeTrue())
		})

		// #777: a run that failed on a timeout while its PR is open is not
		// handed to a human.
		failWith := func(n int, stepErr string) string {
			gh.add(repo, n, "Fix", "body", "rt:ready")
			reconcileOnce()
			chains := pickupChains()
			Expect(chains).To(HaveLen(1))
			c := &chains[0]
			c.Status.Phase = aiv1alpha1.ChainPhaseFailed
			c.Status.LastRunTrigger = c.Annotations[aiv1alpha1.AnnotationChainRun]
			c.Status.CompletedAt = &metav1.Time{Time: clock}
			c.Status.StepStatuses = []aiv1alpha1.ChainStepStatus{
				{Name: "implement", Phase: aiv1alpha1.ChainStepPhaseFailed, Error: stepErr},
			}
			Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
			return c.Name
		}
		openPR := func(n int, draft bool, labels ...string) {
			gh.addPR(repo, 70+n, fmt.Sprintf("rt/%d-fix", n), labels...)
			gh.mu.Lock()
			defer gh.mu.Unlock()
			p := &gh.pulls[repo][len(gh.pulls[repo])-1]
			p.HTMLURL = fmt.Sprintf("https://github.com/%s/pull/%d", repo, p.Number)
			p.Draft = draft
		}

		It("links the open PR of a timed-out run instead of labelling needs-human", func() {
			name := failWith(777, "step timed out after 3600s")
			openPR(777, false)
			tick()
			Expect(gh.labels(repo, 777)).To(BeEmpty())
			comments := gh.commentsOn(repo, 777)
			Expect(comments).To(HaveLen(2))
			Expect(comments[1]).To(ContainSubstring("https://github.com/" + repo + "/pull/847"))
			Expect(comments[1]).To(ContainSubstring("`implement`: step timed out"))
			Expect(handled(name)).To(BeTrue())
			events := r.Recorder.(*record.FakeRecorder).Events
			var got []string
			for len(events) > 0 {
				got = append(got, <-events)
			}
			Expect(got).To(ContainElement(ContainSubstring("FailedRunHasOpenPR")))
			Expect(slices.DeleteFunc(got, func(e string) bool { return !strings.Contains(e, "FailedRunHasOpenPR") })).To(HaveLen(2)) // RoundTable and Chain
		})

		It("matches the PR by a Closes #N body when the branch differs", func() {
			name := failWith(778, "step timed out after 3600s")
			gh.addPR(repo, 90, "feature/other")
			gh.mu.Lock()
			gh.pulls[repo][0].Body = "Fixes things.\n\nCloses #778"
			gh.mu.Unlock()
			tick()
			Expect(gh.labels(repo, 778)).To(BeEmpty())
			Expect(gh.commentsOn(repo, 778)).To(HaveLen(2))
			Expect(handled(name)).To(BeTrue())
		})

		It("labels needs-human when the rt/<issue>- PR is in another repo", func() {
			failWith(784, "step timed out after 3600s")
			gh.addPR("acme/other", 91, "rt/784-fix")
			tick()
			Expect(gh.labels(repo, 784)).To(ConsistOf("needs-human"))
		})

		It("labels needs-human when the PR of a timed-out run is a draft", func() {
			name := failWith(779, "step timed out after 3600s")
			openPR(779, true)
			tick()
			Expect(gh.labels(repo, 779)).To(ConsistOf("needs-human"))
			Expect(handled(name)).To(BeTrue())
		})

		It("labels needs-human when the PR of a timed-out run carries needs-human", func() {
			failWith(783, "step timed out after 3600s")
			openPR(783, false, "needs-human")
			tick()
			Expect(gh.labels(repo, 783)).To(ConsistOf("needs-human"))
		})

		It("labels needs-human when a run failed without a timeout although its PR is open", func() {
			failWith(780, "knight returned garbage")
			openPR(780, false)
			tick()
			Expect(gh.labels(repo, 780)).To(ConsistOf("needs-human"))
		})

		It("labels needs-human when a timed-out run has no PR", func() {
			failWith(781, "step timed out after 3600s")
			gh.addPR(repo, 99, "rt/782-other")
			tick()
			Expect(gh.labels(repo, 781)).To(ConsistOf("needs-human"))
		})

		It("retries on the next poll when GitHub fails, and keeps the chain until handled", func() {
			name := fail()
			rt := getRT()
			rt.Spec.IssuePickup.TTLSecondsAfterFinished = new(int32(1))
			Expect(k8sClient.Update(ctx, rt)).To(Succeed())
			gh.failComments = 1
			tick()
			Expect(getRT().Status.IssuePickup.LastError).To(ContainSubstring("hand back failed chain"))
			Expect(handled(name)).To(BeFalse())
			Expect(pickupChains()).To(HaveLen(1)) // not garbage-collected yet
			tick()
			Expect(gh.labels(repo, 314)).To(ConsistOf("needs-human"))
			Expect(handled(name)).To(BeTrue())
		})
	})

	// postRun: chain (#538): the chain's steps verify, hand back and close a
	// split parent; the operator only releases the issue.
	Context("postRun: chain", func() {
		BeforeEach(func() {
			rt := getRT()
			rt.Spec.IssuePickup.PostRun = aiv1alpha1.IssuePickupPostRunChain
			Expect(k8sClient.Update(ctx, rt)).To(Succeed())
		})
		// run picks up issue n and finishes its chain with phase and steps.
		run := func(n int, phase aiv1alpha1.ChainPhase, steps ...aiv1alpha1.ChainStepStatus) string {
			gh.add(repo, n, "Fix", "body", "rt:ready")
			reconcileOnce()
			chains := pickupChains()
			Expect(chains).To(HaveLen(1))
			c := &chains[0]
			c.Status.Phase = phase
			c.Status.LastRunTrigger = c.Annotations[aiv1alpha1.AnnotationChainRun]
			c.Status.CompletedAt = &metav1.Time{Time: clock}
			c.Status.StepStatuses = steps
			Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
			return c.Name
		}
		handled := func(name string) bool {
			c := &aiv1alpha1.Chain{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, c)).To(Succeed())
			return c.Annotations[aiv1alpha1.AnnotationPickupVerified] == c.Status.LastRunTrigger
		}
		ok := func(name, out string) aiv1alpha1.ChainStepStatus {
			return aiv1alpha1.ChainStepStatus{Name: name, Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: out}
		}

		It("does not verify the merge step: no review, no queueing, no needs-human", func() {
			// builtin would post the review and label the issue for the closed PR.
			gh.setPR(github.PullRequest{State: "closed"})
			name := run(301, aiv1alpha1.ChainPhaseSucceeded,
				ok("review", "PRs: https://github.com/acme/widgets/pull/7\nVerdict: ship it"),
				ok("merge", "queued"), ok("verify", "ok"))
			tick()
			tick()
			Expect(gh.reviews).To(BeEmpty())
			Expect(gh.autoMerged).To(BeEmpty())
			Expect(gh.labels(repo, 301)).To(ConsistOf("rt:in-progress"))
			Expect(gh.commentsOn(repo, 301)).To(HaveLen(1)) // the claim only
			Expect(handled(name)).To(BeTrue())
		})

		It("does not close a split parent", func() {
			gh.add(repo, 320, "Part one", "Part of #319")
			gh.mu.Lock()
			gh.find(repo, 320).State = "closed"
			gh.find(repo, 320).StateReason = "completed"
			gh.mu.Unlock()
			name := run(319, aiv1alpha1.ChainPhaseSucceeded,
				ok("plan", "```json\n{\"coder\":\"x\",\"split\":true,\"issues\":[{\"number\":320,\"title\":\"a\"}]}\n```"),
				ok("merge", "queued"))
			tick()
			tick()
			gh.mu.Lock()
			state := gh.find(repo, 319).State
			gh.mu.Unlock()
			Expect(state).NotTo(Equal("closed"))
			Expect(gh.labels(repo, 319)).To(ConsistOf("rt:in-progress"))
			c := &aiv1alpha1.Chain{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, c)).To(Succeed())
			Expect(c.Annotations).NotTo(HaveKey(aiv1alpha1.AnnotationPickupParentDone))
		})

		It("frees the issue when the final verify step failed", func() {
			name := run(538, aiv1alpha1.ChainPhaseFailed,
				ok("merge", "queued"),
				aiv1alpha1.ChainStepStatus{Name: "verify", Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "pr-verify: not found"},
				aiv1alpha1.ChainStepStatus{Name: "handback", Phase: aiv1alpha1.ChainStepPhaseSkipped, Reason: "ChainFailed"})
			Expect(pickupChains()[0].Status.Phase).To(Equal(aiv1alpha1.ChainPhaseFailed))
			tick()
			Expect(gh.labels(repo, 538)).To(ConsistOf("needs-human"))
			comments := gh.commentsOn(repo, 538)
			Expect(comments).To(HaveLen(2))
			Expect(comments[1]).To(ContainSubstring("`verify`: pr-verify: not found"))
			Expect(handled(name)).To(BeTrue())

			By("releasing once per run")
			tick()
			Expect(gh.commentsOn(repo, 538)).To(HaveLen(2))
		})

		It("records a merge timeout the verify step passed as succeeded and leaves the issue to the queue (#591)", func() {
			gh.setPR(github.PullRequest{State: "closed", Merged: true})
			name := run(593, aiv1alpha1.ChainPhasePartiallySucceeded,
				ok("review", "PRs: https://github.com/acme/widgets/pull/7\nVerdict: ship it"),
				aiv1alpha1.ChainStepStatus{Name: "merge", Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "step timed out after 3600s"},
				ok("verify", "rt-verify: ok"),
				aiv1alpha1.ChainStepStatus{Name: "handback", Phase: aiv1alpha1.ChainStepPhaseSkipped, Reason: stepReasonWhenFalse})
			tick()
			c := &aiv1alpha1.Chain{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, c)).To(Succeed())
			Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseSucceeded))
			Expect(c.Status.StepStatuses[1].Reason).To(Equal(stepReasonQueuedAtTimeout))
			Expect(c.Status.StepStatuses[1].Output).To(ContainSubstring("https://github.com/acme/widgets/pull/7 merged"))
			Expect(gh.labels(repo, 593)).To(ConsistOf("rt:in-progress"))
			Expect(gh.commentsOn(repo, 593)).To(HaveLen(1))
			Expect(handled(name)).To(BeTrue())
		})

		It("only removes in-progress when a step already handed the issue back", func() {
			name := run(540, aiv1alpha1.ChainPhasePartiallySucceeded,
				aiv1alpha1.ChainStepStatus{Name: "review", Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "timeout"},
				ok("handback", "labelled"))
			gh.mu.Lock()
			gh.find(repo, 540).Labels = append(gh.find(repo, 540).Labels, github.Label{Name: "needs-human"})
			gh.mu.Unlock()
			tick()
			Expect(gh.labels(repo, 540)).To(ConsistOf("needs-human"))
			Expect(gh.commentsOn(repo, 540)).To(HaveLen(1))
			Expect(handled(name)).To(BeTrue())
		})

		It("leaves an issue the handback step already released alone, then garbage-collects the chain", func() {
			rt := getRT()
			rt.Spec.IssuePickup.TTLSecondsAfterFinished = new(int32(1))
			Expect(k8sClient.Update(ctx, rt)).To(Succeed())
			name := run(541, aiv1alpha1.ChainPhasePartiallySucceeded,
				aiv1alpha1.ChainStepStatus{Name: "review", Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "timeout"},
				ok("handback", "labelled"))
			gh.setLabels(541, "needs-human")
			tick()
			Expect(gh.labels(repo, 541)).To(ConsistOf("needs-human"))
			Expect(gh.commentsOn(repo, 541)).To(HaveLen(1))
			Expect(handled(name)).To(BeTrue())
			tick()
			Expect(pickupChains()).To(BeEmpty())
		})
	})

	It("rejects a readyLabel equal to inProgressLabel", func() {
		rt := getRT()
		rt.Spec.IssuePickup.InProgressLabel = "rt:ready"
		err := k8sClient.Update(ctx, rt)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("readyLabel must differ"))
	})
})

var _ = Describe("IssuePickup helpers", func() {
	It("derives DNS-safe chain names", func() {
		Expect(pickupChainName("pr-pipeline", "roundtable-k8s/roundtable", 224)).To(Equal("pr-pipeline-roundtable-224"))
		Expect(pickupChainName("pr-pipeline", "Acme/My_Repo.go", 1)).To(Equal("pr-pipeline-my-repo-go-1"))
		long := pickupChainName(strings.Repeat("a", 250), "o/r", 12345)
		Expect(len(long)).To(BeNumerically("<=", 253))
		Expect(long).To(HaveSuffix("-12345"))
	})

	It("flags issues that need a human", func() {
		pats, err := compileHumanPatterns([]string{`\.github/workflows`, `(?i)\bsecrets?\b`, `(?i)\b(auth|oauth|authn|authz|authentication|authori[sz]ation|credentials?)\b`})
		Expect(err).NotTo(HaveOccurred())
		Expect(needsHuman(&github.Issue{Title: "Rotate the Secret"}, "needs-human", pats)).To(ContainSubstring("Secret"))
		Expect(needsHuman(&github.Issue{Body: "fix OAuth login"}, "needs-human", pats)).To(ContainSubstring("OAuth"))
		Expect(needsHuman(&github.Issue{Title: "author field in docs"}, "needs-human", pats)).To(BeEmpty())
		Expect(needsHuman(&github.Issue{Labels: []github.Label{{Name: "Needs-Human"}}}, "needs-human", pats)).To(ContainSubstring("labelled"))
		_, err = compileHumanPatterns([]string{"("})
		Expect(err).To(HaveOccurred())
	})

	It("treats a pending run trigger as active", func() {
		c := &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{aiv1alpha1.AnnotationChainRun: "2"}}}
		c.Status.LastRunTrigger = "1"
		c.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
		Expect(pickupChainActive(c)).To(BeTrue())
		c.Status.LastRunTrigger = "2"
		Expect(pickupChainActive(c)).To(BeFalse())
		c.Status.Phase = aiv1alpha1.ChainPhaseRunning
		Expect(pickupChainActive(c)).To(BeTrue())
	})
})
