package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/github"
)

// TestFailureJobsMergeQueueUsesFailureWindow: the merge-queue branch of
// failureJobs lists merge group runs with github.MergeGroupFailureWindow
// (72 h), not the 24 h window of the listing.
func TestFailureJobsMergeQueueUsesFailureWindow(t *testing.T) {
	var mu sync.Mutex
	var created []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/actions/runs"):
			mu.Lock()
			created = append(created, r.URL.Query().Get("created"))
			mu.Unlock()
			_, _ = io.WriteString(w, `{"workflow_runs":[{"id":11,"head_sha":"abc","conclusion":"failure"},{"id":12,"head_sha":"other","conclusion":"failure"}]}`)
		case strings.HasSuffix(r.URL.Path, "/actions/runs/11/jobs"):
			_, _ = io.WriteString(w, `{"jobs":[{"id":5,"name":"unit","conclusion":"failure"},{"id":6,"name":"lint","conclusion":"success"}]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	rec := &aiv1alpha1.PRFeedbackPullRequest{Repo: "o/r", FailureKind: failureKindMergeQueue, LastFailure: "mq:abc"}
	jobs, err := failureJobs(context.Background(), github.NewClient(srv.URL, "tok"), rec)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != 5 {
		t.Fatalf("jobs = %+v, want only the failed job of run abc", jobs)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(created) == 0 {
		t.Fatal("no merge group run listing")
	}
	raw, ok := strings.CutPrefix(created[0], ">=")
	if !ok {
		t.Fatalf("created = %q, want a >= qualifier", created[0])
	}
	at, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(at) - github.MergeGroupFailureWindow; d < -time.Minute || d > time.Minute {
		t.Fatalf("created = %q, want about %v ago", created[0], github.MergeGroupFailureWindow)
	}
}

// ghRateLimitFake serves one open pull request; limitPull / limitChecks make
// the GetPull / ListCheckRuns call answer with a primary rate limit.
type ghRateLimitFake struct {
	limitPull, limitChecks bool
	mu                     sync.Mutex
	checkCalls             int
	pullCalls              map[int]int
}

func (f *ghRateLimitFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	limit := func() {
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"message":"API rate limit exceeded"}`)
	}
	switch {
	case strings.HasSuffix(r.URL.Path, "/pulls"):
		_ = json.NewEncoder(w).Encode([]github.PullRequest{{Number: 1}, {Number: 2}})
	case strings.Contains(r.URL.Path, "/pulls/"):
		n, _ := strconv.Atoi(r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:])
		if f.pullCalls == nil {
			f.pullCalls = map[int]int{}
		}
		f.pullCalls[n]++
		if f.limitPull {
			limit()
			return
		}
		_, _ = io.WriteString(w, `{"mergeable_state":"clean"}`)
	case strings.Contains(r.URL.Path, "/check-runs"):
		f.checkCalls++
		if f.limitChecks {
			limit()
			return
		}
		_, _ = io.WriteString(w, `{"check_runs":[]}`)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// TestListGitHubPRFailuresAbortsOnRateLimit: a rate limit on GetPull or
// ListCheckRuns aborts the listing with the limit error instead of recording
// a per-PR problem and carrying on.
func TestListGitHubPRFailuresAbortsOnRateLimit(t *testing.T) {
	src := &aiv1alpha1.TriggerGitHubSource{
		Checks: aiv1alpha1.TriggerGitHubChecksFailing,
	}
	for name, f := range map[string]*ghRateLimitFake{
		"GetPull":       {limitPull: true},
		"ListCheckRuns": {limitChecks: true},
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(f)
			defer srv.Close()
			cands, _, problems, err := listGitHubPRFailures(context.Background(), github.NewClient(srv.URL, "tok"), src, "o/r")
			if _, limited := github.RateLimited(err); !limited {
				t.Fatalf("err = %v, want a rate limit error", err)
			}
			if len(cands) != 0 || len(problems) != 0 {
				t.Errorf("candidates %v / problems %v, want none", cands, problems)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.pullCalls[2] != 0 {
				t.Errorf("second pull was read after the limit: %v", f.pullCalls)
			}
			if name == "GetPull" && f.checkCalls != 0 {
				t.Errorf("check runs were read after the GetPull limit")
			}
		})
	}
}

// commentFake keeps issue comments in memory.
type commentFake struct {
	mu       sync.Mutex
	comments map[int][]string
}

func (f *commentFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/") // repos/o/r/issues/N/comments
	if len(parts) != 6 || parts[5] != "comments" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	n, _ := strconv.Atoi(parts[4])
	if r.Method == http.MethodPost {
		var body struct{ Body string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.comments[n] = append(f.comments[n], body.Body)
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{}`)
		return
	}
	out := []map[string]any{}
	for i, b := range f.comments[n] {
		out = append(out, map[string]any{"id": i + 1, "body": b})
	}
	_ = json.NewEncoder(w).Encode(out)
}

// TestCommentOnceEmptyRunChains: two chains with no run trigger on one issue
// each get their comment, and a retry of the same chain does not repeat it.
func TestCommentOnceEmptyRunChains(t *testing.T) {
	f := &commentFake{comments: map[int][]string{}}
	srv := httptest.NewServer(f)
	defer srv.Close()
	gh := github.NewClient(srv.URL, "tok")
	ctx := context.Background()
	a := &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: "plan-a"}}
	b := &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: "plan-b"}}
	for _, c := range []*aiv1alpha1.Chain{a, b, a} {
		if err := commentOnce(ctx, gh, "o/r", 5, runMarkerKey(c), "failed: "+c.Name); err != nil {
			t.Fatal(err)
		}
	}
	got := f.comments[5]
	if len(got) != 2 {
		t.Fatalf("comments = %q, want one per chain", got)
	}
	for i, name := range []string{"plan-a", "plan-b"} {
		if !strings.Contains(got[i], "failed: "+name) || !strings.Contains(got[i], fmt.Sprintf("<!-- rt-run:%s -->", name)) {
			t.Errorf("comment %d = %q", i, got[i])
		}
	}
}

// TestHandBackExhaustedRetriesAnnotationConflict: the hand-back annotation
// write re-reads the run and retries on a 409; a conflict that never clears
// reports that the hand-back was applied but not recorded.
func TestHandBackExhaustedRetriesAnnotationConflict(t *testing.T) {
	s := k8sruntime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	newRun := func() *aiv1alpha1.Chain {
		return &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "default"}}
	}
	trg := &aiv1alpha1.Trigger{
		ObjectMeta: metav1.ObjectMeta{Name: "trg", Namespace: "default"},
		Spec: aiv1alpha1.TriggerSpec{Source: aiv1alpha1.TriggerSource{GitHub: &aiv1alpha1.TriggerGitHubSource{
			// An empty hand-back touches no GitHub state: only the annotation is written.
			OnMaxRuns: &aiv1alpha1.TriggerGitHubHandBack{},
		}}},
	}
	cand := githubCandidate{item: githubItem{Repo: "o/r", Number: 3}}
	build := func(failures int, patches *int) client.Client {
		return fake.NewClientBuilder().WithScheme(s).WithObjects(newRun()).
			WithInterceptorFuncs(interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
					*patches++
					if *patches <= failures {
						return apierrors.NewConflict(schema.GroupResource{Group: "ai.roundtable.io", Resource: "chains"}, obj.GetName(), nil)
					}
					return c.Patch(ctx, obj, p, opts...)
				},
			}).Build()
	}

	t.Run("retries after a conflict", func(t *testing.T) {
		patches := 0
		r := &TriggerReconciler{Client: build(2, &patches)}
		run := newRun()
		if err := r.handBackExhausted(context.Background(), nil, trg, TriggerEvent{}, cand, run, "o/r#3", 3); err != nil {
			t.Fatalf("handBackExhausted = %v, want the conflict retried", err)
		}
		if patches != 3 {
			t.Errorf("patches = %d, want 3 (two conflicts, then success)", patches)
		}
		stored := &aiv1alpha1.Chain{}
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), stored); err != nil {
			t.Fatal(err)
		}
		if got := stored.Annotations[aiv1alpha1.AnnotationTriggerHandedBack]; got != "o/r#3" {
			t.Errorf("stored annotation = %q, want o/r#3", got)
		}
		if run.Annotations[aiv1alpha1.AnnotationTriggerHandedBack] != "o/r#3" {
			t.Errorf("run passed in was not updated: %v", run.Annotations)
		}
	})

	t.Run("a conflict that never clears is reported", func(t *testing.T) {
		patches := 0
		r := &TriggerReconciler{Client: build(1<<30, &patches)}
		run := newRun()
		err := r.handBackExhausted(context.Background(), nil, trg, TriggerEvent{}, cand, run, "o/r#3", 3)
		if err == nil || !strings.Contains(err.Error(), "already applied") {
			t.Fatalf("err = %v, want the hand-back-applied-but-unrecorded error", err)
		}
		if patches < 2 {
			t.Errorf("patches = %d, want the write retried", patches)
		}
		if run.Annotations[aiv1alpha1.AnnotationTriggerHandedBack] != "" {
			t.Errorf("run marked handed back despite the failed write")
		}
	})
}
