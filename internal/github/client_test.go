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

package github

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	gh "github.com/google/go-github/v93/github"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/roundtable-k8s/roundtable/pkg/metrics"
)

// pagedServer serves total items over pages of size per, wrapping each page
// with wrap and advertising the next page in a Link header. It records the
// requested page numbers.
func pagedServer(t *testing.T, total int, wrap func(items []map[string]any) any, pages *[]string) *httptest.Server {
	t.Helper()
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := 1
		if v := r.URL.Query().Get("page"); v != "" {
			p, _ = strconv.Atoi(v)
		}
		*pages = append(*pages, strconv.Itoa(p))
		start, end := (p-1)*100, min(p*100, total)
		items := make([]map[string]any, 0, end-start)
		for i := start; i < end; i++ {
			items = append(items, map[string]any{"id": i + 1, "number": i + 1})
		}
		if end < total {
			u := *r.URL
			q := u.Query()
			q.Set("page", strconv.Itoa(p+1))
			w.Header().Set("Link", `<`+srv.URL+u.Path+"?"+q.Encode()+`>; rel="next"`)
		}
		_ = json.NewEncoder(w).Encode(wrap(items))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func bare(items []map[string]any) any { return items }

func TestListOpenIssuesFiltersPullRequests(t *testing.T) {
	var gotPath, gotQuery, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery, gotAuth = r.URL.Path, r.URL.RawQuery, r.Header.Get("Authorization")
		_, _ = io.WriteString(w, `[{"number":1,"title":"a","labels":[{"name":"rt:ready"}]},
			{"number":2,"title":"pr","pull_request":{}}]`)
	}))
	defer srv.Close()

	issues, err := NewClient(srv.URL, "tok").ListOpenIssues(context.Background(), "acme/widgets", "rt:ready")
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 1 || issues[0].Number != 1 || !issues[0].HasLabel("RT:Ready") {
		t.Fatalf("issues = %+v", issues)
	}
	if gotPath != "/repos/acme/widgets/issues" || gotAuth != "Bearer tok" {
		t.Fatalf("path %q auth %q", gotPath, gotAuth)
	}
	if want := "direction=asc&labels=rt%3Aready&per_page=100&sort=created&state=open"; gotQuery != want {
		t.Fatalf("query %q, want %q", gotQuery, want)
	}
}

func TestRemoveLabelIgnoresMissing(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"message":"Label does not exist"}`)
	}))
	defer srv.Close()

	if err := NewClient(srv.URL, "").RemoveLabel(context.Background(), "acme/widgets", 3, "rt:ready"); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/repos/acme/widgets/issues/3/labels/rt:ready" {
		t.Fatalf("path %q", gotPath)
	}
}

func TestErrorResponseSurfacesStatusAndMessage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"message":"Resource not accessible by integration"}`)
	}))
	defer srv.Close()

	err := NewClient(srv.URL, "t").Comment(context.Background(), "acme/widgets", 3, "hi")
	var apiErr *gh.ErrorResponse
	if !errors.As(err, &apiErr) || apiErr.Response.StatusCode != http.StatusForbidden || apiErr.Message != "Resource not accessible by integration" {
		t.Fatalf("err = %v", err)
	}
	if _, err := NewClient(srv.URL, "t").ListOpenIssues(context.Background(), "no-slash", "x"); err == nil {
		t.Fatal("want error for bad repo")
	}
}

func TestListOpenPullRequestsFiltersByLabel(t *testing.T) {
	var gotPath, gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery = r.URL.Path, r.URL.RawQuery
		_, _ = io.WriteString(w, `[{"number":1,"labels":[{"name":"Breaking"}],"head":{"ref":"rt/1-x"}},
			{"number":2,"labels":[{"name":"docs"}],"head":{"ref":"rt/2-y"}}]`)
	}))
	defer srv.Close()

	prs, err := NewClient(srv.URL, "tok").ListOpenPullRequests(context.Background(), "acme/widgets", "breaking")
	if err != nil {
		t.Fatal(err)
	}
	if len(prs) != 1 || prs[0].Number != 1 || prs[0].Head.Ref != "rt/1-x" {
		t.Fatalf("prs = %+v", prs)
	}
	if gotPath != "/repos/acme/widgets/pulls" || gotQuery != "per_page=100&state=open" {
		t.Fatalf("path %q query %q", gotPath, gotQuery)
	}
}

func TestListOpenPullsFollowsPages(t *testing.T) {
	var pages []string
	srv := pagedServer(t, 103, bare, &pages)
	pulls, err := NewClient(srv.URL, "tok").ListOpenPulls(context.Background(), "o/r")
	if err != nil {
		t.Fatal(err)
	}
	if len(pulls) != 103 || len(pages) != 2 {
		t.Fatalf("got %d pulls over pages %v", len(pulls), pages)
	}
}

func TestListOpenIssuesFollowsPages(t *testing.T) {
	var pages []string
	srv := pagedServer(t, 230, bare, &pages)
	c := NewClient(srv.URL, "tok")
	issues, err := c.ListOpenIssues(context.Background(), "o/r", "rt:ready")
	if err != nil || len(issues) != 230 || len(pages) != 3 {
		t.Fatalf("got %d issues over pages %v err %v", len(issues), pages, err)
	}
	pages = nil
	issues, err = c.ListOpenIssuesWithLabel(context.Background(), "o/r", "rt:ready")
	if err != nil || len(issues) != 230 || len(pages) != 3 {
		t.Fatalf("got %d issues over pages %v err %v", len(issues), pages, err)
	}
}

func TestListOpenPullRequestsFollowsPages(t *testing.T) {
	var pages []string
	srv := pagedServer(t, 150, func(items []map[string]any) any {
		for _, it := range items {
			it["labels"] = []map[string]any{{"name": "breaking"}}
		}
		return items
	}, &pages)
	prs, err := NewClient(srv.URL, "tok").ListOpenPullRequests(context.Background(), "o/r", "breaking")
	if err != nil || len(prs) != 150 || len(pages) != 2 {
		t.Fatalf("got %d prs over pages %v err %v", len(prs), pages, err)
	}
}

func TestListReviewsFollowsPages(t *testing.T) {
	var pages []string
	srv := pagedServer(t, 120, bare, &pages)
	reviews, err := NewClient(srv.URL, "tok").ListReviews(context.Background(), "o/r", 7)
	if err != nil || len(reviews) != 120 || len(pages) != 2 || reviews[119].ID != 120 {
		t.Fatalf("got %d reviews over pages %v err %v", len(reviews), pages, err)
	}
}

func TestListCheckRunsFollowsPages(t *testing.T) {
	var pages []string
	srv := pagedServer(t, 130, func(items []map[string]any) any {
		return map[string]any{"total_count": 130, "check_runs": items}
	}, &pages)
	runs, err := NewClient(srv.URL, "tok").ListCheckRuns(context.Background(), "o/r", "abc")
	if err != nil || len(runs) != 130 || len(pages) != 2 {
		t.Fatalf("got %d check runs over pages %v err %v", len(runs), pages, err)
	}
}

func TestListRunJobsFollowsPages(t *testing.T) {
	var pages []string
	srv := pagedServer(t, 101, func(items []map[string]any) any {
		return map[string]any{"total_count": 101, "jobs": items}
	}, &pages)
	jobs, err := NewClient(srv.URL, "tok").ListRunJobs(context.Background(), "o/r", 9)
	if err != nil || len(jobs) != 101 || len(pages) != 2 {
		t.Fatalf("got %d jobs over pages %v err %v", len(jobs), pages, err)
	}
}

func TestListMergeGroupRunsFollowsPages(t *testing.T) {
	var pages []string
	srv := pagedServer(t, 103, func(items []map[string]any) any {
		return map[string]any{"total_count": 103, "workflow_runs": items}
	}, &pages)
	runs, err := NewClient(srv.URL, "tok").ListMergeGroupRuns(context.Background(), "o/r")
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 103 || len(pages) != 2 || runs[0].ID != 1 || runs[102].ID != 103 {
		t.Fatalf("got %d runs over pages %v", len(runs), pages)
	}
}

func TestListMergeGroupRunsStopsAtMaxPages(t *testing.T) {
	var pages []string
	srv := pagedServer(t, 100*(maxPages+3), func(items []map[string]any) any {
		return map[string]any{"workflow_runs": items}
	}, &pages)
	counter := metrics.GithubPaginationTruncatedTotal.WithLabelValues("list_merge_group_runs")
	before := testutil.ToFloat64(counter)
	runs, err := NewClient(srv.URL, "tok").ListMergeGroupRuns(context.Background(), "o/r")
	if err != nil {
		t.Fatal(err)
	}
	if len(pages) != maxPages || len(runs) != maxPages*100 {
		t.Fatalf("pages=%v runs=%d", pages, len(runs))
	}
	if got := testutil.ToFloat64(counter) - before; got != 1 {
		t.Fatalf("truncated counter delta = %v, want 1", got)
	}
}

func TestEnableAutoMergeSendsGraphQL(t *testing.T) {
	var path, body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		path, body = r.URL.Path, string(b)
		_, _ = io.WriteString(w, `{"errors":[{"message":"nope"}]}`)
	}))
	defer srv.Close()
	err := NewClient(srv.URL, "tok").EnableAutoMerge(context.Background(), "PR_1")
	if path != "/graphql" || !strings.Contains(body, "enablePullRequestAutoMerge") || !strings.Contains(body, "PR_1") {
		t.Fatalf("path %q body %s", path, body)
	}
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("err = %v", err)
	}
}

func TestLoadMergeQueueState(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = io.WriteString(w, `{"data":{"node":{"isInMergeQueue":false,"mergeQueueEntry":{"id":"E1"}}}}`)
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "tok")
	pr := &PullRequest{State: "open", NodeID: "PR_1"}
	if err := c.LoadMergeQueueState(context.Background(), pr); err != nil {
		t.Fatal(err)
	}
	if !pr.InMergeQueue || !pr.Queued() {
		t.Fatalf("want queued via mergeQueueEntry: %+v", pr)
	}
	for _, skip := range []*PullRequest{
		{State: "open", NodeID: "PR_1", AutoMerge: &struct{}{}}, {State: "closed", NodeID: "PR_1"},
		{State: "open", NodeID: "PR_1", Draft: true}, {State: "open", Merged: true, NodeID: "PR_1"}, {State: "open"},
	} {
		if err := c.LoadMergeQueueState(context.Background(), skip); err != nil || skip.InMergeQueue {
			t.Fatalf("skip %+v: %v", skip, err)
		}
	}
	if calls != 1 {
		t.Fatalf("graphql calls = %d, want 1", calls)
	}
}

func TestCommentKeepsLiteralAngleBrackets(t *testing.T) {
	var body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		body = string(b)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	if err := NewClient(srv.URL, "tok").Comment(context.Background(), "o/r", 1, "<<<A & B>>>"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(body, "<<<A & B>>>") {
		t.Fatalf("body escaped: %s", body)
	}
}

func TestJobLogFollowsRedirect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/logs") {
			http.Redirect(w, r, "http://"+r.Host+"/blob/log.txt", http.StatusFound)
			return
		}
		_, _ = io.WriteString(w, "line 1\n##[error]boom\n")
	}))
	defer srv.Close()
	got, err := NewClient(srv.URL, "tok").JobLog(context.Background(), "o/r", 5)
	if err != nil || got != "line 1\n##[error]boom\n" {
		t.Fatalf("log %q err %v", got, err)
	}
}

func TestJobLogErrorOmitsSignedURL(t *testing.T) {
	var blobURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/logs") {
			http.Redirect(w, r, blobURL, http.StatusFound)
			return
		}
		hj, _ := w.(http.Hijacker)
		conn, _, _ := hj.Hijack()
		_ = conn.Close()
	}))
	defer srv.Close()
	blobURL = "http://" + srv.Listener.Addr().String() + "/blob/log.txt?sig=SECRETSIG&se=2030"
	_, err := NewClient(srv.URL, "tok").JobLog(context.Background(), "o/r", 5)
	if err == nil {
		t.Fatal("want error")
	}
	for _, bad := range []string{"SECRETSIG", "?", "sig=", "/blob/log.txt"} {
		if strings.Contains(err.Error(), bad) {
			t.Fatalf("error leaks %q: %v", bad, err)
		}
	}
}

// createdSince returns the time a created qualifier ">=<time>" names, failing
// the test if the qualifier has another shape.
func createdSince(t *testing.T, query string) time.Time {
	t.Helper()
	raw, ok := strings.CutPrefix(query, ">=")
	if !ok {
		t.Fatalf("created = %q, want a >= qualifier", query)
	}
	at, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("created = %q: %v", query, err)
	}
	return at
}

func TestMergeGroupCreatedFilterIsTimestampGranular(t *testing.T) {
	now := time.Date(2026, 11, 27, 7, 30, 15, 0, time.UTC)
	if got, want := mergeGroupCreatedFilter(now, mergeGroupWindow), ">=2026-11-26T07:30:15Z"; got != want {
		t.Fatalf("filter = %q, want %q", got, want)
	}
	if got, want := mergeGroupCreatedFilter(now, MergeGroupFailureWindow), ">=2026-11-24T07:30:15Z"; got != want {
		t.Fatalf("failure filter = %q, want %q", got, want)
	}
}

func TestListMergeGroupRunsSendsCreatedFilter(t *testing.T) {
	var query string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.Query().Get("created")
		_, _ = io.WriteString(w, `{"workflow_runs":[]}`)
	}))
	defer srv.Close()
	if _, err := NewClient(srv.URL, "tok").ListMergeGroupRuns(context.Background(), "o/r"); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(createdSince(t, query)) - mergeGroupWindow; d < -time.Minute || d > time.Minute {
		t.Fatalf("created = %q, want about %v ago", query, mergeGroupWindow)
	}
}

func TestListMergeGroupRunsWithinUsesWindow(t *testing.T) {
	var query string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.Query().Get("created")
		_, _ = io.WriteString(w, `{"workflow_runs":[]}`)
	}))
	defer srv.Close()
	if _, err := NewClient(srv.URL, "tok").ListMergeGroupRunsWithin(context.Background(), "o/r", MergeGroupFailureWindow); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(createdSince(t, query)) - MergeGroupFailureWindow; d < -time.Minute || d > time.Minute {
		t.Fatalf("created = %q, want about %v ago", query, MergeGroupFailureWindow)
	}
}

func TestIsNotFound(t *testing.T) {
	er := func(code int) error { return &gh.ErrorResponse{Response: &http.Response{StatusCode: code}} }
	if !IsNotFound(er(404)) || !IsNotFound(er(410)) || IsNotFound(er(500)) || IsNotFound(errors.New("x")) {
		t.Fatal("IsNotFound wrong")
	}
}

func TestListIssuesByQuery(t *testing.T) {
	var query string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.RawQuery
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Remaining", "4321")
		w.Header().Set("X-RateLimit-Reset", "1900000000")
		_, _ = io.WriteString(w, `[
			{"number": 1, "title": "an issue", "html_url": "u1", "user": {"login": "alice"}, "labels": [{"name": "ready"}]},
			{"number": 2, "title": "a pull", "pull_request": {"url": "x"}, "user": {"login": "bob"}}
		]`)
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "tok")

	issues, rate, err := c.ListIssuesByQuery(context.Background(), "acme/widgets", IssueQuery{Labels: []string{"ready", "bug"}, State: "all"})
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 1 || issues[0].Number != 1 || issues[0].User.Login != "alice" || !issues[0].HasLabel("ready") {
		t.Fatalf("issues = %+v", issues)
	}
	if !strings.Contains(query, "labels=ready%2Cbug") || !strings.Contains(query, "state=all") {
		t.Errorf("query = %q", query)
	}
	if rate.Limit != 5000 || rate.Remaining != 4321 || rate.Reset.Unix() != 1900000000 {
		t.Errorf("rate = %+v", rate)
	}

	pulls, _, err := c.ListIssuesByQuery(context.Background(), "acme/widgets", IssueQuery{Labels: []string{"ready"}, PullRequests: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(pulls) != 1 || pulls[0].Number != 2 || !strings.Contains(query, "state=open") {
		t.Fatalf("pulls = %+v, query = %q", pulls, query)
	}
}

func TestRateLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset", "1900000000")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"message": "API rate limit exceeded"}`)
	}))
	defer srv.Close()
	_, _, err := NewClient(srv.URL, "tok").ListIssuesByQuery(context.Background(), "acme/widgets", IssueQuery{Labels: []string{"x"}})
	reset, ok := RateLimited(err)
	if !ok || reset.Unix() != 1900000000 {
		t.Fatalf("RateLimited(%v) = %v, %v", err, reset, ok)
	}
	if _, ok := RateLimited(errors.New("boom")); ok {
		t.Error("plain error reported as rate limited")
	}
}
