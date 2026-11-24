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
	"strings"
	"testing"
)

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

func TestAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"message":"Resource not accessible by integration"}`)
	}))
	defer srv.Close()

	err := NewClient(srv.URL, "t").Comment(context.Background(), "acme/widgets", 3, "hi")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusForbidden || apiErr.Message != "Resource not accessible by integration" {
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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pages = append(pages, r.URL.Query().Get("page"))
		n := 100
		if r.URL.Query().Get("page") == "2" {
			n = 3
		}
		out := make([]map[string]any, n)
		for i := range out {
			out[i] = map[string]any{"number": i + 1}
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer srv.Close()

	pulls, err := NewClient(srv.URL, "tok").ListOpenPulls(context.Background(), "o/r")
	if err != nil {
		t.Fatal(err)
	}
	if len(pulls) != 103 || len(pages) != 2 {
		t.Fatalf("got %d pulls over pages %v", len(pulls), pages)
	}
}

func TestListMergeGroupRunsFollowsPages(t *testing.T) {
	var pages []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/o/r/actions/runs" || r.URL.Query().Get("event") != "merge_group" {
			t.Errorf("unexpected request %s", r.URL)
		}
		p := r.URL.Query().Get("page")
		pages = append(pages, p)
		n, base := 100, 0
		if p == "2" {
			n, base = 3, 100
		}
		runs := make([]map[string]any, n)
		for i := range runs {
			runs[i] = map[string]any{"id": base + i + 1}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"workflow_runs": runs})
	}))
	defer srv.Close()

	runs, err := NewClient(srv.URL, "tok").ListMergeGroupRuns(context.Background(), "o/r")
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 103 || len(pages) != 2 || runs[0].ID != 1 || runs[102].ID != 103 {
		t.Fatalf("got %d runs over pages %v", len(runs), pages)
	}
}

func TestListMergeGroupRunsStopsAtMaxPages(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		runs := make([]map[string]any, 100)
		for i := range runs {
			runs[i] = map[string]any{"id": i}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"workflow_runs": runs})
	}))
	defer srv.Close()

	runs, err := NewClient(srv.URL, "tok").ListMergeGroupRuns(context.Background(), "o/r")
	if err != nil {
		t.Fatal(err)
	}
	if calls != maxPages || len(runs) != maxPages*100 {
		t.Fatalf("calls=%d runs=%d", calls, len(runs))
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

func TestJobLogReturnsRawText(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "line 1\n##[error]boom\n")
	}))
	defer srv.Close()
	got, err := NewClient(srv.URL, "tok").JobLog(context.Background(), "o/r", 5)
	if err != nil || got != "line 1\n##[error]boom\n" {
		t.Fatalf("log %q err %v", got, err)
	}
}

func TestIsNotFound(t *testing.T) {
	if !IsNotFound(&APIError{Status: 404}) || !IsNotFound(&APIError{Status: 410}) || IsNotFound(&APIError{Status: 500}) || IsNotFound(errors.New("x")) {
		t.Fatal("IsNotFound wrong")
	}
}
