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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/roundtable-k8s/roundtable/internal/github"
)

const testRepo = "acme/widgets"

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler())
	t.Cleanup(srv.Close)
	return srv
}

// seed opens an issue through the control route and returns its number.
func seed(t *testing.T, srv *httptest.Server, labels ...string) int {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"repo": testRepo, "title": "fix the thing", "body": "details", "labels": labels})
	resp, err := http.Post(srv.URL+"/_stub/issues", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /_stub/issues: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("seed status = %d", resp.StatusCode)
	}
	var iss struct {
		Number int `json:"number"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&iss); err != nil {
		t.Fatalf("decode seed: %v", err)
	}
	return iss.Number
}

type repoView struct {
	Labels []string `json:"labels"`
	Issues []struct {
		Number   int      `json:"number"`
		State    string   `json:"state"`
		Labels   []string `json:"labels"`
		Comments []string `json:"comments"`
	} `json:"issues"`
}

func readState(t *testing.T, srv *httptest.Server) repoView {
	t.Helper()
	resp, err := http.Get(srv.URL + "/_stub/state?repo=" + testRepo)
	if err != nil {
		t.Fatalf("GET /_stub/state: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("state status = %d", resp.StatusCode)
	}
	var v repoView
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	return v
}

// TestClientClaimFlow drives the operator's own internal/github client against
// the stub: the same calls the issue pickup controller makes to claim an issue.
func TestClientClaimFlow(t *testing.T) {
	srv := newTestServer(t)
	num := seed(t, srv, "ready")
	ctx := context.Background()
	c := github.NewClient(srv.URL, "test-token")

	issues, err := c.ListOpenIssues(ctx, testRepo, "ready")
	if err != nil {
		t.Fatalf("ListOpenIssues: %v", err)
	}
	if len(issues) != 1 || issues[0].Number != num || issues[0].Title != "fix the thing" {
		t.Fatalf("ListOpenIssues = %+v, want issue %d", issues, num)
	}
	if none, err := c.ListOpenIssues(ctx, testRepo, "other"); err != nil || len(none) != 0 {
		t.Fatalf("ListOpenIssues(other) = %+v, %v; want empty", none, err)
	}

	got, err := c.GetIssue(ctx, testRepo, num)
	if err != nil || got.Number != num || !got.HasLabel("ready") {
		t.Fatalf("GetIssue = %+v, %v", got, err)
	}

	if err := c.AddLabels(ctx, testRepo, num, "in-progress"); err != nil {
		t.Fatalf("AddLabels: %v", err)
	}
	if err := c.RemoveLabel(ctx, testRepo, num, "ready"); err != nil {
		t.Fatalf("RemoveLabel: %v", err)
	}
	// A label the issue does not carry is not an error for the client.
	if err := c.RemoveLabel(ctx, testRepo, num, "ready"); err != nil {
		t.Fatalf("RemoveLabel (absent): %v", err)
	}
	if err := c.Comment(ctx, testRepo, num, "claimed by the operator"); err != nil {
		t.Fatalf("Comment: %v", err)
	}

	if after, err := c.ListOpenIssues(ctx, testRepo, "ready"); err != nil || len(after) != 0 {
		t.Fatalf("ListOpenIssues(ready) after claim = %+v, %v; want empty", after, err)
	}

	v := readState(t, srv)
	if len(v.Issues) != 1 {
		t.Fatalf("state issues = %+v", v.Issues)
	}
	is := v.Issues[0]
	if !reflect.DeepEqual(is.Labels, []string{"in-progress"}) {
		t.Errorf("labels = %v, want [in-progress]", is.Labels)
	}
	if !reflect.DeepEqual(is.Comments, []string{"claimed by the operator"}) {
		t.Errorf("comments = %v", is.Comments)
	}
}

func TestClientEnsureLabel(t *testing.T) {
	srv := newTestServer(t)
	seed(t, srv)
	c := github.NewClient(srv.URL, "test-token")
	ctx := context.Background()

	created, err := c.EnsureLabel(ctx, testRepo, "ready", "0e8a16", "ready for the operator")
	if err != nil || !created {
		t.Fatalf("EnsureLabel (new) = %v, %v; want true, nil", created, err)
	}
	created, err = c.EnsureLabel(ctx, testRepo, "ready", "ffffff", "changed")
	if err != nil || created {
		t.Fatalf("EnsureLabel (existing) = %v, %v; want false, nil", created, err)
	}
	if v := readState(t, srv); !reflect.DeepEqual(v.Labels, []string{"ready"}) {
		t.Errorf("repo labels = %v, want [ready]", v.Labels)
	}
}

func TestClientRejectsMissingToken(t *testing.T) {
	srv := newTestServer(t)
	seed(t, srv, "ready")
	c := github.NewClient(srv.URL, "")
	_, err := c.ListOpenIssues(context.Background(), testRepo, "ready")
	if err == nil {
		t.Fatal("ListOpenIssues without a token succeeded; want 401")
	}
	if github.IsNotFound(err) {
		t.Errorf("401 reported as not found: %v", err)
	}
}

func TestRawRoutesAndStatuses(t *testing.T) {
	srv := newTestServer(t)
	num := seed(t, srv, "ready")

	tests := []struct {
		name   string
		method string
		path   string
		auth   bool
		body   string
		want   int
	}{
		{"no auth on repos route", http.MethodGet, "/repos/acme/widgets/issues", false, "", http.StatusUnauthorized},
		{"unknown route", http.MethodGet, "/repos/acme/widgets/pulls", true, "", http.StatusNotFound},
		{"unknown repo", http.MethodGet, "/repos/acme/nope/issues", true, "", http.StatusNotFound},
		{"bad issue number", http.MethodGet, "/repos/acme/widgets/issues/abc", true, "", http.StatusNotFound},
		{"comment without body", http.MethodPost, "/repos/acme/widgets/issues/" + strconv.Itoa(num) + "/comments", true, `{}`, http.StatusUnprocessableEntity},
		{"duplicate label create", http.MethodPost, "/repos/acme/widgets/labels", true, `{"name":"ready"}`, http.StatusUnprocessableEntity},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, srv.URL+tt.path, strings.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			if tt.auth {
				req.Header.Set("Authorization", "token test-token")
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestUnknownRouteAnswersJSON(t *testing.T) {
	srv := newTestServer(t)
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/repos/acme/widgets/pulls", nil)
	req.Header.Set("Authorization", "token test-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("content type = %q", ct)
	}
	var body map[string]string
	if err := json.Unmarshal(b, &body); err != nil || body["message"] != "Not Found" {
		t.Errorf("body = %s, want JSON message", b)
	}
}

func TestSeedValidatesRepo(t *testing.T) {
	srv := newTestServer(t)
	for _, repo := range []string{"", "acme", "acme/widgets/extra", "/widgets"} {
		body, _ := json.Marshal(map[string]string{"repo": repo, "title": "t"})
		resp, err := http.Post(srv.URL+"/_stub/issues", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("repo %q: status = %d, want 400", repo, resp.StatusCode)
		}
	}
}

