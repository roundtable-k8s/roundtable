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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient(t *testing.T) {
	var gotAuth, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repos/o/r/pulls":
			if r.URL.Query().Get("state") != "open" {
				t.Errorf("state = %q", r.URL.Query().Get("state"))
			}
			_ = json.NewEncoder(w).Encode([]map[string]any{{
				"number": 5, "html_url": "https://github.com/o/r/pull/5",
				"user": map[string]any{"login": "dunce-bot[bot]"}, "labels": []map[string]any{{"name": "Needs-Human"}},
				"head": map[string]any{"sha": "abc"},
			}})
		case r.URL.Path == "/repos/o/r/commits/abc/check-runs":
			if r.URL.Query().Get("filter") != "all" {
				t.Errorf("filter = %q", r.URL.Query().Get("filter"))
			}
			_, _ = io.WriteString(w, `{"check_runs":[{"id":7,"name":"lint","status":"completed","conclusion":"failure","app":{"slug":"github-actions"}}]}`)
		case r.URL.Path == "/repos/o/r/actions/jobs/7/logs":
			_, _ = io.WriteString(w, "line 1\n##[error]boom\n")
		case r.Method == http.MethodPost && r.URL.Path == "/repos/o/r/issues/5/labels":
			b, _ := io.ReadAll(r.Body)
			gotBody = string(b)
		case r.URL.Path == "/repos/o/r/pulls/9":
			http.Error(w, `{"message":"Not Found"}`, http.StatusNotFound)
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL)
			http.Error(w, "nope", http.StatusTeapot)
		}
	}))
	defer srv.Close()
	ctx := context.Background()
	c := New(srv.URL, "tok")

	pulls, err := c.ListOpenPulls(ctx, "o/r")
	if err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer tok" {
		t.Errorf("Authorization = %q", gotAuth)
	}
	if len(pulls) != 1 || pulls[0].Number != 5 || pulls[0].User.Login != "dunce-bot[bot]" || pulls[0].Head.SHA != "abc" {
		t.Fatalf("pulls = %+v", pulls)
	}
	if !pulls[0].HasLabel("needs-human") {
		t.Error("label match should ignore case")
	}

	runs, err := c.ListCheckRuns(ctx, "o/r", "abc")
	if err != nil || len(runs) != 1 || !runs[0].IsActions() || runs[0].Conclusion != "failure" {
		t.Fatalf("check runs = %+v, %v", runs, err)
	}
	log, err := c.JobLog(ctx, "o/r", 7)
	if err != nil || log != "line 1\n##[error]boom\n" {
		t.Fatalf("log = %q, %v", log, err)
	}
	if err := c.AddLabels(ctx, "o/r", 5, "needs-human"); err != nil {
		t.Fatal(err)
	}
	if gotBody != `{"labels":["needs-human"]}` {
		t.Errorf("labels body = %s", gotBody)
	}
	if _, err := c.GetPull(ctx, "o/r", 9); !IsNotFound(err) {
		t.Errorf("want not found, got %v", err)
	}
}
