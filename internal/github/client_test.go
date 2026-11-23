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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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
