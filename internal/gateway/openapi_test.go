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

package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func openAPIService(t *testing.T, missions, runs, hooks bool, schedules ...bool) *Service {
	t.Helper()
	svc := NewService(&Resolver{}, nil, Options{Namespace: testNS, Version: "9.9.9"}, logr.Discard())
	k8s := fake.NewClientBuilder().Build()
	if missions {
		svc.Missions = k8s
	}
	if runs {
		svc.Runs = k8s
	}
	if hooks {
		svc.Hooks = k8s
	}
	if len(schedules) > 0 && schedules[0] {
		svc.ScheduleReader = k8s
	}
	return svc
}

// documented lists the "METHOD /path" operations of a document.
func documented(t *testing.T, doc map[string]any) []string {
	t.Helper()
	out := make([]string, 0, 32)
	for path, item := range doc["paths"].(map[string]any) {
		for method := range item.(map[string]any) {
			out = append(out, strings.ToUpper(method)+" "+path)
		}
	}
	sort.Strings(out)
	return out
}

// roundTrip marshals the document the way the endpoint serves it.
func roundTrip(t *testing.T, svc *Service) map[string]any {
	t.Helper()
	doc, err := svc.openAPIDocument()
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestOpenAPIRoutesInSync(t *testing.T) {
	svc := openAPIService(t, true, true, true, true)
	routes := svc.routes()
	served := make([]string, 0, len(routes))
	for _, rt := range routes {
		served = append(served, rt.pattern)
		if _, ok := openAPIOperations[rt.pattern]; !ok {
			t.Errorf("route %q is served but missing from the OpenAPI document (add it to openAPIOperations)", rt.pattern)
		}
	}
	for pattern := range openAPIOperations {
		if !slices.Contains(served, pattern) {
			t.Errorf("OpenAPI document describes %q, which is not served", pattern)
		}
	}
	sort.Strings(served)
	if got := documented(t, roundTrip(t, svc)); !slices.Equal(got, served) {
		t.Errorf("document operations = %v, want %v", got, served)
	}
}

func TestOpenAPIFeatureGroups(t *testing.T) {
	has := func(doc map[string]any, prefix string) bool {
		for _, op := range documented(t, doc) {
			if strings.Contains(op, " "+prefix) {
				return true
			}
		}
		return false
	}
	off := roundTrip(t, openAPIService(t, false, false, false))
	for _, p := range []string{"/v1/missions", "/v1/chains", "/v1/runs", "/v1/hooks", "/v1/schedules"} {
		if has(off, p) {
			t.Errorf("%s documented although its feature is disabled", p)
		}
	}
	for _, p := range []string{"/v1/knights", "/v1/tasks", "/v1/dead-letters", "/openapi.json", "/healthz"} {
		if !has(off, p) {
			t.Errorf("%s missing from the document", p)
		}
	}
	on := roundTrip(t, openAPIService(t, true, true, true, true))
	for _, p := range []string{"/v1/missions", "/v1/chains", "/v1/runs", "/v1/hooks", "/v1/schedules"} {
		if !has(on, p) {
			t.Errorf("%s missing although its feature is enabled", p)
		}
	}
	if has(roundTrip(t, openAPIService(t, true, false, false)), "/v1/runs") {
		t.Error("runs documented with only missions enabled")
	}
}

func TestOpenAPIDocumentIsStructurallyValid(t *testing.T) {
	doc := roundTrip(t, openAPIService(t, true, true, true))
	if doc["openapi"] != "3.1.0" {
		t.Errorf("openapi = %v", doc["openapi"])
	}
	info := doc["info"].(map[string]any)
	if info["version"] != "9.9.9" || info["title"] == "" {
		t.Errorf("info = %v", info)
	}
	comps := doc["components"].(map[string]any)
	if _, ok := comps["securitySchemes"].(map[string]any)["bearerAuth"]; !ok {
		t.Error("bearerAuth security scheme missing")
	}

	// Every $ref resolves.
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			if r, ok := x["$ref"].(string); ok {
				parts := strings.Split(strings.TrimPrefix(r, "#/components/"), "/")
				group, _ := comps[parts[0]].(map[string]any)
				if len(parts) != 2 || group[parts[1]] == nil {
					t.Errorf("unresolved $ref %q", r)
				}
			}
			for _, c := range x {
				walk(c)
			}
		case []any:
			for _, c := range x {
				walk(c)
			}
		}
	}
	walk(doc)

	tmpl := regexp.MustCompile(`\{([^}]+)\}`)
	for path, item := range doc["paths"].(map[string]any) {
		for method, o := range item.(map[string]any) {
			op := o.(map[string]any)
			name := strings.ToUpper(method) + " " + path
			if op["summary"] == nil {
				t.Errorf("%s: no summary", name)
			}
			resp, _ := op["responses"].(map[string]any)
			if len(resp) == 0 {
				t.Errorf("%s: no responses", name)
			}
			declared := map[string]bool{}
			params, _ := op["parameters"].([]any)
			for _, p := range params {
				pm := p.(map[string]any)
				if pm["in"] == "path" {
					declared[pm["name"].(string)] = true
				}
			}
			for _, m := range tmpl.FindAllStringSubmatch(path, -1) {
				if !declared[m[1]] {
					t.Errorf("%s: path parameter %q not declared", name, m[1])
				}
				delete(declared, m[1])
			}
			if len(declared) > 0 {
				t.Errorf("%s: declares path parameters not in the path: %v", name, declared)
			}
		}
	}

	// Only healthz and the webhook route opt out of the bearer token.
	for _, c := range []struct {
		path, method string
		public       bool
	}{{"/healthz", "get", true}, {"/v1/hooks/{namespace}/{trigger}", "post", true}, {"/v1/tasks", "get", false}} {
		op := doc["paths"].(map[string]any)[c.path].(map[string]any)[c.method].(map[string]any)
		sec, set := op["security"].([]any)
		if c.public != (set && len(sec) == 0) {
			t.Errorf("%s %s: security = %v, public = %v", c.method, c.path, op["security"], c.public)
		}
	}
}

func TestOpenAPIEndpointRequiresAuth(t *testing.T) {
	svc := openAPIService(t, true, false, false)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(testToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	auth, err := NewTokenAuth(tokenFile, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(Handler(svc, auth))
	defer srv.Close()

	get := func(token string) (*http.Response, []byte) {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/openapi.json", nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp, b
	}
	if resp, _ := get(""); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("unauthenticated status = %d, want 401", resp.StatusCode)
	}
	resp, body := get(testToken)
	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		t.Fatalf("status = %d, content-type = %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatal(err)
	}
	if doc["openapi"] != "3.1.0" {
		t.Errorf("openapi = %v", doc["openapi"])
	}
	if _, ok := doc["paths"].(map[string]any)["/v1/missions"]; !ok {
		t.Error("/v1/missions missing from the OpenAPI document with missions enabled")
	}
}
