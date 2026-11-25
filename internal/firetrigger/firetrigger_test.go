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

package firetrigger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const saToken = "projected-sa-token"

func tokenFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// fakeGateway answers POST /v1/triggers/{namespace}/{name}/fire with handler
// and records the requests it got.
type fakeGateway struct {
	*httptest.Server
	paths, auths, bodies []string
}

func newFakeGateway(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) *fakeGateway {
	t.Helper()
	g := &fakeGateway{}
	g.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		g.paths = append(g.paths, r.Method+" "+r.URL.Path)
		g.auths = append(g.auths, r.Header.Get("Authorization"))
		g.bodies = append(g.bodies, string(body))
		handler(w, r)
	}))
	t.Cleanup(g.Close)
	return g
}

func run(args ...string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := Run(context.Background(), args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func TestRunSuccess(t *testing.T) {
	g := newFakeGateway(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"subject": "rt.events.schedule.roundtable.nightly", "stream": "rt_EVENTS", "seq": 7, "key": "2026-11-25T02:00:00Z"})
	})
	code, out, errOut := run("--gateway", g.URL+"/", "--trigger", "roundtable/nightly",
		"--token-file", tokenFile(t, saToken+"\n"), "--scheduled-time", "2026-11-25T03:00:00+01:00")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	if !strings.Contains(out, "key 2026-11-25T02:00:00Z") || !strings.Contains(out, "seq 7") {
		t.Errorf("stdout = %q", out)
	}
	if len(g.paths) != 1 || g.paths[0] != "POST /v1/triggers/roundtable/nightly/fire" {
		t.Fatalf("requests = %v", g.paths)
	}
	if g.auths[0] != "Bearer "+saToken {
		t.Errorf("Authorization = %q", g.auths[0])
	}
	if g.bodies[0] != `{"scheduledTime":"2026-11-25T02:00:00Z"}` {
		t.Errorf("body = %s", g.bodies[0])
	}

	// No scheduled time: an empty object, the gateway uses now.
	if code, _, errOut := run("--gateway", g.URL, "--trigger", "roundtable/nightly", "--token-file", tokenFile(t, saToken)); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	if g.bodies[1] != "{}" {
		t.Errorf("body without scheduled time = %s", g.bodies[1])
	}
}

func TestRunJobName(t *testing.T) {
	g := newFakeGateway(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"subject":"s","seq":1,"key":"k"}`))
	})
	at := time.Date(2026, 11, 25, 2, 0, 0, 0, time.UTC)
	job := fmt.Sprintf("trigger-nightly-%d", at.Unix()/60)
	if code, _, errOut := run("--gateway", g.URL, "--trigger", "roundtable/nightly",
		"--token-file", tokenFile(t, saToken), "--job-name", job); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	if g.bodies[0] != `{"scheduledTime":"2026-11-25T02:00:00Z"}` {
		t.Errorf("body = %s", g.bodies[0])
	}
	for _, bad := range []string{"nightly", "nightly-", "nightly-abc", "nightly-0"} {
		if code, _, _ := run("--gateway", g.URL, "--trigger", "roundtable/nightly",
			"--token-file", tokenFile(t, saToken), "--job-name", bad); code != 2 {
			t.Errorf("job name %q: exit %d, want 2", bad, code)
		}
	}
}

func TestRunForbidden(t *testing.T) {
	g := newFakeGateway(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"forbidden: roundtable/runner may not fire Trigger roundtable/nightly"}`))
	})
	code, _, errOut := run("--gateway", g.URL, "--trigger", "roundtable/nightly", "--token-file", tokenFile(t, saToken))
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(errOut, "403") || !strings.Contains(errOut, "may not fire Trigger roundtable/nightly") {
		t.Errorf("stderr = %q", errOut)
	}
}

func TestRunTimeout(t *testing.T) {
	release := make(chan struct{})
	g := newFakeGateway(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	defer close(release)
	start := time.Now()
	code, _, errOut := run("--gateway", g.URL, "--trigger", "roundtable/nightly",
		"--token-file", tokenFile(t, saToken), "--timeout", "200ms")
	if code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(errOut, "did not answer within 200ms") {
		t.Errorf("stderr = %q", errOut)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("took %s", d)
	}
}

func TestRunBadArguments(t *testing.T) {
	tf := tokenFile(t, saToken)
	for name, args := range map[string][]string{
		"no trigger":        {"--gateway", "http://gw", "--token-file", tf},
		"trigger not ns/nm": {"--gateway", "http://gw", "--trigger", "nightly", "--token-file", tf},
		"bad time":          {"--gateway", "http://gw", "--trigger", "a/b", "--token-file", tf, "--scheduled-time", "2am"},
		"extra argument":    {"--gateway", "http://gw", "--trigger", "a/b", "--token-file", tf, "extra"},
		"unknown flag":      {"--nope"},
	} {
		if code, _, _ := run(args...); code != 2 {
			t.Errorf("%s: exit %d, want 2", name, code)
		}
	}
	for name, args := range map[string][]string{
		"no gateway":     {"--trigger", "a/b", "--token-file", tf},
		"gateway scheme": {"--gateway", "ftp://gw", "--trigger", "a/b", "--token-file", tf},
		"missing token":  {"--gateway", "http://gw", "--trigger", "a/b", "--token-file", filepath.Join(t.TempDir(), "none")},
		"empty token":    {"--gateway", "http://gw", "--trigger", "a/b", "--token-file", tokenFile(t, "\n")},
	} {
		t.Setenv("ROUNDTABLE_GATEWAY_URL", "")
		if code, _, errOut := run(args...); code != 1 || errOut == "" {
			t.Errorf("%s: exit %d (%q), want 1", name, code, errOut)
		}
	}
}
