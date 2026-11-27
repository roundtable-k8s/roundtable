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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/chainrun"
)

type runEnv struct {
	env
	k8s client.Client
}

func step(name string, deps ...string) aiv1alpha1.ChainStep {
	return aiv1alpha1.ChainStep{Name: name, KnightRef: corev1.LocalObjectReference{Name: "operator"},
		Task: "do " + name, DependsOn: deps}
}

// templateChain is an opted-in Chain: fetch -> summarise.
func templateChain(name string, invokable bool) *aiv1alpha1.Chain {
	c := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Labels: map[string]string{}},
		Spec: aiv1alpha1.ChainSpec{
			Description: "fetch and summarise",
			Steps:       []aiv1alpha1.ChainStep{step("fetch"), step("summarise", "fetch")},
			Schedule:    "0 * * * *",
			Input:       "template input",
		},
	}
	if invokable {
		c.Labels[aiv1alpha1.LabelInvokable] = "true"
	}
	return c
}

// newRunEnv serves the gateway with chain runs enabled over a fake Kubernetes
// API holding an opted-in chain "digest", a chain "private" without the label
// and a run copy "digest-pickup-1" (labelled, but itself a copy).
func newRunEnv(t *testing.T, opts Options, enabled bool, extra ...client.Object) *runEnv {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = aiv1alpha1.AddToScheme(scheme)
	copyChain := templateChain("digest-pickup-1", true)
	copyChain.Labels[chainrun.LabelRunSource] = chainrun.SourcePickup
	objs := append([]client.Object{
		knight("operator", "claude", "", false),
		templateChain("digest", true),
		templateChain("private", false),
		copyChain,
	}, extra...)
	k8s := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&aiv1alpha1.Chain{}).Build()

	if opts.Namespace == "" {
		opts.Namespace = testNS
	}
	if opts.MaxTimeout == 0 {
		opts.MaxTimeout = 30 * time.Minute
	}
	if opts.Runs.PollInterval == 0 {
		opts.Runs.PollInterval = 20 * time.Millisecond
	}
	svc := NewService(&Resolver{Client: k8s}, nil, opts, logr.Discard())
	if enabled {
		svc.Runs = k8s
	}
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(testToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	auth, err := NewTokenAuth(tokenFile, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(Handler(svc, auth))
	t.Cleanup(srv.Close)
	return &runEnv{env: env{svc: svc, auth: auth, server: srv}, k8s: k8s}
}

// runs returns the gateway's run copies.
func (e *runEnv) runs(t *testing.T) []aiv1alpha1.Chain {
	t.Helper()
	var list aiv1alpha1.ChainList
	if err := e.k8s.List(context.Background(), &list, client.InNamespace(testNS),
		client.MatchingLabels{chainrun.LabelRunSource: chainrun.SourceGateway}); err != nil {
		t.Fatal(err)
	}
	return list.Items
}

// finish plays the chain controller: it waits for a run copy and completes
// its run with the given phase and per-step outputs.
func (e *runEnv) finish(t *testing.T, phase aiv1alpha1.ChainPhase, outputs map[string]string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if runs := e.runs(t); len(runs) == 1 {
			c := &runs[0]
			now := metav1.Now()
			c.Status.Phase = phase
			c.Status.RunID = "run-123"
			c.Status.LastRunTrigger = c.Annotations[aiv1alpha1.AnnotationChainRun]
			c.Status.StartedAt, c.Status.CompletedAt = &now, &now
			c.Status.StepStatuses = nil
			for _, s := range c.Spec.Steps {
				ss := aiv1alpha1.ChainStepStatus{Name: s.Name, Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: outputs[s.Name]}
				if phase == aiv1alpha1.ChainPhaseFailed && s.Name == "summarise" {
					ss.Phase, ss.Error = aiv1alpha1.ChainStepPhaseFailed, "knight timed out"
				}
				c.Status.StepStatuses = append(c.Status.StepStatuses, ss)
			}
			if err := e.k8s.Status().Update(context.Background(), c); err != nil {
				t.Error(err)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Error("no run copy appeared")
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunChainCreatesCopyWithInput(t *testing.T) {
	e := newRunEnv(t, Options{}, true)

	resp, out := e.do(t, http.MethodPost, "/v1/chains/digest/runs", testToken,
		map[string]any{"input": map[string]any{"topic": "NATS", "days": 7}})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d: %v", resp.StatusCode, out)
	}
	runs := e.runs(t)
	if len(runs) != 1 {
		t.Fatalf("want one run copy, got %d", len(runs))
	}
	c := runs[0]
	if resp.Header.Get("Location") != "/v1/runs/"+testNS+"/"+c.Name || out["run"] != c.Name {
		t.Fatalf("Location %q, run %v, copy %s", resp.Header.Get("Location"), out["run"], c.Name)
	}
	if out["chain"] != "digest" || out["phase"] != "Pending" || out["done"] != false {
		t.Fatalf("record: %v", out)
	}
	if !strings.HasPrefix(c.Name, "digest-gw-") {
		t.Errorf("name %q", c.Name)
	}
	if c.Spec.Input != `{"days":7,"topic":"NATS"}` {
		t.Errorf("input %q", c.Spec.Input)
	}
	if c.Spec.Schedule != "" || len(c.Spec.Steps) != 2 {
		t.Errorf("spec: schedule %q, %d steps", c.Spec.Schedule, len(c.Spec.Steps))
	}
	if c.Labels[aiv1alpha1.LabelInvokable] != "" || c.Labels[aiv1alpha1.LabelCreatedBy] != aiv1alpha1.CreatedByGateway {
		t.Errorf("labels: %v", c.Labels)
	}
	if c.Annotations[annotationRunOf] != "digest" || c.Annotations[aiv1alpha1.AnnotationChainRun] == "" {
		t.Errorf("annotations: %v", c.Annotations)
	}

	// A string input is used as is.
	e2 := newRunEnv(t, Options{}, true)
	e2.do(t, http.MethodPost, "/v1/chains/digest/runs", testToken, map[string]any{"input": "plain text"})
	if runs := e2.runs(t); len(runs) != 1 || runs[0].Spec.Input != "plain text" {
		t.Fatalf("string input: %+v", runs)
	}
	// No input keeps the chain's own.
	e3 := newRunEnv(t, Options{}, true)
	e3.do(t, http.MethodPost, "/v1/chains/digest/runs", testToken, map[string]any{})
	if runs := e3.runs(t); len(runs) != 1 || runs[0].Spec.Input != "template input" {
		t.Fatalf("default input: %+v", runs)
	}
}

func TestRunChainRejects(t *testing.T) {
	suspended := templateChain("paused", true)
	suspended.Spec.Suspend = true
	mission := templateChain("mission-scoped", true)
	mission.Spec.MissionRef = &corev1.LocalObjectReference{Name: "m"}
	e := newRunEnv(t, Options{AllowedNamespaces: []string{"other"}}, true, suspended, mission)

	cases := []struct {
		name, path string
		body       any
		code       int
	}{
		{"not opted in", "/v1/chains/private/runs", map[string]any{}, http.StatusForbidden},
		{"run copy", "/v1/chains/digest-pickup-1/runs", map[string]any{}, http.StatusForbidden},
		{"mission chain", "/v1/chains/mission-scoped/runs", map[string]any{}, http.StatusForbidden},
		{"unknown chain", "/v1/chains/nope/runs", map[string]any{}, http.StatusNotFound},
		{"bad name", "/v1/chains/Bad_Name/runs", map[string]any{}, http.StatusNotFound},
		{"namespace not allowed", "/v1/chains/digest/runs", map[string]any{"namespace": "kube-system"}, http.StatusBadRequest},
		{"input too large", "/v1/chains/digest/runs", map[string]any{"input": strings.Repeat("a", maxRunInputBytes+1)}, http.StatusBadRequest},
		{"negative wait", "/v1/chains/digest/runs", map[string]any{"waitSeconds": -1}, http.StatusBadRequest},
		{"unknown field", "/v1/chains/digest/runs", map[string]any{"steps": []any{}}, http.StatusBadRequest},
		{"suspended", "/v1/chains/paused/runs", map[string]any{}, http.StatusServiceUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, out := e.do(t, http.MethodPost, tc.path, testToken, tc.body)
			if resp.StatusCode != tc.code {
				t.Fatalf("status %d, want %d: %v", resp.StatusCode, tc.code, out)
			}
		})
	}
	if runs := e.runs(t); len(runs) != 0 {
		t.Fatalf("rejected requests created %d runs", len(runs))
	}
}

func TestRunChainWaitReturnsOutput(t *testing.T) {
	e := newRunEnv(t, Options{}, true)
	go e.finish(t, aiv1alpha1.ChainPhaseSucceeded, map[string]string{"fetch": "raw", "summarise": "the summary"})

	resp, out := e.do(t, http.MethodPost, "/v1/chains/digest/runs", testToken,
		map[string]any{"input": "x", "waitSeconds": 10})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %v", resp.StatusCode, out)
	}
	if out["output"] != "the summary" || out["phase"] != "Succeeded" || out["done"] != true || out["runId"] != "run-123" {
		t.Fatalf("record: %v", out)
	}
	steps, _ := out["steps"].([]any)
	if len(steps) != 2 || steps[0].(map[string]any)["output"] != "raw" {
		t.Fatalf("steps: %v", steps)
	}
	if resp.Header.Get("Location") != "" {
		t.Errorf("finished run has Location %q", resp.Header.Get("Location"))
	}
}

func TestRunChainAcceptedThenGetWaits(t *testing.T) {
	e := newRunEnv(t, Options{}, true)
	resp, out := e.do(t, http.MethodPost, "/v1/chains/digest/runs", testToken, map[string]any{"waitSeconds": 0})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d: %v", resp.StatusCode, out)
	}
	loc := resp.Header.Get("Location")

	// Still pending: GET without a wait is 202 too.
	if resp, out := e.do(t, http.MethodGet, loc, testToken, nil); resp.StatusCode != http.StatusAccepted || out["done"] != false {
		t.Fatalf("pending GET %d: %v", resp.StatusCode, out)
	}

	go func() {
		time.Sleep(100 * time.Millisecond)
		e.finish(t, aiv1alpha1.ChainPhaseSucceeded, map[string]string{"summarise": "done!"})
	}()
	resp, out = e.do(t, http.MethodGet, loc+"?waitSeconds=10", testToken, nil)
	if resp.StatusCode != http.StatusOK || out["output"] != "done!" {
		t.Fatalf("GET wait %d: %v", resp.StatusCode, out)
	}

	// A wait that ends before the run does returns 202 with the run so far.
	e2 := newRunEnv(t, Options{DefaultTimeout: time.Second, MaxTimeout: time.Second}, true)
	_, out = e2.do(t, http.MethodPost, "/v1/chains/digest/runs", testToken, map[string]any{})
	start := time.Now()
	resp, _ = e2.do(t, http.MethodGet, "/v1/runs/"+testNS+"/"+out["run"].(string)+"?waitSeconds=60", testToken, nil)
	if resp.StatusCode != http.StatusAccepted || time.Since(start) > 5*time.Second {
		t.Fatalf("capped wait: status %d after %s", resp.StatusCode, time.Since(start))
	}
}

func TestGetAndListRuns(t *testing.T) {
	e := newRunEnv(t, Options{}, true)
	e.do(t, http.MethodPost, "/v1/chains/digest/runs", testToken, map[string]any{})

	// Chains that are not run copies are not runs.
	for _, path := range []string{"/v1/runs/" + testNS + "/digest", "/v1/runs/" + testNS + "/nope"} {
		if resp, out := e.do(t, http.MethodGet, path, testToken, nil); resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s: %d %v", path, resp.StatusCode, out)
		}
	}

	resp, out := e.do(t, http.MethodGet, "/v1/runs?chain=digest", testToken, nil)
	if resp.StatusCode != http.StatusOK || len(out["runs"].([]any)) != 1 {
		t.Fatalf("list: %d %v", resp.StatusCode, out)
	}
	for _, q := range []string{"?chain=other", "?phase=Succeeded"} {
		if _, out := e.do(t, http.MethodGet, "/v1/runs"+q, testToken, nil); len(out["runs"].([]any)) != 0 {
			t.Errorf("list %s: %v", q, out)
		}
	}
	if _, out := e.do(t, http.MethodGet, "/v1/runs?phase=pending", testToken, nil); len(out["runs"].([]any)) != 1 {
		t.Errorf("list phase=pending: %v", out)
	}

	resp, out = e.do(t, http.MethodGet, "/v1/chains", testToken, nil)
	chains, _ := out["chains"].([]any)
	if resp.StatusCode != http.StatusOK || len(chains) != 1 || chains[0].(map[string]any)["name"] != "digest" ||
		chains[0].(map[string]any)["description"] != "fetch and summarise" {
		t.Fatalf("list chains: %d %v", resp.StatusCode, out)
	}
}

func TestRunsDisabled(t *testing.T) {
	e := newRunEnv(t, Options{}, false)
	for _, path := range []string{"/v1/chains", "/v1/runs"} {
		if resp, _ := e.do(t, http.MethodGet, path, testToken, nil); resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s: %d", path, resp.StatusCode)
		}
	}
	if resp, _ := e.do(t, http.MethodPost, "/v1/chains/digest/runs", testToken, map[string]any{}); resp.StatusCode != http.StatusNotFound {
		t.Errorf("POST: %d", resp.StatusCode)
	}
	if _, err := e.svc.RunChain(context.Background(), "digest", RunChainRequest{}); err == nil {
		t.Error("RunChain worked with runs disabled")
	}
}

func TestFinalStep(t *testing.T) {
	cases := map[string]struct {
		steps []aiv1alpha1.ChainStep
		want  string
	}{
		"single":        {[]aiv1alpha1.ChainStep{step("a")}, "a"},
		"sequence":      {[]aiv1alpha1.ChainStep{step("a"), step("b", "a"), step("c", "b")}, "c"},
		"diamond":       {[]aiv1alpha1.ChainStep{step("a"), step("b", "a"), step("c", "a"), step("d", "b", "c")}, "d"},
		"two ends":      {[]aiv1alpha1.ChainStep{step("a"), step("b", "a"), step("c", "a")}, ""},
		"no dependency": {[]aiv1alpha1.ChainStep{step("a"), step("b")}, ""},
	}
	for name, tc := range cases {
		if got := finalStep(tc.steps); got != tc.want {
			t.Errorf("%s: got %q, want %q", name, got, tc.want)
		}
	}

	// Several final steps: no output, steps only. Outputs are truncated.
	c := templateChain("x-gw-1", false)
	c.Labels[chainrun.LabelRunSource] = chainrun.SourceGateway
	c.Spec.Steps = []aiv1alpha1.ChainStep{step("a"), step("b")}
	c.Status = aiv1alpha1.ChainStatus{Phase: aiv1alpha1.ChainPhaseSucceeded, StepStatuses: []aiv1alpha1.ChainStepStatus{
		{Name: "a", Output: strings.Repeat("x", maxRunStepOutput+10)}, {Name: "b", Output: "b"},
	}}
	r := runRecord(c, true)
	if r.Output != "" || len(r.Steps) != 2 || len(r.Steps[0].Output) != maxRunStepOutput || !r.Steps[0].OutputTruncated {
		t.Fatalf("record: output %q, steps %d", r.Output, len(r.Steps))
	}
}

func TestSweepRuns(t *testing.T) {
	old := metav1.NewTime(time.Now().Add(-48 * time.Hour))
	recent := metav1.NewTime(time.Now().Add(-time.Hour))
	mk := func(name, source string, phase aiv1alpha1.ChainPhase, done *metav1.Time) *aiv1alpha1.Chain {
		c := templateChain(name, false)
		c.Labels[chainrun.LabelRunSource] = source
		c.Annotations = map[string]string{aiv1alpha1.AnnotationChainRun: "t"}
		c.Status = aiv1alpha1.ChainStatus{Phase: phase, CompletedAt: done, LastRunTrigger: "t"}
		return c
	}
	e := newRunEnv(t, Options{}, true,
		mk("expired-gw-1", chainrun.SourceGateway, aiv1alpha1.ChainPhaseSucceeded, &old),
		mk("expired-gw-2", chainrun.SourceGateway, aiv1alpha1.ChainPhaseFailed, &old),
		mk("fresh-gw-1", chainrun.SourceGateway, aiv1alpha1.ChainPhaseSucceeded, &recent),
		mk("running-gw-1", chainrun.SourceGateway, aiv1alpha1.ChainPhaseRunning, &old),
		mk("pickup-1", chainrun.SourcePickup, aiv1alpha1.ChainPhaseSucceeded, &old),
	)
	// The seeded objects keep their status.
	for _, name := range []string{"expired-gw-1", "expired-gw-2", "fresh-gw-1", "running-gw-1", "pickup-1"} {
		c := &aiv1alpha1.Chain{}
		if err := e.k8s.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, c); err != nil {
			t.Fatal(err)
		}
		if c.Status.Phase == "" {
			t.Fatalf("%s lost its status", name)
		}
	}

	n, err := e.svc.SweepRuns(context.Background())
	if err != nil || n != 2 {
		t.Fatalf("swept %d, err %v", n, err)
	}
	var list aiv1alpha1.ChainList
	_ = e.k8s.List(context.Background(), &list)
	left := make([]string, 0, len(list.Items))
	for _, c := range list.Items {
		left = append(left, c.Name)
	}
	if strings.Contains(strings.Join(left, ","), "expired") || len(left) != 6 {
		t.Fatalf("left: %v", left)
	}
}

func TestMCPRuns(t *testing.T) {
	e := newRunEnv(t, Options{}, true)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	c := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "v0"}, nil)
	cs, err := c.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:   e.server.URL + "/mcp",
		HTTPClient: &http.Client{Transport: bearer{http.DefaultTransport}},
		MaxRetries: -1,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cs.Close() }()

	call := func(name string, args map[string]any) (*mcp.CallToolResult, map[string]any) {
		t.Helper()
		res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var sc map[string]any
		b, _ := json.Marshal(res.StructuredContent)
		_ = json.Unmarshal(b, &sc)
		return res, sc
	}
	text := func(res *mcp.CallToolResult) string {
		if len(res.Content) == 0 {
			return ""
		}
		tc, _ := res.Content[0].(*mcp.TextContent)
		if tc == nil {
			return ""
		}
		return tc.Text
	}

	res, sc := call("list_chains", map[string]any{})
	if res.IsError || len(sc["chains"].([]any)) != 1 {
		t.Fatalf("list_chains: %s %v", text(res), sc)
	}

	go e.finish(t, aiv1alpha1.ChainPhaseSucceeded, map[string]string{"summarise": "mcp summary"})
	res, sc = call("run_chain", map[string]any{"chain": "digest", "input": map[string]any{"q": 1}, "waitSeconds": 10})
	if res.IsError || text(res) != "mcp summary" || sc["done"] != true {
		t.Fatalf("run_chain: %s %v", text(res), sc)
	}
	run, _ := sc["run"].(string)
	if runs := e.runs(t); len(runs) != 1 || runs[0].Spec.Input != `{"q":1}` {
		t.Fatalf("copy: %+v", runs)
	}

	res, sc = call("get_run", map[string]any{"run": run})
	if res.IsError || text(res) != "mcp summary" || sc["run"] != run {
		t.Fatalf("get_run: %s %v", text(res), sc)
	}
	res, sc = call("list_runs", map[string]any{"chain": "digest"})
	if res.IsError || len(sc["runs"].([]any)) != 1 {
		t.Fatalf("list_runs: %s %v", text(res), sc)
	}

	// Pending runs tell the caller to poll; failed runs are tool errors.
	e2 := newRunEnv(t, Options{}, true)
	cs2, err := c.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:   e2.server.URL + "/mcp",
		HTTPClient: &http.Client{Transport: bearer{http.DefaultTransport}},
		MaxRetries: -1,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cs2.Close() }()
	r2, err := cs2.CallTool(ctx, &mcp.CallToolParams{Name: "run_chain", Arguments: map[string]any{"chain": "digest"}})
	if err != nil || r2.IsError || !strings.Contains(text(r2), "get_run") {
		t.Fatalf("pending run_chain: %v %s", err, text(r2))
	}
	e2.finish(t, aiv1alpha1.ChainPhaseFailed, nil)
	name := e2.runs(t)[0].Name
	r2, err = cs2.CallTool(ctx, &mcp.CallToolParams{Name: "get_run", Arguments: map[string]any{"run": name}})
	if err != nil || !r2.IsError || !strings.Contains(text(r2), "knight timed out") {
		t.Fatalf("failed get_run: %v %v %s", err, r2.IsError, text(r2))
	}
	r2, err = cs2.CallTool(ctx, &mcp.CallToolParams{Name: "run_chain", Arguments: map[string]any{"chain": "private"}})
	if err != nil || !r2.IsError {
		t.Fatalf("run_chain on a chain without the label: %v %v", err, r2)
	}
}

func TestRunChainIdempotencyKey(t *testing.T) {
	e := newRunEnv(t, Options{}, true)
	path := "/v1/chains/digest/runs"

	r1, o1 := e.doKey(t, path, "run-1", map[string]any{})
	r2, o2 := e.doKey(t, path, "run-1", map[string]any{})
	if r1.StatusCode != http.StatusAccepted || r2.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d / %d: %v %v", r1.StatusCode, r2.StatusCode, o1, o2)
	}
	if r1.Header.Get("Idempotent-Replay") != "" || r2.Header.Get("Idempotent-Replay") != "true" {
		t.Fatalf("replay headers %q / %q", r1.Header.Get("Idempotent-Replay"), r2.Header.Get("Idempotent-Replay"))
	}
	runs := e.runs(t)
	if len(runs) != 1 || o1["run"] != o2["run"] || o1["run"] != runs[0].Name {
		t.Fatalf("want one run, got %d: %v %v", len(runs), o1, o2)
	}
	if want := "digest-gw-" + idempotencyHash(testNS, "digest", "run-1")[:8]; runs[0].Name != want {
		t.Errorf("run name %q, want %q", runs[0].Name, want)
	}

	// Another key, or none, makes another run.
	e.doKey(t, path, "run-2", map[string]any{})
	e.doKey(t, path, "", map[string]any{})
	if n := len(e.runs(t)); n != 3 {
		t.Fatalf("want 3 runs, got %d", n)
	}
	if r, _ := e.doKey(t, path, strings.Repeat("a", 256), map[string]any{}); r.StatusCode != http.StatusBadRequest {
		t.Fatalf("long key: %d", r.StatusCode)
	}
}

// playCancel plays the chain controller for a cancelled run: once the cancel
// annotation shows up it fails the run, with its steps carrying the reason.
func (e *runEnv) playCancel(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if runs := e.runs(t); len(runs) == 1 {
			c := &runs[0]
			if reason, ok := c.Annotations[aiv1alpha1.AnnotationChainCancel]; ok {
				now := metav1.Now()
				c.Status.Phase, c.Status.RunID = aiv1alpha1.ChainPhaseFailed, "run-123"
				c.Status.LastRunTrigger = c.Annotations[aiv1alpha1.AnnotationChainRun]
				c.Status.StartedAt, c.Status.CompletedAt = &now, &now
				for _, s := range c.Spec.Steps {
					c.Status.StepStatuses = append(c.Status.StepStatuses, aiv1alpha1.ChainStepStatus{
						Name: s.Name, Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "cancelled: " + reason})
				}
				if err := e.k8s.Status().Update(context.Background(), c); err != nil {
					t.Error(err)
				}
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("run was never annotated for cancellation")
}

func TestCancelRun(t *testing.T) {
	e := newRunEnv(t, Options{}, true)
	resp, out := e.do(t, http.MethodPost, "/v1/chains/digest/runs", testToken, map[string]any{"input": "x"})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("start: %d %v", resp.StatusCode, out)
	}
	run, _ := out["run"].(string)
	path := "/v1/runs/" + testNS + "/" + run + "/cancel"

	go e.playCancel(t)
	resp, out = e.do(t, http.MethodPost, path, testToken, map[string]any{"reason": "wrong input"})
	if resp.StatusCode != http.StatusOK || out["phase"] != "Failed" || out["done"] != true {
		t.Fatalf("cancel: %d %v", resp.StatusCode, out)
	}
	steps, _ := out["steps"].([]any)
	if len(steps) != 2 || steps[0].(map[string]any)["error"] != "cancelled: wrong input" {
		t.Fatalf("steps: %v", steps)
	}

	// A finished run can't be cancelled again.
	resp, out = e.do(t, http.MethodPost, path, testToken, nil)
	if resp.StatusCode != http.StatusConflict || out["state"] != "Failed" {
		t.Fatalf("cancel finished: %d %v", resp.StatusCode, out)
	}

	// A run another source started is refused, not hidden.
	resp, out = e.do(t, http.MethodPost, "/v1/runs/"+testNS+"/digest-pickup-1/cancel", testToken, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("cancel pickup run: %d %v, want 403", resp.StatusCode, out)
	}
	// Chains that are not runs, and unknown runs, are 404.
	for _, name := range []string{"digest", "nope"} {
		resp, _ = e.do(t, http.MethodPost, "/v1/runs/"+testNS+"/"+name+"/cancel", testToken, nil)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("cancel %s: %d, want 404", name, resp.StatusCode)
		}
	}
	if c := e.runs(t)[0]; c.Annotations[aiv1alpha1.AnnotationChainCancel] != "wrong input" {
		t.Errorf("annotation: %v", c.Annotations)
	}
}

func TestCancelRunDisabledAndBadBody(t *testing.T) {
	e := newRunEnv(t, Options{}, false)
	if resp, _ := e.do(t, http.MethodPost, "/v1/runs/"+testNS+"/x/cancel", testToken, nil); resp.StatusCode != http.StatusNotFound {
		t.Errorf("disabled: %d", resp.StatusCode)
	}
	e = newRunEnv(t, Options{}, true)
	if resp, _ := e.do(t, http.MethodPost, "/v1/runs/"+testNS+"/x/cancel", testToken, map[string]any{"bogus": 1}); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad body: %d", resp.StatusCode)
	}
}

func TestMCPCancelRun(t *testing.T) {
	e := newRunEnv(t, Options{}, true)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "v0"}, nil)
	cs, err := c.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:   e.server.URL + "/mcp",
		HTTPClient: &http.Client{Transport: bearer{http.DefaultTransport}},
		MaxRetries: -1,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cs.Close() }()
	text := func(r *mcp.CallToolResult) string {
		if tc, _ := r.Content[0].(*mcp.TextContent); tc != nil {
			return tc.Text
		}
		return ""
	}

	r, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "run_chain", Arguments: map[string]any{"chain": "digest"}})
	if err != nil || r.IsError {
		t.Fatalf("run_chain: %v %v", err, r)
	}
	run := e.runs(t)[0].Name

	go e.playCancel(t)
	r, err = cs.CallTool(ctx, &mcp.CallToolParams{Name: "cancel_run", Arguments: map[string]any{"run": run, "reason": "not needed"}})
	if err != nil || r.IsError || !strings.Contains(text(r), "cancelled: not needed") {
		t.Fatalf("cancel_run: %v %v %s", err, r.IsError, text(r))
	}

	// A finished run and an unknown run are tool errors.
	for _, name := range []string{run, "nope"} {
		r, err = cs.CallTool(ctx, &mcp.CallToolParams{Name: "cancel_run", Arguments: map[string]any{"run": name}})
		if err != nil || !r.IsError {
			t.Errorf("cancel_run %s: %v %v", name, err, r)
		}
	}
}

func TestRunChainBusyCreatesNothing(t *testing.T) {
	e := newRunEnv(t, Options{MaxInFlight: 1}, true)
	path := "/v1/chains/digest/runs"
	e.svc.inflight <- struct{}{} // another wait holds the only slot

	resp, out := e.do(t, http.MethodPost, path, testToken, map[string]any{"waitSeconds": 1})
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("waiting run: %d %v", resp.StatusCode, out)
	}
	if n := len(e.runs(t)); n != 0 {
		t.Fatalf("busy call left %d runs", n)
	}

	// An asynchronous run takes no slot and still works.
	if resp, out = e.do(t, http.MethodPost, path, testToken, map[string]any{}); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("async run: %d %v", resp.StatusCode, out)
	}
	<-e.svc.inflight

	// A waiting replay takes one slot, not two.
	r1, o1 := e.doKey(t, path, "k", map[string]any{})
	if r1.StatusCode != http.StatusAccepted {
		t.Fatalf("keyed run: %d %v", r1.StatusCode, o1)
	}
	r2, o2 := e.doKey(t, path, "k", map[string]any{"waitSeconds": 1})
	if r2.StatusCode != http.StatusAccepted || r2.Header.Get("Idempotent-Replay") != "true" {
		t.Fatalf("waiting replay: %d %v", r2.StatusCode, o2)
	}
	if n := len(e.svc.inflight); n != 0 {
		t.Fatalf("slot leaked: %d held", n)
	}
}

func TestCancelResultFailedForAnotherReason(t *testing.T) {
	rec := &RunRecord{Namespace: "ns", Run: "r1", Chain: "c", Done: true, Phase: "Failed",
		Steps: []RunStep{{Name: "s", Error: "boom"}}}
	text := cancelResult(rec).Content[0].(*mcp.TextContent).Text
	if strings.Contains(text, "cancelled") {
		t.Fatalf("text = %q, must not say cancelled", text)
	}
	rec.Steps[0].Error = "cancelled: by user"
	if text := cancelResult(rec).Content[0].(*mcp.TextContent).Text; !strings.Contains(text, "was cancelled: by user") {
		t.Fatalf("text = %q", text)
	}
}

func TestListRunsAllSources(t *testing.T) {
	created := time.Now().Add(-90 * time.Second)
	started := metav1.NewTime(time.Now().Add(-30 * time.Second))
	mk := func(name, source string, ann map[string]string, phase aiv1alpha1.ChainPhase) *aiv1alpha1.Chain {
		c := templateChain(name, false)
		c.Labels[chainrun.LabelRunSource] = source
		c.Annotations = ann
		c.CreationTimestamp = metav1.NewTime(created)
		c.Status.Phase = phase
		if phase == aiv1alpha1.ChainPhaseRunning {
			c.Status.StartedAt = &started
		}
		return c
	}
	e := newRunEnv(t, Options{}, true,
		mk("a-trigger", chainrun.SourceTrigger, map[string]string{aiv1alpha1.AnnotationRunTemplate: "nightly"}, aiv1alpha1.ChainPhaseRunning),
		mk("b-pickup", chainrun.SourcePickup, map[string]string{aiv1alpha1.AnnotationRunTemplate: "fix",
			aiv1alpha1.AnnotationPickupRepo: "o/r", aiv1alpha1.AnnotationPickupIssue: "12"}, aiv1alpha1.ChainPhaseRunning),
		mk("c-done", chainrun.SourcePickup, map[string]string{aiv1alpha1.AnnotationPickupRepo: "o/r",
			aiv1alpha1.AnnotationPickupIssue: "13"}, aiv1alpha1.ChainPhaseSucceeded),
	)
	e.do(t, http.MethodPost, "/v1/chains/digest/runs", testToken, map[string]any{})

	list := func(q string) []map[string]any {
		t.Helper()
		resp, out := e.do(t, http.MethodGet, "/v1/runs"+q, testToken, nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("list %s: %d %v", q, resp.StatusCode, out)
		}
		raw := out["runs"].([]any)
		runs := make([]map[string]any, 0, len(raw))
		for _, r := range raw {
			runs = append(runs, r.(map[string]any))
		}
		return runs
	}
	if got := list(""); len(got) != 1 || got[0]["source"] != "gateway" || got[0]["chain"] != "digest" {
		t.Errorf("default list: %v", got)
	}
	// digest-pickup-1 from newRunEnv is a pickup run too: 3 pickup + trigger + gateway.
	if got := list("?source=all"); len(got) != 5 {
		t.Errorf("source=all: %d runs, want 5", len(got))
	}
	if got := list("?source=trigger"); len(got) != 1 || got[0]["chain"] != "nightly" {
		t.Errorf("source=trigger: %v", got)
	}
	active := list("?source=all&active=true")
	if len(active) != 4 { // two running + the pending gateway and fixture runs
		t.Errorf("active: %d runs: %v", len(active), active)
	}
	for _, r := range list("?source=all&phase=Running") {
		if age := r["ageSeconds"].(float64); age < 89 || age > 100 {
			t.Errorf("ageSeconds %v", age)
		}
		if run := r["runningSeconds"].(float64); run < 29 || run > 40 {
			t.Errorf("runningSeconds %v", run)
		}
	}
	got := list("?source=all&issue=o/r%2312")
	if len(got) != 1 || got[0]["run"] != "b-pickup" || got[0]["chain"] != "fix" ||
		got[0]["issue"].(map[string]any)["number"] != float64(12) {
		t.Errorf("issue filter: %v", got)
	}
	for _, q := range []string{"?source=bogus", "?issue=nope", "?active=maybe"} {
		if resp, _ := e.do(t, http.MethodGet, "/v1/runs"+q, testToken, nil); resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: %d, want 400", q, resp.StatusCode)
		}
	}

	resp, out := e.do(t, http.MethodGet, "/v1/runs/"+testNS+"/a-trigger", testToken, nil)
	if resp.StatusCode != http.StatusAccepted || out["source"] != "trigger" {
		t.Errorf("get trigger run: %d %v", resp.StatusCode, out)
	}
	resp, out = e.do(t, http.MethodPost, "/v1/runs/"+testNS+"/a-trigger/cancel", testToken, nil)
	if resp.StatusCode != http.StatusForbidden || !strings.Contains(fmt.Sprint(out["error"]), "only gateway runs") {
		t.Errorf("cancel trigger run: %d %v", resp.StatusCode, out)
	}
}

func TestListRunsAllSendsLabelSelector(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = aiv1alpha1.AddToScheme(scheme)
	run := templateChain("r-pickup", true)
	run.Labels[chainrun.LabelRunSource] = chainrun.SourcePickup
	odd := templateChain("odd", true)
	odd.Labels[chainrun.LabelRunSource] = "mystery"
	var selectors []string
	k8s := interceptor.NewClient(fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(templateChain("plain", true), run, odd).Build(), interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			lo := &client.ListOptions{}
			lo.ApplyOptions(opts)
			if lo.LabelSelector != nil {
				selectors = append(selectors, lo.LabelSelector.String())
			}
			return c.List(ctx, list, opts...)
		},
	})
	svc := NewService(&Resolver{Client: k8s}, nil, Options{Namespace: testNS}, logr.Discard())
	svc.Runs = k8s
	got, err := svc.ListRuns(t.Context(), RunFilter{Source: "all"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Run != "r-pickup" {
		t.Errorf("runs = %v", got)
	}
	if len(selectors) != 1 || selectors[0] != chainrun.LabelRunSource {
		t.Errorf("label selectors = %v, want [%s]", selectors, chainrun.LabelRunSource)
	}
}

func TestRerunRun(t *testing.T) {
	e := newRunEnv(t, Options{}, true)
	resp, out := e.do(t, http.MethodPost, "/v1/chains/digest/runs", testToken, map[string]any{"input": "original"})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("start: %d %v", resp.StatusCode, out)
	}
	first, _ := out["run"].(string)
	path := "/v1/runs/" + testNS + "/" + first + "/rerun"

	// A running run is a conflict.
	resp, out = e.do(t, http.MethodPost, path, testToken, nil)
	if resp.StatusCode != http.StatusConflict || out["state"] != "Pending" {
		t.Fatalf("rerun running: %d %v", resp.StatusCode, out)
	}

	e.finish(t, aiv1alpha1.ChainPhaseFailed, nil)
	resp, out = e.do(t, http.MethodPost, path, testToken, nil)
	if resp.StatusCode != http.StatusAccepted || out["rerunOf"] != first || out["chain"] != "digest" || out["done"] != false {
		t.Fatalf("rerun: %d %v", resp.StatusCode, out)
	}
	second, _ := out["run"].(string)
	if second == first {
		t.Fatalf("rerun reused the run name %s", first)
	}
	var got *aiv1alpha1.Chain
	for _, c := range e.runs(t) {
		if c.Name == second {
			got = &c
		}
	}
	if got == nil {
		t.Fatalf("no copy %s", second)
	}
	if got.Spec.Input != "original" || got.Annotations[annotationRerunOf] != first || got.Annotations[annotationRunOf] != "digest" {
		t.Errorf("copy: input %q annotations %v", got.Spec.Input, got.Annotations)
	}

	// An input override wins. The first run is still finished.
	resp, out = e.do(t, http.MethodPost, path, testToken, map[string]any{"input": map[string]any{"a": 1}})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("rerun override: %d %v", resp.StatusCode, out)
	}
	for _, c := range e.runs(t) {
		if c.Name == out["run"] && c.Spec.Input != `{"a":1}` {
			t.Errorf("override input %q", c.Spec.Input)
		}
	}

	// The original run does not carry rerunOf.
	if _, out = e.do(t, http.MethodGet, "/v1/runs/"+testNS+"/"+first, testToken, nil); out["rerunOf"] != nil {
		t.Errorf("original rerunOf: %v", out["rerunOf"])
	}
}

func TestRerunRunRejects(t *testing.T) {
	e := newRunEnv(t, Options{}, true)
	resp, out := e.do(t, http.MethodPost, "/v1/chains/digest/runs", testToken, map[string]any{})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("start: %d %v", resp.StatusCode, out)
	}
	run, _ := out["run"].(string)
	e.finish(t, aiv1alpha1.ChainPhaseSucceeded, nil)
	path := "/v1/runs/" + testNS + "/" + run + "/rerun"

	// Not a gateway run, or unknown: 404.
	for _, name := range []string{"digest-pickup-1", "digest", "nope"} {
		if resp, _ = e.do(t, http.MethodPost, "/v1/runs/"+testNS+"/"+name+"/rerun", testToken, nil); resp.StatusCode != http.StatusNotFound {
			t.Errorf("rerun %s: %d, want 404", name, resp.StatusCode)
		}
	}
	// A bad body is 400.
	if resp, _ = e.do(t, http.MethodPost, path, testToken, map[string]any{"bogus": 1}); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad body: %d", resp.StatusCode)
	}

	// The template is gone: 404.
	tmpl := &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: "digest", Namespace: testNS}}
	if err := e.k8s.Delete(context.Background(), tmpl); err != nil {
		t.Fatal(err)
	}
	if resp, _ = e.do(t, http.MethodPost, path, testToken, nil); resp.StatusCode != http.StatusNotFound {
		t.Errorf("missing template: %d, want 404", resp.StatusCode)
	}

	// The template lost its opt-in: 403.
	if err := e.k8s.Create(context.Background(), templateChain("digest", false)); err != nil {
		t.Fatal(err)
	}
	if resp, _ = e.do(t, http.MethodPost, path, testToken, nil); resp.StatusCode != http.StatusForbidden {
		t.Errorf("non-invokable template: %d, want 403", resp.StatusCode)
	}
}

func TestRerunRunWithoutTemplateLink(t *testing.T) {
	e := newRunEnv(t, Options{}, true)
	resp, out := e.do(t, http.MethodPost, "/v1/chains/digest/runs", testToken, map[string]any{})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("start: %d %v", resp.StatusCode, out)
	}
	run, _ := out["run"].(string)
	e.finish(t, aiv1alpha1.ChainPhaseSucceeded, nil)

	// Drop the run-of annotation from the finished copy: the gateway no
	// longer knows which Chain to rerun.
	ctx := context.Background()
	var c aiv1alpha1.Chain
	if err := e.k8s.Get(ctx, client.ObjectKey{Namespace: testNS, Name: run}, &c); err != nil {
		t.Fatal(err)
	}
	delete(c.Annotations, annotationRunOf)
	if err := e.k8s.Update(ctx, &c); err != nil {
		t.Fatal(err)
	}

	resp, out = e.do(t, http.MethodPost, "/v1/runs/"+testNS+"/"+run+"/rerun", testToken, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("rerun without run-of: %d %v, want 404", resp.StatusCode, out)
	}
	if msg, _ := out["error"].(string); !strings.Contains(msg, "has no template chain") {
		t.Errorf("error = %q, want it to say the run has no template chain", msg)
	}
}

func TestRerunRunDisabled(t *testing.T) {
	e := newRunEnv(t, Options{}, false)
	if resp, _ := e.do(t, http.MethodPost, "/v1/runs/"+testNS+"/x/rerun", testToken, nil); resp.StatusCode != http.StatusNotFound {
		t.Errorf("disabled: %d", resp.StatusCode)
	}
}

func TestMCPRerunRun(t *testing.T) {
	e := newRunEnv(t, Options{}, true)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "v0"}, nil)
	cs, err := c.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:   e.server.URL + "/mcp",
		HTTPClient: &http.Client{Transport: bearer{http.DefaultTransport}},
		MaxRetries: -1,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cs.Close() }()

	if r, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "run_chain", Arguments: map[string]any{"chain": "digest", "input": "hello"}}); err != nil || r.IsError {
		t.Fatalf("run_chain: %v %v", err, r)
	}
	run := e.runs(t)[0].Name
	r, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "rerun_run", Arguments: map[string]any{"run": run}})
	if err != nil || !r.IsError {
		t.Fatalf("rerun of a running run should be a tool error: %v %v", err, r)
	}
	e.finish(t, aiv1alpha1.ChainPhaseSucceeded, nil)
	r, err = cs.CallTool(ctx, &mcp.CallToolParams{Name: "rerun_run", Arguments: map[string]any{"run": run}})
	if err != nil || r.IsError {
		t.Fatalf("rerun_run: %v %v", err, r)
	}
	if len(e.runs(t)) != 2 {
		t.Fatalf("runs: %d, want 2", len(e.runs(t)))
	}
	for _, rc := range e.runs(t) {
		if rc.Name != run && (rc.Spec.Input != "hello" || rc.Annotations[annotationRerunOf] != run) {
			t.Errorf("rerun copy: input %q annotations %v", rc.Spec.Input, rc.Annotations)
		}
	}
}
