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
	return &runEnv{env: env{svc: svc, server: srv}, k8s: k8s}
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

	// Chains the gateway didn't start are not runs.
	for _, path := range []string{"/v1/runs/" + testNS + "/digest", "/v1/runs/" + testNS + "/digest-pickup-1", "/v1/runs/" + testNS + "/nope"} {
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
