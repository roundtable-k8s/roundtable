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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

const (
	testToken = "s3cret-token-0123456789"
	testNS    = "roundtable"
	prefix    = "rt-dev"
)

// fakeKnight answers tasks on <prefix>.tasks.> the way a knight runtime does:
// it publishes the result to <prefix>.results.<taskId>. Tasks containing
// "hold" wait until release is closed; "fail" returns an error.
type fakeKnight struct {
	t       *testing.T
	nc      *nats.Conn
	release chan struct{}

	mu   sync.Mutex
	seen map[string]natspkg.TaskPayload // by subject's knight token
	last natspkg.TaskPayload
}

func startFakeKnight(t *testing.T, url string) *fakeKnight {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	k := &fakeKnight{t: t, nc: nc, release: make(chan struct{}), seen: map[string]natspkg.TaskPayload{}}
	// Real knights consume the tasks stream through a durable consumer.
	sub, err := js.Subscribe(prefix+".tasks.>", func(m *nats.Msg) {
		_ = m.Ack()
		var p natspkg.TaskPayload
		if err := json.Unmarshal(m.Data, &p); err != nil {
			t.Errorf("fake knight: bad payload: %v", err)
			return
		}
		k.mu.Lock()
		k.last = p
		k.seen[m.Subject] = p
		k.mu.Unlock()
		go k.answer(js, p)
	}, nats.Durable("Fake-consumer"), nats.ManualAck(), nats.BindStream("rt_dev_tasks"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe(); nc.Close() })
	return k
}

func (k *fakeKnight) answer(js nats.JetStreamContext, p natspkg.TaskPayload) {
	if strings.Contains(p.Task, "hold") {
		<-k.release
	}
	model := p.Model
	if model == "" {
		model = "configured-model"
	}
	var res any
	switch {
	case strings.Contains(p.Task, "fail"):
		res = natspkg.TaskResult{TaskID: p.TaskID, Error: "boom", Model: model}
	case strings.Contains(p.Task, "pi-format"):
		ok := true
		res = natspkg.TaskResult{TaskID2: p.TaskID, Result: "pi says hi", Success: &ok, Model: model}
	default:
		res = natspkg.TaskResult{TaskID: p.TaskID, Output: "echo[" + p.RunID + "]: " + p.Task, Model: model}
	}
	data, _ := json.Marshal(res)
	if _, err := js.Publish(natspkg.ResultSubject(prefix, p.TaskID), data); err != nil {
		k.t.Errorf("fake knight: publish result: %v", err)
	}
}

func (k *fakeKnight) lastPayload() natspkg.TaskPayload {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.last
}

type env struct {
	svc    *Service
	server *httptest.Server
	knight *fakeKnight
	nc     *nats.Conn
}

func runNATS(t *testing.T) *natsserver.Server {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	s := natstest.RunServer(&opts)
	t.Cleanup(s.Shutdown)
	return s
}

func newEnv(t *testing.T, opts Options) *env {
	t.Helper()
	ns := runNATS(t)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, _ := nc.JetStream()
	// Same streams the RoundTable controller creates (WorkQueue default).
	for name, subj := range map[string]string{"rt_dev_tasks": prefix + ".tasks.>", "rt_dev_results": prefix + ".results.>"} {
		if _, err := js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{subj}, Retention: nats.WorkQueuePolicy}); err != nil {
			t.Fatal(err)
		}
	}

	scheme := runtime.NewScheme()
	_ = aiv1alpha1.AddToScheme(scheme)
	objs := []runtime.Object{
		&aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: testNS},
			Spec: aiv1alpha1.RoundTableSpec{
				NATS:         aiv1alpha1.RoundTableNATS{SubjectPrefix: prefix, TasksStream: "rt_dev_tasks", ResultsStream: "rt_dev_results"},
				ModelAliases: map[string]string{"deep": "opus"},
			},
		},
		knight("operator", "claude", "ghcr.io/roundtable-k8s/claude-knight:latest", false),
		knight("reviewer", "review", "ghcr.io/roundtable-k8s/pi-knight:latest", false),
		knight("sleepy", "ops", "", true),
	}
	k8s := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()

	bus, err := NewNATSBus(nc, "gateway_tasks_test", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Namespace == "" {
		opts.Namespace = testNS
	}
	if opts.DefaultTimeout == 0 {
		opts.DefaultTimeout = 5 * time.Second
	}
	svc := NewService(&Resolver{Client: k8s}, bus, opts, logr.Discard())

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
	return &env{svc: svc, server: srv, knight: startFakeKnight(t, ns.ClientURL()), nc: nc}
}

func knight(name, domain, image string, suspended bool) *aiv1alpha1.Knight {
	return &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Labels: map[string]string{aiv1alpha1.LabelRoundTable: "dev"}},
		Spec: aiv1alpha1.KnightSpec{
			Domain: domain, Image: image, Model: "default", Suspend: suspended,
			NATS: aiv1alpha1.KnightNATS{Subjects: []string{prefix + ".tasks." + domain + ".>"}},
		},
	}
}

func (e *env) do(t *testing.T, method, path, token string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var rd *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	} else {
		rd = bytes.NewReader(nil)
	}
	req, _ := http.NewRequest(method, e.server.URL+path, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp, out
}

func TestInvokeReturnsResult(t *testing.T) {
	e := newEnv(t, Options{})
	resp, body := e.do(t, "POST", "/v1/knights/operator/invoke", testToken, InvokeRequest{Task: "hello", Model: "deep"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %v", resp.StatusCode, body)
	}
	if body["status"] != string(TaskSucceeded) {
		t.Fatalf("status = %v", body)
	}
	runID, _ := body["runId"].(string)
	if runID == "" || body["output"] != "echo["+runID+"]: hello" {
		t.Fatalf("output = %v (runId %q)", body["output"], runID)
	}
	// The alias resolved through the RoundTable and went out with the task.
	if body["requestedModel"] != "opus" || body["model"] != "opus" {
		t.Fatalf("models = %v / %v", body["requestedModel"], body["model"])
	}
	p := e.knight.lastPayload()
	if p.TaskID != body["taskId"] || p.RunID != runID || p.Model != "opus" || p.Task != "hello" {
		t.Fatalf("payload = %+v", p)
	}
	if body["resultSubject"] != prefix+".results."+p.TaskID {
		t.Fatalf("resultSubject = %v", body["resultSubject"])
	}

	// The recorded result was removed from the WorkQueue results stream
	// (nobody else acks it) ...
	js, _ := e.nc.JetStream()
	deadline := time.Now().Add(5 * time.Second)
	for {
		info, err := js.StreamInfo("rt_dev_results")
		if err != nil {
			t.Fatal(err)
		}
		if info.State.Msgs == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("results stream still holds %d messages", info.State.Msgs)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// ... and GET still returns it, from the task record.
	for range 2 {
		resp, got := e.do(t, "GET", "/v1/tasks/"+p.TaskID, testToken, nil)
		if resp.StatusCode != http.StatusOK || got["output"] != body["output"] || got["status"] != string(TaskSucceeded) {
			t.Fatalf("GET = %d %v", resp.StatusCode, got)
		}
	}
}

func TestInvokeTaskSubject(t *testing.T) {
	e := newEnv(t, Options{})
	resp, body := e.do(t, "POST", "/v1/knights/reviewer/invoke", testToken, InvokeRequest{Task: "pi-format"})
	if resp.StatusCode != http.StatusOK || body["output"] != "pi says hi" || body["status"] != string(TaskSucceeded) {
		t.Fatalf("got %d %v", resp.StatusCode, body)
	}
	e.knight.mu.Lock()
	_, ok := e.knight.seen[prefix+".tasks.review.reviewer"]
	e.knight.mu.Unlock()
	if !ok {
		t.Fatalf("task not published on %s.tasks.review.reviewer: %v", prefix, e.knight.seen)
	}
	// Absent model means "use your configured model".
	if p := e.knight.lastPayload(); p.Model != "" {
		t.Fatalf("model sent = %q", p.Model)
	}
}

func TestInvokeTimeoutThenGet(t *testing.T) {
	e := newEnv(t, Options{})
	resp, body := e.do(t, "POST", "/v1/knights/operator/invoke", testToken,
		map[string]any{"task": "hold on", "timeoutSeconds": 1})
	if resp.StatusCode != http.StatusAccepted || body["status"] != string(TaskPending) {
		t.Fatalf("got %d %v", resp.StatusCode, body)
	}
	taskID, _ := body["taskId"].(string)
	if taskID == "" || resp.Header.Get("Location") != "/v1/tasks/"+taskID {
		t.Fatalf("taskId %q location %q", taskID, resp.Header.Get("Location"))
	}
	_, got := e.do(t, "GET", "/v1/tasks/"+taskID, testToken, nil)
	if got["status"] != string(TaskPending) {
		t.Fatalf("before release: %v", got)
	}

	close(e.knight.release)
	// Nobody is waiting now: GET reads the result from the results stream.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, got = e.do(t, "GET", "/v1/tasks/"+taskID, testToken, nil)
		if got["status"] != string(TaskPending) || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got["status"] != string(TaskSucceeded) || !strings.HasSuffix(got["output"].(string), "hold on") || got["completedAt"] == nil {
		t.Fatalf("after release: %v", got)
	}
}

func TestInvokeZeroTimeoutIsAsync(t *testing.T) {
	e := newEnv(t, Options{})
	close(e.knight.release)
	resp, body := e.do(t, "POST", "/v1/knights/operator/invoke", testToken, InvokeRequest{Task: "hold", TimeoutSeconds: new(0)})
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d %v", resp.StatusCode, body)
	}
	if body["taskId"] == "" {
		t.Fatalf("no taskId: %v", body)
	}
}

func TestFollowUpReusesRunID(t *testing.T) {
	e := newEnv(t, Options{})
	_, first := e.do(t, "POST", "/v1/knights/operator/invoke", testToken, InvokeRequest{Task: "one"})
	runID := first["runId"].(string)
	_, second := e.do(t, "POST", "/v1/knights/operator/invoke", testToken, InvokeRequest{Task: "two", RunID: runID})
	if second["runId"] != runID || second["taskId"] == first["taskId"] {
		t.Fatalf("second = %v", second)
	}
	if p := e.knight.lastPayload(); p.RunID != runID {
		t.Fatalf("payload runId = %q, want %q", p.RunID, runID)
	}

	resp, list := e.do(t, "GET", "/v1/tasks?runId="+runID, testToken, nil)
	tasks, _ := list["tasks"].([]any)
	if resp.StatusCode != http.StatusOK || len(tasks) != 2 || tasks[0].(map[string]any)["taskId"] != second["taskId"] {
		t.Fatalf("list = %d %v", resp.StatusCode, list)
	}
}

func TestKnightFailure(t *testing.T) {
	e := newEnv(t, Options{})
	resp, body := e.do(t, "POST", "/v1/knights/operator/invoke", testToken, InvokeRequest{Task: "please fail"})
	if resp.StatusCode != http.StatusOK || body["status"] != string(TaskFailed) || body["error"] != "boom" {
		t.Fatalf("got %d %v", resp.StatusCode, body)
	}
}

func TestInvokeErrors(t *testing.T) {
	e := newEnv(t, Options{})
	cases := []struct {
		name, path string
		body       any
		want       int
	}{
		{"unknown knight", "/v1/knights/nobody/invoke", InvokeRequest{Task: "x"}, http.StatusNotFound},
		{"empty task", "/v1/knights/operator/invoke", InvokeRequest{Task: " "}, http.StatusBadRequest},
		{"model wrong for runtime", "/v1/knights/operator/invoke", InvokeRequest{Task: "x", Model: "openrouter/z-ai/glm-5.3"}, http.StatusBadRequest},
		{"pi model needs provider", "/v1/knights/reviewer/invoke", InvokeRequest{Task: "x", Model: "opus"}, http.StatusBadRequest},
		{"bad runId", "/v1/knights/operator/invoke", InvokeRequest{Task: "x", RunID: "a.>"}, http.StatusBadRequest},
		{"namespace not allowed", "/v1/knights/operator/invoke", InvokeRequest{Task: "x", Namespace: "kube-system"}, http.StatusBadRequest},
		{"unknown field", "/v1/knights/operator/invoke", map[string]any{"task": "x", "bogus": 1}, http.StatusBadRequest},
		{"suspended", "/v1/knights/sleepy/invoke", InvokeRequest{Task: "x"}, http.StatusServiceUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := e.do(t, "POST", tc.path, testToken, tc.body)
			if resp.StatusCode != tc.want {
				t.Fatalf("status %d, want %d: %v", resp.StatusCode, tc.want, body)
			}
		})
	}
	resp, _ := e.do(t, "GET", "/v1/tasks/nope", testToken, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET unknown task: %d", resp.StatusCode)
	}
	resp, _ = e.do(t, "GET", "/v1/tasks/"+"a*b", testToken, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET wildcard task: %d", resp.StatusCode)
	}
}

func TestMaxInFlight(t *testing.T) {
	e := newEnv(t, Options{MaxInFlight: 1})
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.do(t, "POST", "/v1/knights/operator/invoke", testToken, InvokeRequest{Task: "hold", TimeoutSeconds: new(5)})
	}()
	// Wait until the first invocation holds the only slot.
	deadline := time.Now().Add(5 * time.Second)
	for len(e.svc.inflight) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	resp, body := e.do(t, "POST", "/v1/knights/operator/invoke", testToken, InvokeRequest{Task: "x"})
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status %d: %v", resp.StatusCode, body)
	}
	close(e.knight.release)
	<-done
}

func TestAuth(t *testing.T) {
	e := newEnv(t, Options{})
	for _, tok := range []string{"", "wrong-token-0123456789"} {
		for _, path := range []string{"/v1/knights", "/v1/tasks", "/mcp"} {
			resp, _ := e.do(t, "GET", path, tok, nil)
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("%s with token %q: %d", path, tok, resp.StatusCode)
			}
		}
	}
	resp, _ := e.do(t, "POST", "/v1/knights/operator/invoke", "", InvokeRequest{Task: "x"})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("invoke without token: %d", resp.StatusCode)
	}
	resp, _ = e.do(t, "GET", "/healthz", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz: %d", resp.StatusCode)
	}
	resp, body := e.do(t, "GET", "/v1/knights", testToken, nil)
	if knights, _ := body["knights"].([]any); resp.StatusCode != http.StatusOK || len(knights) != 3 {
		t.Fatalf("knights: %d %v", resp.StatusCode, body)
	}
}

func TestTokenFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	write := func(s string) {
		if err := os.WriteFile(path, []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("\n")
	if _, err := NewTokenAuth(path, 0); err == nil {
		t.Fatal("empty token file accepted")
	}
	write("short\n")
	if _, err := NewTokenAuth(path, 0); err == nil {
		t.Fatal("short token accepted")
	}
	// Two tokens during a rotation; both work, and a rewrite is picked up.
	write("old-token-0123456789\nnew-token-0123456789\n")
	a, err := NewTokenAuth(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !a.valid("old-token-0123456789") || !a.valid("new-token-0123456789") || a.valid("other-token-0123456789") {
		t.Fatal("rotation tokens")
	}
	write("new-token-0123456789\n")
	if a.valid("old-token-0123456789") || !a.valid("new-token-0123456789") {
		t.Fatal("rotated token not picked up")
	}
	// An unreadable file keeps the last good tokens.
	_ = os.Remove(path)
	if !a.valid("new-token-0123456789") {
		t.Fatal("lost tokens when the file vanished")
	}
}

type bearer struct{ base http.RoundTripper }

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+testToken)
	return b.base.RoundTrip(r)
}

func TestMCP(t *testing.T) {
	e := newEnv(t, Options{})
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

	tools, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, tool := range tools.Tools {
		names[tool.Name] = true
	}
	for _, want := range []string{"list_knights", "invoke_knight", "get_task", "list_tasks"} {
		if !names[want] {
			t.Fatalf("missing tool %s in %v", want, names)
		}
	}

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
		if tc, ok := res.Content[0].(*mcp.TextContent); ok {
			return tc.Text
		}
		return ""
	}

	res, sc := call("list_knights", map[string]any{})
	if res.IsError || len(sc["knights"].([]any)) != 3 {
		t.Fatalf("list_knights: %v %v", text(res), sc)
	}

	res, sc = call("invoke_knight", map[string]any{"knight": "operator", "task": "hi there", "model": "sonnet"})
	if res.IsError || !strings.HasSuffix(text(res), "hi there") || sc["model"] != "sonnet" {
		t.Fatalf("invoke_knight: %q %v", text(res), sc)
	}
	runID := sc["runId"].(string)

	res, sc = call("invoke_knight", map[string]any{"knight": "operator", "task": "follow up", "runId": runID})
	if res.IsError || text(res) != "echo["+runID+"]: follow up" {
		t.Fatalf("follow-up: %q %v", text(res), sc)
	}

	res, sc = call("invoke_knight", map[string]any{"knight": "operator", "task": "hold", "timeoutSeconds": 0})
	if res.IsError || sc["status"] != string(TaskPending) || !strings.Contains(text(res), "get_task") {
		t.Fatalf("pending invoke: %q %v", text(res), sc)
	}
	pendingID := sc["taskId"].(string)
	close(e.knight.release)
	deadline := time.Now().Add(5 * time.Second)
	for {
		res, sc = call("get_task", map[string]any{"taskId": pendingID})
		if sc["status"] != string(TaskPending) || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if res.IsError || sc["status"] != string(TaskSucceeded) {
		t.Fatalf("get_task: %q %v", text(res), sc)
	}

	res, _ = call("invoke_knight", map[string]any{"knight": "operator", "task": "fail now"})
	if !res.IsError || !strings.Contains(text(res), "boom") {
		t.Fatalf("failed task: %q", text(res))
	}
	res, _ = call("invoke_knight", map[string]any{"knight": "nobody", "task": "x"})
	if !res.IsError || !strings.Contains(text(res), "not found") {
		t.Fatalf("unknown knight: %q", text(res))
	}

	res, sc = call("list_tasks", map[string]any{"runId": runID})
	if res.IsError || len(sc["tasks"].([]any)) != 2 {
		t.Fatalf("list_tasks: %q %v", text(res), sc)
	}
	res, sc = call("list_tasks", map[string]any{"status": "failed", "limit": 1})
	if res.IsError || len(sc["tasks"].([]any)) != 1 {
		t.Fatalf("list_tasks failed: %q %v", text(res), sc)
	}
}
