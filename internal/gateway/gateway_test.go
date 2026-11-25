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
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/pkg/metrics"
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
	done    chan struct{} // closed on cleanup; stops held answers
	wg      sync.WaitGroup

	mu      sync.Mutex
	stopped bool
	seen    map[string]natspkg.TaskPayload // by subject's knight token
	last    natspkg.TaskPayload
	count   int // tasks received
}

func startFakeKnight(t *testing.T, url string) *fakeKnight {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	k := &fakeKnight{t: t, nc: nc, release: make(chan struct{}), done: make(chan struct{}), seen: map[string]natspkg.TaskPayload{}}
	// Real knights consume the tasks stream through a durable consumer.
	cons, err := js.CreateOrUpdateConsumer(context.Background(), "rt_dev_tasks", jetstream.ConsumerConfig{Durable: "Fake-consumer", AckPolicy: jetstream.AckExplicitPolicy})
	if err != nil {
		t.Fatal(err)
	}
	cc, err := cons.Consume(func(m jetstream.Msg) {
		_ = m.Ack()
		var p natspkg.TaskPayload
		if err := json.Unmarshal(m.Data(), &p); err != nil {
			t.Errorf("fake knight: bad payload: %v", err)
			return
		}
		k.mu.Lock()
		k.last = p
		k.count++
		k.seen[m.Subject()] = p
		if k.stopped {
			k.mu.Unlock()
			return
		}
		k.wg.Add(1)
		k.mu.Unlock()
		go func() {
			defer k.wg.Done()
			k.answer(js, p)
		}()
	})
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup runs before the NATS server shuts down (LIFO): stop accepting
	// tasks, release held answers and wait for every goroutine so none can
	// touch t after the test returns.
	t.Cleanup(func() {
		cc.Stop()
		k.mu.Lock()
		k.stopped = true
		k.mu.Unlock()
		close(k.done)
		k.wg.Wait()
		nc.Close()
	})
	return k
}

func (k *fakeKnight) answer(js jetstream.JetStream, p natspkg.TaskPayload) {
	if strings.Contains(p.Task, "hold") {
		select {
		case <-k.release:
		case <-k.done:
			return
		}
	}
	model := p.Model
	if model == "" {
		model = "configured-model"
	}
	var res any
	switch {
	case strings.Contains(p.Task, "fail"):
		res = natspkg.TaskResult{TaskID: p.TaskID, Error: "boom", Model: model}
	case strings.Contains(p.Task, "quota"):
		res = natspkg.TaskResult{TaskID: p.TaskID, Error: "usage limit reached", UsageLimited: true, Model: model}
	case strings.Contains(p.Task, "structured"):
		res = natspkg.TaskResult{TaskID: p.TaskID, Output: "reviewed", Model: model, Verdict: "needs_changes",
			PRs: []natspkg.PRRef{{Repo: "o/r", Number: 3, URL: "https://github.com/o/r/pull/3", Action: "updated"}}}
	case strings.Contains(p.Task, "prose-result"):
		res = natspkg.TaskResult{TaskID: p.TaskID, Output: "Opened #245.\nVerdict: SHIP IT", Model: model}
	case strings.Contains(p.Task, "pi-format"):
		ok := true
		res = natspkg.TaskResult{TaskID2: p.TaskID, Result: "pi says hi", Success: &ok, Model: model}
	default:
		res = natspkg.TaskResult{TaskID: p.TaskID, Output: "echo[" + p.RunID + "]: " + p.Task, Model: model}
	}
	data, _ := json.Marshal(res)
	if _, err := js.Publish(context.Background(), natspkg.ResultSubject(prefix, p.TaskID), data); err != nil {
		k.t.Errorf("fake knight: publish result: %v", err)
	}
}

func (k *fakeKnight) received() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.count
}

func (k *fakeKnight) lastPayload() natspkg.TaskPayload {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.last
}

type env struct {
	svc    *Service
	auth   *TokenAuth
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
	js, _ := jetstream.New(nc)
	// Same streams the RoundTable controller creates (WorkQueue default).
	for name, subj := range map[string]string{"rt_dev_tasks": prefix + ".tasks.>", "rt_dev_results": prefix + ".results.>"} {
		if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: name, Subjects: []string{subj}, Retention: jetstream.WorkQueuePolicy}); err != nil {
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

	bus, err := NewNATSBus(context.Background(), nc, "gateway_tasks_test", time.Hour)
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
	return &env{svc: svc, auth: auth, server: srv, knight: startFakeKnight(t, ns.ClientURL()), nc: nc}
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
	js, _ := jetstream.New(e.nc)
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := js.Stream(context.Background(), "rt_dev_results")
		if err != nil {
			t.Fatal(err)
		}
		info, err := st.Info(context.Background())
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

func TestInvokeStructuredResult(t *testing.T) {
	e := newEnv(t, Options{})
	_, body := e.do(t, "POST", "/v1/knights/operator/invoke", testToken, InvokeRequest{Task: "structured"})
	if body["verdict"] != "needs_changes" {
		t.Fatalf("verdict = %v", body["verdict"])
	}
	prs, _ := body["prs"].([]any)
	if len(prs) != 1 || prs[0].(map[string]any)["number"] != float64(3) || prs[0].(map[string]any)["action"] != "updated" {
		t.Fatalf("prs = %v", body["prs"])
	}

	// Prose fallback for knights that do not report the fields.
	_, body = e.do(t, "POST", "/v1/knights/operator/invoke", testToken, InvokeRequest{Task: "prose-result"})
	prs, _ = body["prs"].([]any)
	if body["verdict"] != "ship_it" || len(prs) != 1 || prs[0].(map[string]any)["number"] != float64(245) {
		t.Fatalf("fallback: verdict=%v prs=%v", body["verdict"], body["prs"])
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

func TestKnightUsageLimited(t *testing.T) {
	e := newEnv(t, Options{})
	resp, body := e.do(t, "POST", "/v1/knights/operator/invoke", testToken, InvokeRequest{Task: "over quota"})
	if resp.StatusCode != http.StatusOK || body["status"] != string(TaskFailed) || body["usageLimited"] != true {
		t.Fatalf("invoke: got %d %v", resp.StatusCode, body)
	}
	resp, got := e.do(t, "GET", "/v1/tasks/"+body["taskId"].(string), testToken, nil)
	if resp.StatusCode != http.StatusOK || got["status"] != string(TaskFailed) || got["usageLimited"] != true {
		t.Fatalf("get: got %d %v", resp.StatusCode, got)
	}
	// An ordinary failure carries no usageLimited key.
	_, plain := e.do(t, "POST", "/v1/knights/operator/invoke", testToken, InvokeRequest{Task: "please fail"})
	if _, ok := plain["usageLimited"]; ok {
		t.Fatalf("plain failure has usageLimited: %v", plain)
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
	res, _ = call("invoke_knight", map[string]any{"knight": "operator", "task": "over quota"})
	if !res.IsError || !strings.HasPrefix(text(res), "usage limited: ") {
		t.Fatalf("usage-limited task: %q", text(res))
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

// doKey is do with an Idempotency-Key header.
func (e *env) doKey(t *testing.T, path, key string, body any) (*http.Response, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, e.server.URL+path, bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+testToken)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
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

func TestInvokeIdempotencyKey(t *testing.T) {
	e := newEnv(t, Options{})
	path := "/v1/knights/operator/invoke"

	r1, b1 := e.doKey(t, path, "k-1", InvokeRequest{Task: "hello"})
	if r1.StatusCode != http.StatusOK || r1.Header.Get("Idempotent-Replay") != "" {
		t.Fatalf("first: %d %v replay=%q", r1.StatusCode, b1, r1.Header.Get("Idempotent-Replay"))
	}
	r2, b2 := e.doKey(t, path, "k-1", InvokeRequest{Task: "hello"})
	if r2.StatusCode != http.StatusOK || r2.Header.Get("Idempotent-Replay") != "true" {
		t.Fatalf("replay: %d %v replay=%q", r2.StatusCode, b2, r2.Header.Get("Idempotent-Replay"))
	}
	if b1["taskId"] != b2["taskId"] || b2["output"] != b1["output"] {
		t.Fatalf("replay differs: %v vs %v", b1, b2)
	}
	if n := e.knight.received(); n != 1 {
		t.Fatalf("published %d tasks, want 1", n)
	}

	// Same key, different task: 422 and nothing published.
	r3, b3 := e.doKey(t, path, "k-1", InvokeRequest{Task: "other"})
	if r3.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("different body: %d %v", r3.StatusCode, b3)
	}
	if n := e.knight.received(); n != 1 {
		t.Fatalf("published %d tasks after 422, want 1", n)
	}

	// Another key, or none, runs again; the key is scoped to the knight.
	_, b4 := e.doKey(t, path, "k-2", InvokeRequest{Task: "hello"})
	_, b5 := e.doKey(t, path, "", InvokeRequest{Task: "hello"})
	_, b6 := e.doKey(t, "/v1/knights/reviewer/invoke", "k-1", InvokeRequest{Task: "hello"})
	if b4["taskId"] == b1["taskId"] || b5["taskId"] == b1["taskId"] || b6["taskId"] == b1["taskId"] {
		t.Fatalf("new key/none/other knight must make a new task")
	}
	if n := e.knight.received(); n != 4 {
		t.Fatalf("published %d tasks, want 4", n)
	}

	// Invalid keys are 400.
	for _, k := range []string{strings.Repeat("a", 256), "caf\u00e9"} {
		if r, b := e.doKey(t, path, k, InvokeRequest{Task: "hello"}); r.StatusCode != http.StatusBadRequest {
			t.Fatalf("key %q: %d %v", k, r.StatusCode, b)
		}
	}
}

func TestInvokeIdempotencyKeyReplayPending(t *testing.T) {
	e := newEnv(t, Options{})
	zero := 0
	path := "/v1/knights/operator/invoke"
	r1, b1 := e.doKey(t, path, "p", InvokeRequest{Task: "hold", TimeoutSeconds: &zero})
	r2, b2 := e.doKey(t, path, "p", InvokeRequest{Task: "hold", TimeoutSeconds: &zero})
	if r1.StatusCode != http.StatusAccepted || r2.StatusCode != http.StatusAccepted ||
		r2.Header.Get("Idempotent-Replay") != "true" || b1["taskId"] != b2["taskId"] {
		t.Fatalf("pending replay: %d/%d %v %v", r1.StatusCode, r2.StatusCode, b1, b2)
	}
	close(e.knight.release)
}

func TestRequestMetrics(t *testing.T) {
	metrics.GatewayRequestsTotal.Reset()
	metrics.GatewayRequestDurationSeconds.Reset()
	e := newEnv(t, Options{})
	e.do(t, "POST", "/v1/knights/operator/invoke", testToken, InvokeRequest{Task: "hello"})
	e.do(t, "POST", "/v1/knights/nobody/invoke", testToken, InvokeRequest{Task: "hello"})
	e.do(t, "GET", "/v1/knights", "", nil)
	e.do(t, "GET", "/v1/knights", testToken, nil)
	e.do(t, "GET", "/v1/tasks/some-id", testToken, nil)
	e.do(t, "GET", "/scanner/probe-1", testToken, nil)
	e.do(t, "GET", "/scanner/probe-2", testToken, nil)

	for _, c := range []struct {
		route, code, table string
		want               float64
	}{
		// The table is the knight's RoundTable; the path holds no knight name.
		{"POST /v1/knights/{name}/invoke", "200", "dev", 1},
		{"POST /v1/knights/{name}/invoke", "404", "none", 1},
		{"GET /v1/knights", "200", "none", 1},
		{"GET /v1/tasks/{taskId}", "404", "none", 1},
		// Unknown paths share one route label.
		{"unmatched", "404", "none", 2},
	} {
		if got := testutil.ToFloat64(metrics.GatewayRequestsTotal.WithLabelValues(c.route, c.code, c.table, CallerToken)); got != c.want {
			t.Errorf("requests{%s,%s,%s} = %v, want %v", c.route, c.code, c.table, got, c.want)
		}
	}
	// A refused token has no caller.
	if got := testutil.ToFloat64(metrics.GatewayRequestsTotal.WithLabelValues(unmatchedRoute, "401", noTable, callerNone)); got != 1 {
		t.Errorf("refused requests = %v, want 1", got)
	}
	if n := testutil.CollectAndCount(metrics.GatewayRequestDurationSeconds); n != 4 {
		t.Errorf("duration series = %d, want 4 (one per route template)", n)
	}
}
