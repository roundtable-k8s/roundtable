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
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/prometheus/client_golang/prometheus/testutil"
	authnv1 "k8s.io/api/authentication/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/pkg/metrics"
)

// fakeSAToken is a token the fake API server knows.
type fakeSAToken struct {
	user      string
	audiences []string
	exp       time.Time
}

// fakeReviewer plays the API server's TokenReview: a token authenticates
// only for an audience it was issued for and only before it expires.
type fakeReviewer struct {
	mu     sync.Mutex
	now    func() time.Time
	tokens map[string]fakeSAToken
	err    error
	calls  int
}

func (f *fakeReviewer) Review(_ context.Context, token string, audiences []string) (authnv1.TokenReviewStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return authnv1.TokenReviewStatus{}, f.err
	}
	t, ok := f.tokens[token]
	if !ok || (!t.exp.IsZero() && !f.now().Before(t.exp)) {
		return authnv1.TokenReviewStatus{Error: "invalid bearer token"}, nil
	}
	var matched []string
	for _, a := range audiences {
		if slices.Contains(t.audiences, a) {
			matched = append(matched, a)
		}
	}
	if len(matched) == 0 {
		return authnv1.TokenReviewStatus{Error: "token audiences is invalid for the target audiences"}, nil
	}
	return authnv1.TokenReviewStatus{Authenticated: true, Audiences: matched,
		User: authnv1.UserInfo{Username: t.user}}, nil
}

func (f *fakeReviewer) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// fakeJWT is a token shaped like a projected ServiceAccount token, expiring
// at exp (unsigned: only the fake reviewer checks it).
func fakeJWT(id string, exp time.Time) string {
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(`{"alg":"RS256"}`)) + "." +
		enc(fmt.Appendf(nil, `{"sub":%q,"exp":%d}`, id, exp.Unix())) + "." + enc([]byte("sig"))
}

// clock is a settable test clock.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// Tokens of the SA fixture.
type saFixture struct {
	clock    *clock
	reviewer *fakeReviewer
	auth     *ServiceAccountAuth
	// valid is jobs/runner's token for the gateway's audience, expiring in
	// 10 minutes; the others are refused one way or another.
	valid, otherAudience, expired, user, outsider string
}

func newSAFixture(allowed ...string) *saFixture {
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	f := &saFixture{clock: c}
	exp := c.t.Add(10 * time.Minute)
	f.valid = fakeJWT("valid", exp)
	f.otherAudience = fakeJWT("other-audience", exp)
	f.expired = fakeJWT("expired", c.t.Add(-time.Minute))
	f.user = fakeJWT("user", exp)
	f.outsider = fakeJWT("outsider", exp)
	f.reviewer = &fakeReviewer{now: c.now, tokens: map[string]fakeSAToken{
		f.valid:         {user: "system:serviceaccount:jobs:runner", audiences: []string{"roundtable-gateway"}, exp: exp},
		f.otherAudience: {user: "system:serviceaccount:jobs:runner", audiences: []string{"https://kubernetes.default.svc"}, exp: exp},
		f.expired:       {user: "system:serviceaccount:jobs:runner", audiences: []string{"roundtable-gateway"}, exp: c.t.Add(-time.Minute)},
		f.user:          {user: "alice", audiences: []string{"roundtable-gateway"}, exp: exp},
		f.outsider:      {user: "system:serviceaccount:other:runner", audiences: []string{"roundtable-gateway"}, exp: exp},
	}}
	f.auth = &ServiceAccountAuth{Reviewer: f.reviewer, AllowedNamespaces: allowed, now: c.now}
	return f
}

func TestServiceAccountAuth(t *testing.T) {
	f := newSAFixture("jobs", "ops")
	ctx := context.Background()

	c, err := f.auth.Authenticate(ctx, f.valid)
	if err != nil || c != (Caller{Kind: CallerServiceAccount, Name: "jobs/runner"}) {
		t.Fatalf("valid token: %+v, %v", c, err)
	}

	for _, tc := range []struct {
		name, token string
		code        int
	}{
		{"another audience", f.otherAudience, http.StatusUnauthorized},
		{"expired", f.expired, http.StatusUnauthorized},
		{"unknown", "not-a-token-at-all", http.StatusUnauthorized},
		{"empty", "", http.StatusUnauthorized},
		{"not a ServiceAccount", f.user, http.StatusForbidden},
		{"namespace not allowed", f.outsider, http.StatusForbidden},
	} {
		if _, err := f.auth.Authenticate(ctx, tc.token); err == nil || authStatus(err) != tc.code {
			t.Errorf("%s: err %v (status %d), want %d", tc.name, err, authStatus(err), tc.code)
		}
	}

	// Any namespace when the list is empty or holds "*".
	for _, allowed := range [][]string{nil, {"*"}} {
		g := newSAFixture(allowed...)
		if c, err := g.auth.Authenticate(ctx, g.outsider); err != nil || c.Name != "other/runner" {
			t.Errorf("allowed %v: %+v, %v", allowed, c, err)
		}
	}
}

func TestServiceAccountAuthSkipsReviewForExpiredJWT(t *testing.T) {
	f := newSAFixture()
	if _, err := f.auth.Authenticate(context.Background(), f.expired); authStatus(err) != http.StatusUnauthorized {
		t.Fatalf("expired: %v", err)
	}
	if n := f.reviewer.callCount(); n != 0 {
		t.Fatalf("expired JWT sent to TokenReview (%d calls)", n)
	}
}

func TestServiceAccountAuthCache(t *testing.T) {
	f := newSAFixture()
	ctx := context.Background()
	auth := func(tok string) error { _, err := f.auth.Authenticate(ctx, tok); return err }

	// A success is cached...
	f.auth.MaxCacheTTL = time.Hour
	for range 3 {
		if err := auth(f.valid); err != nil {
			t.Fatal(err)
		}
	}
	f.clock.advance(9*time.Minute + 29*time.Second)
	if err := auth(f.valid); err != nil {
		t.Fatal(err)
	}
	if n := f.reviewer.callCount(); n != 1 {
		t.Fatalf("reviews = %d, want 1 (cached)", n)
	}
	// ...but not past shortly before the token expires (expiry - 30s): at
	// 9m31s the token is reviewed again, and past its expiry it is refused.
	f.clock.advance(2 * time.Second)
	if err := auth(f.valid); err != nil {
		t.Fatal(err)
	}
	if n := f.reviewer.callCount(); n != 2 {
		t.Fatalf("reviews = %d, want 2 (cache entry outlived expiry-skew)", n)
	}
	f.clock.advance(30 * time.Second)
	if err := auth(f.valid); authStatus(err) != http.StatusUnauthorized {
		t.Fatalf("token used after expiry: %v", err)
	}

	// MaxCacheTTL caps a success with a far expiry.
	g := newSAFixture()
	g.auth.MaxCacheTTL = time.Minute
	long := fakeJWT("long", g.clock.now().Add(24*time.Hour))
	g.reviewer.tokens[long] = fakeSAToken{user: "system:serviceaccount:jobs:runner", audiences: []string{"roundtable-gateway"}}
	_, _ = g.auth.Authenticate(ctx, long)
	g.clock.advance(61 * time.Second)
	_, _ = g.auth.Authenticate(ctx, long)
	if n := g.reviewer.callCount(); n != 2 {
		t.Fatalf("reviews = %d, want 2 (MaxCacheTTL)", n)
	}

	// A refusal is cached only for FailureTTL.
	h := newSAFixture()
	for range 3 {
		_, _ = h.auth.Authenticate(ctx, h.user)
	}
	if n := h.reviewer.callCount(); n != 1 {
		t.Fatalf("reviews = %d, want 1 (refusal cached briefly)", n)
	}
	h.clock.advance(6 * time.Second)
	_, _ = h.auth.Authenticate(ctx, h.user)
	if n := h.reviewer.callCount(); n != 2 {
		t.Fatalf("reviews = %d, want 2 (refusal cached past FailureTTL)", n)
	}

	// A failed TokenReview is a 503 and is not cached.
	h.reviewer.err = errors.New("apiserver down")
	_, err := h.auth.Authenticate(ctx, fakeJWT("fresh", h.clock.now().Add(time.Hour)))
	if !errors.Is(err, ErrUnavailable) || authStatus(err) != http.StatusServiceUnavailable {
		t.Fatalf("review error: %v", err)
	}
	if h.auth.cached() != 1 {
		t.Fatalf("cache holds %d entries, want only the refusal", h.auth.cached())
	}
}

func TestServiceAccountAuthCacheBounded(t *testing.T) {
	f := newSAFixture()
	f.auth.MaxEntries = 3
	for i := range 10 {
		tok := fakeJWT(fmt.Sprint("t", i), f.clock.now().Add(time.Hour))
		f.reviewer.tokens[tok] = fakeSAToken{user: "system:serviceaccount:jobs:runner", audiences: []string{"roundtable-gateway"}}
		if _, err := f.auth.Authenticate(context.Background(), tok); err != nil {
			t.Fatal(err)
		}
		if n := f.auth.cached(); n > 3 {
			t.Fatalf("cache holds %d entries, bound is 3", n)
		}
	}
}

func TestJWTExpiry(t *testing.T) {
	exp := time.Unix(1_900_000_000, 0)
	if got, ok := jwtExpiry(fakeJWT("x", exp)); !ok || !got.Equal(exp) {
		t.Fatalf("exp = %v, %v", got, ok)
	}
	for _, tok := range []string{"opaque-token-0123456789", "a.b.c", "a.e30.c"} {
		if _, ok := jwtExpiry(tok); ok {
			t.Errorf("%q has an expiry", tok)
		}
	}
}

// bearerToken sends the given bearer token.
type bearerToken struct {
	token string
	base  http.RoundTripper
}

func (b bearerToken) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return b.base.RoundTrip(r)
}

func TestHTTPServiceAccountAuth(t *testing.T) {
	metrics.GatewayRequestsTotal.Reset()
	e := newEnv(t, Options{})
	f := newSAFixture("jobs")
	e.auth.ServiceAccounts = f.auth

	resp, body := e.do(t, "POST", "/v1/knights/operator/invoke", f.valid, InvokeRequest{Task: "hello"})
	if resp.StatusCode != http.StatusOK || body["caller"] != "jobs/runner" {
		t.Fatalf("ServiceAccount invoke: %d %v", resp.StatusCode, body)
	}
	resp, body = e.do(t, "GET", "/v1/tasks/"+body["taskId"].(string), f.valid, nil)
	if resp.StatusCode != http.StatusOK || body["caller"] != "jobs/runner" {
		t.Fatalf("stored task record: %d %v", resp.StatusCode, body)
	}
	// Static tokens keep working and are recorded by name.
	resp, body = e.do(t, "POST", "/v1/knights/operator/invoke", testToken, InvokeRequest{Task: "hello"})
	if resp.StatusCode != http.StatusOK || body["caller"] != defaultTokenName {
		t.Fatalf("static token invoke: %d %v", resp.StatusCode, body)
	}

	for _, tc := range []struct {
		name, token string
		code        int
	}{
		{"another audience", f.otherAudience, http.StatusUnauthorized},
		{"expired", f.expired, http.StatusUnauthorized},
		{"unknown", "garbage-token-0123456789", http.StatusUnauthorized},
		{"not a ServiceAccount", f.user, http.StatusForbidden},
		{"namespace not allowed", f.outsider, http.StatusForbidden},
	} {
		if resp, body := e.do(t, "GET", "/v1/knights", tc.token, nil); resp.StatusCode != tc.code {
			t.Errorf("%s: %d %v, want %d", tc.name, resp.StatusCode, body, tc.code)
		}
	}

	route := "POST /v1/knights/{name}/invoke"
	if got := testutil.ToFloat64(metrics.GatewayRequestsTotal.WithLabelValues(route, "200", "dev", CallerServiceAccount)); got != 1 {
		t.Errorf("serviceaccount requests = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.GatewayRequestsTotal.WithLabelValues(route, "200", "dev", CallerToken)); got != 1 {
		t.Errorf("token requests = %v, want 1", got)
	}

	// Without ServiceAccount auth, a ServiceAccount token is just unknown.
	e.auth.ServiceAccounts = nil
	if resp, _ := e.do(t, "GET", "/v1/knights", f.valid, nil); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("ServiceAccount auth off: %d", resp.StatusCode)
	}
}

func TestMCPServiceAccountAuth(t *testing.T) {
	e := newEnv(t, Options{})
	f := newSAFixture()
	e.auth.ServiceAccounts = f.auth
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	connect := func(token string) (*mcp.ClientSession, error) {
		c := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "v0"}, nil)
		return c.Connect(ctx, &mcp.StreamableClientTransport{
			Endpoint:   e.server.URL + "/mcp",
			HTTPClient: &http.Client{Transport: bearerToken{token, http.DefaultTransport}},
			MaxRetries: -1,
		}, nil)
	}
	if cs, err := connect(f.user); err == nil {
		_ = cs.Close()
		t.Fatal("MCP accepted a non-ServiceAccount token")
	}
	cs, err := connect(f.valid)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cs.Close() }()
	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "invoke_knight",
		Arguments: map[string]any{"knight": "operator", "task": "hello"}})
	if err != nil || res.IsError {
		t.Fatalf("invoke_knight: %v %+v", err, res)
	}
	rec, _ := res.StructuredContent.(map[string]any)
	if rec["caller"] != "jobs/runner" {
		t.Fatalf("MCP task record caller: %v", res.StructuredContent)
	}
}

func TestNamedStaticTokens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("dashboard dash-token-0123456789\nplain-token-0123456789\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	a, err := NewTokenAuth(path, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if name, ok := a.match("dash-token-0123456789"); !ok || name != "dashboard" {
		t.Errorf("named token: %q %v", name, ok)
	}
	if name, ok := a.match("plain-token-0123456789"); !ok || name != defaultTokenName {
		t.Errorf("unnamed token: %q %v", name, ok)
	}
	if _, ok := a.match("dashboard dash-token-0123456789"); ok {
		t.Error("the whole line matched as a token")
	}
	for _, bad := range []string{"ns/name some-token-0123456789\n", "a b c-token-0123456789\n", "name short\n"} {
		if err := os.WriteFile(path, []byte(bad), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := NewTokenAuth(path, time.Minute); err == nil {
			t.Errorf("token file %q accepted", bad)
		}
	}
}

func TestCallerAnnotations(t *testing.T) {
	ctx := WithCaller(context.Background(), Caller{Kind: CallerServiceAccount, Name: "jobs/runner"})

	r := newRunEnv(t, Options{}, true)
	rec, err := r.svc.RunChain(ctx, "digest", RunChainRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if runs := r.runs(t); len(runs) != 1 || runs[0].Annotations[aiv1alpha1.AnnotationCaller] != "jobs/runner" {
		t.Fatalf("run copy annotations: %+v", runs)
	}
	if rec.Caller != "jobs/runner" {
		t.Errorf("run record caller %q", rec.Caller)
	}

	m := newMissionEnv(t, Options{}, true)
	req := StartMissionRequest{Objective: "o", SuccessCriteria: "s", RoundTable: "dev", CostBudgetUSD: 1,
		Knights: []string{"operator"}, Chains: []MissionChainRequest{{Name: "research"}}}
	v, err := m.svc.StartMission(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.mission(t, v.Name).Annotations[aiv1alpha1.AnnotationCaller]; got != "jobs/runner" || v.Caller != "jobs/runner" {
		t.Fatalf("mission caller: annotation %q, view %q", got, v.Caller)
	}

	// No caller (a direct Service call), no annotation.
	v, err = m.svc.StartMission(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := m.mission(t, v.Name).Annotations[aiv1alpha1.AnnotationCaller]; ok {
		t.Error("annotation without a caller")
	}
}
