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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

const (
	hookBearer   = "hook-bearer-token-0123456789"
	hookHMACKey  = "hook-hmac-key-0123456789"
	hookOtherNS  = "elsewhere"
	eventsStream = "rt_dev_tasks_EVENTS"
)

type hookEnv struct {
	server *httptest.Server
	js     jetstream.JetStream
	auth   *TokenAuth
	svc    *Service
}

func hookSecret(ns, name, value string, labelled bool) *corev1.Secret {
	s := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data:       map[string][]byte{"key": []byte(value + "\n")},
	}
	if labelled {
		s.Labels = map[string]string{aiv1alpha1.LabelWebhookSecret: "true"}
	}
	return s
}

func hookTrigger(ns, name string, auth aiv1alpha1.TriggerWebhookAuth) *aiv1alpha1.Trigger {
	return &aiv1alpha1.Trigger{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: aiv1alpha1.TriggerSpec{
			Source:        aiv1alpha1.TriggerSource{Webhook: &aiv1alpha1.TriggerWebhookSource{Auth: auth}},
			Target:        aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: "target"}},
			RoundTableRef: &corev1.LocalObjectReference{Name: "dev"},
		},
	}
}

func secretRef(name string) *corev1.SecretKeySelector {
	return &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: name}, Key: "key"}
}

// newHookEnv starts NATS with the table's events stream and a gateway whose
// fake API holds webhook Triggers and their Secrets in testNS and
// hookOtherNS. allowed are extra namespaces callers may target.
func newHookEnv(t *testing.T, hooks HookOptions, enabled bool, allowed ...string) *hookEnv {
	t.Helper()
	srv := runNATS(t)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, _ := jetstream.New(nc)
	// The events stream the RoundTable controller creates (Limits retention).
	if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name: eventsStream, Subjects: []string{prefix + ".events.>"}, Duplicates: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = aiv1alpha1.AddToScheme(scheme)
	rt := func(ns string) *aiv1alpha1.RoundTable {
		return &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: ns},
			Spec: aiv1alpha1.RoundTableSpec{NATS: aiv1alpha1.RoundTableNATS{
				SubjectPrefix: prefix, TasksStream: "rt_dev_tasks", ResultsStream: "rt_dev_results"}},
		}
	}
	natsTrigger := &aiv1alpha1.Trigger{
		ObjectMeta: metav1.ObjectMeta{Name: "nats-only", Namespace: testNS},
		Spec: aiv1alpha1.TriggerSpec{
			Source: aiv1alpha1.TriggerSource{NATS: &aiv1alpha1.TriggerNATSSource{Subject: prefix + ".events.x"}},
			Target: aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: "target"}},
		},
	}
	objs := []runtime.Object{
		rt(testNS), rt(hookOtherNS), natsTrigger,
		hookSecret(testNS, "bearer", hookBearer, true),
		hookSecret(testNS, "hmac", hookHMACKey, true),
		hookSecret(testNS, "unlabelled", hookBearer, false),
		hookSecret(hookOtherNS, "bearer", hookBearer, true),
		hookSecret(testNS, "short", "too-short", true),
		hookTrigger(testNS, "short", aiv1alpha1.TriggerWebhookAuth{BearerTokenSecretRef: secretRef("short")}),
		hookTrigger(testNS, "alerts", aiv1alpha1.TriggerWebhookAuth{BearerTokenSecretRef: secretRef("bearer")}),
		hookTrigger(testNS, "signed", aiv1alpha1.TriggerWebhookAuth{HMACSecretRef: secretRef("hmac")}),
		hookTrigger(testNS, "unlabelled", aiv1alpha1.TriggerWebhookAuth{BearerTokenSecretRef: secretRef("unlabelled")}),
		hookTrigger(hookOtherNS, "alerts", aiv1alpha1.TriggerWebhookAuth{BearerTokenSecretRef: secretRef("bearer")}),
		fireScheduleTrigger(testNS, "nightly"), fireScheduleTrigger(hookOtherNS, "nightly"),
		&aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: "tabled", Namespace: testNS},
			Spec:       aiv1alpha1.ChainSpec{RoundTableRef: &corev1.LocalObjectReference{Name: "dev"}},
		},
		func() *aiv1alpha1.Trigger {
			trg := fireScheduleTrigger(testNS, "viachain")
			trg.Spec.RoundTableRef = nil // the target Chain's table
			trg.Spec.Target.ChainRef.Name = "tabled"
			return trg
		}(),
	}
	k8s := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	bus, err := NewNATSBus(context.Background(), nc, "gateway_tasks_test", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(&Resolver{Client: k8s}, bus, Options{Namespace: testNS, AllowedNamespaces: allowed, Hooks: hooks}, logr.Discard())
	if enabled {
		svc.Hooks = k8s
	}
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(testToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	auth, err := NewTokenAuth(tokenFile, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(Handler(svc, auth))
	t.Cleanup(server.Close)
	return &hookEnv{server: server, js: js, auth: auth, svc: svc}
}

func (e *hookEnv) post(t *testing.T, path string, body []byte, headers map[string]string) (*http.Response, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, e.server.URL+path, bytes.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	out := map[string]any{"_raw": string(raw)}
	_ = json.Unmarshal(raw, &out)
	return resp, out
}

func (e *hookEnv) msgs(t *testing.T) uint64 {
	t.Helper()
	st, err := e.js.Stream(context.Background(), eventsStream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := st.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Msgs
}

func sign(key string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func TestHookBearer(t *testing.T) {
	e := newHookEnv(t, HookOptions{}, true)
	body := []byte(`{"alerts":[{"status":"firing"}]}`)
	path := "/v1/hooks/" + testNS + "/alerts"

	for name, auth := range map[string]string{
		"none":          "",
		"wrong":         "Bearer not-the-right-token-at-all",
		"gateway token": "Bearer " + testToken,
		"wrong scheme":  "Basic " + hookBearer,
	} {
		h := map[string]string{"Content-Type": "application/json"}
		if auth != "" {
			h["Authorization"] = auth
		}
		resp, out := e.post(t, path, body, h)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s: status %d %v", name, resp.StatusCode, out)
		}
	}
	if n := e.msgs(t); n != 0 {
		t.Fatalf("refused deliveries published %d messages", n)
	}

	resp, out := e.post(t, path, body, map[string]string{
		"Authorization": "Bearer " + hookBearer, "Content-Type": "application/json",
		"Ce-Type": "com.example.alert", "Ce-Id": "abc-1", "X-Custom": "dropped",
	})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d %v", resp.StatusCode, out)
	}
	subject := natspkg.WebhookEventSubject(prefix, testNS, "alerts")
	if out["subject"] != subject || out["stream"] != eventsStream || out["seq"] != float64(1) {
		t.Fatalf("result = %v", out)
	}
	st, _ := e.js.Stream(context.Background(), eventsStream)
	msg, err := st.GetLastMsgForSubject(context.Background(), subject)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(msg.Data, body) {
		t.Fatalf("data = %s", msg.Data)
	}
	h := msg.Header
	if h.Get("Content-Type") != "application/json" || h.Get("Ce-Type") != "com.example.alert" ||
		h.Get(nats.MsgIdHdr) != testNS+"/alerts/abc-1" {
		t.Fatalf("headers = %v", h)
	}
	if h.Get("Authorization") != "" || h.Get("X-Custom") != "" {
		t.Fatalf("forwarded headers it should not: %v", h)
	}
}

func TestHookHMAC(t *testing.T) {
	e := newHookEnv(t, HookOptions{}, true)
	body := []byte(`{"involvedObject":{"kind":"Kustomization"},"severity":"error"}`)
	path := "/v1/hooks/" + testNS + "/signed"

	for name, h := range map[string]map[string]string{
		"github style": {"X-Hub-Signature-256": sign(hookHMACKey, body)},
		"flux style":   {"X-Signature": sign(hookHMACKey, body), "X-Github-Delivery": "d-1"},
	} {
		resp, out := e.post(t, path, body, h)
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("%s: status %d %v", name, resp.StatusCode, out)
		}
	}
	for name, h := range map[string]map[string]string{
		"none":           {},
		"wrong key":      {"X-Hub-Signature-256": sign("another-key-0123456789", body)},
		"other body":     {"X-Hub-Signature-256": sign(hookHMACKey, []byte(`{}`))},
		"sha1":           {"X-Hub-Signature-256": strings.Replace(sign(hookHMACKey, body), "sha256", "sha1", 1)},
		"not hex":        {"X-Hub-Signature-256": "sha256=zz"},
		"bearer instead": {"Authorization": "Bearer " + hookHMACKey},
		"gateway token":  {"Authorization": "Bearer " + testToken},
	} {
		resp, out := e.post(t, path, body, h)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s: status %d %v", name, resp.StatusCode, out)
		}
	}
	if n := e.msgs(t); n != 2 {
		t.Fatalf("stream holds %d messages, want 2", n)
	}
}

func TestHookOversizeBody(t *testing.T) {
	e := newHookEnv(t, HookOptions{MaxBodyBytes: 64}, true)
	body := bytes.Repeat([]byte("x"), 65)
	resp, out := e.post(t, "/v1/hooks/"+testNS+"/alerts", body, map[string]string{"Authorization": "Bearer " + hookBearer})
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d %v", resp.StatusCode, out)
	}
	if n := e.msgs(t); n != 0 {
		t.Fatalf("oversize body published %d messages", n)
	}
}

func TestHookDuplicateDeliveryPublishesOnce(t *testing.T) {
	e := newHookEnv(t, HookOptions{}, true)
	path := "/v1/hooks/" + testNS + "/alerts"
	for _, idHeader := range []string{"X-GitHub-Delivery", "Ce-Id", "Idempotency-Key"} {
		h := map[string]string{"Authorization": "Bearer " + hookBearer, idHeader: "same-" + idHeader}
		resp, first := e.post(t, path, []byte(`{"n":1}`), h)
		if resp.StatusCode != http.StatusAccepted || first["duplicate"] != nil {
			t.Fatalf("%s first: %d %v", idHeader, resp.StatusCode, first)
		}
		resp, again := e.post(t, path, []byte(`{"n":1}`), h)
		if resp.StatusCode != http.StatusAccepted || again["duplicate"] != true || again["seq"] != first["seq"] {
			t.Fatalf("%s again: %d %v (first %v)", idHeader, resp.StatusCode, again, first)
		}
	}
	if n := e.msgs(t); n != 3 {
		t.Fatalf("stream holds %d messages, want 3", n)
	}

	// The id is scoped to the Trigger: another Trigger's delivery with the
	// same id is not a duplicate.
	resp, out := e.post(t, "/v1/hooks/"+testNS+"/signed", []byte(`{}`), map[string]string{
		"X-Hub-Signature-256": sign(hookHMACKey, []byte(`{}`)), "Ce-Id": "same-Ce-Id"})
	if resp.StatusCode != http.StatusAccepted || out["duplicate"] != nil {
		t.Fatalf("other trigger: %d %v", resp.StatusCode, out)
	}

	resp, out = e.post(t, path, []byte(`{}`), map[string]string{"Authorization": "Bearer " + hookBearer, "Ce-Id": "has space"})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad id: %d %v", resp.StatusCode, out)
	}
}

func TestHookNotFoundLooksTheSame(t *testing.T) {
	e := newHookEnv(t, HookOptions{}, true)
	h := map[string]string{"Authorization": "Bearer " + hookBearer}
	paths := []string{
		"/v1/hooks/" + testNS + "/missing",
		"/v1/hooks/" + testNS + "/nats-only",
		// A Trigger exists there with this credential, but the namespace is
		// not in allowedNamespaces.
		"/v1/hooks/" + hookOtherNS + "/alerts",
		"/v1/hooks/" + testNS + "/Not_A_Name",
	}
	bodies := make([]string, 0, len(paths))
	for _, path := range paths {
		resp, out := e.post(t, path, []byte(`{}`), h)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s: status %d %v", path, resp.StatusCode, out)
		}
		bodies = append(bodies, out["_raw"].(string))
	}
	for _, b := range bodies[1:] {
		if b != bodies[0] {
			t.Fatalf("404 bodies differ: %q", bodies)
		}
	}
	if n := e.msgs(t); n != 0 {
		t.Fatalf("published %d messages", n)
	}
}

func TestHookAllowedNamespace(t *testing.T) {
	e := newHookEnv(t, HookOptions{}, true, hookOtherNS)
	resp, out := e.post(t, "/v1/hooks/"+hookOtherNS+"/alerts", []byte(`{}`), map[string]string{"Authorization": "Bearer " + hookBearer})
	if resp.StatusCode != http.StatusAccepted || out["subject"] != natspkg.WebhookEventSubject(prefix, hookOtherNS, "alerts") {
		t.Fatalf("status %d %v", resp.StatusCode, out)
	}
}

func TestHookUnlabelledSecretIsRefused(t *testing.T) {
	e := newHookEnv(t, HookOptions{}, true)
	resp, out := e.post(t, "/v1/hooks/"+testNS+"/unlabelled", []byte(`{}`), map[string]string{"Authorization": "Bearer " + hookBearer})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status %d %v", resp.StatusCode, out)
	}
}

func TestHookRateLimit(t *testing.T) {
	e := newHookEnv(t, HookOptions{Rate: 0.001, Burst: 2}, true)
	h := map[string]string{"Authorization": "Bearer " + hookBearer}
	for i := range 2 {
		if resp, out := e.post(t, "/v1/hooks/"+testNS+"/alerts", []byte(`{}`), h); resp.StatusCode != http.StatusAccepted {
			t.Fatalf("delivery %d: %d %v", i, resp.StatusCode, out)
		}
	}
	resp, out := e.post(t, "/v1/hooks/"+testNS+"/alerts", []byte(`{}`), h)
	if resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("status %d %v", resp.StatusCode, out)
	}
	// The limit is per Trigger.
	resp, out = e.post(t, "/v1/hooks/"+testNS+"/signed", []byte(`{}`), map[string]string{"X-Hub-Signature-256": sign(hookHMACKey, []byte(`{}`))})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("other trigger: %d %v", resp.StatusCode, out)
	}
}

func TestHooksDisabledNeedGatewayAuth(t *testing.T) {
	e := newHookEnv(t, HookOptions{}, false)
	resp, out := e.post(t, "/v1/hooks/"+testNS+"/alerts", []byte(`{}`), map[string]string{"Authorization": "Bearer " + hookBearer})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status %d %v", resp.StatusCode, out)
	}
}

// errReader fails Get for Secrets with err, and delegates everything else.
type errReader struct {
	client.Reader
	err error
}

func (r errReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Secret); ok {
		return r.err
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestHookCredentialReadErrors(t *testing.T) {
	secretGR := schema.GroupResource{Resource: "secrets"}
	for name, tc := range map[string]struct {
		err  error
		want int
	}{
		"not found": {apierrors.NewNotFound(secretGR, "bearer"), http.StatusUnauthorized},
		"forbidden": {apierrors.NewForbidden(secretGR, "bearer", errors.New("no")), http.StatusUnauthorized},
		"timeout":   {errors.New("etcdserver: request timed out"), http.StatusServiceUnavailable},
		"cancelled": {context.Canceled, http.StatusServiceUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			e := newHookEnv(t, HookOptions{}, true)
			e.svc.Hooks = errReader{Reader: e.svc.Hooks, err: tc.err}
			resp, out := e.post(t, "/v1/hooks/"+testNS+"/alerts", []byte(`{}`), map[string]string{"Authorization": "Bearer " + hookBearer})
			if resp.StatusCode != tc.want {
				t.Fatalf("status %d %v, want %d", resp.StatusCode, out, tc.want)
			}
			raw := out["_raw"].(string)
			if tc.want == http.StatusServiceUnavailable {
				if resp.Header.Get("Retry-After") != "5" {
					t.Fatalf("Retry-After %q", resp.Header.Get("Retry-After"))
				}
				for _, leak := range []string{"bearer", "secret", "found", "timed out", "exist"} {
					if strings.Contains(strings.ToLower(raw), leak) {
						t.Fatalf("503 body %q mentions %q", raw, leak)
					}
				}
			} else if out["error"] != errHookUnauthorized.Error() {
				t.Fatalf("401 body %q differs from a bad token's", raw)
			}
		})
	}
}

// TestHookShortCredentialLooksLikeBadToken: a labelled Secret whose key is
// shorter than minTokenLength is an unusable credential, answered with the
// same 401 and headers as a wrong token, so the body does not tell a sender
// that the Trigger is misconfigured.
func TestHookShortCredentialLooksLikeBadToken(t *testing.T) {
	e := newHookEnv(t, HookOptions{}, true)
	auth := map[string]string{"Authorization": "Bearer too-short"}
	badResp, badOut := e.post(t, "/v1/hooks/"+testNS+"/alerts", []byte(`{}`), auth)
	shortResp, shortOut := e.post(t, "/v1/hooks/"+testNS+"/short", []byte(`{}`), map[string]string{"Authorization": "Bearer too-short"})
	if shortResp.StatusCode != http.StatusUnauthorized || badResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status short %d, bad %d", shortResp.StatusCode, badResp.StatusCode)
	}
	if shortOut["_raw"] != badOut["_raw"] {
		t.Fatalf("short-credential body %q differs from a bad token's %q", shortOut["_raw"], badOut["_raw"])
	}
	if shortResp.Header.Get("WWW-Authenticate") != badResp.Header.Get("WWW-Authenticate") {
		t.Fatalf("WWW-Authenticate %q, want %q", shortResp.Header.Get("WWW-Authenticate"), badResp.Header.Get("WWW-Authenticate"))
	}
}

// countingReader counts Gets of Triggers.
type countingReader struct {
	client.Reader
	triggerGets *int
}

func (r countingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*aiv1alpha1.Trigger); ok {
		*r.triggerGets++
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

// TestHookNegativeCache: a repeated probe of an unknown Trigger within
// hookMissTTL reads the API once; a found Trigger is read on every request;
// after the window the unknown name is read again.
func TestHookNegativeCache(t *testing.T) {
	e := newHookEnv(t, HookOptions{}, true)
	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	e.svc.now = func() time.Time { return clock }
	gets := 0
	e.svc.Hooks = countingReader{Reader: e.svc.Hooks, triggerGets: &gets}
	auth := map[string]string{"Authorization": "Bearer " + hookBearer}

	for i := 0; i < 3; i++ {
		resp, _ := e.post(t, "/v1/hooks/"+testNS+"/ghost", []byte(`{}`), auth)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("unknown probe %d: status %d", i, resp.StatusCode)
		}
	}
	if gets != 1 {
		t.Fatalf("unknown Trigger read %d times in the window, want 1", gets)
	}

	gets = 0
	for i := 0; i < 2; i++ {
		if resp, out := e.post(t, "/v1/hooks/"+testNS+"/alerts", []byte(`{}`), auth); resp.StatusCode != http.StatusAccepted {
			t.Fatalf("found Trigger %d: status %d %v", i, resp.StatusCode, out)
		}
	}
	if gets != 2 {
		t.Fatalf("found Trigger read %d times for 2 requests, want 2", gets)
	}

	clock = clock.Add(hookMissTTL)
	gets = 0
	if resp, _ := e.post(t, "/v1/hooks/"+testNS+"/ghost", []byte(`{}`), auth); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d after the window", resp.StatusCode)
	}
	if gets != 1 {
		t.Fatalf("unknown Trigger read %d times after the window, want 1", gets)
	}
}
