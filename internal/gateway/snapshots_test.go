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
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

type snapEnv struct {
	env
	k8s client.Client
}

func snapKnight(name string, mutate func(*aiv1alpha1.Knight)) *aiv1alpha1.Knight {
	k := &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS}}
	if mutate != nil {
		mutate(k)
	}
	return k
}

// newSnapEnv serves the gateway over a fake API holding the knights
// "galahad" (pvc, two snapshots and a failed condition), "ephemeral",
// "sleeper" (suspended) and "fresh".
func newSnapEnv(t *testing.T, enabled bool) *snapEnv {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = aiv1alpha1.AddToScheme(scheme)
	created := metav1.NewTime(time.Date(2026, 11, 26, 3, 0, 0, 0, time.UTC))
	galahad := snapKnight("galahad", nil)
	galahad.Status.Workspace = &aiv1alpha1.KnightWorkspaceStatus{
		LastSnapshotRequest: "1",
		Snapshots: []aiv1alpha1.WorkspaceSnapshotStatus{
			{Name: "galahad-ws-100", CreatedAt: &created, ReadyToUse: true},
			{Name: "galahad-ws-200", CreatedAt: &created},
		},
	}
	meta.SetStatusCondition(&galahad.Status.Conditions, metav1.Condition{
		Type: aiv1alpha1.ConditionKnightWorkspaceSnapshots, Status: metav1.ConditionFalse,
		Reason: aiv1alpha1.ReasonSnapshotFailed, Message: "boom"})
	k8s := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		galahad,
		snapKnight("ephemeral", func(k *aiv1alpha1.Knight) {
			k.Spec.Workspace = &aiv1alpha1.KnightWorkspace{Mode: aiv1alpha1.WorkspaceModeEphemeral}
		}),
		snapKnight("sleeper", func(k *aiv1alpha1.Knight) { k.Spec.Suspend = true }),
		snapKnight("fresh", nil),
	).WithStatusSubresource(&aiv1alpha1.Knight{}).Build()
	// WithObjects drops status with a status subresource: write it back.
	if err := k8s.Status().Update(t.Context(), galahad); err != nil {
		t.Fatal(err)
	}

	svc := NewService(&Resolver{Client: k8s}, nil, Options{Namespace: testNS, MaxTimeout: time.Minute,
		Runs: RunOptions{PollInterval: 10 * time.Millisecond}}, logr.Discard())
	if enabled {
		svc.Knights = k8s
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
	return &snapEnv{env: env{svc: svc, server: srv}, k8s: k8s}
}

func (e *snapEnv) knight(t *testing.T, name string) *aiv1alpha1.Knight {
	t.Helper()
	k := &aiv1alpha1.Knight{}
	if err := e.k8s.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, k); err != nil {
		t.Fatal(err)
	}
	return k
}

func TestListSnapshots(t *testing.T) {
	e := newSnapEnv(t, true)
	resp, body := e.do(t, http.MethodGet, "/v1/knights/galahad/snapshots", testToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %v", resp.StatusCode, body)
	}
	snaps := body["snapshots"].([]any)
	if len(snaps) != 2 || snaps[0].(map[string]any)["name"] != "galahad-ws-100" || snaps[0].(map[string]any)["readyToUse"] != true ||
		snaps[1].(map[string]any)["readyToUse"] != false {
		t.Errorf("snapshots = %v", snaps)
	}
	cond := body["condition"].(map[string]any)
	if cond["status"] != "False" || cond["reason"] != aiv1alpha1.ReasonSnapshotFailed || cond["message"] != "boom" {
		t.Errorf("condition = %v", cond)
	}
	if body["lastSnapshotRequest"] != "1" {
		t.Errorf("lastSnapshotRequest = %v", body["lastSnapshotRequest"])
	}

	// A knight without snapshots lists none (an empty array, not null).
	_, body = e.do(t, http.MethodGet, "/v1/knights/fresh/snapshots", testToken, nil)
	if s, ok := body["snapshots"].([]any); !ok || len(s) != 0 || body["condition"] != nil {
		t.Errorf("fresh = %v", body)
	}

	resp, _ = e.do(t, http.MethodGet, "/v1/knights/nobody/snapshots", testToken, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown knight: status %d", resp.StatusCode)
	}
}

func TestSnapshotKnightSetsAnnotation(t *testing.T) {
	e := newSnapEnv(t, true)
	resp, body := e.do(t, http.MethodPost, "/v1/knights/galahad/snapshot", testToken, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d: %v", resp.StatusCode, body)
	}
	k := e.knight(t, "galahad")
	req := k.Annotations[aiv1alpha1.AnnotationKnightSnapshot]
	if _, err := strconv.ParseInt(req, 10, 64); err != nil || req != body["request"] {
		t.Errorf("annotation %q, response %v", req, body)
	}
	if k.Annotations[aiv1alpha1.AnnotationKnightSnapshotCaller] == "" || body["caller"] == "" {
		t.Errorf("caller not recorded: %v", k.Annotations)
	}
	if body["expectedName"] == "" || body["note"] != nil {
		t.Errorf("response = %v", body)
	}
}

func TestSnapshotKnightRefusals(t *testing.T) {
	e := newSnapEnv(t, true)
	resp, body := e.do(t, http.MethodPost, "/v1/knights/ephemeral/snapshot", testToken, nil)
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("ephemeral: status %d: %v", resp.StatusCode, body)
	}
	if want := "workspace.mode is ephemeral; only pvc workspaces can be snapshotted"; !strings.Contains(body["error"].(string), want) {
		t.Errorf("error = %v", body["error"])
	}
	if v := e.knight(t, "ephemeral").Annotations[aiv1alpha1.AnnotationKnightSnapshot]; v != "" {
		t.Errorf("annotation set on an ephemeral knight: %q", v)
	}
	resp, _ = e.do(t, http.MethodPost, "/v1/knights/nobody/snapshot", testToken, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown knight: status %d", resp.StatusCode)
	}
	resp, _ = e.do(t, http.MethodPost, "/v1/knights/galahad/snapshot?waitSeconds=-1", testToken, nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("negative wait: status %d", resp.StatusCode)
	}
}

func TestSnapshotKnightSameSecondConflict(t *testing.T) {
	e := newSnapEnv(t, true)
	fixed := time.Date(2026, 11, 27, 10, 0, 0, 100, time.UTC)
	now := fixed
	e.svc.now = func() time.Time { return now }

	if _, err := e.svc.SnapshotKnight(t.Context(), "", "galahad", nil); err != nil {
		t.Fatal(err)
	}
	now = fixed.Add(400 * time.Millisecond) // same second, different nanoseconds
	_, err := e.svc.SnapshotKnight(t.Context(), "", "galahad", nil)
	var conflict *ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("want a ConflictError, got %v", err)
	}
	resp, _ := e.do(t, http.MethodPost, "/v1/knights/galahad/snapshot", testToken, nil)
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status %d", resp.StatusCode)
	}

	// The next second is fine, and the values differ.
	first := e.knight(t, "galahad").Annotations[aiv1alpha1.AnnotationKnightSnapshot]
	now = fixed.Add(time.Second)
	if _, err := e.svc.SnapshotKnight(t.Context(), "", "galahad", nil); err != nil {
		t.Fatal(err)
	}
	if second := e.knight(t, "galahad").Annotations[aiv1alpha1.AnnotationKnightSnapshot]; second == first {
		t.Errorf("annotation did not change: %q", second)
	}
}

func TestSnapshotKnightSuspended(t *testing.T) {
	e := newSnapEnv(t, true)
	resp, body := e.do(t, http.MethodPost, "/v1/knights/sleeper/snapshot?waitSeconds=30", testToken, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d: %v", resp.StatusCode, body)
	}
	if body["note"] == nil || body["note"] == "" {
		t.Errorf("no note: %v", body)
	}
	if e.knight(t, "sleeper").Annotations[aiv1alpha1.AnnotationKnightSnapshot] == "" {
		t.Error("annotation not set")
	}
}

func TestSnapshotKnightWait(t *testing.T) {
	e := newSnapEnv(t, true)
	// Stand in for the controller: once the request arrives, list a snapshot
	// that is not ready, then flip it to ready.
	go func() {
		ctx := context.Background()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			k := &aiv1alpha1.Knight{}
			if err := e.k8s.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "fresh"}, k); err != nil {
				return
			}
			req := k.Annotations[aiv1alpha1.AnnotationKnightSnapshot]
			if req == "" {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			k.Status.Workspace = &aiv1alpha1.KnightWorkspaceStatus{LastSnapshotRequest: req,
				Snapshots: []aiv1alpha1.WorkspaceSnapshotStatus{{Name: "fresh-ws-1"}}}
			_ = e.k8s.Status().Update(ctx, k)
			time.Sleep(100 * time.Millisecond)
			k.Status.Workspace.Snapshots[0].ReadyToUse = true
			_ = e.k8s.Status().Update(ctx, k)
			return
		}
	}()
	start := time.Now()
	resp, body := e.do(t, http.MethodPost, "/v1/knights/fresh/snapshot?waitSeconds=10", testToken, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d: %v", resp.StatusCode, body)
	}
	if body["ready"] != true || body["snapshot"].(map[string]any)["name"] != "fresh-ws-1" {
		t.Errorf("response = %v", body)
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("waited %s", time.Since(start))
	}

	// Nobody handles a request for "galahad" within the wait: 202, not ready.
	resp, body = e.do(t, http.MethodPost, "/v1/knights/galahad/snapshot?waitSeconds=1", testToken, nil)
	if resp.StatusCode != http.StatusAccepted || body["ready"] != nil {
		t.Errorf("timeout: status %d, %v", resp.StatusCode, body)
	}
	if c, _ := body["condition"].(map[string]any); c["reason"] != aiv1alpha1.ReasonSnapshotFailed {
		t.Errorf("condition = %v", body["condition"])
	}
}

func TestSnapshotRouteDisabled(t *testing.T) {
	e := newSnapEnv(t, false)
	resp, _ := e.do(t, http.MethodPost, "/v1/knights/galahad/snapshot", testToken, nil)
	if resp.StatusCode == http.StatusAccepted {
		t.Error("snapshot served although disabled")
	}
	resp, _ = e.do(t, http.MethodGet, "/v1/knights/galahad/snapshots", testToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("list: status %d", resp.StatusCode)
	}
}

func TestSnapshotMCPTools(t *testing.T) {
	e := newSnapEnv(t, true)
	ctx := t.Context()
	cs := connectMCP(ctx, t, e.server.URL)
	defer func() { _ = cs.Close() }()
	tools, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, tl := range tools.Tools {
		have[tl.Name] = true
	}
	if !have["list_knight_snapshots"] || !have["snapshot_knight"] {
		t.Errorf("tools = %v", have)
	}
	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "snapshot_knight", Arguments: map[string]any{"name": "fresh"}})
	if err != nil || res.IsError {
		t.Fatalf("snapshot_knight: %v %+v", err, res)
	}
	if e.knight(t, "fresh").Annotations[aiv1alpha1.AnnotationKnightSnapshot] == "" {
		t.Error("annotation not set")
	}
	res, err = cs.CallTool(ctx, &mcp.CallToolParams{Name: "list_knight_snapshots", Arguments: map[string]any{"name": "galahad"}})
	if err != nil || res.IsError {
		t.Fatalf("list_knight_snapshots: %v %+v", err, res)
	}
}

func TestRequestSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name  string
		snaps []string
		want  string
	}{
		{"newest", []string{"k-ws-1", "k-ws-2"}, "k-ws-2"},
		{"scheduled last is skipped", []string{"k-ws-1", "k-ws-sched-9"}, "k-ws-1"},
		{"previous request's name is not matched", []string{"k-ws-5", "k-ws-6"}, "k-ws-6"},
		{"only scheduled", []string{"k-ws-sched-9"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var snaps []SnapshotInfo
			for _, n := range tc.snaps {
				snaps = append(snaps, SnapshotInfo{Name: n})
			}
			sn, ok := requestSnapshot(snaps)
			if (tc.want != "") != ok || sn.Name != tc.want {
				t.Errorf("got %q, %v; want %q", sn.Name, ok, tc.want)
			}
		})
	}
}

func TestWaitSnapshotEnds(t *testing.T) {
	const req = "1000000000000"
	for _, tc := range []struct {
		name      string
		status    func(k *aiv1alpha1.Knight)
		wantReady bool
		wantSnap  string
		early     bool
	}{
		{"scheduled snapshot not reported", func(k *aiv1alpha1.Knight) {
			k.Status.Workspace = &aiv1alpha1.KnightWorkspaceStatus{LastSnapshotRequest: req,
				Snapshots: []aiv1alpha1.WorkspaceSnapshotStatus{
					{Name: "fresh-ws-1", ReadyToUse: true}, {Name: "fresh-ws-sched-2", ReadyToUse: true}}}
		}, true, "fresh-ws-1", true},
		{"failed condition ends the wait", func(k *aiv1alpha1.Knight) {
			meta.SetStatusCondition(&k.Status.Conditions, metav1.Condition{
				Type: aiv1alpha1.ConditionKnightWorkspaceSnapshots, Status: metav1.ConditionFalse,
				Reason: aiv1alpha1.ReasonSnapshotFailed, Message: "boom"})
		}, false, "", true},
		{"newer request ends the wait", func(k *aiv1alpha1.Knight) {
			k.Status.Workspace = &aiv1alpha1.KnightWorkspaceStatus{LastSnapshotRequest: "2000000000000"}
		}, false, "", true},
		{"older request keeps waiting", func(k *aiv1alpha1.Knight) {
			k.Status.Workspace = &aiv1alpha1.KnightWorkspaceStatus{LastSnapshotRequest: "5"}
		}, false, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newSnapEnv(t, true)
			k := e.knight(t, "fresh")
			tc.status(k)
			if err := e.k8s.Status().Update(t.Context(), k); err != nil {
				t.Fatal(err)
			}
			out := &SnapshotRequest{Knight: "fresh", Namespace: testNS, Request: req}
			start := time.Now()
			// Ending early is checked against a wait far longer than any
			// plausible slow runner; waiting on is checked against a short
			// one that can only elapse in full.
			wait := 30 * time.Second
			if !tc.early {
				wait = 300 * time.Millisecond
			}
			if err := e.svc.waitSnapshot(t.Context(), out, wait); err != nil {
				t.Fatal(err)
			}
			if out.Ready != tc.wantReady || (tc.wantSnap != "") != (out.Snapshot != nil) ||
				(out.Snapshot != nil && out.Snapshot.Name != tc.wantSnap) {
				t.Errorf("ready=%v snapshot=%v", out.Ready, out.Snapshot)
			}
			if took := time.Since(start); tc.early && took >= 20*time.Second || !tc.early && took < wait {
				t.Errorf("early = %v, waited %s of %s", tc.early, took, wait)
			}
		})
	}
}
