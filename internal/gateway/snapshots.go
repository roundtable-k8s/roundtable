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
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
)

// Workspace snapshots: the gateway only sets the ai.roundtable.io/snapshot
// annotation the Knight controller already serves (docs/workspace-snapshots.md)
// and reads status.workspace back.

// SnapshotsEnabled reports whether the snapshot request endpoint is served.
func (s *Service) SnapshotsEnabled() bool { return s.Knights != nil }

// snapshotPatchRetries bounds re-reads after a write conflict.
const snapshotPatchRetries = 5

// SnapshotInfo is one workspace VolumeSnapshot of a knight.
type SnapshotInfo struct {
	Name       string     `json:"name"`
	CreatedAt  *time.Time `json:"createdAt,omitempty"`
	ReadyToUse bool       `json:"readyToUse"`
}

// SnapshotCondition is the knight's WorkspaceSnapshots condition.
type SnapshotCondition struct {
	Status  string `json:"status"`
	Reason  string `json:"reason"`
	Message string `json:"message,omitempty"`
}

// KnightSnapshots is the answer to GET /v1/knights/{name}/snapshots.
type KnightSnapshots struct {
	Knight              string             `json:"knight"`
	Namespace           string             `json:"namespace"`
	LastSnapshotRequest string             `json:"lastSnapshotRequest,omitempty"`
	Snapshots           []SnapshotInfo     `json:"snapshots"`
	Condition           *SnapshotCondition `json:"condition,omitempty"`
}

// SnapshotRequest is the answer to POST /v1/knights/{name}/snapshot.
type SnapshotRequest struct {
	Knight    string `json:"knight"`
	Namespace string `json:"namespace"`
	// Request is the value set on the snapshot annotation (unix nanoseconds).
	Request string `json:"request"`
	// ExpectedName is <knight>-ws-<unix seconds>. The operator names the
	// snapshot with its own clock, so it can differ by a second (and for a
	// suspended knight the name is set when the knight resumes); the list is
	// authoritative.
	ExpectedName string `json:"expectedName"`
	Caller       string `json:"caller,omitempty"`
	Note         string `json:"note,omitempty"`
	// Ready, Snapshot and Condition are filled after a wait.
	Ready     bool               `json:"ready,omitempty"`
	Snapshot  *SnapshotInfo      `json:"snapshot,omitempty"`
	Condition *SnapshotCondition `json:"condition,omitempty"`
}

// scheduledSnapshotMarker is in the name of snapshots the controller takes on
// the knight's snapshot schedule (<knight>-ws-sched-<unix seconds>).
const scheduledSnapshotMarker = "-ws-sched-"

// requestSnapshot picks the snapshot a handled request took: the newest one
// that is not a scheduled one (a scheduled snapshot can be created in the same
// reconcile and be listed last). The name is not matched against
// SnapshotRequest.ExpectedName: the controller names snapshots with its own
// clock, so the name can belong to an earlier request. The controller creates
// the snapshot before it records LastSnapshotRequest, and lists snapshots by
// creation time, so the newest non-scheduled one is exact.
func requestSnapshot(snaps []SnapshotInfo) (SnapshotInfo, bool) {
	for i := len(snaps) - 1; i >= 0; i-- {
		if !strings.Contains(snaps[i].Name, scheduledSnapshotMarker) {
			return snaps[i], true
		}
	}
	return SnapshotInfo{}, false
}

// snapshotWaitEnded reports whether the wait for request can stop before the
// timeout without a snapshot: a newer request replaced it, or the
// WorkspaceSnapshots condition went False/SnapshotFailed after it was made.
// The condition is not tied to a request, so a failed scheduled snapshot or a
// create error the controller retries can also end the wait; the caller then
// reads the condition from the answer.
func snapshotWaitEnded(view *KnightSnapshots, k *aiv1alpha1.Knight, request string) bool {
	reqNanos, err := strconv.ParseInt(request, 10, 64)
	if err != nil {
		return false
	}
	if last, err := strconv.ParseInt(view.LastSnapshotRequest, 10, 64); err == nil && last > reqNanos {
		return true
	}
	c := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightWorkspaceSnapshots)
	return c != nil && c.Status == metav1.ConditionFalse && c.Reason == aiv1alpha1.ReasonSnapshotFailed &&
		c.LastTransitionTime.UnixNano() >= reqNanos
}

func knightSnapshots(k *aiv1alpha1.Knight) *KnightSnapshots {
	out := &KnightSnapshots{Knight: k.Name, Namespace: k.Namespace, Snapshots: []SnapshotInfo{}}
	if ws := k.Status.Workspace; ws != nil {
		out.LastSnapshotRequest = ws.LastSnapshotRequest
		for _, sn := range ws.Snapshots {
			info := SnapshotInfo{Name: sn.Name, ReadyToUse: sn.ReadyToUse}
			if sn.CreatedAt != nil {
				t := sn.CreatedAt.Time
				info.CreatedAt = &t
			}
			out.Snapshots = append(out.Snapshots, info)
		}
	}
	if c := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightWorkspaceSnapshots); c != nil {
		out.Condition = &SnapshotCondition{Status: string(c.Status), Reason: c.Reason, Message: c.Message}
	}
	return out
}

// ListSnapshots returns the knight's workspace snapshots, its
// WorkspaceSnapshots condition and the last handled request.
func (s *Service) ListSnapshots(ctx context.Context, namespace, name string) (*KnightSnapshots, error) {
	ns, err := s.namespace(namespace)
	if err != nil {
		return nil, err
	}
	k, err := s.Resolver.Knight(ctx, ns, name)
	if err != nil {
		return nil, err
	}
	return knightSnapshots(k), nil
}

// SnapshotKnight asks the operator to snapshot the knight's workspace by
// setting the snapshot annotation to a fresh value, then optionally waits up
// to waitSeconds for the snapshot to be readyToUse.
func (s *Service) SnapshotKnight(ctx context.Context, namespace, name string, waitSeconds *int) (*SnapshotRequest, error) {
	if s.Knights == nil {
		return nil, fmt.Errorf("%w: snapshots are disabled on this gateway", ErrNotFound)
	}
	ns, err := s.namespace(namespace)
	if err != nil {
		return nil, err
	}
	wait, err := s.missionWait(waitSeconds)
	if err != nil {
		return nil, err
	}
	if wait > 0 {
		// A waiting call holds a slot, like the other waits.
		release, err := s.acquireSlot()
		if err != nil {
			return nil, err
		}
		defer release()
	}
	caller := callerOr(ctx)
	now := s.now()
	out := &SnapshotRequest{Knight: name, Namespace: ns, Caller: caller,
		Request:      strconv.FormatInt(now.UnixNano(), 10),
		ExpectedName: fmt.Sprintf("%s-ws-%d", name, now.Unix())}

	key := types.NamespacedName{Namespace: ns, Name: name}
	for attempt := 0; ; attempt++ {
		k := &aiv1alpha1.Knight{}
		if err := s.Knights.Get(ctx, key, k); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("%w: knight %s/%s", ErrNotFound, ns, name)
			}
			return nil, k8sError("get knight", err)
		}
		if knightpkg.EphemeralWorkspace(k) {
			return nil, fmt.Errorf("%w: workspace.mode is ephemeral; only pvc workspaces can be snapshotted", ErrUnprocessable)
		}
		// The controller names a snapshot by the second, so a second request
		// in the same second would collide with the first.
		if prev, err := strconv.ParseInt(k.Annotations[aiv1alpha1.AnnotationKnightSnapshot], 10, 64); err == nil &&
			time.Unix(0, prev).Unix() == now.Unix() {
			return nil, &ConflictError{State: "tooSoon",
				Reason: fmt.Sprintf("knight %s/%s was already asked for a snapshot in this second; retry shortly", ns, name)}
		}
		patch := client.MergeFromWithOptions(k.DeepCopy(), client.MergeFromWithOptimisticLock{})
		if k.Annotations == nil {
			k.Annotations = map[string]string{}
		}
		k.Annotations[aiv1alpha1.AnnotationKnightSnapshot] = out.Request
		k.Annotations[aiv1alpha1.AnnotationKnightSnapshotCaller] = caller
		err := s.Knights.Patch(ctx, k, patch)
		if err == nil {
			if k.Spec.Suspend {
				out.Note = "the knight is suspended: the snapshot is taken when it resumes"
			}
			break
		}
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w: knight %s/%s", ErrNotFound, ns, name)
		}
		if !apierrors.IsConflict(err) || attempt >= snapshotPatchRetries {
			return nil, k8sError("patch knight", err)
		}
	}
	s.Log.Info("Requested workspace snapshot", "namespace", ns, "knight", name, "request", out.Request, "caller", caller)
	if wait > 0 && out.Note == "" {
		return out, s.waitSnapshot(ctx, out, wait)
	}
	return out, nil
}

// waitSnapshot polls until the controller handled the request and its
// snapshot is readyToUse, or the wait ends (the request then stays as
// answered, with the current condition).
func (s *Service) waitSnapshot(ctx context.Context, out *SnapshotRequest, wait time.Duration) error {
	wctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	tick := time.NewTicker(s.Opts.Runs.PollInterval)
	defer tick.Stop()
	for {
		k := &aiv1alpha1.Knight{}
		if err := s.Knights.Get(ctx, types.NamespacedName{Namespace: out.Namespace, Name: out.Knight}, k); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("%w: knight %s/%s was deleted", ErrNotFound, out.Namespace, out.Knight)
			}
			return k8sError("get knight", err)
		}
		view := knightSnapshots(k)
		out.Condition = view.Condition
		if view.LastSnapshotRequest == out.Request {
			if sn, ok := requestSnapshot(view.Snapshots); ok {
				out.Snapshot = &sn
				if sn.ReadyToUse {
					out.Ready = true
					return nil
				}
			}
		}
		if snapshotWaitEnded(view, k, out.Request) {
			return nil
		}
		select {
		case <-wctx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		case <-tick.C:
		}
	}
}

func (s *Service) handleListSnapshots(w http.ResponseWriter, r *http.Request) {
	v, err := s.ListSnapshots(r.Context(), r.URL.Query().Get("namespace"), r.PathValue("name"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Service) handleSnapshotKnight(w http.ResponseWriter, r *http.Request) {
	wait, err := queryInt(r, "waitSeconds")
	if err != nil {
		writeError(w, err)
		return
	}
	v, err := s.SnapshotKnight(r.Context(), r.URL.Query().Get("namespace"), r.PathValue("name"), wait)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, v)
}

// KnightSnapshotInput is the input of the list_knight_snapshots tool.
type KnightSnapshotInput struct {
	Name      string `json:"name" jsonschema:"name of the knight"`
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace of the knight; omit for the default"`
}

// SnapshotKnightInput is the input of the snapshot_knight tool.
type SnapshotKnightInput struct {
	KnightSnapshotInput
	WaitSeconds *int `json:"waitSeconds,omitempty" jsonschema:"seconds to wait for the snapshot to be ready (default 0 = return at once)"`
}

func addSnapshotTools(s *mcp.Server, svc *Service) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "list_knight_snapshots",
		Description: "List a knight's workspace snapshots (name, creation time, readyToUse), the WorkspaceSnapshots " +
			"condition (why a request failed) and the last handled request.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in KnightSnapshotInput) (*mcp.CallToolResult, *KnightSnapshots, error) {
		v, err := svc.ListSnapshots(ctx, in.Namespace, in.Name)
		return nil, v, err
	})
	if !svc.SnapshotsEnabled() {
		return
	}
	mcp.AddTool(s, &mcp.Tool{
		Name: "snapshot_knight",
		Description: "Take a snapshot of a knight's workspace (pvc workspaces only), for example before a risky mission. " +
			"A suspended knight snapshots on resume. Optionally waits for the snapshot to be ready; " +
			"check list_knight_snapshots for the outcome.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in SnapshotKnightInput) (*mcp.CallToolResult, *SnapshotRequest, error) {
		v, err := svc.SnapshotKnight(ctx, in.Namespace, in.Name, in.WaitSeconds)
		return nil, v, err
	})
}
