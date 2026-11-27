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

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	rtruntime "github.com/roundtable-k8s/roundtable/pkg/runtime"
)

const (
	// snapshotKnightLabel labels VolumeSnapshots with their knight's name.
	snapshotKnightLabel = "roundtable.io/knight"
	// snapshotRequestLabel records (as a hash) the annotation value a snapshot
	// served, so a lost status.workspace.lastSnapshotRequest cannot cause a
	// second snapshot for the same request.
	snapshotRequestLabel = "roundtable.io/snapshot-request"
	// defaultSnapshotKeep is the retention when spec.workspace.snapshots.keep is unset.
	defaultSnapshotKeep = 3
	// snapshotProbeInterval is the minimum gap between rechecks of the
	// VolumeSnapshot API when it was missing at startup.
	snapshotProbeInterval = time.Minute
	// snapshotAPIMissingMessage is the condition message while the API is
	// not served.
	snapshotAPIMissingMessage = "the snapshot.storage.k8s.io/v1 VolumeSnapshot API is not served; install the external snapshotter, the operator will look again in a minute"
)

// snapshotsAvailable reports whether the VolumeSnapshot API can be used. It is
// true when it was served at startup; otherwise the API is probed again, at
// most once per snapshotProbeInterval, and a positive answer is kept. Callers
// invoke it only when a snapshot or restore is requested.
func (r *KnightReconciler) snapshotsAvailable(ctx context.Context) bool {
	if r.SnapshotsEnabled {
		return true
	}
	if r.SnapshotAPIProbe == nil {
		return false
	}
	r.snapProbeMu.Lock()
	defer r.snapProbeMu.Unlock()
	if r.snapLate {
		return true
	}
	now := r.now()
	if !r.snapLastProbe.IsZero() && now.Sub(r.snapLastProbe) < snapshotProbeInterval {
		return false
	}
	r.snapLastProbe = now
	served, err := r.SnapshotAPIProbe()
	if err != nil {
		logf.FromContext(ctx).Error(err, "Failed to recheck the VolumeSnapshot API")
		return false
	}
	if served {
		logf.FromContext(ctx).Info("VolumeSnapshot API is now served")
		r.snapLate = true
	}
	return served
}

// snapshotsLate reports whether the API was only found after startup, in
// which case VolumeSnapshots are not watched and readiness must be polled.
func (r *KnightReconciler) snapshotsLate() bool {
	r.snapProbeMu.Lock()
	defer r.snapProbeMu.Unlock()
	return !r.SnapshotsEnabled && r.snapLate
}

func (r *KnightReconciler) setSnapshotCondition(knight *aiv1alpha1.Knight, status metav1.ConditionStatus, reason, msg string) {
	meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
		Type: aiv1alpha1.ConditionKnightWorkspaceSnapshots, Status: status, Reason: reason, Message: msg,
		ObservedGeneration: knight.Generation,
	})
}

// workspaceClaimName is the PVC holding the knight's /data workspace.
func workspaceClaimName(knight *aiv1alpha1.Knight) string {
	if w := knight.Spec.Workspace; w != nil && w.ExistingClaim != "" {
		return w.ExistingClaim
	}
	return knight.Name
}

// reconcileSnapshots takes a VolumeSnapshot of the workspace PVC when the
// ai.roundtable.io/snapshot annotation holds a value not handled yet, mirrors
// the knight's snapshots into status.workspace and prunes the oldest beyond
// spec.workspace.snapshots.keep once the newest is ready.
//
// It returns how long to wait before looking again (0 for never). That is
// snapshotProbeInterval while the API is missing and a snapshot was requested,
// and RequeueSlow while a snapshot is not ready and the API was found after
// startup (no VolumeSnapshot watch is running then).
func (r *KnightReconciler) reconcileSnapshots(ctx context.Context, knight *aiv1alpha1.Knight) (time.Duration, error) {
	value, requested := knight.Annotations[aiv1alpha1.AnnotationKnightSnapshot]

	if !r.SnapshotsEnabled && !r.snapshotsLate() && (!requested || !r.snapshotsAvailable(ctx)) {
		if requested {
			r.setSnapshotCondition(knight, metav1.ConditionFalse, aiv1alpha1.ReasonSnapshotAPIUnavailable,
				snapshotAPIMissingMessage)
			return snapshotProbeInterval, nil
		}
		meta.RemoveStatusCondition(&knight.Status.Conditions, aiv1alpha1.ConditionKnightWorkspaceSnapshots)
		return 0, nil
	}
	if knightpkg.EphemeralWorkspace(knight) {
		if requested {
			r.setSnapshotCondition(knight, metav1.ConditionFalse, aiv1alpha1.ReasonWorkspaceNotPersistent,
				"workspace.mode is ephemeral; only pvc workspaces can be snapshotted")
		} else {
			meta.RemoveStatusCondition(&knight.Status.Conditions, aiv1alpha1.ConditionKnightWorkspaceSnapshots)
		}
		return 0, nil
	}

	ws := knight.Status.Workspace
	if ws == nil {
		ws = &aiv1alpha1.KnightWorkspaceStatus{}
	}
	if !requested && len(ws.Snapshots) == 0 {
		meta.RemoveStatusCondition(&knight.Status.Conditions, aiv1alpha1.ConditionKnightWorkspaceSnapshots)
		return 0, nil
	}

	fail := func(err error) error {
		r.setSnapshotCondition(knight, metav1.ConditionFalse, aiv1alpha1.ReasonSnapshotFailed, err.Error())
		return err
	}

	snaps, err := r.listSnapshots(ctx, knight)
	if err != nil {
		return 0, fail(err)
	}
	if requested && value != ws.LastSnapshotRequest {
		if !snapshotServed(snaps, value) {
			if err := r.createSnapshot(ctx, knight, value); err != nil {
				return 0, fail(err)
			}
			// The new snapshot may not be visible in the cache yet; the
			// request label keeps the next reconcile from duplicating it.
			if snaps, err = r.listSnapshots(ctx, knight); err != nil {
				return 0, fail(err)
			}
		}
		ws.LastSnapshotRequest = value
	}
	keep := int32(defaultSnapshotKeep)
	if s := knight.Spec.Workspace; s != nil && s.Snapshots != nil && s.Snapshots.Keep != nil {
		keep = *s.Snapshots.Keep
	}
	if n := len(snaps); n > int(keep) && snapshotReady(snaps[n-1]) {
		for _, old := range snaps[:n-int(keep)] {
			if err := r.Delete(ctx, old); err != nil && !apierrors.IsNotFound(err) {
				return 0, fail(fmt.Errorf("deleting VolumeSnapshot %s: %w", old.GetName(), err))
			}
		}
		snaps = snaps[n-int(keep):]
	}

	ws.Snapshots = nil
	for _, s := range snaps {
		created := s.GetCreationTimestamp()
		ws.Snapshots = append(ws.Snapshots, aiv1alpha1.WorkspaceSnapshotStatus{
			Name: s.GetName(), CreatedAt: &created, ReadyToUse: snapshotReady(s),
		})
	}
	knight.Status.Workspace = ws
	r.setSnapshotCondition(knight, metav1.ConditionTrue, aiv1alpha1.ReasonSnapshotsReady,
		fmt.Sprintf("%d workspace snapshot(s)", len(ws.Snapshots)))
	if r.snapshotsLate() {
		for _, st := range ws.Snapshots {
			if !st.ReadyToUse {
				return RequeueSlow, nil
			}
		}
	}
	return 0, nil
}

func snapshotReady(s *unstructured.Unstructured) bool {
	ready, _, _ := unstructured.NestedBool(s.Object, "status", "readyToUse")
	return ready
}

// snapshotRequestHash is the label value identifying an annotation value.
func snapshotRequestHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:8])
}

// snapshotServed reports whether a snapshot already exists for the request value.
func snapshotServed(snaps []*unstructured.Unstructured, value string) bool {
	h := snapshotRequestHash(value)
	for _, s := range snaps {
		if s.GetLabels()[snapshotRequestLabel] == h {
			return true
		}
	}
	return false
}

// createSnapshot creates <knight>-ws-<unix-ts> for the workspace PVC. An
// existing snapshot of that name (same second) counts as created.
func (r *KnightReconciler) createSnapshot(ctx context.Context, knight *aiv1alpha1.Knight, request string) error {
	vs := &unstructured.Unstructured{}
	vs.SetGroupVersionKind(rtruntime.VolumeSnapshotGVK)
	vs.SetName(fmt.Sprintf("%s-ws-%d", knight.Name, r.now().Unix()))
	vs.SetNamespace(knight.Namespace)
	vs.SetLabels(map[string]string{
		snapshotKnightLabel:            knight.Name,
		snapshotRequestLabel:           snapshotRequestHash(request),
		"app.kubernetes.io/managed-by": "roundtable-operator",
	})
	spec := map[string]any{
		"source": map[string]any{"persistentVolumeClaimName": workspaceClaimName(knight)},
	}
	if w := knight.Spec.Workspace; w != nil && w.Snapshots != nil && w.Snapshots.ClassName != "" {
		spec["volumeSnapshotClassName"] = w.Snapshots.ClassName
	}
	vs.Object["spec"] = spec
	if err := controllerutil.SetControllerReference(knight, vs, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, vs); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("creating VolumeSnapshot: %w", err)
	}
	logf.FromContext(ctx).Info("Workspace snapshot requested", "snapshot", vs.GetName())
	return nil
}

// listSnapshots returns the VolumeSnapshots labelled for and controlled by the
// knight, oldest first. Hand-made snapshots that merely carry the label are
// ignored so retention never deletes them.
func (r *KnightReconciler) listSnapshots(ctx context.Context, knight *aiv1alpha1.Knight) ([]*unstructured.Unstructured, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(rtruntime.VolumeSnapshotGVK.GroupVersion().WithKind("VolumeSnapshotList"))
	if err := r.List(ctx, list, client.InNamespace(knight.Namespace),
		client.MatchingLabels{snapshotKnightLabel: knight.Name}); err != nil {
		return nil, fmt.Errorf("listing VolumeSnapshots: %w", err)
	}
	out := make([]*unstructured.Unstructured, 0, len(list.Items))
	for i := range list.Items {
		if !metav1.IsControlledBy(&list.Items[i], knight) {
			continue
		}
		out = append(out, &list.Items[i])
	}
	sort.SliceStable(out, func(i, j int) bool {
		ti, tj := out[i].GetCreationTimestamp(), out[j].GetCreationTimestamp()
		if !ti.Equal(&tj) {
			return ti.Before(&tj)
		}
		return out[i].GetName() < out[j].GetName()
	})
	return out, nil
}
