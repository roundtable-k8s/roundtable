package controller

import (
	"context"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
)

// reconcileMemory creates the knight's memory PVC <knight>-memory
// (ReadWriteMany, spec.memory.size, owned by the Knight so it is deleted with
// it) when spec.memory is set without existingClaim, and sets the
// MemoryShared condition. An existingClaim is never created, changed or
// deleted. An existing PVC is left as it is: size and storage class only apply
// on creation.
func (r *KnightReconciler) reconcileMemory(ctx context.Context, knight *aiv1alpha1.Knight) error {
	m := knight.Spec.Memory
	if m == nil || m.ExistingClaim != "" {
		meta.RemoveStatusCondition(&knight.Status.Conditions, aiv1alpha1.ConditionKnightMemoryShared)
		return nil
	}

	name := knightpkg.MemoryClaimName(knight)
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: knight.Namespace}, pvc)
	switch {
	case apierrors.IsNotFound(err):
		pvc = newMemoryPVC(knight)
		if err := controllerutil.SetControllerReference(knight, pvc, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, pvc); err != nil {
			return fmt.Errorf("memory PVC create failed: %w", err)
		}
		logf.FromContext(ctx).Info("Memory PVC created", "name", name)
	case err != nil:
		return fmt.Errorf("memory PVC get failed: %w", err)
	default:
		// Never mount another object's claim as this knight's memory (for
		// example the workspace PVC of a knight named "<knight>-memory").
		if owner := metav1.GetControllerOf(pvc); owner != nil && owner.UID != knight.UID {
			return fmt.Errorf("memory PVC %s is owned by %s %s, not this Knight; rename one of them",
				name, owner.Kind, owner.Name)
		}
	}

	r.setMemoryCondition(knight, pvc)
	return nil
}

// newMemoryPVC renders the per-knight memory PVC.
func newMemoryPVC(knight *aiv1alpha1.Knight) *corev1.PersistentVolumeClaim {
	m := knight.Spec.Memory
	size := resource.MustParse("1Gi")
	if m.Size != nil && !m.Size.IsZero() {
		size = m.Size.DeepCopy()
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      knightpkg.MemoryClaimName(knight),
			Namespace: knight.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "knight",
				"app.kubernetes.io/instance":   knight.Name,
				"app.kubernetes.io/component":  "memory",
				"app.kubernetes.io/managed-by": "roundtable-operator",
				"roundtable.io/domain":         knight.Spec.Domain,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: size},
			},
		},
	}
	if m.StorageClassName != "" {
		pvc.Spec.StorageClassName = new(m.StorageClassName)
	}
	return pvc
}

// setMemoryCondition sets MemoryShared for a knight that can run more than one
// pod once its memory PVC is bound: True when the claim is ReadWriteMany,
// False (MemoryNotRWX) otherwise. In every other case the condition is removed.
func (r *KnightReconciler) setMemoryCondition(knight *aiv1alpha1.Knight, pvc *corev1.PersistentVolumeClaim) {
	multiPod := knightpkg.SpecReplicas(knight) > 1 ||
		(knight.Spec.MaxReplicas != nil && *knight.Spec.MaxReplicas > 1)
	if !multiPod || pvc.Status.Phase != corev1.ClaimBound {
		meta.RemoveStatusCondition(&knight.Status.Conditions, aiv1alpha1.ConditionKnightMemoryShared)
		return
	}
	cond := metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightMemoryShared,
		Status:             metav1.ConditionTrue,
		Reason:             aiv1alpha1.ReasonMemoryRWX,
		Message:            fmt.Sprintf("Memory PVC %s is bound ReadWriteMany; every pod mounts it", pvc.Name),
		ObservedGeneration: knight.Generation,
	}
	if !slices.Contains(pvc.Status.AccessModes, corev1.ReadWriteMany) {
		cond.Status = metav1.ConditionFalse
		cond.Reason = aiv1alpha1.ReasonMemoryNotRWX
		cond.Message = fmt.Sprintf("Memory PVC %s is bound with access modes %v, not ReadWriteMany, but the knight "+
			"can run more than one pod; pods on other nodes can't mount it. Set spec.memory.storageClassName to a "+
			"class that supports ReadWriteMany and recreate the PVC", pvc.Name, pvc.Status.AccessModes)
		prev := meta.FindStatusCondition(knight.Status.Conditions, aiv1alpha1.ConditionKnightMemoryShared)
		if (prev == nil || prev.Reason != cond.Reason) && r.Recorder != nil {
			// event-reasons: MemoryNotRWX
			r.Recorder.Event(knight, corev1.EventTypeWarning, cond.Reason, cond.Message)
		}
	}
	meta.SetStatusCondition(&knight.Status.Conditions, cond)
}
