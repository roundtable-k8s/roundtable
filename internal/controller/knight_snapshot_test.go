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
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	rtruntime "github.com/roundtable-k8s/roundtable/pkg/runtime"
)

// volumeSnapshotCRD is a minimal stand-in for the external-snapshotter CRD.
func volumeSnapshotCRD() *apiextensionsv1.CustomResourceDefinition {
	preserve := true
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: "volumesnapshots.snapshot.storage.k8s.io",
			// *.k8s.io groups need an approval annotation.
			Annotations: map[string]string{"api-approved.kubernetes.io": "https://github.com/kubernetes-csi/external-snapshotter/pull/419"},
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "snapshot.storage.k8s.io",
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: "volumesnapshots", Singular: "volumesnapshot", Kind: "VolumeSnapshot", ListKind: "VolumeSnapshotList",
			},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v1", Served: true, Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: &preserve},
				},
			}},
		},
	}
}

func snapshotsOf(k *aiv1alpha1.Knight) []unstructured.Unstructured {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(rtruntime.VolumeSnapshotGVK.GroupVersion().WithKind("VolumeSnapshotList"))
	Expect(k8sClient.List(ctx, list, client.InNamespace(k.Namespace),
		client.MatchingLabels{snapshotKnightLabel: k.Name})).To(Succeed())
	return list.Items
}

func markSnapshotsReady(k *aiv1alpha1.Knight) {
	for _, s := range snapshotsOf(k) {
		Expect(unstructured.SetNestedField(s.Object, true, "status", "readyToUse")).To(Succeed())
		Expect(k8sClient.Update(ctx, &s)).To(Succeed())
	}
}

func annotateSnapshot(k *aiv1alpha1.Knight, v string) {
	got := getKnight(k)
	if got.Annotations == nil {
		got.Annotations = map[string]string{}
	}
	got.Annotations[aiv1alpha1.AnnotationKnightSnapshot] = v
	Expect(k8sClient.Update(ctx, got)).To(Succeed())
}

var _ = Describe("Knight workspace snapshots (#339)", func() {
	snapReconciler := func(enabled bool) (*KnightReconciler, *time.Time) {
		r := newScalingReconciler(false)
		r.SnapshotsEnabled = enabled
		clock := time.Unix(1_700_000_000, 0)
		r.Now = func() time.Time { return clock }
		return r, &clock
	}
	pvcKnight := func(name string, keep *int32) *aiv1alpha1.Knight {
		return scalingKnight("default", name, func(s *aiv1alpha1.KnightSpec) {
			s.Workspace = &aiv1alpha1.KnightWorkspace{
				Snapshots: &aiv1alpha1.WorkspaceSnapshots{ClassName: "fast", Keep: keep},
			}
		})
	}
	cond := func(k *aiv1alpha1.Knight) *metav1.Condition {
		return meta.FindStatusCondition(getKnight(k).Status.Conditions, aiv1alpha1.ConditionKnightWorkspaceSnapshots)
	}

	Describe("with the snapshot API", Ordered, func() {
		BeforeAll(func() {
			_, err := envtest.InstallCRDs(cfg, envtest.CRDInstallOptions{
				CRDs: []*apiextensionsv1.CustomResourceDefinition{volumeSnapshotCRD()},
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("takes one snapshot per new annotation value and ignores a repeated one", func() {
			k := pvcKnight("snap-once", nil)
			Expect(k8sClient.Create(ctx, k)).To(Succeed())
			defer deleteKnight(k)
			r, _ := snapReconciler(true)

			reconcileKnight(r, k)
			Expect(snapshotsOf(k)).To(BeEmpty())
			Expect(cond(k)).To(BeNil())

			annotateSnapshot(k, "a")
			reconcileKnight(r, k)
			snaps := snapshotsOf(k)
			Expect(snaps).To(HaveLen(1))
			vs := snaps[0]
			Expect(vs.GetName()).To(Equal("snap-once-ws-1700000000"))
			src, _, _ := unstructured.NestedString(vs.Object, "spec", "source", "persistentVolumeClaimName")
			Expect(src).To(Equal("snap-once"))
			class, _, _ := unstructured.NestedString(vs.Object, "spec", "volumeSnapshotClassName")
			Expect(class).To(Equal("fast"))
			Expect(metav1.IsControlledBy(&vs, getKnight(k))).To(BeTrue())

			got := getKnight(k)
			Expect(got.Status.Workspace.LastSnapshotRequest).To(Equal("a"))
			Expect(got.Status.Workspace.Snapshots).To(HaveLen(1))
			Expect(got.Status.Workspace.Snapshots[0].ReadyToUse).To(BeFalse())
			Expect(cond(k).Status).To(Equal(metav1.ConditionTrue))

			// Same value again, later: nothing new.
			r.Now = func() time.Time { return time.Unix(1_700_000_100, 0) }
			reconcileKnight(r, k)
			Expect(snapshotsOf(k)).To(HaveLen(1))
		})

		It("does not duplicate a snapshot when lastSnapshotRequest is lost", func() {
			k := pvcKnight("snap-lost", nil)
			Expect(k8sClient.Create(ctx, k)).To(Succeed())
			defer deleteKnight(k)
			r, _ := snapReconciler(true)

			annotateSnapshot(k, "a")
			reconcileKnight(r, k)
			Expect(snapshotsOf(k)).To(HaveLen(1))

			got := getKnight(k)
			got.Status.Workspace = nil
			Expect(k8sClient.Status().Update(ctx, got)).To(Succeed())
			r.Now = func() time.Time { return time.Unix(1_700_000_001, 0) }
			reconcileKnight(r, k)
			Expect(snapshotsOf(k)).To(HaveLen(1))
			Expect(getKnight(k).Status.Workspace.LastSnapshotRequest).To(Equal("a"))
		})

		It("never prunes snapshots the knight does not own", func() {
			k := pvcKnight("snap-foreign", new(int32(1)))
			Expect(k8sClient.Create(ctx, k)).To(Succeed())
			defer deleteKnight(k)
			r, _ := snapReconciler(true)

			foreign := &unstructured.Unstructured{}
			foreign.SetGroupVersionKind(rtruntime.VolumeSnapshotGVK)
			foreign.SetName("snap-foreign-manual")
			foreign.SetNamespace("default")
			foreign.SetLabels(map[string]string{snapshotKnightLabel: k.Name})
			foreign.Object["spec"] = map[string]any{"source": map[string]any{"persistentVolumeClaimName": "x"}}
			Expect(k8sClient.Create(ctx, foreign)).To(Succeed())

			for i := 1; i <= 2; i++ {
				ts := time.Unix(1_700_000_000+int64(i), 0)
				r.Now = func() time.Time { return ts }
				annotateSnapshot(k, fmt.Sprint(i))
				reconcileKnight(r, k)
				markSnapshotsReady(k)
				reconcileKnight(r, k)
			}
			owned := snapshotsOf(k)
			names := make([]string, 0, len(owned))
			for _, s := range owned {
				names = append(names, s.GetName())
			}
			Expect(names).To(ContainElement("snap-foreign-manual"))
			Expect(names).To(HaveLen(2)) // foreign + newest owned (keep=1)
		})

		It("keeps only `keep` snapshots once the newest is ready", func() {
			k := pvcKnight("snap-keep", new(int32(2)))
			Expect(k8sClient.Create(ctx, k)).To(Succeed())
			defer deleteKnight(k)
			r, _ := snapReconciler(true)

			for i := 1; i <= 3; i++ {
				ts := time.Unix(1_700_000_000+int64(i), 0)
				r.Now = func() time.Time { return ts }
				annotateSnapshot(k, fmt.Sprint(i))
				reconcileKnight(r, k)
				if i < 3 {
					markSnapshotsReady(k)
					reconcileKnight(r, k)
				}
			}
			// The third isn't ready yet: nothing pruned.
			Expect(snapshotsOf(k)).To(HaveLen(3))

			markSnapshotsReady(k)
			reconcileKnight(r, k)
			snaps := snapshotsOf(k)
			Expect(snaps).To(HaveLen(2))
			names := []string{snaps[0].GetName(), snaps[1].GetName()}
			Expect(names).To(ConsistOf("snap-keep-ws-1700000002", "snap-keep-ws-1700000003"))
			status := getKnight(k).Status.Workspace.Snapshots
			Expect(status).To(HaveLen(2))
			Expect(status[1].ReadyToUse).To(BeTrue())
		})

		It("ignores the annotation in ephemeral mode", func() {
			k := scalingKnight("default", "snap-eph", ephemeral)
			k.Annotations = map[string]string{aiv1alpha1.AnnotationKnightSnapshot: "x"}
			Expect(k8sClient.Create(ctx, k)).To(Succeed())
			defer deleteKnight(k)
			r, _ := snapReconciler(true)
			reconcileKnight(r, k)
			Expect(snapshotsOf(k)).To(BeEmpty())
			c := cond(k)
			Expect(c).NotTo(BeNil())
			Expect(c.Status).To(Equal(metav1.ConditionFalse))
			Expect(c.Reason).To(Equal(aiv1alpha1.ReasonWorkspaceNotPersistent))
		})
	})

	It("reports SnapshotAPIUnavailable without the API and creates nothing", func() {
		k := pvcKnight("snap-noapi", nil)
		k.Annotations = map[string]string{aiv1alpha1.AnnotationKnightSnapshot: "x"}
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		defer deleteKnight(k)
		r, _ := snapReconciler(false)
		reconcileKnight(r, k)
		reconcileKnight(r, k)
		c := cond(k)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(aiv1alpha1.ReasonSnapshotAPIUnavailable))
		Expect(getKnight(k).Status.Workspace).To(BeNil())
	})
})

var _ = Describe("Knight snapshot API recheck (#725)", func() {
	It("notices the API after startup, probing at most once a minute", func() {
		_, err := envtest.InstallCRDs(cfg, envtest.CRDInstallOptions{
			CRDs: []*apiextensionsv1.CustomResourceDefinition{volumeSnapshotCRD()},
		})
		Expect(err).NotTo(HaveOccurred())

		k := scalingKnight("default", "snap-late", func(s *aiv1alpha1.KnightSpec) {
			s.Workspace = &aiv1alpha1.KnightWorkspace{
				Snapshots: &aiv1alpha1.WorkspaceSnapshots{ClassName: "fast"},
			}
		})
		k.Annotations = map[string]string{aiv1alpha1.AnnotationKnightSnapshot: "x"}
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		defer deleteKnight(k)

		r := newScalingReconciler(false)
		clock := time.Unix(1_700_000_000, 0)
		r.Now = func() time.Time { return clock }
		served, calls := false, 0
		r.SnapshotAPIProbe = func() (bool, error) { calls++; return served, nil }
		cond := func() *metav1.Condition {
			return meta.FindStatusCondition(getKnight(k).Status.Conditions, aiv1alpha1.ConditionKnightWorkspaceSnapshots)
		}

		reconcileRes := func() reconcile.Result {
			res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: k.Name, Namespace: k.Namespace}})
			Expect(err).NotTo(HaveOccurred())
			return res
		}
		res := reconcileRes()
		Expect(calls).To(Equal(1))
		Expect(res.RequeueAfter).To(Equal(time.Minute))
		Expect(cond().Reason).To(Equal(aiv1alpha1.ReasonSnapshotAPIUnavailable))
		Expect(cond().Message).NotTo(ContainSubstring("restart"))
		Expect(snapshotsOf(k)).To(BeEmpty())

		// Within the minute the probe is not called again.
		clock = clock.Add(30 * time.Second)
		served = true
		reconcileRes()
		Expect(calls).To(Equal(1))
		Expect(snapshotsOf(k)).To(BeEmpty())

		// After the minute the API is found and the snapshot is created.
		clock = clock.Add(31 * time.Second)
		res = reconcileRes()
		Expect(calls).To(Equal(2))
		snaps := snapshotsOf(k)
		Expect(snaps).To(HaveLen(1))
		Expect(snaps[0].GetName()).To(Equal(fmt.Sprintf("snap-late-ws-%d", clock.Unix())))
		Expect(cond().Reason).To(Equal(aiv1alpha1.ReasonSnapshotsReady))
		// Not ready yet and no watch: the knight is polled.
		Expect(res.RequeueAfter).To(Equal(RequeueSlow))

		// The positive answer is kept; no further probing.
		reconcileRes()
		Expect(calls).To(Equal(2))
	})
})

var _ = Describe("Knight workspace from a snapshot (#341)", func() {
	newSnapshot := func(name string, ready bool, restoreSize int64) {
		vs := &unstructured.Unstructured{}
		vs.SetGroupVersionKind(rtruntime.VolumeSnapshotGVK)
		vs.SetName(name)
		vs.SetNamespace("default")
		Expect(unstructured.SetNestedField(vs.Object, "src", "spec", "source", "persistentVolumeClaimName")).To(Succeed())
		Expect(k8sClient.Create(ctx, vs)).To(Succeed())
		if ready {
			Expect(unstructured.SetNestedField(vs.Object, true, "status", "readyToUse")).To(Succeed())
			Expect(unstructured.SetNestedField(vs.Object, restoreSize, "status", "restoreSize")).To(Succeed())
			Expect(k8sClient.Update(ctx, vs)).To(Succeed())
		}
	}
	forkKnight := func(name, snap, size string) *aiv1alpha1.Knight {
		return scalingKnight("default", name, func(s *aiv1alpha1.KnightSpec) {
			q := resource.MustParse(size)
			s.Workspace = &aiv1alpha1.KnightWorkspace{FromSnapshot: snap, Size: &q}
		})
	}
	getPVC := func(name string) (*corev1.PersistentVolumeClaim, error) {
		pvc := &corev1.PersistentVolumeClaim{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, pvc)
		return pvc, err
	}
	ready := func(k *aiv1alpha1.Knight) *metav1.Condition {
		return meta.FindStatusCondition(getKnight(k).Status.Conditions, aiv1alpha1.ConditionKnightWorkspaceReady)
	}
	fork := func() *KnightReconciler {
		r := newScalingReconciler(false)
		r.SnapshotsEnabled = true
		return r
	}

	BeforeEach(func() {
		_, err := envtest.InstallCRDs(cfg, envtest.CRDInstallOptions{
			CRDs: []*apiextensionsv1.CustomResourceDefinition{volumeSnapshotCRD()},
		})
		Expect(err).NotTo(HaveOccurred())
	})

	It("creates the PVC with the snapshot data source and the larger size", func() {
		newSnapshot("fork-src-big", true, 5<<30)
		k := forkKnight("fork-big", "fork-src-big", "1Gi")
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		defer deleteKnight(k)
		reconcileKnight(fork(), k)

		pvc, err := getPVC("fork-big")
		Expect(err).NotTo(HaveOccurred())
		ds := pvc.Spec.DataSourceRef
		Expect(ds).NotTo(BeNil())
		Expect(*ds.APIGroup).To(Equal("snapshot.storage.k8s.io"))
		Expect(ds.Kind).To(Equal("VolumeSnapshot"))
		Expect(ds.Name).To(Equal("fork-src-big"))
		got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		Expect(got.Cmp(resource.MustParse("5Gi"))).To(Equal(0))
		Expect(ready(k).Reason).To(Equal(aiv1alpha1.ReasonRestoredFromSnapshot))

		// spec.workspace.size wins when it is larger than restoreSize.
		newSnapshot("fork-src-small", true, 1<<30)
		k2 := forkKnight("fork-small", "fork-src-small", "3Gi")
		Expect(k8sClient.Create(ctx, k2)).To(Succeed())
		defer deleteKnight(k2)
		reconcileKnight(fork(), k2)
		pvc, err = getPVC("fork-small")
		Expect(err).NotTo(HaveOccurred())
		got = pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		Expect(got.Cmp(resource.MustParse("3Gi"))).To(Equal(0))
	})

	It("creates no PVC while the snapshot is missing or not ready, then creates it", func() {
		k := forkKnight("fork-wait", "fork-src-wait", "1Gi")
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		defer deleteKnight(k)
		r := fork()

		reconcileKnight(r, k)
		_, err := getPVC("fork-wait")
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
		c := ready(k)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(aiv1alpha1.ReasonSnapshotNotReady))

		newSnapshot("fork-src-wait", false, 0)
		reconcileKnight(r, k)
		_, err = getPVC("fork-wait")
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
		Expect(ready(k).Reason).To(Equal(aiv1alpha1.ReasonSnapshotNotReady))

		vs := &unstructured.Unstructured{}
		vs.SetGroupVersionKind(rtruntime.VolumeSnapshotGVK)
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "fork-src-wait", Namespace: "default"}, vs)).To(Succeed())
		Expect(unstructured.SetNestedField(vs.Object, true, "status", "readyToUse")).To(Succeed())
		Expect(k8sClient.Update(ctx, vs)).To(Succeed())
		reconcileKnight(r, k)
		_, err = getPVC("fork-wait")
		Expect(err).NotTo(HaveOccurred())
		Expect(ready(k).Status).To(Equal(metav1.ConditionTrue))
	})

	It("leaves an existing PVC untouched and says fromSnapshot was ignored", func() {
		existing := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "fork-exists", Namespace: "default"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("2Gi")},
				},
			},
		}
		Expect(k8sClient.Create(ctx, existing)).To(Succeed())
		k := forkKnight("fork-exists", "fork-src-none", "1Gi")
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		defer deleteKnight(k)
		reconcileKnight(fork(), k)

		pvc, err := getPVC("fork-exists")
		Expect(err).NotTo(HaveOccurred())
		Expect(pvc.Spec.DataSourceRef).To(BeNil())
		got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		Expect(got.Cmp(resource.MustParse("2Gi"))).To(Equal(0))
		c := ready(k)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionTrue))
		Expect(c.Reason).To(Equal(aiv1alpha1.ReasonFromSnapshotIgnored))
	})

	It("rejects fromSnapshot with existingClaim or ephemeral mode", func() {
		k := scalingKnight("default", "fork-bad-claim", func(s *aiv1alpha1.KnightSpec) {
			s.Workspace = &aiv1alpha1.KnightWorkspace{FromSnapshot: "x", ExistingClaim: "y"}
		})
		err := k8sClient.Create(ctx, k)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("fromSnapshot cannot be combined with workspace.existingClaim"))

		k = scalingKnight("default", "fork-bad-eph", func(s *aiv1alpha1.KnightSpec) {
			s.Workspace = &aiv1alpha1.KnightWorkspace{FromSnapshot: "x", Mode: aiv1alpha1.WorkspaceModeEphemeral}
		})
		err = k8sClient.Create(ctx, k)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("fromSnapshot cannot be combined with workspace.mode: ephemeral"))
	})
})
