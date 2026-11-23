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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
)

var _ = Describe("Knight tools image", func() {
	newKnight := func(name string, tools []string) *aiv1alpha1.Knight {
		k := &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name + "-uid")},
			Spec:       aiv1alpha1.KnightSpec{Domain: "security"},
		}
		if tools != nil {
			k.Spec.Tools = &aiv1alpha1.KnightTools{Nix: tools}
		}
		return k
	}

	BeforeEach(func() {
		GinkgoT().Setenv("KNIGHT_TOOLS_IMAGE_REPO", "registry.example.com/knight-tools")
		GinkgoT().Setenv("KNIGHT_TOOLS_IMAGE_PUSH_REPO", "registry.rt.svc:5000/knight-tools")
		GinkgoT().Setenv("KNIGHT_TOOLS_IMAGE_PUSH_INSECURE", "true")
	})

	// ── buildToolsImageJob (pure) ─────────────────────────────────────────
	Describe("buildToolsImageJob", func() {
		It("builds from the knight's flake and pushes to the push repository", func() {
			r := &KnightReconciler{}
			k := newKnight("galahad", []string{"nmap"})
			tag := knightpkg.ToolsImageTag(k)
			job := r.buildToolsImageJob(k)

			Expect(job.Name).To(Equal("galahad-toolsimg-" + tag))
			pod := job.Spec.Template.Spec
			Expect(pod.RestartPolicy).To(Equal(corev1.RestartPolicyNever))
			Expect(pod.Containers).To(HaveLen(1))

			c := pod.Containers[0]
			Expect(c.Image).To(Equal(knightpkg.ToolsBuilderImage()))
			Expect(c.Command).To(Equal([]string{"sh", "-c", toolsImageBuildScript}))

			env := map[string]string{}
			for _, e := range c.Env {
				env[e.Name] = e.Value
			}
			Expect(env["TOOLS_IMAGE_PUSH_REF"]).To(Equal("registry.rt.svc:5000/knight-tools:" + tag))
			Expect(env["TOOLS_IMAGE_MANIFEST_URL"]).To(Equal("http://registry.rt.svc:5000/v2/knight-tools/manifests/" + tag))
			Expect(env["TOOLS_IMAGE_COPY_ARGS"]).To(Equal("--dest-tls-verify=false"))
			Expect(env["TMPDIR"]).To(Equal("/work"))

			// The flake comes from the knight ConfigMap; no PVC anywhere.
			vols := map[string]corev1.Volume{}
			for _, v := range pod.Volumes {
				vols[v.Name] = v
				Expect(v.PersistentVolumeClaim).To(BeNil())
			}
			Expect(vols["flake"].ConfigMap.Name).To(Equal("knight-galahad-config"))
			Expect(vols["flake"].ConfigMap.Items).To(Equal([]corev1.KeyToPath{{Key: toolsImageFlakeKey, Path: "flake.nix"}}))
			Expect(vols["work"].EmptyDir).NotTo(BeNil())
		})

		It("omits the insecure flag when pushing over TLS", func() {
			GinkgoT().Setenv("KNIGHT_TOOLS_IMAGE_PUSH_INSECURE", "")
			job := (&KnightReconciler{}).buildToolsImageJob(newKnight("bors", []string{"jq"}))
			for _, e := range job.Spec.Template.Spec.Containers[0].Env {
				if e.Name == "TOOLS_IMAGE_COPY_ARGS" {
					Expect(e.Value).To(BeEmpty())
				}
			}
		})
	})

	// ── reconcileToolsImage (envtest) ─────────────────────────────────────
	Describe("reconcileToolsImage", func() {
		var r *KnightReconciler

		BeforeEach(func() {
			r = &KnightReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: record.NewFakeRecorder(10),
			}
		})

		getJob := func(k *aiv1alpha1.Knight) (*batchv1.Job, error) {
			job := &batchv1.Job{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: toolsImageJobName(k), Namespace: k.Namespace}, job)
			return job, err
		}

		It("creates no Job and clears the status for a knight without Nix tools", func() {
			k := newKnight("gareth", nil)
			k.Status.ToolsImage = "registry.example.com/knight-tools:stale"
			Expect(r.reconcileToolsImage(ctx, k)).To(Succeed())
			Expect(k.Status.ToolsImage).To(BeEmpty())
		})

		It("creates a build Job, then records the image once the Job succeeds", func() {
			k := newKnight("lamorak", []string{"nmap", "jq"})
			ref := knightpkg.ToolsImageRef(k)

			Expect(r.reconcileToolsImage(ctx, k)).To(Succeed())
			job, err := getJob(k)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = k8sClient.Delete(ctx, job) }()
			Expect(job.OwnerReferences).To(HaveLen(1))
			Expect(job.OwnerReferences[0].Name).To(Equal("lamorak"))
			Expect(k.Status.ToolsImage).To(BeEmpty(), "not published while the Job is still running")

			// A second pass while the Job runs changes nothing.
			Expect(r.reconcileToolsImage(ctx, k)).To(Succeed())
			Expect(k.Status.ToolsImage).To(BeEmpty())

			now := metav1.Now()
			job.Status.Succeeded = 1
			job.Status.StartTime = &now
			job.Status.CompletionTime = &now
			job.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: now},
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: now},
			}
			Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

			Expect(r.reconcileToolsImage(ctx, k)).To(Succeed())
			Expect(k.Status.ToolsImage).To(Equal(ref))
		})

		It("does not build when the current image is already recorded", func() {
			k := newKnight("tor", []string{"curl"})
			k.Status.ToolsImage = knightpkg.ToolsImageRef(k)
			Expect(r.reconcileToolsImage(ctx, k)).To(Succeed())
			_, err := getJob(k)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("keeps the pod on the published image until a changed tool list is built", func() {
			nixVolumeRef := func(k *aiv1alpha1.Knight) string {
				for _, v := range r.BuildPodSpec(ctx, k).Volumes {
					if v.Name == "nix" && v.Image != nil {
						return v.Image.Reference
					}
				}
				return ""
			}

			k := newKnight("palamedes", []string{"curl"})
			oldRef := knightpkg.ToolsImageRef(k)
			k.Status.ToolsImage = oldRef
			Expect(nixVolumeRef(k)).To(Equal(oldRef))

			// Tool list changes: a Job starts, the pod spec does not move.
			k.Spec.Tools.Nix = []string{"curl", "nmap"}
			newRef := knightpkg.ToolsImageRef(k)
			Expect(r.reconcileToolsImage(ctx, k)).To(Succeed())
			job, err := getJob(k)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = k8sClient.Delete(ctx, job) }()
			Expect(nixVolumeRef(k)).To(Equal(oldRef), "pod must stay on the old tools while the new image builds")

			// Job succeeds: the same reconcile pass now yields the new image.
			now := metav1.Now()
			job.Status.Succeeded = 1
			job.Status.StartTime = &now
			job.Status.CompletionTime = &now
			job.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: now},
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: now},
			}
			Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
			Expect(r.reconcileToolsImage(ctx, k)).To(Succeed())
			Expect(nixVolumeRef(k)).To(Equal(newRef))
		})

		It("builds again when the tool list changes", func() {
			k := newKnight("ector", []string{"curl"})
			k.Status.ToolsImage = knightpkg.ToolsImageRef(k)
			k.Spec.Tools.Nix = []string{"curl", "nmap"}

			Expect(r.reconcileToolsImage(ctx, k)).To(Succeed())
			job, err := getJob(k)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = k8sClient.Delete(ctx, job) }()
			Expect(job.Name).To(HaveSuffix(knightpkg.ToolsImageTag(k)))
		})
	})

	// ── reconcileNixBuild dispatch ────────────────────────────────────────
	It("takes the tools image path instead of the shared store when a repository is set", func() {
		r := &KnightReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(10)}
		k := newKnight("dinadan", []string{"jq"})

		requeue, err := r.reconcileNixBuild(ctx, k)
		Expect(err).NotTo(HaveOccurred())
		Expect(requeue).To(BeZero())

		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: toolsImageJobName(k), Namespace: "default"}, job)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, job) }()
		Expect(job.Labels["roundtable.io/purpose"]).To(Equal("tools-image-build"))
	})
})
