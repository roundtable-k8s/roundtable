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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

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
				// Registry reports every image present unless a test says otherwise.
				ToolsImageMissing: func(context.Context, string) bool { return false },
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

		It("rebuilds when the registry has lost the published image", func() {
			k := newKnight("bedivere", []string{"curl"})
			ref := knightpkg.ToolsImageRef(k)
			k.Status.ToolsImage = ref

			var probed []string
			r.ToolsImageMissing = func(_ context.Context, url string) bool {
				probed = append(probed, url)
				return true
			}

			Expect(r.reconcileToolsImage(ctx, k)).To(Succeed())
			Expect(probed).To(Equal([]string{knightpkg.ToolsImageManifestURL(k)}))
			Expect(k.Status.ToolsImage).To(BeEmpty(), "a lost image is no longer published")
			job, err := getJob(k)
			Expect(err).NotTo(HaveOccurred(), "a new build Job is created")
			defer func() { _ = k8sClient.Delete(ctx, job) }()

			// The pod keeps referencing the same tag, so a running knight is not rolled.
			Expect(knightpkg.MountedToolsImageRef(k)).To(Equal(ref))
		})

		It("removes the finished Job of a lost image so the rebuild is not skipped", func() {
			k := newKnight("lucan", []string{"curl", "jq"})
			ref := knightpkg.ToolsImageRef(k)

			// First build: Job created and completed, image recorded.
			Expect(r.reconcileToolsImage(ctx, k)).To(Succeed())
			job, err := getJob(k)
			Expect(err).NotTo(HaveOccurred())
			oldUID := job.UID
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

			// Registry loses it. A fresh reconciler has no memory of the earlier check.
			r2 := &KnightReconciler{
				Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(10),
				ToolsImageMissing: func(context.Context, string) bool { return true },
			}
			Expect(r2.reconcileToolsImage(ctx, k)).To(Succeed())
			Expect(k.Status.ToolsImage).To(BeEmpty())

			// The stale Job goes away, and the next pass creates a new one
			// instead of reading the old success as "published".
			Eventually(func() bool {
				j, err := getJob(k)
				return apierrors.IsNotFound(err) || (err == nil && j.UID != oldUID)
			}).Should(BeTrue())
			r2.ToolsImageMissing = func(context.Context, string) bool { return false }
			Expect(r2.reconcileToolsImage(ctx, k)).To(Succeed())
			Expect(k.Status.ToolsImage).To(BeEmpty(), "not published again until the new Job succeeds")
			newJob, err := getJob(k)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = k8sClient.Delete(ctx, newJob) }()
			Expect(newJob.UID).NotTo(Equal(oldUID))
		})

		It("checks the registry at most once per interval for an image", func() {
			k := newKnight("pellinore", []string{"curl", "whois"})
			k.Status.ToolsImage = knightpkg.ToolsImageRef(k)
			calls := 0
			r.ToolsImageMissing = func(context.Context, string) bool { calls++; return false }

			Expect(r.reconcileToolsImage(ctx, k)).To(Succeed())
			Expect(r.reconcileToolsImage(ctx, k)).To(Succeed())
			Expect(calls).To(Equal(1))
			Expect(k.Status.ToolsImage).NotTo(BeEmpty())
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

	// ── registry probe ────────────────────────────────────────────────────
	Describe("probeToolsImageMissing", func() {
		It("reports missing only on a 404 from the registry", func() {
			status := http.StatusOK
			var gotMethod, gotAccept string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				gotMethod, gotAccept = req.Method, req.Header.Get("Accept")
				w.WriteHeader(status)
			}))
			url := srv.URL + "/v2/knight-tools/manifests/abc"

			Expect(probeToolsImageMissing(ctx, url)).To(BeFalse())
			// GET, so a registry with pull-based retention sees the check as a pull.
			Expect(gotMethod).To(Equal(http.MethodGet))
			Expect(gotAccept).To(ContainSubstring("application/vnd.oci.image.manifest.v1+json"))

			status = http.StatusNotFound
			Expect(probeToolsImageMissing(ctx, url)).To(BeTrue())

			// A broken or unreachable registry is not evidence the image is gone.
			status = http.StatusInternalServerError
			Expect(probeToolsImageMissing(ctx, url)).To(BeFalse())
			srv.Close()
			Expect(probeToolsImageMissing(ctx, url)).To(BeFalse())
		})
	})

	// ── reconcileNixTools ────────────────────────────────────────
	It("asks to be requeued to re-verify a published image", func() {
		r := &KnightReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(10),
			ToolsImageMissing: func(context.Context, string) bool { return false },
		}
		k := newKnight("sagramore", []string{"jq", "tree"})
		k.Status.ToolsImage = knightpkg.ToolsImageRef(k)

		requeue, err := r.reconcileNixTools(ctx, k)
		Expect(err).NotTo(HaveOccurred())
		Expect(requeue).To(Equal(toolsImageVerifyInterval))
	})

	It("warns and builds nothing when no tools image repository is configured", func() {
		GinkgoT().Setenv("KNIGHT_TOOLS_IMAGE_REPO", "")
		rec := record.NewFakeRecorder(10)
		r := &KnightReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: rec}
		k := newKnight("agravain", []string{"jq"})

		requeue, err := r.reconcileNixTools(ctx, k)
		Expect(err).NotTo(HaveOccurred())
		Expect(requeue).To(BeZero())
		Expect(rec.Events).To(Receive(ContainSubstring("ToolsImageNotConfigured")))

		job := &batchv1.Job{}
		err = k8sClient.Get(ctx, types.NamespacedName{Name: "agravain-toolsimg-" + knightpkg.ToolsImageTag(k), Namespace: "default"}, job)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	It("rejects a flake ref whose dir escapes the repository, warns InvalidFlakeRef and builds nothing", func() {
		rec := record.NewFakeRecorder(10)
		r := &KnightReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: rec}
		k := newKnight("badflake", nil)
		k.Spec.Tools = &aiv1alpha1.KnightTools{Flakes: []aiv1alpha1.KnightToolsFlake{
			{Ref: "github:roundtable-k8s/roundtable/0123456789abcdef0123456789abcdef01234567?dir=../escape", Packages: []string{"jq"}},
		}}

		requeue, err := r.reconcileNixTools(ctx, k)
		Expect(err).To(MatchError(ContainSubstring("spec.tools.flakes")))
		Expect(err).To(MatchError(ContainSubstring("parent-directory")))
		Expect(requeue).To(BeZero())
		Expect(rec.Events).To(Receive(And(ContainSubstring("Warning"), ContainSubstring("InvalidFlakeRef"))))

		jobs := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, jobs, client.InNamespace("default"), client.MatchingLabels{"roundtable.io/purpose": "tools-image-build"})).To(Succeed())
		for _, j := range jobs.Items {
			Expect(j.Name).NotTo(HavePrefix("badflake-"))
		}
	})

	It("builds a tools image for a knight with Nix tools", func() {
		r := &KnightReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(10)}
		k := newKnight("dinadan", []string{"jq"})

		requeue, err := r.reconcileNixTools(ctx, k)
		Expect(err).NotTo(HaveOccurred())
		Expect(requeue).To(BeZero())

		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: toolsImageJobName(k), Namespace: "default"}, job)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, job) }()
		Expect(job.Labels["roundtable.io/purpose"]).To(Equal("tools-image-build"))
	})
})

var _ = Describe("Knight tools image rollout hold", func() {
	const ns = "default"

	BeforeEach(func() {
		GinkgoT().Setenv("KNIGHT_TOOLS_IMAGE_REPO", "registry.example.com/knight-tools")
		GinkgoT().Setenv("KNIGHT_TOOLS_IMAGE_PUSH_REPO", "registry.rt.svc:5000/knight-tools")
	})

	finishJob := func(k *aiv1alpha1.Knight, succeeded bool) {
		job := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: toolsImageJobName(k), Namespace: ns}, job)).To(Succeed())
		now := metav1.Now()
		job.Status.StartTime = &now
		if succeeded {
			job.Status.Succeeded = 1
			job.Status.CompletionTime = &now
			job.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: now},
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: now},
			}
		} else {
			job.Status.Failed = *job.Spec.BackoffLimit + 1
			job.Status.Conditions = []batchv1.JobCondition{
				{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, LastTransitionTime: now},
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: now},
			}
		}
		Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())
	}

	It("keeps the Deployment on the old template until the new tools image is published", func() {
		name := "tools-hold"
		nn := types.NamespacedName{Name: name, Namespace: ns}
		k := &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "security",
				Model:  "claude-sonnet-4-20250514",
				Skills: []string{"security", "shared"},
				NATS: aiv1alpha1.KnightNATS{
					URL: "nats://nats.test:4222", Subjects: []string{"test.tasks.security.>"},
					Stream: "test_tasks", ResultsStream: "test_results", MaxDeliver: 1,
				},
				Tools: &aiv1alpha1.KnightTools{Nix: []string{"curl"}},
			},
		}
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		defer func() {
			cur := &aiv1alpha1.Knight{}
			if err := k8sClient.Get(ctx, nn, cur); err == nil {
				cur.Finalizers = nil
				_ = k8sClient.Update(ctx, cur)
				_ = k8sClient.Delete(ctx, cur)
			}
		}()

		r := &KnightReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(100),
			ToolsImageMissing: func(context.Context, string) bool { return false },
		}
		reconcileOnce := func() *aiv1alpha1.Knight {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
			cur := &aiv1alpha1.Knight{}
			Expect(k8sClient.Get(ctx, nn, cur)).To(Succeed())
			return cur
		}
		getDeploy := func() *appsv1.Deployment {
			d := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, nn, d)).To(Succeed())
			return d
		}
		cond := func(k *aiv1alpha1.Knight) *metav1.Condition {
			return meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightToolsImageReady)
		}
		setSpec := func(mut func(*aiv1alpha1.Knight)) {
			cur := &aiv1alpha1.Knight{}
			Expect(k8sClient.Get(ctx, nn, cur)).To(Succeed())
			mut(cur)
			Expect(k8sClient.Update(ctx, cur)).To(Succeed())
		}
		cleanupJobs := func() {
			jobs := &batchv1.JobList{}
			Expect(k8sClient.List(ctx, jobs, client.InNamespace(ns))).To(Succeed())
			for i := range jobs.Items {
				if strings.HasPrefix(jobs.Items[i].Name, name+"-toolsimg-") {
					_ = k8sClient.Delete(ctx, &jobs.Items[i], client.PropagationPolicy(metav1.DeletePropagationBackground))
				}
			}
		}
		defer cleanupJobs()
		defer func() {
			_ = k8sClient.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
		}()

		// Initial build: no workload yet, so the Deployment is created and the Job starts.
		cur := reconcileOnce()
		Expect(cond(cur)).NotTo(BeNil())
		Expect(cond(cur).Reason).To(Equal(aiv1alpha1.ReasonToolsImageBuilding))
		finishJob(cur, true)
		cur = reconcileOnce()
		Expect(cur.Status.ToolsImage).To(Equal(knightpkg.ToolsImageRef(cur)))
		Expect(cond(cur).Status).To(Equal(metav1.ConditionTrue))
		reconcileOnce() // settle the Deployment on the published image
		oldTemplate, err := json.Marshal(getDeploy().Spec.Template)
		Expect(err).NotTo(HaveOccurred())

		// Spec change that needs a new image, plus an unrelated pod-affecting change.
		setSpec(func(k *aiv1alpha1.Knight) {
			k.Spec.Tools.Nix = []string{"curl", "nmap"}
			k.Spec.Model = "claude-opus-4-20250514"
		})
		cur = reconcileOnce()
		newJob := toolsImageJobName(cur)
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: newJob, Namespace: ns}, &batchv1.Job{})).To(Succeed())
		Expect(cond(cur).Reason).To(Equal(aiv1alpha1.ReasonToolsImageBuilding))
		held, err := json.Marshal(getDeploy().Spec.Template)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(held)).To(Equal(string(oldTemplate)), "pod template must not change while the build runs")

		// Failed build: still no rollout, and the condition says so.
		finishJob(cur, false)
		cur = reconcileOnce()
		Expect(cond(cur).Status).To(Equal(metav1.ConditionFalse))
		Expect(cond(cur).Reason).To(Equal(aiv1alpha1.ReasonToolsImageBuildFailed))
		held, err = json.Marshal(getDeploy().Spec.Template)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(held)).To(Equal(string(oldTemplate)), "a failed build must not roll the pod")

		// Build recovers (Job replaced and succeeds): one rollout with the new spec and image.
		Expect(k8sClient.Delete(ctx, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: newJob, Namespace: ns}},
			client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: newJob, Namespace: ns}, &batchv1.Job{}))
		}).Should(BeTrue())
		cur = reconcileOnce() // recreates the Job
		finishJob(cur, true)
		cur = reconcileOnce()
		Expect(cur.Status.ToolsImage).To(Equal(knightpkg.ToolsImageRef(cur)))
		Expect(cond(cur).Status).To(Equal(metav1.ConditionTrue))
		rolled, err := json.Marshal(getDeploy().Spec.Template)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(rolled)).NotTo(Equal(string(oldTemplate)))
		Expect(string(rolled)).To(ContainSubstring(knightpkg.ToolsImageRef(cur)))
	})
})
