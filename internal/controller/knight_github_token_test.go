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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	rtruntime "github.com/roundtable-k8s/roundtable/pkg/runtime"
)

// expectGitHubTokenMount asserts the pod mounts secretName/key as the
// refreshable token file and exports only its path.
func expectGitHubTokenMount(spec corev1.PodSpec, secretName, key string) {
	var vol *corev1.Volume
	for i := range spec.Volumes {
		if spec.Volumes[i].Name == "github-token" {
			vol = &spec.Volumes[i]
		}
	}
	Expect(vol).NotTo(BeNil(), "github-token volume")
	Expect(vol.Secret).NotTo(BeNil())
	Expect(vol.Secret.SecretName).To(Equal(secretName))
	Expect(vol.Secret.Items).To(ConsistOf(corev1.KeyToPath{Key: key, Path: "token"}))

	app := spec.Containers[0]
	Expect(app.VolumeMounts).To(ContainElement(corev1.VolumeMount{
		Name:      "github-token",
		MountPath: "/var/run/secrets/roundtable/github",
		ReadOnly:  true,
	}))
	Expect(app.Env).To(ContainElement(corev1.EnvVar{
		Name:  "GITHUB_TOKEN_FILE",
		Value: "/var/run/secrets/roundtable/github/token",
	}))
}

var _ = Describe("Knight GitHub token file", func() {
	ctx := context.Background()

	var (
		rtName     string
		knightName string
		reconciler *KnightReconciler
	)

	newKnight := func(name string, labels map[string]string, gh *aiv1alpha1.GitHubConfig) *aiv1alpha1.Knight {
		return &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: labels},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "code",
				Skills: []string{"shared"},
				NATS: aiv1alpha1.KnightNATS{
					URL:           "nats://nats.test:4222",
					Subjects:      []string{"gh.tasks.code.>"},
					Stream:        "gh_tasks",
					ResultsStream: "gh_results",
				},
				GitHub: gh,
			},
		}
	}

	reconcileTwice := func(name string) {
		req := reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}}
		_, _ = reconciler.Reconcile(ctx, req) // adds the finalizer
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
	}

	getDeployment := func(name string) *appsv1.Deployment {
		d := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, d)).To(Succeed())
		return d
	}

	BeforeEach(func() {
		rtName = "gh-fleet"
		knightName = "gh-knight"
		reconciler = &KnightReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: record.NewFakeRecorder(100),
		}
		rt := &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: "default"},
			Spec: aiv1alpha1.RoundTableSpec{
				NATS: aiv1alpha1.RoundTableNATS{SubjectPrefix: "gh", TasksStream: "gh_tasks", ResultsStream: "gh_results"},
				// key omitted: the CRD defaults it to "token".
				GitHub: &aiv1alpha1.GitHubConfig{
					TokenSecretRef: &aiv1alpha1.GitHubTokenSecretRef{Name: "roundtable-github-token"},
				},
			},
		}
		Expect(k8sClient.Create(ctx, rt)).To(Succeed())
	})

	AfterEach(func() {
		for _, name := range []string{knightName, knightName + "-own"} {
			k := &aiv1alpha1.Knight{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, k); err == nil {
				k.Finalizers = nil
				_ = k8sClient.Update(ctx, k)
				_ = k8sClient.Delete(ctx, k)
			}
			_ = k8sClient.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}})
		}
		_ = k8sClient.Delete(ctx, &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: "default"}})
	})

	It("defaults tokenSecretRef.key to token", func() {
		rt := &aiv1alpha1.RoundTable{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rtName, Namespace: "default"}, rt)).To(Succeed())
		Expect(rt.Spec.GitHub.TokenSecretRef.Key).To(Equal("token"))
	})

	It("mounts the RoundTable's token Secret into a member knight's Deployment", func() {
		Expect(k8sClient.Create(ctx, newKnight(knightName,
			map[string]string{aiv1alpha1.LabelRoundTable: rtName}, nil))).To(Succeed())
		reconcileTwice(knightName)
		expectGitHubTokenMount(getDeployment(knightName).Spec.Template.Spec, "roundtable-github-token", "token")
	})

	It("lets a Knight override the RoundTable's Secret", func() {
		name := knightName + "-own"
		Expect(k8sClient.Create(ctx, newKnight(name,
			map[string]string{aiv1alpha1.LabelRoundTable: rtName},
			&aiv1alpha1.GitHubConfig{TokenSecretRef: &aiv1alpha1.GitHubTokenSecretRef{Name: "own-token", Key: "gh"}},
		))).To(Succeed())
		reconcileTwice(name)
		expectGitHubTokenMount(getDeployment(name).Spec.Template.Spec, "own-token", "gh")
	})

	It("mounts nothing for a knight outside any table without an override", func() {
		Expect(k8sClient.Create(ctx, newKnight(knightName, nil, nil))).To(Succeed())
		reconcileTwice(knightName)
		spec := getDeployment(knightName).Spec.Template.Spec
		for _, v := range spec.Volumes {
			Expect(v.Name).NotTo(Equal("github-token"))
		}
		for _, e := range spec.Containers[0].Env {
			Expect(e.Name).NotTo(Equal("GITHUB_TOKEN_FILE"))
		}
	})

	It("gives the Sandbox backend the same mount", func() {
		k := newKnight(knightName, map[string]string{aiv1alpha1.LabelRoundTable: rtName}, nil)
		Expect(k8sClient.Create(ctx, k)).To(Succeed())

		// envtest has no agent-sandbox CRDs: store the Sandbox in a fake
		// client, but resolve the RoundTable from the real API server.
		s := k8sruntime.NewScheme()
		Expect(aiv1alpha1.AddToScheme(s)).To(Succeed())
		Expect(sandboxv1alpha1.AddToScheme(s)).To(Succeed())
		sandboxClient := fake.NewClientBuilder().WithScheme(s).Build()
		backend := rtruntime.NewSandboxBackend(sandboxClient, s, "", reconciler.BuildPodSpec)
		Expect(backend.Reconcile(ctx, k)).To(Succeed())

		sb := &sandboxv1alpha1.Sandbox{}
		Expect(sandboxClient.Get(ctx, client.ObjectKeyFromObject(k), sb)).To(Succeed())
		expectGitHubTokenMount(sb.Spec.PodTemplate.Spec, "roundtable-github-token", "token")
	})

	It("stamps the RoundTable's Secret onto warm-pool knights", func() {
		poolRT := &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: "gh-pool", Namespace: "default"},
			Spec: aiv1alpha1.RoundTableSpec{
				NATS: aiv1alpha1.RoundTableNATS{SubjectPrefix: "ghp", TasksStream: "ghp_tasks", ResultsStream: "ghp_results"},
				GitHub: &aiv1alpha1.GitHubConfig{
					TokenSecretRef: &aiv1alpha1.GitHubTokenSecretRef{Name: "roundtable-github-token", Key: "token"},
				},
				WarmPool: &aiv1alpha1.WarmPoolConfig{
					Size: 1,
					Template: aiv1alpha1.KnightSpec{
						Domain: "general",
						Skills: []string{"general"},
						NATS:   aiv1alpha1.KnightNATS{Subjects: []string{"ghp.tasks.general.>"}},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, poolRT)).To(Succeed())
		defer func() {
			knights := &aiv1alpha1.KnightList{}
			_ = k8sClient.List(ctx, knights, client.InNamespace("default"),
				client.MatchingLabels{aiv1alpha1.LabelRoundTable: poolRT.Name})
			for i := range knights.Items {
				_ = k8sClient.Delete(ctx, &knights.Items[i])
			}
			_ = k8sClient.Delete(ctx, poolRT)
		}()

		rtReconciler := &RoundTableReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(10)}
		Expect(rtReconciler.createWarmKnight(ctx, poolRT)).To(Succeed())

		knights := &aiv1alpha1.KnightList{}
		Expect(k8sClient.List(ctx, knights, client.InNamespace("default"),
			client.MatchingLabels{aiv1alpha1.LabelRoundTable: poolRT.Name})).To(Succeed())
		Expect(knights.Items).To(HaveLen(1))
		gh := knights.Items[0].Spec.GitHub
		Expect(gh).NotTo(BeNil())
		Expect(gh.TokenSecretRef).To(Equal(&aiv1alpha1.GitHubTokenSecretRef{Name: "roundtable-github-token", Key: "token"}))
	})
})
