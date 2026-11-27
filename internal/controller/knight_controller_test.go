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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

var _ = Describe("Knight Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-knight"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		BeforeEach(func() {
			By("creating the custom resource for the Kind Knight")
			knight := &aiv1alpha1.Knight{}
			err := k8sClient.Get(ctx, typeNamespacedName, knight)
			if err != nil && errors.IsNotFound(err) {
				resource := &aiv1alpha1.Knight{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: aiv1alpha1.KnightSpec{
						Domain: "security",
						Model:  "claude-sonnet-4-20250514",
						Skills: []string{"security", "shared"},
						NATS: aiv1alpha1.KnightNATS{
							URL:           "nats://nats.test:4222",
							Subjects:      []string{"test.tasks.security.>"},
							Stream:        "test_tasks",
							ResultsStream: "test_results",
							MaxDeliver:    1,
						},
						Concurrency:        2,
						TaskTimeoutSeconds: 120,
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &aiv1alpha1.Knight{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				By("Cleanup the specific resource instance Knight")
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}
		})

		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &KnightReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: record.NewFakeRecorder(100),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Checking the Knight has a finalizer")
			knight := &aiv1alpha1.Knight{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, knight)).To(Succeed())
			Expect(knight.Finalizers).To(ContainElement(knightFinalizer))
		})

		It("should create a ConfigMap with knight configuration", func() {
			controllerReconciler := &KnightReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: record.NewFakeRecorder(100),
			}

			// Run reconciliation twice — first adds finalizer, second creates resources
			_, _ = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      "knight-test-knight-config",
				Namespace: "default",
			}, cm)).To(Succeed())

			Expect(cm.Data["KNIGHT_SKILLS"]).To(Equal("security,shared"))
			Expect(cm.Data).NotTo(HaveKey("mise.toml"))
			Expect(cm.Data).NotTo(HaveKey("apt.txt"))
			Expect(cm.Labels["roundtable.io/domain"]).To(Equal("security"))
		})

		It("should create a PVC for the knight workspace", func() {
			controllerReconciler := &KnightReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: record.NewFakeRecorder(100),
			}

			_, _ = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			pvc := &corev1.PersistentVolumeClaim{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      "test-knight",
				Namespace: "default",
			}, pvc)).To(Succeed())
		})

		It("should create a Deployment with correct containers", func() {
			controllerReconciler := &KnightReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: record.NewFakeRecorder(100),
			}

			_, _ = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, deploy)).To(Succeed())

			// Should have 3 containers: knight + skill-filter + the default platform git-sync
			Expect(deploy.Spec.Template.Spec.Containers).To(HaveLen(3))
			Expect(deploy.Spec.Template.Spec.Containers[0].Name).To(Equal("app"))
			Expect(deploy.Spec.Template.Spec.Containers[1].Name).To(Equal("skill-filter"))
			Expect(deploy.Spec.Template.Spec.Containers[2].Name).To(Equal("git-sync-platform"))

			// Check labels
			Expect(deploy.Labels["roundtable.io/domain"]).To(Equal("security"))

			// Check automount is enabled (knights may need in-cluster access)
			Expect(*deploy.Spec.Template.Spec.AutomountServiceAccountToken).To(BeTrue())
		})
	})

	Describe("deriveResultsPrefix", func() {
		It("returns empty string for empty subjects", func() {
			Expect(deriveResultsPrefix(nil)).To(Equal(""))
			Expect(deriveResultsPrefix([]string{})).To(Equal(""))
		})

		It("extracts table-prefix from tasks subject", func() {
			subjects := []string{"table-prefix.tasks.>"}
			Expect(deriveResultsPrefix(subjects)).To(Equal("table-prefix.results"))
		})

		It("extracts rt-dev prefix", func() {
			subjects := []string{"rt-dev.tasks.planning.>"}
			Expect(deriveResultsPrefix(subjects)).To(Equal("rt-dev.results"))
		})

		It("extracts chelonian prefix", func() {
			subjects := []string{"chelonian.tasks.mission.abc123"}
			Expect(deriveResultsPrefix(subjects)).To(Equal("chelonian.results"))
		})

		It("uses first valid subject with .tasks.", func() {
			subjects := []string{"bogus", "myfleet.tasks.something"}
			Expect(deriveResultsPrefix(subjects)).To(Equal("myfleet.results"))
		})

		It("falls back to dot-split for malformed subject without .tasks.", func() {
			subjects := []string{"something.else.entirely"}
			Expect(deriveResultsPrefix(subjects)).To(Equal("something.results"))
		})

		It("returns empty string for single-segment subject", func() {
			subjects := []string{"nope"}
			Expect(deriveResultsPrefix(subjects)).To(Equal(""))
		})
	})

	Describe("cleanupStaleRuntime", func() {
		var (
			ctx                context.Context
			reconciler         *KnightReconciler
			knightName         string
			knightNamespace    string
			typeNamespacedName types.NamespacedName
		)

		BeforeEach(func() {
			ctx = context.Background()
			reconciler = &KnightReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: record.NewFakeRecorder(100),
			}
			knightName = "test-runtime-transition"
			knightNamespace = "default"
			typeNamespacedName = types.NamespacedName{
				Name:      knightName,
				Namespace: knightNamespace,
			}
		})

		AfterEach(func() {
			// Clean up knight if it exists
			knight := &aiv1alpha1.Knight{}
			if err := k8sClient.Get(ctx, typeNamespacedName, knight); err == nil {
				_ = k8sClient.Delete(ctx, knight)
			}
			// Clean up deployment if it exists
			deploy := &appsv1.Deployment{}
			if err := k8sClient.Get(ctx, typeNamespacedName, deploy); err == nil {
				_ = k8sClient.Delete(ctx, deploy)
			}
		})

		It("deletes stale Deployment when runtime is sandbox", func() {
			// Create a Knight with default runtime (deployment)
			knight := &aiv1alpha1.Knight{
				ObjectMeta: metav1.ObjectMeta{
					Name:      knightName,
					Namespace: knightNamespace,
				},
				Spec: aiv1alpha1.KnightSpec{
					Domain: "test",
					Model:  "claude-sonnet-4-20250514",
					Skills: []string{"shared"},
					NATS: aiv1alpha1.KnightNATS{
						URL:           "nats://nats.test:4222",
						Subjects:      []string{"test.tasks.>"},
						Stream:        "test_tasks",
						ResultsStream: "test_results",
					},
				},
			}
			Expect(k8sClient.Create(ctx, knight)).To(Succeed())

			// Create a Deployment manually (simulating previous reconcile)
			deploy := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      knightName,
					Namespace: knightNamespace,
				},
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"app": "test"},
					},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{"app": "test"},
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name:  "test",
								Image: "test:latest",
							}},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, deploy)).To(Succeed())

			// Verify deployment exists
			deployCheck := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, deployCheck)).To(Succeed())

			// Switch knight to sandbox runtime
			knight.Spec.Runtime = "sandbox"

			// Call cleanupStaleRuntime
			err := reconciler.cleanupStaleRuntime(ctx, knight)
			Expect(err).NotTo(HaveOccurred())

			// Verify deployment was deleted
			deployAfter := &appsv1.Deployment{}
			err = k8sClient.Get(ctx, typeNamespacedName, deployAfter)
			Expect(errors.IsNotFound(err)).To(BeTrue())
		})

		It("is idempotent when no stale resources exist", func() {
			// Create a Knight with default runtime
			knight := &aiv1alpha1.Knight{
				ObjectMeta: metav1.ObjectMeta{
					Name:      knightName,
					Namespace: knightNamespace,
				},
				Spec: aiv1alpha1.KnightSpec{
					Domain: "test",
					Model:  "claude-sonnet-4-20250514",
					Skills: []string{"shared"},
					NATS: aiv1alpha1.KnightNATS{
						URL:           "nats://nats.test:4222",
						Subjects:      []string{"test.tasks.>"},
						Stream:        "test_tasks",
						ResultsStream: "test_results",
					},
				},
			}
			Expect(k8sClient.Create(ctx, knight)).To(Succeed())

			// Call cleanupStaleRuntime without any stale resources
			err := reconciler.cleanupStaleRuntime(ctx, knight)
			Expect(err).NotTo(HaveOccurred())

			// Call again to verify idempotence
			err = reconciler.cleanupStaleRuntime(ctx, knight)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("Nix store (shared read-only cutover)", func() {
		var (
			ctx                context.Context
			reconciler         *KnightReconciler
			knightName         string
			knightNamespace    string
			typeNamespacedName types.NamespacedName
		)

		BeforeEach(func() {
			ctx = context.Background()
			reconciler = &KnightReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: record.NewFakeRecorder(100),
			}
			knightName = "test-nix-shared"
			knightNamespace = "default"
			typeNamespacedName = types.NamespacedName{Name: knightName, Namespace: knightNamespace}
		})

		AfterEach(func() {
			knight := &aiv1alpha1.Knight{}
			if err := k8sClient.Get(ctx, typeNamespacedName, knight); err == nil {
				_ = k8sClient.Delete(ctx, knight)
				_, _ = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			}
			Eventually(func() bool {
				return errors.IsNotFound(k8sClient.Get(ctx, typeNamespacedName, &aiv1alpha1.Knight{}))
			}, "5s", "100ms").Should(BeTrue())

			// Strip the pvc-protection finalizer (no kube-controller-manager in
			// envtest) so the workspace PVC actually gets deleted.
			key := types.NamespacedName{Name: knightName, Namespace: knightNamespace}
			if pvc := (&corev1.PersistentVolumeClaim{}); k8sClient.Get(ctx, key, pvc) == nil {
				_ = k8sClient.Delete(ctx, pvc)
			}
			Eventually(func() bool {
				pvc := &corev1.PersistentVolumeClaim{}
				err := k8sClient.Get(ctx, key, pvc)
				if errors.IsNotFound(err) {
					return true
				}
				if err == nil && pvc.DeletionTimestamp != nil && len(pvc.Finalizers) > 0 {
					pvc.Finalizers = nil
					_ = k8sClient.Update(ctx, pvc)
				}
				return false
			}, "5s", "100ms").Should(BeTrue())
		})

		It("does not provision a per-knight Nix PVC (knights mount the shared store)", func() {
			By("Creating a knight with Nix tools")
			knight := &aiv1alpha1.Knight{
				ObjectMeta: metav1.ObjectMeta{Name: knightName, Namespace: knightNamespace},
				Spec: aiv1alpha1.KnightSpec{
					Domain: "devops",
					Model:  "claude-sonnet-4-20250514",
					Skills: []string{"shared"},
					NATS: aiv1alpha1.KnightNATS{
						URL:           "nats://nats.test:4222",
						Subjects:      []string{"test.tasks.devops.>"},
						Stream:        "test_tasks",
						ResultsStream: "test_results",
					},
					Tools: &aiv1alpha1.KnightTools{Nix: []string{"nmap", "curl"}},
				},
			}
			Expect(k8sClient.Create(ctx, knight)).To(Succeed())

			By("Reconciling")
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Verifying the workspace PVC exists but no per-knight Nix PVC was created")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: knightName, Namespace: knightNamespace},
				&corev1.PersistentVolumeClaim{})).To(Succeed())

			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      "knight-" + knightName + "-nix",
				Namespace: knightNamespace,
			}, &corev1.PersistentVolumeClaim{})
			Expect(errors.IsNotFound(err)).To(BeTrue(),
				"per-knight Nix PVC must not be created after the shared-store cutover")

			By("Verifying the ConfigMap has the tools data but no mise.toml/apt.txt")
			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      "knight-" + knightName + "-config",
				Namespace: knightNamespace,
			}, cm)).To(Succeed())
			Expect(cm.Data).To(HaveKeyWithValue("KNIGHT_SKILLS", "shared"))
			Expect(cm.Data).To(HaveKey("TOOLS.md"))
			Expect(cm.Data["TOOLS.md"]).To(ContainSubstring("- nmap\n"))
			Expect(cm.Data["TOOLS.md"]).To(ContainSubstring("- curl\n"))
			Expect(cm.Data).NotTo(HaveKey("mise.toml"))
			Expect(cm.Data).NotTo(HaveKey("apt.txt"))
			Expect(cm.Data).NotTo(HaveKey("flake.nix"))
		})
	})
})

var _ = Describe("Knight deprecated maxDeliver", func() {
	ctx := context.Background()

	runTools := func(name string, maxDeliver int32, tools aiv1alpha1.KnightTools, mods ...func(*aiv1alpha1.Knight)) (*aiv1alpha1.Knight, *record.FakeRecorder) {
		k := &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec:       aiv1alpha1.KnightSpec{Domain: "security", Skills: []string{"shared"}},
		}
		k.Spec.NATS.Subjects = []string{"roundtable.tasks.security.>"}
		k.Spec.NATS.MaxDeliver = maxDeliver //nolint:staticcheck // exercising the deprecated field
		k.Spec.Tools = &tools
		for _, m := range mods {
			m(k)
		}
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		DeferCleanup(func() {
			cur := &aiv1alpha1.Knight{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(k), cur); err == nil {
				cur.Finalizers = nil
				_ = k8sClient.Update(ctx, cur)
				_ = k8sClient.Delete(ctx, cur)
			}
		})
		rec := record.NewFakeRecorder(100)
		r := &KnightReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: rec}
		for range 3 {
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(k)})
		}
		got := &aiv1alpha1.Knight{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(k), got)).To(Succeed())
		return got, rec
	}
	run := func(name string, maxDeliver int32) (*aiv1alpha1.Knight, *record.FakeRecorder) {
		return runTools(name, maxDeliver, aiv1alpha1.KnightTools{})
	}

	drain := drainEvents

	It("warns once and sets the condition when maxDeliver is above 1", func() {
		got, rec := run("maxdeliver-three", 3)
		c := meta.FindStatusCondition(got.Status.Conditions, aiv1alpha1.ConditionKnightDeprecatedFieldsUsed)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionTrue))
		Expect(c.Message).To(ContainSubstring("spec.nats.maxDeliver"))
		n := 0
		for _, e := range drain(rec) {
			if strings.Contains(e, "MaxDeliverIgnored") {
				n++
			}
		}
		Expect(n).To(Equal(1), "event once per generation despite repeated reconciles")
	})

	It("is silent when maxDeliver is 1", func() {
		got, rec := run("maxdeliver-one", 1)
		Expect(meta.FindStatusCondition(got.Status.Conditions, aiv1alpha1.ConditionKnightDeprecatedFieldsUsed)).To(BeNil())
		for _, e := range drain(rec) {
			Expect(e).NotTo(ContainSubstring("MaxDeliverIgnored"))
		}
	})

	It("is silent when maxDeliver is unset", func() {
		got, rec := run("maxdeliver-unset", 0)
		Expect(meta.FindStatusCondition(got.Status.Conditions, aiv1alpha1.ConditionKnightDeprecatedFieldsUsed)).To(BeNil())
		for _, e := range drain(rec) {
			Expect(e).NotTo(ContainSubstring("MaxDeliverIgnored"))
		}
	})

	It("is silent when only nix and flakes are set", func() {
		got, rec := runTools("tools-nix", 0, aiv1alpha1.KnightTools{
			Nix:    []string{"jq"},
			Flakes: []aiv1alpha1.KnightToolsFlake{{Ref: "github:roundtable-k8s/roundtable/" + strings.Repeat("a", 40), Packages: []string{"hello"}}},
		})
		Expect(meta.FindStatusCondition(got.Status.Conditions, aiv1alpha1.ConditionKnightDeprecatedFieldsUsed)).To(BeNil())
		for _, e := range drainEvents(rec) {
			Expect(e).NotTo(ContainSubstring("Deprecated"))
		}
	})

})

var _ = Describe("Knight mcpServers on an unsupported runtime", func() {
	ctx := context.Background()

	run := func(name, image string) *aiv1alpha1.Knight {
		k := &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "security", Skills: []string{"shared"}, Image: image,
				MCPServers: []aiv1alpha1.KnightMCPServer{{Name: "vault", URL: "http://vault/mcp"}},
			},
		}
		k.Spec.NATS.Subjects = []string{"roundtable.tasks.security.>"}
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		DeferCleanup(func() {
			cur := &aiv1alpha1.Knight{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(k), cur); err == nil {
				cur.Finalizers = nil
				_ = k8sClient.Update(ctx, cur)
				_ = k8sClient.Delete(ctx, cur)
			}
		})
		r := &KnightReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(100)}
		for range 3 {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(k)})
			Expect(err).NotTo(HaveOccurred())
		}
		got := &aiv1alpha1.Knight{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(k), got)).To(Succeed())
		return got
	}

	It("sets Degraded/MCPServersUnsupported for a pi-knight image", func() {
		got := run("mcp-pi", "ghcr.io/roundtable-k8s/pi-knight:latest")
		c := meta.FindStatusCondition(got.Status.Conditions, aiv1alpha1.ConditionKnightDegraded)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionTrue))
		Expect(c.Reason).To(Equal(aiv1alpha1.ReasonMCPServersUnsupported))
	})

	It("does not set it for a claude-knight image", func() {
		got := run("mcp-claude", "ghcr.io/roundtable-k8s/claude-knight:latest")
		Expect(meta.FindStatusCondition(got.Status.Conditions, aiv1alpha1.ConditionKnightDegraded)).To(BeNil())
	})
})

var _ = Describe("Knight mcpServers headersFrom", func() {
	ctx := context.Background()
	const secretValue = "Bearer s3cr3t-header-value"

	newKnight := func(name string, headers ...aiv1alpha1.KnightMCPHeader) *aiv1alpha1.Knight {
		k := &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "security", Skills: []string{"shared"}, Image: "ghcr.io/roundtable-k8s/claude-knight:latest",
				MCPServers: []aiv1alpha1.KnightMCPServer{{Name: "vault", URL: "http://vault/mcp", HeadersFrom: headers}},
			},
		}
		k.Spec.NATS.Subjects = []string{"roundtable.tasks.security.>"}
		return k
	}
	header := func(name, secret, key string) aiv1alpha1.KnightMCPHeader {
		return aiv1alpha1.KnightMCPHeader{Name: name, ValueFrom: aiv1alpha1.KnightMCPHeaderSource{
			SecretKeyRef: aiv1alpha1.KnightSecretKeyRef{Name: secret, Key: key},
		}}
	}
	create := func(k *aiv1alpha1.Knight) {
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		DeferCleanup(func() {
			cur := &aiv1alpha1.Knight{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(k), cur); err == nil {
				cur.Finalizers = nil
				_ = k8sClient.Update(ctx, cur)
				_ = k8sClient.Delete(ctx, cur)
			}
		})
	}
	reconcileKnight := func(k *aiv1alpha1.Knight) (*aiv1alpha1.Knight, reconcile.Result) {
		r := &KnightReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(100)}
		var res reconcile.Result
		for range 3 {
			var err error
			res, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(k)})
			Expect(err).NotTo(HaveOccurred())
		}
		got := &aiv1alpha1.Knight{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(k), got)).To(Succeed())
		return got, res
	}

	It("sets Degraded=MCPServersIgnored when MCP_SERVERS_JSON is valueFrom or unparsable", func() {
		for name, env := range map[string]corev1.EnvVar{
			"mcp-ignored-vf":  {Name: "MCP_SERVERS_JSON", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "user-mcp"}, Key: "k"}}},
			"mcp-ignored-bad": {Name: "MCP_SERVERS_JSON", Value: "{not json"},
		} {
			k := newKnight(name)
			k.Spec.Env = []corev1.EnvVar{env}
			create(k)
			got, _ := reconcileKnight(k)
			c := meta.FindStatusCondition(got.Status.Conditions, aiv1alpha1.ConditionKnightDegraded)
			Expect(c).NotTo(BeNil(), name)
			Expect(c.Status).To(Equal(metav1.ConditionTrue))
			Expect(c.Reason).To(Equal(aiv1alpha1.ReasonMCPServersIgnored))

			got.Spec.Env = nil
			Expect(k8sClient.Update(ctx, got)).To(Succeed())
			got, _ = reconcileKnight(k)
			Expect(meta.FindStatusCondition(got.Status.Conditions, aiv1alpha1.ConditionKnightDegraded)).To(BeNil(), name)
		}
	})

	It("sets MCPHeadersReady=False naming a missing key, and clears it once the key exists", func() {
		sec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "mcp-hdr-secret", Namespace: "default"},
			Data:       map[string][]byte{"other": []byte("x")},
		}
		Expect(k8sClient.Create(ctx, sec)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, sec) })
		k := newKnight("mcp-hdr-missing", header("Authorization", "mcp-hdr-secret", "token"))
		create(k)

		got, res := reconcileKnight(k)
		c := meta.FindStatusCondition(got.Status.Conditions, aiv1alpha1.ConditionKnightMCPHeadersReady)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(aiv1alpha1.ReasonMCPHeaderSecretMissing))
		Expect(c.Message).To(ContainSubstring(`key "token" in Secret "mcp-hdr-secret"`))
		Expect(res.RequeueAfter).To(BeNumerically(">", 0), "secrets are not watched, so poll")

		sec.Data["token"] = []byte(secretValue)
		Expect(k8sClient.Update(ctx, sec)).To(Succeed())
		got, _ = reconcileKnight(k)
		c = meta.FindStatusCondition(got.Status.Conditions, aiv1alpha1.ConditionKnightMCPHeadersReady)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionTrue))

		// Resolved references keep being polled so a later delete is noticed.
		_, res = reconcileKnight(k)
		Expect(res.RequeueAfter).To(Equal(RequeueVerySlow))
		Expect(k8sClient.Delete(ctx, sec)).To(Succeed())
		got, res = reconcileKnight(k)
		c = meta.FindStatusCondition(got.Status.Conditions, aiv1alpha1.ConditionKnightMCPHeadersReady)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(aiv1alpha1.ReasonMCPHeaderSecretMissing))
		Expect(res.RequeueAfter).To(Equal(RequeueSlow))
		sec2 := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "mcp-hdr-secret", Namespace: "default"},
			Data:       map[string][]byte{"token": []byte(secretValue)},
		}
		Expect(k8sClient.Create(ctx, sec2)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, sec2) })
		got, _ = reconcileKnight(k)

		// The value never reaches status, the ConfigMap or the Deployment.
		for _, o := range []struct {
			name string
			obj  client.Object
		}{{k.Name, got}, {"knight-" + k.Name + "-config", &corev1.ConfigMap{}}, {k.Name, &appsv1.Deployment{}}} {
			obj := o.obj
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: o.name, Namespace: k.Namespace}, obj)).To(Succeed())
			b, err := json.Marshal(obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(b)).NotTo(ContainSubstring("s3cr3t-header-value"), "%T", obj)
		}
		deploy := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(k), deploy)).To(Succeed())
		var ref *corev1.SecretKeySelector
		for _, e := range deploy.Spec.Template.Spec.Containers[0].Env {
			if e.Name == "MCP_VAULT_AUTHORIZATION" && e.ValueFrom != nil {
				ref = e.ValueFrom.SecretKeyRef
			}
		}
		Expect(ref).NotTo(BeNil())
		Expect(ref.Name).To(Equal("mcp-hdr-secret"))
		Expect(ref.Key).To(Equal("token"))
	})

	It("names a missing Secret", func() {
		k := newKnight("mcp-hdr-nosecret", header("Authorization", "mcp-hdr-absent", "token"))
		create(k)
		got, _ := reconcileKnight(k)
		c := meta.FindStatusCondition(got.Status.Conditions, aiv1alpha1.ConditionKnightMCPHeadersReady)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Message).To(ContainSubstring(`Secret "mcp-hdr-absent"`))
	})

	It("rejects a duplicate header name on one server", func() {
		err := k8sClient.Create(ctx, newKnight("mcp-hdr-dup", header("Authorization", "s", "a"), header("Authorization", "s", "b")))
		Expect(errors.IsInvalid(err)).To(BeTrue(), "%v", err)
	})

	It("reports a header repeated in a different case", func() {
		sec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "mcp-hdr-case", Namespace: "default"},
			Data:       map[string][]byte{"a": []byte("x"), "b": []byte("y")},
		}
		Expect(k8sClient.Create(ctx, sec)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, sec) })
		k := newKnight("mcp-hdr-case", header("Authorization", "mcp-hdr-case", "a"), header("authorization", "mcp-hdr-case", "b"))
		create(k)
		got, _ := reconcileKnight(k)
		c := meta.FindStatusCondition(got.Status.Conditions, aiv1alpha1.ConditionKnightMCPHeadersReady)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(aiv1alpha1.ReasonMCPHeaderDuplicate))
		Expect(c.Message).To(ContainSubstring(`server "vault"`))
	})

	It("rejects an invalid header name", func() {
		err := k8sClient.Create(ctx, newKnight("mcp-hdr-badname", header("Bad Header:", "s", "a")))
		Expect(errors.IsInvalid(err)).To(BeTrue(), "%v", err)
	})
})

func drainEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func countEvents(rec *record.FakeRecorder, reason string) int {
	n := 0
	for _, e := range drainEvents(rec) {
		if strings.Contains(e, reason) {
			n++
		}
	}
	return n
}
