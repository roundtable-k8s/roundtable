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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	rtruntime "github.com/roundtable-k8s/roundtable/pkg/runtime"
)

// scaledObjectCRD is a minimal stand-in for KEDA's ScaledObject CRD: enough
// for the API server to serve keda.sh/v1alpha1 ScaledObject.
func scaledObjectCRD() *apiextensionsv1.CustomResourceDefinition {
	preserve := true
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "scaledobjects.keda.sh"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "keda.sh",
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: "scaledobjects", Singular: "scaledobject", Kind: "ScaledObject", ListKind: "ScaledObjectList",
			},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v1alpha1", Served: true, Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type:                   "object",
						XPreserveUnknownFields: &preserve,
					},
				},
			}},
		},
	}
}

func scalingKnight(ns, name string, mutate func(*aiv1alpha1.KnightSpec)) *aiv1alpha1.Knight {
	k := &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: aiv1alpha1.KnightSpec{
			Domain: "test",
			Model:  "claude-sonnet-4-20250514",
			Skills: []string{"shared"},
			NATS: aiv1alpha1.KnightNATS{
				URL:           "nats://nats.test:4222",
				Subjects:      []string{"test.tasks.>"},
				Stream:        "rt_test_tasks",
				ResultsStream: "rt_test_results",
			},
		},
	}
	if mutate != nil {
		mutate(&k.Spec)
	}
	return k
}

func ephemeral(spec *aiv1alpha1.KnightSpec) {
	spec.Workspace = &aiv1alpha1.KnightWorkspace{Mode: aiv1alpha1.WorkspaceModeEphemeral, Size: new(resource.MustParse("2Gi"))}
}

func newScalingReconciler(kedaEnabled bool) *KnightReconciler {
	return &KnightReconciler{
		Client:       k8sClient,
		Scheme:       k8sClient.Scheme(),
		Recorder:     record.NewFakeRecorder(1000),
		DefaultImage: "ghcr.io/roundtable-k8s/pi-knight:test",
		KEDAEnabled:  kedaEnabled,
	}
}

func reconcileKnight(r *KnightReconciler, k *aiv1alpha1.Knight) {
	_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: k.Name, Namespace: k.Namespace}})
	Expect(err).NotTo(HaveOccurred())
}

func getKnight(k *aiv1alpha1.Knight) *aiv1alpha1.Knight {
	got := &aiv1alpha1.Knight{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: k.Name, Namespace: k.Namespace}, got)).To(Succeed())
	return got
}

func getKnightDeployment(k *aiv1alpha1.Knight) *appsv1.Deployment {
	d := &appsv1.Deployment{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Name: k.Name, Namespace: k.Namespace}, d)).To(Succeed())
	return d
}

func deleteKnight(k *aiv1alpha1.Knight) {
	got := &aiv1alpha1.Knight{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: k.Name, Namespace: k.Namespace}, got); err == nil {
		got.Finalizers = nil
		_ = k8sClient.Update(ctx, got)
		_ = k8sClient.Delete(ctx, got)
	}
	_ = k8sClient.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: k.Name, Namespace: k.Namespace}})
	_ = k8sClient.Delete(ctx, &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: k.Name, Namespace: k.Namespace}})
}

var _ = Describe("Horizontally scaled knights (#215)", func() {
	Describe("spec validation", func() {
		DescribeTable("rejects invalid scaling specs",
			func(name string, mutate func(*aiv1alpha1.KnightSpec), msg string) {
				err := k8sClient.Create(ctx, scalingKnight("default", name, mutate))
				Expect(err).To(HaveOccurred())
				Expect(apierrors.IsInvalid(err)).To(BeTrue(), err.Error())
				Expect(err.Error()).To(ContainSubstring(msg))
			},
			Entry("replicas > 1 on the default RWO PVC", "val-rwo", func(s *aiv1alpha1.KnightSpec) {
				s.Replicas = new(int32(3))
			}, "need workspace.mode: ephemeral"),
			Entry("maxReplicas > 1 on the default RWO PVC", "val-rwo-max", func(s *aiv1alpha1.KnightSpec) {
				s.MaxReplicas = new(int32(4))
			}, "need workspace.mode: ephemeral"),
			Entry("maxReplicas below replicas", "val-max", func(s *aiv1alpha1.KnightSpec) {
				ephemeral(s)
				s.Replicas = new(int32(3))
				s.MaxReplicas = new(int32(2))
			}, "maxReplicas must be greater than or equal to replicas"),
			Entry("sandbox with replicas", "val-sandbox", func(s *aiv1alpha1.KnightSpec) {
				ephemeral(s)
				s.Runtime = "sandbox"
				s.Replicas = new(int32(2))
			}, "runtime: sandbox runs a single pod"),
			Entry("ephemeral with existingClaim", "val-claim", func(s *aiv1alpha1.KnightSpec) {
				s.Workspace = &aiv1alpha1.KnightWorkspace{Mode: aiv1alpha1.WorkspaceModeEphemeral, ExistingClaim: "data"}
			}, "existingClaim cannot be combined with workspace.mode: ephemeral"),
		)

		It("accepts multiple replicas with an ephemeral workspace or an existing claim", func() {
			a := scalingKnight("default", "val-ok-ephemeral", func(s *aiv1alpha1.KnightSpec) {
				ephemeral(s)
				s.Replicas = new(int32(3))
				s.MaxReplicas = new(int32(6))
			})
			b := scalingKnight("default", "val-ok-rwx", func(s *aiv1alpha1.KnightSpec) {
				s.Workspace = &aiv1alpha1.KnightWorkspace{ExistingClaim: "shared-rwx"}
				s.Replicas = new(int32(2))
			})
			Expect(k8sClient.Create(ctx, a)).To(Succeed())
			Expect(k8sClient.Create(ctx, b)).To(Succeed())
			Expect(getKnight(a).Spec.Workspace.Mode).To(Equal(aiv1alpha1.WorkspaceModeEphemeral))
			deleteKnight(a)
			deleteKnight(b)
		})
	})

	Describe("drain on rollout", func() {
		var k *aiv1alpha1.Knight
		AfterEach(func() { deleteKnight(k) })

		It("rolls ephemeral knights with surge 1 / unavailable 0 and a task-timeout grace period", func() {
			k = scalingKnight("default", "drain-ephemeral", func(s *aiv1alpha1.KnightSpec) {
				ephemeral(s)
				s.TaskTimeoutSeconds = 300
			})
			Expect(k8sClient.Create(ctx, k)).To(Succeed())
			reconcileKnight(newScalingReconciler(false), k)

			d := getKnightDeployment(k)
			Expect(d.Spec.Strategy.Type).To(Equal(appsv1.RollingUpdateDeploymentStrategyType))
			Expect(d.Spec.Strategy.RollingUpdate.MaxSurge.IntValue()).To(Equal(1))
			Expect(d.Spec.Strategy.RollingUpdate.MaxUnavailable.IntValue()).To(Equal(0))
			Expect(*d.Spec.Template.Spec.TerminationGracePeriodSeconds).To(Equal(int64(360)))
		})

		It("rolls a shared existingClaim knight with several replicas", func() {
			k = scalingKnight("default", "drain-rwx", func(s *aiv1alpha1.KnightSpec) {
				s.Workspace = &aiv1alpha1.KnightWorkspace{ExistingClaim: "shared-rwx"}
				s.Replicas = new(int32(2))
			})
			Expect(k8sClient.Create(ctx, k)).To(Succeed())
			reconcileKnight(newScalingReconciler(false), k)

			d := getKnightDeployment(k)
			Expect(d.Spec.Strategy.Type).To(Equal(appsv1.RollingUpdateDeploymentStrategyType))
			Expect(d.Spec.Strategy.RollingUpdate.MaxUnavailable.IntValue()).To(Equal(0))
		})

		It("keeps Recreate for a single RWO PVC knight but still sets the grace period", func() {
			k = scalingKnight("default", "drain-rwo", nil)
			Expect(k8sClient.Create(ctx, k)).To(Succeed())
			reconcileKnight(newScalingReconciler(false), k)

			d := getKnightDeployment(k)
			Expect(d.Spec.Strategy.Type).To(Equal(appsv1.RecreateDeploymentStrategyType))
			Expect(d.Spec.Strategy.RollingUpdate).To(BeNil())
			// taskTimeoutSeconds is unset: the operator default (120s) applies.
			Expect(*d.Spec.Template.Spec.TerminationGracePeriodSeconds).To(Equal(int64(knightpkg.DefaultTaskTimeoutSeconds) + 60))
		})
	})

	Describe("without KEDA", func() {
		var k *aiv1alpha1.Knight
		AfterEach(func() { deleteKnight(k) })

		It("runs spec.replicas ephemeral pods with a PDB and reports Autoscaled=False", func() {
			k = scalingKnight("default", "scale-nokeda", func(s *aiv1alpha1.KnightSpec) {
				ephemeral(s)
				s.Replicas = new(int32(2))
				s.MaxReplicas = new(int32(5))
			})
			Expect(k8sClient.Create(ctx, k)).To(Succeed())
			r := newScalingReconciler(false)
			reconcileKnight(r, k)

			d := getKnightDeployment(k)
			Expect(*d.Spec.Replicas).To(Equal(int32(2)))
			Expect(d.Spec.Strategy.Type).To(Equal(appsv1.RollingUpdateDeploymentStrategyType))
			var data *corev1.Volume
			for i := range d.Spec.Template.Spec.Volumes {
				if d.Spec.Template.Spec.Volumes[i].Name == "data" {
					data = &d.Spec.Template.Spec.Volumes[i]
				}
			}
			Expect(data).NotTo(BeNil())
			Expect(data.EmptyDir).NotTo(BeNil())
			Expect(data.EmptyDir.SizeLimit.String()).To(Equal("2Gi"))

			// No workspace PVC in ephemeral mode.
			err := k8sClient.Get(ctx, types.NamespacedName{Name: k.Name, Namespace: k.Namespace}, &corev1.PersistentVolumeClaim{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			pdb := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: k.Name, Namespace: k.Namespace}, pdb)).To(Succeed())
			Expect(pdb.Spec.MaxUnavailable.IntValue()).To(Equal(1))
			Expect(pdb.Spec.Selector.MatchLabels).To(HaveKeyWithValue("app.kubernetes.io/instance", k.Name))

			got := getKnight(k)
			cond := meta.FindStatusCondition(got.Status.Conditions, aiv1alpha1.ConditionKnightAutoscaled)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonKEDAUnavailable))
			Expect(got.Status.Scaling).NotTo(BeNil())
			Expect(got.Status.Scaling.Autoscaled).To(BeFalse())
			Expect(got.Status.Scaling.MinReplicas).To(Equal(int32(2)))
			Expect(got.Status.Scaling.MaxReplicas).To(Equal(int32(2)))
			Expect(got.Status.Scaling.DesiredReplicas).To(HaveValue(Equal(int32(2))))
			Expect(got.Status.Scaling.LastScaleTime).To(BeNil())

			// Back to one pod: the PDB goes away.
			got.Spec.Replicas = new(int32(1))
			got.Spec.MaxReplicas = nil
			Expect(k8sClient.Update(ctx, got)).To(Succeed())
			reconcileKnight(r, k)
			Expect(*getKnightDeployment(k).Spec.Replicas).To(Equal(int32(1)))
			err = k8sClient.Get(ctx, types.NamespacedName{Name: k.Name, Namespace: k.Namespace}, &policyv1.PodDisruptionBudget{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
			Expect(meta.FindStatusCondition(getKnight(k).Status.Conditions, aiv1alpha1.ConditionKnightAutoscaled)).To(BeNil())
		})
	})

	Describe("with KEDA", Ordered, func() {
		var k *aiv1alpha1.Knight

		BeforeAll(func() {
			_, err := envtest.InstallCRDs(cfg, envtest.CRDInstallOptions{
				CRDs: []*apiextensionsv1.CustomResourceDefinition{scaledObjectCRD()},
			})
			Expect(err).NotTo(HaveOccurred())
		})
		AfterAll(func() { deleteKnight(k) })

		It("renders a ScaledObject, leaves replicas to KEDA and removes it on suspend", func() {
			k = scalingKnight("default", "scale-keda", func(s *aiv1alpha1.KnightSpec) {
				ephemeral(s)
				s.MaxReplicas = new(int32(3))
				s.Concurrency = 2
				s.Lifecycle = &aiv1alpha1.KnightLifecycle{SuspendPolicy: "auto", IdleTimeoutSeconds: 600}
			})
			Expect(k8sClient.Create(ctx, k)).To(Succeed())
			r := newScalingReconciler(true)
			reconcileKnight(r, k)

			so := &unstructured.Unstructured{}
			so.SetGroupVersionKind(rtruntime.ScaledObjectGVK)
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: k.Name, Namespace: k.Namespace}, so)).To(Succeed())
			Expect(metav1.IsControlledBy(so, getKnight(k))).To(BeTrue())

			// status.scaling carries the bounds without the ScaledObject; the
			// HPA KEDA would create doesn't exist here, so desired falls back
			// to the Deployment's replicas.
			sc := getKnight(k).Status.Scaling
			Expect(sc).NotTo(BeNil())
			Expect(sc.Autoscaled).To(BeTrue())
			Expect(sc.MaxReplicas).To(Equal(int32(3)))
			Expect(sc.DesiredReplicas).To(HaveValue(Equal(*getKnightDeployment(k).Spec.Replicas)))
			Expect(nestedString(so, "spec", "scaleTargetRef", "name")).To(Equal(k.Name))
			Expect(nestedInt64(so, "spec", "minReplicaCount")).To(Equal(int64(0)))
			Expect(nestedInt64(so, "spec", "maxReplicaCount")).To(Equal(int64(3)))
			Expect(nestedInt64(so, "spec", "cooldownPeriod")).To(Equal(int64(600)))
			triggers, _, _ := unstructured.NestedSlice(so.Object, "spec", "triggers")
			Expect(triggers).To(HaveLen(2))
			trigger := triggers[0].(map[string]any)
			Expect(trigger["type"]).To(Equal("nats-jetstream"))
			Expect(trigger["metadata"]).To(Equal(map[string]any{
				"natsServerMonitoringEndpoint": "nats.test:8222",
				"account":                      "$G",
				"stream":                       "rt_test_tasks",
				"consumer":                     "Scale-keda-consumer",
				"lagThreshold":                 "1",
				"activationLagThreshold":       "0",
			}))
			// In-flight tasks: ack_pending / concurrency (2).
			ackTrigger := triggers[1].(map[string]any)
			Expect(ackTrigger["type"]).To(Equal("metrics-api"))
			Expect(ackTrigger["metricType"]).To(Equal("AverageValue"))
			Expect(ackTrigger["metadata"]).To(Equal(map[string]any{
				"url":                   "http://nats.test:8222/jsz?acc=%24G&streams=true&consumers=true",
				"valueLocation":         `account_details.0.stream_detail.#(name=="rt_test_tasks").consumer_detail.#(name=="Scale-keda-consumer").num_ack_pending`,
				"targetValue":           "2",
				"activationTargetValue": "0",
			}))
			// Default scale-down stabilization window.
			Expect(nestedInt64(so, "spec", "advanced", "horizontalPodAutoscalerConfig", "behavior", "scaleDown", "stabilizationWindowSeconds")).To(Equal(int64(600)))

			// Starts with one pod so the runtime creates its consumer.
			d := getKnightDeployment(k)
			Expect(*d.Spec.Replicas).To(Equal(int32(1)))
			cond := meta.FindStatusCondition(getKnight(k).Status.Conditions, aiv1alpha1.ConditionKnightAutoscaled)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))

			// KEDA scales to zero; the operator leaves it and reports it.
			zero := int32(0)
			d.Spec.Replicas = &zero
			Expect(k8sClient.Update(ctx, d)).To(Succeed())
			reconcileKnight(r, k)
			Expect(*getKnightDeployment(k).Spec.Replicas).To(Equal(int32(0)))
			got := getKnight(k)
			Expect(got.Status.Phase).To(Equal(aiv1alpha1.KnightPhaseSuspended))
			avail := meta.FindStatusCondition(got.Status.Conditions, aiv1alpha1.ConditionKnightAvailable)
			Expect(avail.Reason).To(Equal(aiv1alpha1.ReasonKnightScaledToZero))

			// Manual suspend removes the ScaledObject so KEDA can't wake it.
			got.Spec.Suspend = true
			Expect(k8sClient.Update(ctx, got)).To(Succeed())
			reconcileKnight(r, k)
			err := k8sClient.Get(ctx, types.NamespacedName{Name: k.Name, Namespace: k.Namespace}, so)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("detects the ScaledObject API through discovery", func() {
			// Covered more fully in pkg/runtime; here against a real API server.
			enabled, _, err := rtruntime.ResolveKEDA(rtruntime.KEDAModeAuto, discoveryFor())
			Expect(err).NotTo(HaveOccurred())
			Expect(enabled).To(BeTrue())
		})
	})

	Describe("plan-backed task cap", func() {
		const ns = "plancap"
		var knights []*aiv1alpha1.Knight
		var table *aiv1alpha1.RoundTable

		BeforeEach(func() {
			err := k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
			if err != nil {
				Expect(apierrors.IsAlreadyExists(err)).To(BeTrue())
			}
			table = &aiv1alpha1.RoundTable{
				ObjectMeta: metav1.ObjectMeta{Name: "plans", Namespace: ns},
				Spec: aiv1alpha1.RoundTableSpec{
					Ephemeral: true,
					NATS: aiv1alpha1.RoundTableNATS{
						SubjectPrefix: "rt_test", TasksStream: "rt_test_tasks", ResultsStream: "rt_test_results",
					},
					Policies: &aiv1alpha1.RoundTablePolicies{MaxConcurrentPlanTasks: 4},
				},
			}
			Expect(k8sClient.Create(ctx, table)).To(Succeed())
			member := func(name string, planBacked bool) *aiv1alpha1.Knight {
				k := scalingKnight(ns, name, func(s *aiv1alpha1.KnightSpec) {
					ephemeral(s)
					s.Replicas = new(int32(3))
					s.Concurrency = 2
					s.PlanBacked = &planBacked
				})
				k.Labels = map[string]string{aiv1alpha1.LabelRoundTable: table.Name}
				return k
			}
			knights = []*aiv1alpha1.Knight{member("plan-a", true), member("plan-b", true), member("api-c", false)}
			for _, k := range knights {
				Expect(k8sClient.Create(ctx, k)).To(Succeed())
			}
		})
		AfterEach(func() {
			for _, k := range knights {
				deleteKnight(k)
			}
			_ = k8sClient.Delete(ctx, table)
		})

		replicasOf := func() []int32 {
			out := make([]int32, 0, len(knights))
			for _, k := range knights {
				out = append(out, *getKnightDeployment(k).Spec.Replicas)
			}
			return out
		}

		It("splits the cap across plan-backed knights and leaves the others alone", func() {
			r := newScalingReconciler(false)
			for _, k := range knights {
				reconcileKnight(r, k)
			}
			// cap 4, concurrency 2: one pod each (the floor) for the plan knights.
			Expect(replicasOf()).To(Equal([]int32{1, 1, 3}))
			cond := meta.FindStatusCondition(getKnight(knights[0]).Status.Conditions, aiv1alpha1.ConditionKnightPlanCapLimited)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(meta.FindStatusCondition(getKnight(knights[2]).Status.Conditions, aiv1alpha1.ConditionKnightPlanCapLimited)).To(BeNil())

			// Raise the cap: 10 tasks = 5 pods of concurrency 2, split 3/2 in name order.
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: table.Name, Namespace: ns}, table)).To(Succeed())
			table.Spec.Policies.MaxConcurrentPlanTasks = 10
			Expect(k8sClient.Update(ctx, table)).To(Succeed())
			// All three are table members (they inherit its defaults); two
			// would be requeued for the cap alone.
			Expect(r.roundTableKnightRequests(ctx, table)).To(HaveLen(3))
			for _, k := range knights {
				reconcileKnight(r, k)
			}
			Expect(replicasOf()).To(Equal([]int32{3, 2, 3}))
			cond = meta.FindStatusCondition(getKnight(knights[0]).Status.Conditions, aiv1alpha1.ConditionKnightPlanCapLimited)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		})
	})
})

func discoveryFor() discovery.DiscoveryInterface {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	Expect(err).NotTo(HaveOccurred())
	return dc
}

func nestedString(u *unstructured.Unstructured, fields ...string) string {
	v, found, err := unstructured.NestedString(u.Object, fields...)
	Expect(err).NotTo(HaveOccurred())
	Expect(found).To(BeTrue(), "%v not set", fields)
	return v
}

func nestedInt64(u *unstructured.Unstructured, fields ...string) int64 {
	v, found, err := unstructured.NestedInt64(u.Object, fields...)
	Expect(err).NotTo(HaveOccurred())
	Expect(found).To(BeTrue(), "%v not set", fields)
	return v
}

var _ = Describe("Knight tools.flakes validation (#248)", func() {
	const rev = "0123456789abcdef0123456789abcdef01234567"
	withFlake := func(ref string, pkgs ...string) func(*aiv1alpha1.KnightSpec) {
		return func(s *aiv1alpha1.KnightSpec) {
			s.Tools = &aiv1alpha1.KnightTools{Flakes: []aiv1alpha1.KnightToolsFlake{{Ref: ref, Packages: pkgs}}}
		}
	}

	DescribeTable("rejects",
		func(name string, mutate func(*aiv1alpha1.KnightSpec)) {
			err := k8sClient.Create(ctx, scalingKnight("default", name, mutate))
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err)).To(BeTrue(), err.Error())
		},
		Entry("a branch name", "flk-branch", withFlake("github:roundtable-k8s/roundtable/main", "p")),
		Entry("no rev at all", "flk-norev", withFlake("github:roundtable-k8s/roundtable", "p")),
		Entry("a short sha", "flk-short", withFlake("github:o/r/0123abc", "p")),
		Entry("a floating git ref", "flk-gitref", withFlake("git+https://example.com/r.git?ref=main", "p")),
		Entry("quote injection", "flk-quote", withFlake(`github:o/r/`+rev+`"; evil`, "p")),
		Entry("a dir with parent segment", "flk-dotdot", withFlake("github:o/r/"+rev+"?dir=../x", "p")),
		Entry("a dir that climbs out mid-path", "flk-dotdot2", withFlake("github:o/r/"+rev+"?dir=a/../../x", "p")),
		Entry("an absolute dir", "flk-absdir", withFlake("github:o/r/"+rev+"?dir=/etc", "p")),
		Entry("a url-encoded parent dir", "flk-encdir", withFlake("github:o/r/"+rev+"?dir=%2e%2e/x", "p")),
		Entry("a parent dir in a git+https ref", "flk-gitdotdot", withFlake("git+https://example.com/r.git?dir=a/..&rev="+rev, "p")),
		Entry("no packages", "flk-nopkgs", withFlake("github:o/r/"+rev)),
		Entry("an unsafe package name", "flk-badpkg", withFlake("github:o/r/"+rev, `x"; evil`)),
	)

	DescribeTable("accepts",
		func(name, ref string) {
			k := scalingKnight("default", name, withFlake(ref, "tool", "other-tool_1.2"))
			Expect(k8sClient.Create(ctx, k)).To(Succeed())
			deleteKnight(k)
		},
		Entry("a pinned github ref", "flk-ok-gh", "github:roundtable-k8s/roundtable/"+rev),
		Entry("a pinned github ref with dir", "flk-ok-dir", "github:o/r/"+rev+"?dir=sub/pkg"),
		Entry("a dir with dots in names", "flk-ok-dots", "github:o/r/"+rev+"?dir=a..b/.hidden"),
		Entry("a pinned git+https ref", "flk-ok-git", "git+https://example.com/r.git?rev="+rev),
		Entry("a pinned git+https ref with branch", "flk-ok-gitref", "git+https://example.com/r.git?ref=main&rev="+rev),
	)
})
