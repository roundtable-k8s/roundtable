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

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

var _ = Describe("Chain step resumeRunId", func() {
	const (
		namespace = "default"
		rtName    = "resume-table"
		knight    = "resume-knight"
		chainName = "resume-chain"
	)
	ctx := context.Background()
	nn := types.NamespacedName{Name: chainName, Namespace: namespace}

	var (
		fake *modelFakeNATS
		r    *ChainReconciler
	)

	reconcileN := func(n int) {
		for range n {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
		}
	}
	run := func(input, resume string) *aiv1alpha1.Chain {
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: chainName, Namespace: namespace},
			Spec: aiv1alpha1.ChainSpec{
				RoundTableRef: aiv1alpha1.LocalRef(rtName), TimeoutSeconds: 86400, Input: input,
				Steps: []aiv1alpha1.ChainStep{{Name: "fix", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "fix it", TimeoutSeconds: 120, ResumeRunID: resume}},
			},
		})).To(Succeed())
		reconcileN(3)
		c := &aiv1alpha1.Chain{}
		Expect(k8sClient.Get(ctx, nn, c)).To(Succeed())
		c.Annotations = map[string]string{aiv1alpha1.AnnotationChainRun: "go"}
		Expect(k8sClient.Update(ctx, c)).To(Succeed())
		reconcileN(2)
		Expect(k8sClient.Get(ctx, nn, c)).To(Succeed())
		return c
	}

	BeforeEach(func() {
		fake = &modelFakeNATS{fakeNATSClient: newFakeNATSClient()}
		r = &ChainReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(100),
			NATS:               natspkg.NewProviderWithClient(fake, logr.Discard()),
			DefaultKnightImage: "ghcr.io/roundtable-k8s/claude-knight:v1",
		}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: rtName, Namespace: namespace}, &aiv1alpha1.RoundTable{}); apierrors.IsNotFound(err) {
			Expect(k8sClient.Create(ctx, &aiv1alpha1.RoundTable{
				ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: namespace},
				Spec: aiv1alpha1.RoundTableSpec{NATS: aiv1alpha1.RoundTableNATS{
					URL: "nats://localhost:4222", SubjectPrefix: "rsm", TasksStream: "rsm_tasks", ResultsStream: "rsm_results",
				}},
			})).To(Succeed())
		}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: knight, Namespace: namespace}, &aiv1alpha1.Knight{}); apierrors.IsNotFound(err) {
			Expect(k8sClient.Create(ctx, &aiv1alpha1.Knight{
				ObjectMeta: metav1.ObjectMeta{Name: knight, Namespace: namespace},
				Spec: aiv1alpha1.KnightSpec{
					Domain: "ops", Model: "opus", Skills: []string{"ops"},
					NATS:        aiv1alpha1.KnightNATS{URL: "nats://x:4222", Subjects: []string{"rsm.tasks.ops.>"}, Stream: "rsm_tasks", ResultsStream: "rsm_results"},
					Concurrency: 1, TaskTimeoutSeconds: 120,
				},
			})).To(Succeed())
		}
	})

	AfterEach(func() {
		c := &aiv1alpha1.Chain{}
		if err := k8sClient.Get(ctx, nn, c); err == nil {
			c.Finalizers = nil
			_ = k8sClient.Update(ctx, c)
			_ = k8sClient.Delete(ctx, c)
		}
		Eventually(func() bool { return apierrors.IsNotFound(k8sClient.Get(ctx, nn, &aiv1alpha1.Chain{})) }).Should(BeTrue())
	})

	It("sends the runId from the input so the knight resumes that session", func() {
		c := run(`{"runId": "pipeline-run-1", "pr": 5}`, "json(input).runId")
		Expect(c.Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
		p := fake.payloads()
		Expect(p).To(HaveLen(1))
		Expect(p[0]).To(HaveKeyWithValue("runId", "pipeline-run-1"))
		// The chain keeps its own run identity in the taskId.
		Expect(p[0]["taskId"]).To(ContainSubstring(c.Status.RunID))
		Expect(c.Status.RunID).NotTo(Equal("pipeline-run-1"))
	})

	It("keeps the chain run's runId when the expression yields an empty string", func() {
		c := run(`{"runId": ""}`, "json(input).runId")
		p := fake.payloads()
		Expect(p).To(HaveLen(1))
		Expect(p[0]).To(HaveKeyWithValue("runId", c.Status.RunID))
	})

	It("fails the step without dispatching when the expression errors", func() {
		c := run(`not json`, "json(input).runId")
		Expect(c.Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhaseFailed))
		Expect(c.Status.StepStatuses[0].Error).To(ContainSubstring("resumeRunId"))
		Expect(fake.payloads()).To(BeEmpty())
	})
})

var _ = Describe("PR feedback example", func() {
	It("is accepted by the Chain CRD", func() {
		chain := loadPRFeedbackExample(GinkgoT())
		chain.Namespace = "default"
		chain.Name = "pr-feedback-example"
		chain.Spec.Suspend = true // validate only; never dispatch
		Expect(k8sClient.Create(ctx, chain)).To(Succeed())
		got := &aiv1alpha1.Chain{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: chain.Name, Namespace: chain.Namespace}, got)).To(Succeed())
		Expect(got.Spec.Steps[0].ResumeRunID).To(Equal("json(input).runId"))
		Expect(k8sClient.Delete(ctx, got)).To(Succeed())
	})
})
