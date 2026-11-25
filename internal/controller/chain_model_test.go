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
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go/jetstream"
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

// modelFakeNATS records published tasks and returns a canned result (if set).
type modelFakeNATS struct {
	*fakeNATSClient
	seq    uint64
	result *natspkg.TaskResult
}

func (f *modelFakeNATS) PublishJSONSeq(subject string, v any) (uint64, error) {
	if err := f.PublishJSON(subject, v); err != nil {
		return 0, err
	}
	f.seq++
	return f.seq, nil
}
func (f *modelFakeNATS) ConsumerAckFloor(context.Context, string, string) (uint64, error) {
	return f.seq, nil
}
func (f *modelFakeNATS) ConsumerDeliveredSeq(context.Context, string, string) (uint64, error) {
	return f.seq, nil
}
func (f *modelFakeNATS) FetchOne(
	_ context.Context, _ string, cfg jetstream.ConsumerConfig, _ time.Duration,
) (jetstream.Msg, error) {
	subject := cfg.FilterSubject
	if f.result == nil {
		return nil, nil
	}
	data, err := json.Marshal(f.result)
	if err != nil {
		return nil, err
	}
	return &fakeMsg{subject: subject, data: data}, nil
}

// payloads decodes every published task payload.
func (f *modelFakeNATS) payloads() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]map[string]any, 0, len(f.published))
	for _, raw := range f.published {
		m := map[string]any{}
		Expect(json.Unmarshal(raw, &m)).To(Succeed())
		out = append(out, m)
	}
	return out
}

var _ = Describe("Chain per-step model override", func() {
	const (
		namespace    = "default"
		rtName       = "model-table"
		claudeKnight = "model-claude"
		piKnight     = "model-pi"
		chainName    = "model-chain"
	)
	ctx := context.Background()
	nn := types.NamespacedName{Name: chainName, Namespace: namespace}

	var (
		fake     *modelFakeNATS
		recorder *record.FakeRecorder
		r        *ChainReconciler
	)

	var stepEffort string
	reconcileN := func(n int) {
		for range n {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
		}
	}
	get := func() *aiv1alpha1.Chain {
		c := &aiv1alpha1.Chain{}
		Expect(k8sClient.Get(ctx, nn, c)).To(Succeed())
		return c
	}
	// run creates a one-step chain, triggers it and reconciles until dispatch.
	run := func(knightRef, model string) *aiv1alpha1.Chain {
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: chainName, Namespace: namespace},
			Spec: aiv1alpha1.ChainSpec{
				RoundTableRef: aiv1alpha1.LocalRef(rtName), TimeoutSeconds: 86400,
				Steps: []aiv1alpha1.ChainStep{{Name: "work", KnightRef: corev1.LocalObjectReference{Name: knightRef}, Task: "do it", TimeoutSeconds: 120, Model: model, Effort: stepEffort}},
			},
		})).To(Succeed())
		reconcileN(3)
		c := get()
		if c.Annotations == nil {
			c.Annotations = map[string]string{}
		}
		c.Annotations[aiv1alpha1.AnnotationChainRun] = "go"
		Expect(k8sClient.Update(ctx, c)).To(Succeed())
		reconcileN(2)
		return get()
	}
	knightSpec := func(image string) aiv1alpha1.KnightSpec {
		return aiv1alpha1.KnightSpec{
			Domain: "ops", Model: "default-model", Image: image, Skills: []string{"ops"},
			NATS:        aiv1alpha1.KnightNATS{URL: "nats://x:4222", Subjects: []string{"mdl.tasks.ops.>"}, Stream: "mdl_tasks", ResultsStream: "mdl_results"},
			Concurrency: 1, TaskTimeoutSeconds: 120,
		}
	}

	BeforeEach(func() {
		stepEffort = ""
		fake = &modelFakeNATS{fakeNATSClient: newFakeNATSClient()}
		recorder = record.NewFakeRecorder(100)
		r = &ChainReconciler{
			Client:             k8sClient,
			Scheme:             k8sClient.Scheme(),
			Recorder:           recorder,
			NATS:               natspkg.NewProviderWithClient(fake, logr.Discard()),
			DefaultKnightImage: "ghcr.io/roundtable-k8s/pi-knight:v2",
		}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: rtName, Namespace: namespace}, &aiv1alpha1.RoundTable{}); apierrors.IsNotFound(err) {
			Expect(k8sClient.Create(ctx, &aiv1alpha1.RoundTable{
				ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: namespace},
				Spec: aiv1alpha1.RoundTableSpec{
					NATS: aiv1alpha1.RoundTableNATS{
						URL: "nats://localhost:4222", SubjectPrefix: "mdl", TasksStream: "mdl_tasks", ResultsStream: "mdl_results",
					},
					ModelAliases: map[string]string{"deep": "opus", "review": "openrouter/z-ai/glm-5.3"},
				},
			})).To(Succeed())
		}
		for name, image := range map[string]string{
			claudeKnight: "ghcr.io/roundtable-k8s/claude-knight:v1",
			piKnight:     "", // falls back to DefaultKnightImage (pi-knight)
		} {
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &aiv1alpha1.Knight{}); apierrors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, &aiv1alpha1.Knight{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
					Spec:       knightSpec(image),
				})).To(Succeed())
			}
		}
	})

	AfterEach(func() {
		c := &aiv1alpha1.Chain{}
		if err := k8sClient.Get(ctx, nn, c); err == nil {
			c.Finalizers = nil
			_ = k8sClient.Update(ctx, c)
			_ = k8sClient.Delete(ctx, c)
		}
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, nn, &aiv1alpha1.Chain{}))
		}).Should(BeTrue())
	})

	It("resolves a RoundTable alias and sends the model in the task payload", func() {
		c := run(claudeKnight, "deep")
		Expect(c.Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
		Expect(c.Status.StepStatuses[0].Model).To(Equal("opus"))
		p := fake.payloads()
		Expect(p).To(HaveLen(1))
		Expect(p[0]).To(HaveKeyWithValue("model", "opus"))
	})

	It("sends the step effort in the task payload, and omits it when unset (#375)", func() {
		stepEffort = "high"
		c := run(claudeKnight, "")
		Expect(c.Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
		p := fake.payloads()
		Expect(p).To(HaveLen(1))
		Expect(p[0]).To(HaveKeyWithValue("effort", "high"))
	})

	It("omits effort from the payload when the step sets none (#375)", func() {
		run(claudeKnight, "")
		p := fake.payloads()
		Expect(p).To(HaveLen(1))
		Expect(p[0]).NotTo(HaveKey("effort"))
	})

	It("passes a literal provider/model through to a pi-knight (image from the operator default)", func() {
		c := run(piKnight, "anthropic/claude-sonnet-4-5")
		Expect(c.Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
		p := fake.payloads()
		Expect(p).To(HaveLen(1))
		Expect(p[0]).To(HaveKeyWithValue("model", "anthropic/claude-sonnet-4-5"))
	})

	It("omits the model from the payload when the step has no override", func() {
		c := run(claudeKnight, "")
		Expect(c.Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
		Expect(c.Status.StepStatuses[0].Model).To(BeEmpty())
		p := fake.payloads()
		Expect(p).To(HaveLen(1))
		Expect(p[0]).NotTo(HaveKey("model"))
	})

	It("fails the step without dispatching when the alias targets another runtime", func() {
		c := run(claudeKnight, "review")
		ss := c.Status.StepStatuses[0]
		Expect(ss.Phase).To(Equal(aiv1alpha1.ChainStepPhaseFailed))
		Expect(ss.Error).To(ContainSubstring(`"openrouter/z-ai/glm-5.3" (alias "review") is not a Claude model`))
		Expect(ss.Retries).To(BeZero())
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseFailed))
		Expect(fake.payloads()).To(BeEmpty())
		var events []string
		for len(recorder.Events) > 0 {
			events = append(events, <-recorder.Events)
		}
		Expect(events).To(ContainElement(ContainSubstring("InvalidStepModel")))
	})

	It("fails the step when a bare Claude alias is sent to a pi-knight", func() {
		c := run(piKnight, "sonnet")
		ss := c.Status.StepStatuses[0]
		Expect(ss.Phase).To(Equal(aiv1alpha1.ChainStepPhaseFailed))
		Expect(ss.Error).To(ContainSubstring("not in provider/model form"))
		Expect(fake.payloads()).To(BeEmpty())
	})

	It("records the model the knight reports having used", func() {
		run(claudeKnight, "deep")
		fake.result = &natspkg.TaskResult{Output: "done", Model: "claude-opus-5-5"}
		reconcileN(1)
		c := get()
		Expect(c.Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhaseSucceeded))
		Expect(c.Status.StepStatuses[0].Model).To(Equal("claude-opus-5-5"))
	})

	It("rejects a step model with whitespace at admission", func() {
		err := k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: chainName, Namespace: namespace},
			Spec: aiv1alpha1.ChainSpec{
				RoundTableRef: aiv1alpha1.LocalRef(rtName),
				Steps:         []aiv1alpha1.ChainStep{{Name: "work", KnightRef: corev1.LocalObjectReference{Name: claudeKnight}, Task: "x", Model: "opus --dangerously"}},
			},
		})
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "got %v", err)
	})
})

func (f *modelFakeNATS) PublishJSONSeqCtx(_ context.Context, subject string, v any, _ ...natspkg.PublishOption) (uint64, error) {
	return f.PublishJSONSeq(subject, v)
}
