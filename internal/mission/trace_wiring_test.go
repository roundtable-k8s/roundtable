package mission

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/telemetry"
)

func tracedMission(t *testing.T) *aiv1alpha1.Mission {
	t.Helper()
	start := metav1.NewTime(time.Now())
	m := &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "default", UID: "uid"},
		Spec:       aiv1alpha1.MissionSpec{Objective: "o", RoundTableRef: aiv1alpha1.LocalRef("rt")},
	}
	m.Status.StartedAt = &start
	tp := sdktrace.NewTracerProvider(sdktrace.WithIDGenerator(telemetry.NewIDGenerator()))
	EnsureTrace(context.Background(), tp, m)
	if m.Status.TraceContext == nil || m.Status.TraceContext.Traceparent == "" {
		t.Fatal("mission has no stored trace context")
	}
	return m
}

func TestApplyPlanSetsParentTraceparent(t *testing.T) {
	scheme := k8sruntime.NewScheme()
	if err := aiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	m := tracedMission(t)
	plan := &PlannerOutput{PlanVersion: "v1alpha1", Chains: []PlannerChain{{
		Name:  "c",
		Steps: []aiv1alpha1.ChainStep{{Name: "s", KnightRef: corev1.LocalObjectReference{Name: "k"}, Task: "t"}},
	}}}
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	p := &Planner{Client: c}
	if err := p.applyPlan(context.Background(), m, plan); err != nil {
		t.Fatal(err)
	}
	got := &aiv1alpha1.Chain{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "mission-m-c"}, got); err != nil {
		t.Fatal(err)
	}
	if v := got.Annotations[telemetry.AnnotationParentTraceparent]; v != m.Status.TraceContext.Traceparent {
		t.Errorf("annotation = %q, want %q", v, m.Status.TraceContext.Traceparent)
	}
}

func TestEnsureMissionChainSetsParentTraceparent(t *testing.T) {
	src := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: "tmpl", Namespace: "default"},
		Spec: aiv1alpha1.ChainSpec{
			Steps: []aiv1alpha1.ChainStep{{Name: "s", KnightRef: corev1.LocalObjectReference{Name: "k"}, Task: "t"}},
		},
	}
	cm, c := newChainManager(t, src)
	m := tracedMission(t)
	if err := cm.EnsureMissionChain(context.Background(), m, aiv1alpha1.MissionChainRef{Name: "tmpl", Phase: "Setup"}); err != nil {
		t.Fatal(err)
	}
	got := &aiv1alpha1.Chain{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "mission-m-tmpl"}, got); err != nil {
		t.Fatal(err)
	}
	if v := got.Annotations[telemetry.AnnotationParentTraceparent]; v != m.Status.TraceContext.Traceparent {
		t.Errorf("annotation = %q, want %q", v, m.Status.TraceContext.Traceparent)
	}
}
