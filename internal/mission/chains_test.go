package mission

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func newChainManager(t *testing.T, objs ...client.Object) (*ChainManager, client.Client) {
	t.Helper()
	scheme := k8sruntime.NewScheme()
	if err := aiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithStatusSubresource(&aiv1alpha1.Chain{}).Build()
	return &ChainManager{Client: c, Scheme: scheme}, c
}

func TestUpdateChainStatus(t *testing.T) {
	m := &ChainManager{}
	mission := &aiv1alpha1.Mission{}

	m.UpdateChainStatus(mission, "a", "cr-a", aiv1alpha1.ChainPhaseRunning)
	if len(mission.Status.ChainStatuses) != 1 || mission.Status.ChainStatuses[0].ChainCRName != "cr-a" {
		t.Fatalf("add failed: %+v", mission.Status.ChainStatuses)
	}
	m.UpdateChainStatus(mission, "a", "cr-a2", aiv1alpha1.ChainPhaseSucceeded)
	if len(mission.Status.ChainStatuses) != 1 ||
		mission.Status.ChainStatuses[0].ChainCRName != "cr-a2" ||
		mission.Status.ChainStatuses[0].Phase != aiv1alpha1.ChainPhaseSucceeded {
		t.Fatalf("update failed: %+v", mission.Status.ChainStatuses)
	}
	m.UpdateChainStatus(mission, "b", "cr-b", aiv1alpha1.ChainPhaseFailed)
	if len(mission.Status.ChainStatuses) != 2 || mission.Status.ChainStatuses[0].Name != "a" ||
		mission.Status.ChainStatuses[1].Name != "b" {
		t.Fatalf("append failed: %+v", mission.Status.ChainStatuses)
	}
}

// Regression: meta-mission chains are created directly by the planner as
// mission-<mission>-<chain>; there is no standalone source-chain template.
// EnsureMissionChain must short-circuit on the existing chain instead of
// failing with "source chain not found".
func TestEnsureMissionChainNoOpWhenPlannerCreatedChain(t *testing.T) {
	existing := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: "mission-m-haiku", Namespace: "default"},
		Spec: aiv1alpha1.ChainSpec{
			Steps: []aiv1alpha1.ChainStep{{Name: "s", KnightRef: corev1.LocalObjectReference{Name: "k"}, Task: "t"}},
		},
	}
	cm, _ := newChainManager(t, existing)
	mission := &aiv1alpha1.Mission{ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "default"}}
	if err := cm.EnsureMissionChain(context.Background(), mission, aiv1alpha1.MissionChainRef{Name: "haiku", Phase: "Active"}); err != nil {
		t.Fatalf("expected no-op, got %v", err)
	}
}

func TestEnsureMissionChainCopiesSourceTemplate(t *testing.T) {
	src := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: "tmpl", Namespace: "default"},
		Spec: aiv1alpha1.ChainSpec{
			Description: "src",
			Input:       "orig",
			Steps:       []aiv1alpha1.ChainStep{{Name: "s", KnightRef: corev1.LocalObjectReference{Name: "k"}, Task: "t"}},
		},
	}
	cm, c := newChainManager(t, src)
	mission := &aiv1alpha1.Mission{ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "default", UID: "uid"}}
	if err := cm.EnsureMissionChain(context.Background(), mission,
		aiv1alpha1.MissionChainRef{Name: "tmpl", Phase: "Setup", InputOverride: "override"}); err != nil {
		t.Fatal(err)
	}
	got := &aiv1alpha1.Chain{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "mission-m-tmpl"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Input != "override" || aiv1alpha1.RefName(got.Spec.RoundTableRef) != defaultRoundTableRef ||
		got.Labels["ai.roundtable.io/chain-phase"] != "Setup" || len(got.OwnerReferences) != 1 {
		t.Fatalf("unexpected chain: %+v", got)
	}
}

func TestEnsureMissionChainMissingSource(t *testing.T) {
	cm, _ := newChainManager(t)
	mission := &aiv1alpha1.Mission{ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "default"}}
	if err := cm.EnsureMissionChain(context.Background(), mission, aiv1alpha1.MissionChainRef{Name: "nope"}); err == nil {
		t.Fatal("expected error for missing source chain")
	}
}

func TestTriggerGeneratedChainsStartsIdleOnly(t *testing.T) {
	lbl := map[string]string{aiv1alpha1.LabelMission: "m"}
	idle := &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: "idle", Namespace: "default", Labels: lbl}}
	done := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: "done", Namespace: "default", Labels: lbl},
		Status:     aiv1alpha1.ChainStatus{Phase: aiv1alpha1.ChainPhaseSucceeded},
	}
	cm, c := newChainManager(t, idle, done)
	mission := &aiv1alpha1.Mission{ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "default"}}
	if err := cm.TriggerGeneratedChains(context.Background(), mission); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]aiv1alpha1.ChainPhase{"idle": aiv1alpha1.ChainPhaseRunning, "done": aiv1alpha1.ChainPhaseSucceeded} {
		got := &aiv1alpha1.Chain{}
		if err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: name}, got); err != nil {
			t.Fatal(err)
		}
		if got.Status.Phase != want {
			t.Fatalf("%s phase = %q, want %q", name, got.Status.Phase, want)
		}
	}
}

func TestReconcileChainsTriggersAndAggregates(t *testing.T) {
	src := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: "tmpl", Namespace: "default"},
		Spec:       aiv1alpha1.ChainSpec{Steps: []aiv1alpha1.ChainStep{{Name: "s", KnightRef: corev1.LocalObjectReference{Name: "k"}, Task: "t"}}},
	}
	cm, c := newChainManager(t, src)
	mission := &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "default", UID: "uid"},
		Spec:       aiv1alpha1.MissionSpec{Chains: []aiv1alpha1.MissionChainRef{{Name: "tmpl"}}},
	}
	ctx := context.Background()

	// First pass creates the mission chain and kicks it to Running.
	complete, failed, err := cm.ReconcileChains(ctx, mission)
	if err != nil || complete || failed {
		t.Fatalf("pass 1: complete=%v failed=%v err=%v", complete, failed, err)
	}
	got := &aiv1alpha1.Chain{}
	key := client.ObjectKey{Namespace: "default", Name: "mission-m-tmpl"}
	if err := c.Get(ctx, key, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != aiv1alpha1.ChainPhaseRunning {
		t.Fatalf("phase = %q", got.Status.Phase)
	}

	// Chain succeeds -> allComplete.
	got.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
	if err := c.Status().Update(ctx, got); err != nil {
		t.Fatal(err)
	}
	complete, failed, err = cm.ReconcileChains(ctx, mission)
	if err != nil || !complete || failed {
		t.Fatalf("pass 2: complete=%v failed=%v err=%v", complete, failed, err)
	}
	if len(mission.Status.ChainStatuses) != 1 || mission.Status.ChainStatuses[0].Phase != aiv1alpha1.ChainPhaseSucceeded {
		t.Fatalf("statuses: %+v", mission.Status.ChainStatuses)
	}
}
