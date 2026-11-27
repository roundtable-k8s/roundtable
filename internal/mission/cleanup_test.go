package mission

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

func TestShouldDeleteResources(t *testing.T) {
	tests := []struct {
		name   string
		policy string
		phase  aiv1alpha1.MissionPhase
		want   bool
	}{
		{"Delete always deletes", "Delete", aiv1alpha1.MissionPhaseSucceeded, true},
		{"Retain on success", "Retain", aiv1alpha1.MissionPhaseSucceeded, false},
		{"Retain on failure", "Retain", aiv1alpha1.MissionPhaseFailed, false},
		{"OnSuccess deletes on success", "OnSuccess", aiv1alpha1.MissionPhaseSucceeded, true},
		{"OnSuccess retains on failure", "OnSuccess", aiv1alpha1.MissionPhaseFailed, false},
		{"OnFailure deletes on failure", "OnFailure", aiv1alpha1.MissionPhaseFailed, true},
		{"OnFailure retains on success", "OnFailure", aiv1alpha1.MissionPhaseSucceeded, false},
		{"empty defaults to delete", "", aiv1alpha1.MissionPhaseSucceeded, true},
		{"unknown defaults to delete", "SomethingElse", aiv1alpha1.MissionPhaseFailed, true},
	}
	c := &Cleaner{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &aiv1alpha1.Mission{
				Spec:   aiv1alpha1.MissionSpec{CleanupPolicy: tt.policy},
				Status: aiv1alpha1.MissionStatus{Phase: tt.phase},
			}
			if got := c.ShouldDeleteResources(m); got != tt.want {
				t.Errorf("ShouldDeleteResources() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDeleteEphemeralKnightsSkipsNonEphemeral(t *testing.T) {
	scheme := testScheme(t)
	eph := &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: "m1-eph", Namespace: "ns"}}
	shared := &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: "m1-shared", Namespace: "ns"}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(eph, shared).Build()
	c := &Cleaner{Client: cl}
	m := &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: "m1", Namespace: "ns"},
		Status: aiv1alpha1.MissionStatus{KnightStatuses: []aiv1alpha1.MissionKnightStatus{
			{Name: "eph", Ephemeral: true},
			{Name: "shared"},
			{Name: "gone", Ephemeral: true}, // already deleted: must not error
		}},
	}
	if err := c.DeleteEphemeralKnights(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	list := &aiv1alpha1.KnightList{}
	if err := cl.List(t.Context(), list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || list.Items[0].Name != "m1-shared" {
		t.Errorf("expected only m1-shared to remain, got %v", list.Items)
	}
}

func TestNATSCleanupWithoutProviderIsNoop(t *testing.T) {
	c := &Cleaner{}
	m := &aiv1alpha1.Mission{Status: aiv1alpha1.MissionStatus{NATSTasksStream: "t", NATSResultsStream: "r"}}
	if err := c.DeleteNATSConsumers(context.Background(), m); err != nil {
		t.Errorf("DeleteNATSConsumers: %v", err)
	}
	if err := c.DeleteNATSStreams(context.Background(), m); err != nil {
		t.Errorf("DeleteNATSStreams: %v", err)
	}
	if err := c.StoreResultsToKV(t.Context(), m); err == nil {
		t.Error("StoreResultsToKV without NATS should return an error")
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func clientKey(ns, name string) types.NamespacedName {
	return types.NamespacedName{Namespace: ns, Name: name}
}

// kvCapture records KVPut calls on top of fakeNATS.
type kvCapture struct {
	fakeNATS
	bucket, key string
	value       []byte
}

func (k *kvCapture) KVPut(_ context.Context, bucket, key string, value []byte) error {
	k.bucket, k.key, k.value = bucket, key, value
	return nil
}

func TestStoreResultsToKVIncludesGeneratedChainsAndSkills(t *testing.T) {
	kv := &kvCapture{}
	c := &Cleaner{
		Client: fake.NewClientBuilder().WithScheme(testScheme(t)).Build(),
		NATS:   natspkg.NewProviderWithClient(kv, logr.Discard()),
	}
	m := &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: "m1", Namespace: "ns"},
		Spec: aiv1alpha1.MissionSpec{
			GeneratedChains: []aiv1alpha1.GeneratedChain{{
				Name:  "research",
				Steps: []aiv1alpha1.ChainStep{{Name: "s1", KnightRef: corev1.LocalObjectReference{Name: "k1"}, Task: "do it"}},
			}},
			GeneratedKnights: []aiv1alpha1.MissionKnight{{
				Name:      "k1",
				Ephemeral: true,
				EphemeralSpec: &aiv1alpha1.KnightSpec{
					GeneratedSkills: []aiv1alpha1.GeneratedSkill{{Name: "sk", Content: "# skill"}},
				},
			}},
		},
	}
	if err := c.StoreResultsToKV(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	if kv.bucket != "mission-results" || kv.key != "m1" {
		t.Errorf("stored at %s/%s, want mission-results/m1", kv.bucket, kv.key)
	}
	var doc struct {
		GeneratedChains []aiv1alpha1.GeneratedChain `json:"generatedChains"`
		GeneratedSkills []retainedSkill             `json:"generatedSkills"`
	}
	if err := json.Unmarshal(kv.value, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.GeneratedChains) != 1 || doc.GeneratedChains[0].Name != "research" ||
		len(doc.GeneratedChains[0].Steps) != 1 || doc.GeneratedChains[0].Steps[0].Task != "do it" {
		t.Errorf("generatedChains = %+v", doc.GeneratedChains)
	}
	want := retainedSkill{Name: "sk", Knight: "k1", Content: "# skill"}
	if len(doc.GeneratedSkills) != 1 || doc.GeneratedSkills[0] != want {
		t.Errorf("generatedSkills = %+v, want [%+v]", doc.GeneratedSkills, want)
	}
	if m.Status.ResultsRef == nil || m.Status.ResultsRef.Bucket != "mission-results" || m.Status.ResultsRef.Key != "m1" {
		t.Errorf("resultsRef = %+v", m.Status.ResultsRef)
	}
	if m.Status.ResultsConfigMap != "mission-results.m1" {
		t.Errorf("resultsConfigMap = %q", m.Status.ResultsConfigMap)
	}
}

func TestStoreResultsToKVCostBreakdown(t *testing.T) {
	kv := &kvCapture{}
	c := &Cleaner{
		Client: fake.NewClientBuilder().WithScheme(testScheme(t)).Build(),
		NATS:   natspkg.NewProviderWithClient(kv, logr.Discard()),
	}
	chain := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"},
		Status: aiv1alpha1.ChainStatus{StepStatuses: []aiv1alpha1.ChainStepStatus{
			{Knight: "a", CostUSD: "0.3000"}, {Knight: "b", CostUSD: "0.1000"},
		}},
	}
	ct := &CostTracker{Client: fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(chain).Build()}
	m := &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: "m1", Namespace: "ns"},
		Spec:       aiv1alpha1.MissionSpec{Knights: []aiv1alpha1.MissionKnight{{Name: "a"}, {Name: "b"}}},
		Status: aiv1alpha1.MissionStatus{
			TotalCost:     "0.4000",
			ChainStatuses: []aiv1alpha1.MissionChainStatus{{Name: "x", ChainCRName: "c1"}},
		},
	}
	if _, err := ct.AggregateMissionCost(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	if err := c.StoreResultsToKV(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Costs struct {
			Breakdown []aiv1alpha1.MissionKnightCost `json:"breakdown"`
			Total     string                         `json:"total"`
		} `json:"costs"`
	}
	if err := json.Unmarshal(kv.value, &doc); err != nil {
		t.Fatal(err)
	}
	bd := doc.Costs.Breakdown
	if len(bd) != 2 || bd[0].CostUSD != "0.3000" || bd[1].CostUSD != "0.1000" || doc.Costs.Total != "0.4000" {
		t.Errorf("costs = %+v", doc.Costs)
	}
}

type failingKV struct{ fakeNATS }

func (*failingKV) KVPut(context.Context, string, string, []byte) error { return errors.New("kv down") }

// A failed KV write must not set resultsRef (it would point at nothing).
func TestStoreResultsToKVFailureLeavesRefUnset(t *testing.T) {
	c := &Cleaner{
		Client: fake.NewClientBuilder().WithScheme(testScheme(t)).Build(),
		NATS:   natspkg.NewProviderWithClient(&failingKV{}, logr.Discard()),
	}
	m := &aiv1alpha1.Mission{ObjectMeta: metav1.ObjectMeta{Name: "m1", Namespace: "ns"}}
	if err := c.StoreResultsToKV(t.Context(), m); err == nil {
		t.Fatal("expected an error")
	}
	if m.Status.ResultsRef != nil || m.Status.ResultsConfigMap != "" {
		t.Errorf("resultsRef = %+v, resultsConfigMap = %q; want unset", m.Status.ResultsRef, m.Status.ResultsConfigMap)
	}
}

// The KV entry carries the summary and the chain outputs.
func TestStoreResultsToKVSummaryAndChainOutputs(t *testing.T) {
	kv := &kvCapture{}
	chain := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: "m1-research", Namespace: "ns"},
		Status: aiv1alpha1.ChainStatus{StepStatuses: []aiv1alpha1.ChainStepStatus{
			{Name: "s1", Output: "first"}, {Name: "s2", Output: "final answer"},
		}},
	}
	c := &Cleaner{
		Client: fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(chain).Build(),
		NATS:   natspkg.NewProviderWithClient(kv, logr.Discard()),
	}
	m := &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: "m1", Namespace: "ns"},
		Spec:       aiv1alpha1.MissionSpec{Objective: "obj"},
		Status: aiv1alpha1.MissionStatus{
			Phase:  aiv1alpha1.MissionPhaseCleaningUp,
			Result: "done",
			ChainStatuses: []aiv1alpha1.MissionChainStatus{
				{Name: "research", ChainCRName: "m1-research", Phase: aiv1alpha1.ChainPhaseSucceeded},
			},
		},
	}
	if err := c.StoreResultsToKV(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Summary struct {
			Mission, Objective, Result string
		} `json:"summary"`
		Chains map[string]struct {
			Output string `json:"output"`
			Steps  []struct{ Name, Output string }
		} `json:"chains"`
	}
	if err := json.Unmarshal(kv.value, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Summary.Mission != "m1" || doc.Summary.Objective != "obj" || doc.Summary.Result != "done" {
		t.Errorf("summary = %+v", doc.Summary)
	}
	if got := doc.Chains["research"]; got.Output != "final answer" || len(got.Steps) != 2 {
		t.Errorf("chain = %+v", got)
	}
	if m.Status.ResultsRef == nil || m.Status.ResultsRef.Bucket != "mission-results" || m.Status.ResultsRef.Key != "m1" {
		t.Errorf("resultsRef = %+v", m.Status.ResultsRef)
	}
}

// streamDeleter records DeleteStream calls on top of fakeNATS.
type streamDeleter struct {
	fakeNATS
	deleted []string
}

func (s *streamDeleter) DeleteStream(_ context.Context, name string) error {
	s.deleted = append(s.deleted, name)
	return nil
}

// Cleanup deletes the tasks, results and ephemeral events streams.
func TestDeleteNATSStreamsDeletesEventsStream(t *testing.T) {
	nc := &streamDeleter{}
	c := &Cleaner{NATS: natspkg.NewProviderWithClient(nc, logr.Discard())}
	m := &aiv1alpha1.Mission{Status: aiv1alpha1.MissionStatus{
		NATSTasksStream:   "rt_m1_TASKS",
		NATSResultsStream: "rt_m1_RESULTS",
	}}
	if err := c.DeleteNATSStreams(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	want := []string{"rt_m1_TASKS", "rt_m1_RESULTS", "rt_m1_TASKS_EVENTS"}
	if len(nc.deleted) != len(want) {
		t.Fatalf("deleted %v, want %v", nc.deleted, want)
	}
	for i := range want {
		if nc.deleted[i] != want[i] {
			t.Errorf("deleted[%d] = %q, want %q", i, nc.deleted[i], want[i])
		}
	}
}

// A failed KV write reports the size of the document it tried to store.
func TestStoreResultsToKVFailureReportsSize(t *testing.T) {
	kv := &sizeRecordingFailingKV{}
	c := &Cleaner{
		Client: fake.NewClientBuilder().WithScheme(testScheme(t)).Build(),
		NATS:   natspkg.NewProviderWithClient(kv, logr.Discard()),
	}
	m := &aiv1alpha1.Mission{ObjectMeta: metav1.ObjectMeta{Name: "m1", Namespace: "ns"}}
	err := c.StoreResultsToKV(t.Context(), m)
	if err == nil {
		t.Fatal("expected an error")
	}
	want := fmt.Sprintf("(%d bytes)", len(kv.value))
	if kv.value == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("error %q does not report size %s", err, want)
	}
}

// sizeRecordingFailingKV remembers the value passed to KVPut and fails.
type sizeRecordingFailingKV struct {
	fakeNATS
	value []byte
}

func (s *sizeRecordingFailingKV) KVPut(_ context.Context, _, _ string, value []byte) error {
	s.value = value
	return errors.New("max payload exceeded")
}
