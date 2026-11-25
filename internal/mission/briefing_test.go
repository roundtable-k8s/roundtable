package mission

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go/jetstream"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// fakeNATS is an in-memory natspkg.Client that records publishes and fails
// subjects matched by failSubject.
type fakeNATS struct {
	mu          sync.Mutex
	published   map[string][]byte
	failSubject func(subject string) bool
}

func (f *fakeNATS) subjects() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.published))
	for s := range f.published {
		out = append(out, s)
	}
	return out
}

func (f *fakeNATS) Connect() error    { return nil }
func (f *fakeNATS) Close() error      { return nil }
func (f *fakeNATS) IsConnected() bool { return true }
func (f *fakeNATS) Publish(subject string, data []byte) error {
	if f.failSubject != nil && f.failSubject(subject) {
		return fmt.Errorf("NATS publish to %s failed", subject)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published[subject] = data
	return nil
}
func (f *fakeNATS) PublishJSON(subject string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return f.Publish(subject, data)
}
func (f *fakeNATS) PublishJSONSeq(subject string, v any) (uint64, error) {
	return 0, f.PublishJSON(subject, v)
}
func (f *fakeNATS) CreateStream(context.Context, natspkg.StreamConfig) error { return nil }
func (f *fakeNATS) DeleteStream(context.Context, string) error               { return nil }
func (f *fakeNATS) StreamInfo(context.Context, string) (*jetstream.StreamInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (f *fakeNATS) EnsureConsumer(context.Context, string, string, natspkg.ConsumerConfig) error {
	return nil
}
func (f *fakeNATS) DeleteConsumer(context.Context, string, string) error { return nil }
func (f *fakeNATS) FetchOne(context.Context, string, jetstream.ConsumerConfig, time.Duration) (jetstream.Msg, error) {
	return nil, fmt.Errorf("not implemented")
}
func (f *fakeNATS) KVPut(context.Context, string, string, []byte) error { return nil }
func (f *fakeNATS) KVGet(context.Context, string, string) ([]byte, error) {
	return nil, fmt.Errorf("not found")
}
func (f *fakeNATS) KVDelete(context.Context, string, string) error   { return nil }
func (f *fakeNATS) KVKeys(context.Context, string) ([]string, error) { return nil, nil }
func (f *fakeNATS) ConsumerAckFloor(context.Context, string, string) (uint64, error) {
	return 0, nil
}
func (f *fakeNATS) ConsumerDeliveredSeq(context.Context, string, string) (uint64, error) {
	return 0, nil
}

type briefingEnv struct {
	fake     *fakeNATS
	recorder *record.FakeRecorder
	briefer  *Briefer
	c        client.Client
}

func newBriefingEnv(t *testing.T, objs ...client.Object) *briefingEnv {
	t.Helper()
	scheme := k8sruntime.NewScheme()
	if err := aiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	f := &fakeNATS{published: map[string][]byte{}}
	rec := record.NewFakeRecorder(10)
	return &briefingEnv{
		fake:     f,
		recorder: rec,
		c:        c,
		briefer: &Briefer{
			Client:   c,
			Recorder: rec,
			NATS:     natspkg.NewProviderWithClient(f, logr.Discard()),
		},
	}
}

func briefKnight(name string, subjects ...string) *aiv1alpha1.Knight {
	return &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: aiv1alpha1.KnightSpec{
			Domain: "operator",
			NATS:   aiv1alpha1.KnightNATS{Subjects: subjects},
		},
	}
}

func briefMission(name string, knights ...string) *aiv1alpha1.Mission {
	mks := make([]aiv1alpha1.MissionKnight, 0, len(knights))
	for _, k := range knights {
		mks = append(mks, aiv1alpha1.MissionKnight{Name: k})
	}
	return &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1},
		Spec: aiv1alpha1.MissionSpec{
			Objective: "test objective",
			Briefing:  "test briefing",
			Knights:   mks,
		},
	}
}

func TestPublishBriefingOnlyKnightTaskSubjects(t *testing.T) {
	env := newBriefingEnv(t, briefKnight("a", "rt-dev.tasks.operator.>"))
	if err := env.briefer.PublishBriefing(context.Background(), briefMission("m1", "a")); err != nil {
		t.Fatal(err)
	}
	subjects := env.fake.subjects()
	if len(subjects) != 1 || subjects[0] != "rt-dev.tasks.operator.a" {
		t.Fatalf("unexpected subjects: %v", subjects)
	}
	if strings.HasSuffix(subjects[0], ".briefing") {
		t.Fatal("no stream covers *.briefing")
	}
}

func TestPublishBriefingFallsBackToRoundTablePrefix(t *testing.T) {
	rt := &aiv1alpha1.RoundTable{
		ObjectMeta: metav1.ObjectMeta{Name: "rt", Namespace: "default"},
		Spec:       aiv1alpha1.RoundTableSpec{NATS: aiv1alpha1.RoundTableNATS{SubjectPrefix: "rt-dev"}},
	}
	env := newBriefingEnv(t, briefKnight("b", "malformed-subject"), rt)
	m := briefMission("m2", "b")
	m.Spec.RoundTableRef = aiv1alpha1.LocalRef("rt")
	if err := env.briefer.PublishBriefing(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	subjects := env.fake.subjects()
	if len(subjects) != 1 || subjects[0] != "rt-dev.tasks.operator.b" {
		t.Fatalf("unexpected subjects: %v", subjects)
	}
}

func TestPublishBriefingGenerationBasedTaskID(t *testing.T) {
	env := newBriefingEnv(t, briefKnight("c", "rt-dev.tasks.operator.>"))
	if err := env.briefer.PublishBriefing(context.Background(), briefMission("m3", "c")); err != nil {
		t.Fatal(err)
	}
	var payload natspkg.TaskPayload
	if err := json.Unmarshal(env.fake.published["rt-dev.tasks.operator.c"], &payload); err != nil {
		t.Fatal(err)
	}
	if want := "mission-m3-briefing-c-gen1"; payload.TaskID != want {
		t.Fatalf("TaskID = %q, want %q", payload.TaskID, want)
	}
}

func TestPublishBriefingErrorWhenNoneDelivered(t *testing.T) {
	env := newBriefingEnv(t, briefKnight("d", "rt-dev.tasks.operator.>"))
	env.fake.failSubject = func(string) bool { return true }
	err := env.briefer.PublishBriefing(context.Background(), briefMission("m4", "d"))
	if err == nil || !strings.Contains(err.Error(), "not delivered to any") {
		t.Fatalf("expected not-delivered error, got %v", err)
	}
}

func TestPublishBriefingPartialDeliveryWarns(t *testing.T) {
	env := newBriefingEnv(t,
		briefKnight("e", "rt-dev.tasks.operator.>"),
		briefKnight("f", "rt-dev.tasks.operator.>"))
	env.fake.failSubject = func(s string) bool { return strings.HasSuffix(s, ".f") }
	if err := env.briefer.PublishBriefing(context.Background(), briefMission("m5", "e", "f")); err != nil {
		t.Fatal(err)
	}
	if subjects := env.fake.subjects(); len(subjects) != 1 || subjects[0] != "rt-dev.tasks.operator.e" {
		t.Fatalf("unexpected subjects: %v", subjects)
	}
	select {
	case ev := <-env.recorder.Events:
		if !strings.Contains(ev, "BriefingPartialDelivery") {
			t.Fatalf("unexpected event %q", ev)
		}
	default:
		t.Fatal("expected BriefingPartialDelivery event")
	}
}

func TestPublishBriefingSkipsEphemeralKnights(t *testing.T) {
	env := newBriefingEnv(t)
	m := briefMission("m6")
	m.Spec.Knights = []aiv1alpha1.MissionKnight{{Name: "ghost", Ephemeral: true}}
	if err := env.briefer.PublishBriefing(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if len(env.fake.subjects()) != 0 {
		t.Fatal("expected no publishes")
	}
}

func (f *fakeNATS) PublishJSONCtx(_ context.Context, subject string, v any, _ ...natspkg.PublishOption) error {
	return f.PublishJSON(subject, v)
}

func (f *fakeNATS) PublishJSONSeqCtx(_ context.Context, subject string, v any, _ ...natspkg.PublishOption) (uint64, error) {
	return 0, f.PublishJSON(subject, v)
}
