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
	"reflect"
	"testing"
	"time"

	"github.com/go-logr/logr"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

func cardTestTable() *aiv1alpha1.RoundTable {
	return &aiv1alpha1.RoundTable{
		ObjectMeta: metav1.ObjectMeta{Name: "personal", Namespace: "roundtable"},
		Spec:       aiv1alpha1.RoundTableSpec{NATS: aiv1alpha1.RoundTableNATS{SubjectPrefix: "table-prefix"}},
	}
}

func cardTestKnight() *aiv1alpha1.Knight {
	return &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "galahad", Namespace: "roundtable", Generation: 7},
		Spec: aiv1alpha1.KnightSpec{
			Domain: "security",
			Model:  "spec-model",
			Skills: []string{"security", "general"},
			Tools:  &aiv1alpha1.KnightTools{Nix: []string{"nmap", "whois"}, Apt: []string{"curl"}},
			NATS:   aiv1alpha1.KnightNATS{Subjects: []string{"fleet-a.tasks.security.>"}},
		},
		Status: aiv1alpha1.KnightStatus{Phase: aiv1alpha1.KnightPhaseReady, ReadyReplicas: 1, Model: "status-model"},
	}
}

func TestBuildKnightCardFromSpec(t *testing.T) {
	card := buildKnightCard(cardTestTable(), cardTestKnight(), nil)

	if card.SchemaVersion != 1 || card.Name != "galahad" || card.Version != "7" {
		t.Fatalf("header = %d %q %q", card.SchemaVersion, card.Name, card.Version)
	}
	if card.Description != "Galahad, security knight" {
		t.Fatalf("description = %q", card.Description)
	}
	if !reflect.DeepEqual(card.DefaultInputModes, []string{"text/plain"}) || !reflect.DeepEqual(card.DefaultOutputModes, []string{"text/plain"}) {
		t.Fatalf("modes = %v %v", card.DefaultInputModes, card.DefaultOutputModes)
	}
	// Category-only skills before any runtime data.
	wantSkills := []natspkg.AgentSkill{
		{ID: "security", Name: "security", Description: "security skills", Tags: []string{"security"}, Examples: []string{}},
		{ID: "general", Name: "general", Description: "general skills", Tags: []string{"general"}, Examples: []string{}},
	}
	if !reflect.DeepEqual(card.Skills, wantSkills) {
		t.Fatalf("skills = %+v", card.Skills)
	}
	m := card.RoundTable
	if m.Table != "personal" || m.Knight != "galahad" || m.Domain != "security" || m.Model != "status-model" ||
		m.TaskSubject != "fleet-a.tasks.security.galahad" || m.Runtime != "deployment" || m.Phase != "Ready" ||
		m.ReadyReplicas != 1 || m.Suspended || m.SkillsSource != natspkg.CardSkillsFromSpec || m.UpdatedAt != "" {
		t.Fatalf("roundtable = %+v", m)
	}
	// Tools come from spec.tools.nix.
	if !reflect.DeepEqual(m.Tools, []string{"nmap", "whois"}) {
		t.Fatalf("tools = %v", m.Tools)
	}
}

func TestBuildKnightCardFallbacks(t *testing.T) {
	k := cardTestKnight()
	k.Spec.Tools = nil
	k.Spec.NATS.Subjects = nil
	k.Spec.Suspend = true
	k.Status = aiv1alpha1.KnightStatus{}
	card := buildKnightCard(cardTestTable(), k, nil)
	if card.RoundTable.Model != "spec-model" || card.RoundTable.TaskSubject != "table-prefix.tasks.security.galahad" ||
		!card.RoundTable.Suspended || card.RoundTable.Tools == nil || len(card.RoundTable.Tools) != 0 {
		t.Fatalf("roundtable = %+v", card.RoundTable)
	}
	data, _ := json.Marshal(card)
	var raw map[string]any
	_ = json.Unmarshal(data, &raw)
	if rt := raw["roundtable"].(map[string]any); rt["tools"] == nil {
		t.Fatal("tools must serialize as [] not null")
	}
}

func TestCardDescription(t *testing.T) {
	cases := []struct {
		name     string
		card     *aiv1alpha1.KnightCard
		identity string
		want     string
	}{
		{name: "default", want: "Galahad, security knight"},
		{name: "identity first paragraph", identity: "\n  Security analyst:\n  CVE triage.\n\nSecond paragraph.", want: "Security analyst: CVE triage."},
		{name: "blank identity", identity: " \n\n ", want: "Galahad, security knight"},
		{name: "override wins", card: &aiv1alpha1.KnightCard{Description: " Override. "}, identity: "Identity.", want: "Override."},
		{name: "empty override falls back", card: &aiv1alpha1.KnightCard{Tags: []string{"x"}}, identity: "Identity.", want: "Identity."},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k := cardTestKnight()
			k.Spec.Card = tc.card
			if tc.identity != "" {
				k.Spec.Prompt = &aiv1alpha1.KnightPrompt{Identity: tc.identity}
			}
			if got := cardDescription(k); got != tc.want {
				t.Fatalf("description = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBuildKnightCardRuntimeSkillsAndOverrides(t *testing.T) {
	k := cardTestKnight()
	k.Spec.Card = &aiv1alpha1.KnightCard{
		Tags: []string{"security", "daily"},
		Skills: []aiv1alpha1.KnightCardSkill{
			{ID: "cve-triage", Description: "Overridden.", Examples: []string{"Is CVE-1 exploitable?"}},
			{ID: "brief", Tags: []string{"daily"}},
			{ID: "threat-model", Description: "Added."},
		},
	}
	runtime := []natspkg.RuntimeSkill{
		{Name: "brief", Description: "Daily brief.", Category: "security"},
		{Name: "cve-triage", Description: "Triage CVEs.", Category: "security"},
	}
	card := buildKnightCard(cardTestTable(), k, runtime)

	want := []natspkg.AgentSkill{
		{ID: "brief", Name: "brief", Description: "Daily brief.", Tags: []string{"daily"}, Examples: []string{}},
		{ID: "cve-triage", Name: "cve-triage", Description: "Overridden.", Tags: []string{"security"}, Examples: []string{"Is CVE-1 exploitable?"}},
		{ID: "threat-model", Name: "threat-model", Description: "Added.", Tags: []string{}, Examples: []string{}},
	}
	if !reflect.DeepEqual(card.Skills, want) {
		t.Fatalf("skills = %+v\nwant %+v", card.Skills, want)
	}
	if card.RoundTable.SkillsSource != natspkg.CardSkillsFromRuntime || !reflect.DeepEqual(card.RoundTable.RuntimeSkills, runtime) {
		t.Fatalf("runtime skills = %q %+v", card.RoundTable.SkillsSource, card.RoundTable.RuntimeSkills)
	}
	if !reflect.DeepEqual(card.RoundTable.Tags, []string{"security", "daily"}) {
		t.Fatalf("tags = %v", card.RoundTable.Tags)
	}
	// Overrides must not leak into the runtime skills kept on the card.
	if runtime[1].Description != "Triage CVEs." {
		t.Fatal("runtime skills mutated")
	}
}

func TestKnightTablesMapsToEveryTableInNamespace(t *testing.T) {
	scheme := k8sruntime.NewScheme()
	_ = aiv1alpha1.AddToScheme(scheme)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns"}},
		&aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"}},
		&aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "other"}},
	).Build()
	r := &RoundTableReconciler{Client: c}
	reqs := r.knightTables(context.Background(), &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: "k", Namespace: "ns"}})
	got := map[string]bool{}
	for _, req := range reqs {
		got[req.String()] = true
	}
	if len(got) != 2 || !got["ns/a"] || !got["ns/b"] {
		t.Fatalf("requests = %v", reqs)
	}
}

var _ = Describe("RoundTable Controller - agent cards", func() {
	const (
		namespace = "default"
		table     = "cards-rt"
		bucket    = "test-cards"
	)
	var (
		ns     *natsserver.Server
		nc     *nats.Conn
		r      *RoundTableReconciler
		clock  time.Time
		kvGet  func(key string) (*natspkg.AgentCard, bool)
		member = map[string]string{"cards-test": table}
	)

	reconcileTable := func() {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: table, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())
	}

	newKnight := func(name string) *aiv1alpha1.Knight {
		return &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: map[string]string{"cards-test": table}},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "security",
				Model:  "model-a",
				Skills: []string{"security"},
				Tools:  &aiv1alpha1.KnightTools{Nix: []string{"nmap"}},
				NATS:   aiv1alpha1.KnightNATS{Subjects: []string{"cards.tasks.security.>"}},
			},
		}
	}

	BeforeEach(func() {
		opts := natstest.DefaultTestOptions
		opts.Port = -1
		opts.JetStream = true
		opts.StoreDir = GinkgoT().TempDir()
		ns = natstest.RunServer(&opts)

		cli := natspkg.NewClient(natspkg.Config{URL: ns.ClientURL()}, logr.Discard())
		Expect(cli.Connect()).To(Succeed())
		nc = cli.(natspkg.ConnProvider).Conn()

		clock = time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
		r = &RoundTableReconciler{
			Client:             k8sClient,
			Scheme:             k8sClient.Scheme(),
			Recorder:           record.NewFakeRecorder(100),
			NATS:               natspkg.NewProviderWithClient(cli, logr.Discard()),
			CardsBucket:        bucket,
			ServiceInfoTimeout: 200 * time.Millisecond,
			Now:                func() time.Time { return clock },
		}

		kvGet = func(key string) (*natspkg.AgentCard, bool) {
			js, err := nc.JetStream()
			Expect(err).NotTo(HaveOccurred())
			kv, err := js.KeyValue(bucket)
			if err != nil {
				return nil, false
			}
			entry, err := kv.Get(key)
			if err != nil {
				return nil, false
			}
			card := &natspkg.AgentCard{}
			Expect(json.Unmarshal(entry.Value(), card)).To(Succeed())
			return card, true
		}

		Expect(k8sClient.Create(ctx, &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: table, Namespace: namespace},
			Spec: aiv1alpha1.RoundTableSpec{
				NATS:           aiv1alpha1.RoundTableNATS{SubjectPrefix: "cards", TasksStream: "cards_tasks", ResultsStream: "cards_results"},
				KnightSelector: &metav1.LabelSelector{MatchLabels: member},
			},
		})).To(Succeed())
	})

	AfterEach(func() {
		knights := &aiv1alpha1.KnightList{}
		Expect(k8sClient.List(ctx, knights, client.InNamespace(namespace))).To(Succeed())
		for i := range knights.Items {
			if knights.Items[i].Labels["cards-test"] != "" || knights.Items[i].Name == "percival" {
				_ = k8sClient.Delete(ctx, &knights.Items[i])
			}
		}
		_ = k8sClient.Delete(ctx, &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: table, Namespace: namespace}})
		nc.Close()
		ns.Shutdown()
	})

	It("keeps one card per member knight in NATS KV", func() {
		key := natspkg.CardKey(table, "percival")

		By("creating a Knight in the table")
		k := newKnight("percival")
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		reconcileTable()
		card, ok := kvGet(key)
		Expect(ok).To(BeTrue(), "card %s should exist", key)
		Expect(card.Name).To(Equal("percival"))
		Expect(card.RoundTable.Model).To(Equal("model-a"))
		Expect(card.RoundTable.TaskSubject).To(Equal("cards.tasks.security.percival"))
		Expect(card.RoundTable.Tools).To(Equal([]string{"nmap"}))
		Expect(card.RoundTable.UpdatedAt).To(Equal("2026-11-24T10:00:00Z"))

		By("not rewriting an unchanged card")
		clock = clock.Add(time.Minute)
		reconcileTable()
		card, _ = kvGet(key)
		Expect(card.RoundTable.UpdatedAt).To(Equal("2026-11-24T10:00:00Z"))

		By("changing its model")
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(k), k)).To(Succeed())
		k.Spec.Model = "model-b"
		Expect(k8sClient.Update(ctx, k)).To(Succeed())
		reconcileTable()
		card, _ = kvGet(key)
		Expect(card.RoundTable.Model).To(Equal("model-b"))
		Expect(card.RoundTable.UpdatedAt).To(Equal("2026-11-24T10:01:00Z"))

		By("copying the skills of the running knight")
		svc, err := micro.AddService(nc, micro.Config{
			Name: natspkg.KnightServiceName, Version: "1.0.0",
			Metadata: map[string]string{
				"table": table, "knight": "Percival",
				"skills": `[{"name":"cve-triage","description":"Triage CVEs.","category":"security"}]`,
			},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(k), k)).To(Succeed())
		k.Status.Phase = aiv1alpha1.KnightPhaseReady
		k.Status.ReadyReplicas = 1
		Expect(k8sClient.Status().Update(ctx, k)).To(Succeed())
		clock = clock.Add(time.Minute)
		reconcileTable()
		card, _ = kvGet(key)
		Expect(card.RoundTable.SkillsSource).To(Equal(natspkg.CardSkillsFromRuntime))
		Expect(card.Skills).To(HaveLen(1))
		Expect(card.Skills[0].ID).To(Equal("cve-triage"))
		Expect(card.Skills[0].Description).To(Equal("Triage CVEs."))
		Expect(card.RoundTable.ReadyReplicas).To(Equal(int32(1)))

		By("suspending it: the card stays, with suspended and the skills it had")
		Expect(svc.Stop()).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(k), k)).To(Succeed())
		k.Spec.Suspend = true
		Expect(k8sClient.Update(ctx, k)).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(k), k)).To(Succeed())
		k.Status.Phase = aiv1alpha1.KnightPhaseSuspended
		k.Status.ReadyReplicas = 0
		Expect(k8sClient.Status().Update(ctx, k)).To(Succeed())
		clock = clock.Add(time.Minute)
		reconcileTable()
		card, ok = kvGet(key)
		Expect(ok).To(BeTrue())
		Expect(card.RoundTable.Suspended).To(BeTrue())
		Expect(card.RoundTable.Phase).To(Equal("Suspended"))
		Expect(card.Skills).To(HaveLen(1))
		Expect(card.Skills[0].Description).To(Equal("Triage CVEs."))

		By("deleting it")
		Expect(k8sClient.Delete(ctx, k)).To(Succeed())
		Eventually(func() bool {
			return k8sClient.Get(ctx, client.ObjectKeyFromObject(k), &aiv1alpha1.Knight{}) != nil
		}, 10*time.Second, 100*time.Millisecond).Should(BeTrue())
		reconcileTable()
		_, ok = kvGet(key)
		Expect(ok).To(BeFalse(), "card of a deleted knight should be gone")
	})

	It("drops the card of a knight moved out of the table", func() {
		key := natspkg.CardKey(table, "bedivere")
		k := newKnight("bedivere")
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		reconcileTable()
		_, ok := kvGet(key)
		Expect(ok).To(BeTrue())

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(k), k)).To(Succeed())
		k.Labels = map[string]string{"cards-test-moved": table}
		Expect(k8sClient.Update(ctx, k)).To(Succeed())
		reconcileTable()
		_, ok = kvGet(key)
		Expect(ok).To(BeFalse(), "card of a knight that left the table should be gone")
		_ = k8sClient.Delete(ctx, k)
	})

	It("deletes a deleted knight's card from the knight finalizer path", func() {
		k := newKnight("tristan")
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		reconcileTable()
		_, ok := kvGet(natspkg.CardKey(table, "tristan"))
		Expect(ok).To(BeTrue())

		Expect(deleteKnightCards(ctx, k8sClient, r.NATS, bucket, k)).To(Succeed())
		_, ok = kvGet(natspkg.CardKey(table, "tristan"))
		Expect(ok).To(BeFalse())
	})
})
