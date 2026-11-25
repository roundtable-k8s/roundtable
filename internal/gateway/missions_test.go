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

package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

type missionEnv struct {
	env
	k8s client.Client
}

// newMissionEnv serves the gateway with missions enabled over a fake
// Kubernetes API holding a RoundTable, knights (one labelled planner) and a
// chain. No NATS: the mission endpoints only talk to Kubernetes.
func newMissionEnv(t *testing.T, opts Options, enabled bool, extra ...client.Object) *missionEnv {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = aiv1alpha1.AddToScheme(scheme)
	planner := knight("planner", "planning", "", false)
	planner.Labels[plannerRoleLabel] = "planner"
	objs := append([]client.Object{
		&aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: testNS}},
		knight("operator", "claude", "", false),
		planner,
		&aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: "research", Namespace: testNS}},
	}, extra...)
	k8s := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&aiv1alpha1.Mission{}, &aiv1alpha1.Chain{}).Build()

	if opts.Namespace == "" {
		opts.Namespace = testNS
	}
	if opts.Missions.PollInterval == 0 {
		opts.Missions.PollInterval = 20 * time.Millisecond
	}
	svc := NewService(&Resolver{Client: k8s}, nil, opts, logr.Discard())
	if enabled {
		svc.Missions = k8s
	}
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(testToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	auth, err := NewTokenAuth(tokenFile, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(Handler(svc, auth))
	t.Cleanup(srv.Close)
	return &missionEnv{env: env{svc: svc, server: srv}, k8s: k8s}
}

func (e *missionEnv) missions(t *testing.T) []aiv1alpha1.Mission {
	t.Helper()
	var list aiv1alpha1.MissionList
	if err := e.k8s.List(context.Background(), &list, client.InNamespace(testNS)); err != nil {
		t.Fatal(err)
	}
	return list.Items
}

func (e *missionEnv) mission(t *testing.T, name string) *aiv1alpha1.Mission {
	t.Helper()
	m := &aiv1alpha1.Mission{}
	if err := e.k8s.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, m); err != nil {
		t.Fatal(err)
	}
	return m
}

func validMission() map[string]any {
	return map[string]any{
		"objective":       "Research NATS KV TTL semantics",
		"successCriteria": "a short written summary",
		"roundTable":      "dev",
		"costBudgetUSD":   5,
	}
}

func TestStartMissionCreatesRestrictedMission(t *testing.T) {
	e := newMissionEnv(t, Options{}, true)

	body := validMission()
	body["ttlSeconds"] = 7200
	body["timeoutSeconds"] = 3600
	body["recruitExisting"] = true
	body["knights"] = []string{"operator"}
	body["chains"] = []map[string]any{{"name": "research", "input": "topic: KV"}}
	body["planner"] = map[string]any{"knight": "planner", "context": "use primary sources", "maxChains": 3, "maxKnights": 2}
	resp, out := e.do(t, http.MethodPost, "/v1/missions", testToken, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status %d: %v", resp.StatusCode, out)
	}
	name, _ := out["name"].(string)
	if !strings.HasPrefix(name, "gw-") || resp.Header.Get("Location") != "/v1/missions/"+testNS+"/"+name {
		t.Fatalf("name %q, Location %q", name, resp.Header.Get("Location"))
	}
	if out["phase"] != "Pending" || out["done"] != false || out["createdByGateway"] != true {
		t.Fatalf("projection: %v", out)
	}

	m := e.mission(t, name)
	sp := m.Spec
	if m.Labels[aiv1alpha1.LabelCreatedBy] != aiv1alpha1.CreatedByGateway {
		t.Errorf("labels: %v", m.Labels)
	}
	if sp.Objective != body["objective"] || sp.SuccessCriteria != body["successCriteria"] ||
		aiv1alpha1.RefName(sp.RoundTableRef) != "dev" || sp.CostBudgetUSD != "5" ||
		sp.TTLSeconds != 7200 || sp.TimeoutSeconds != 3600 || !sp.RecruitExisting {
		t.Errorf("spec: %+v", sp)
	}
	if !sp.MetaMission || sp.Planner == nil || aiv1alpha1.RefName(sp.Planner.KnightRef) != "planner" ||
		sp.Planner.Context != "use primary sources" || sp.Planner.MaxChains != 3 || sp.Planner.MaxKnights != 2 ||
		sp.Planner.EphemeralSpec != nil || sp.Planner.TemplateName != "" || sp.Planner.AllowSkillGeneration {
		t.Errorf("planner: %+v", sp.Planner)
	}
	if len(sp.Knights) != 1 || sp.Knights[0] != (aiv1alpha1.MissionKnight{Name: "operator"}) {
		t.Errorf("knights: %+v", sp.Knights)
	}
	if len(sp.Chains) != 1 || sp.Chains[0] != (aiv1alpha1.MissionChainRef{Name: "research", InputOverride: "topic: KV"}) {
		t.Errorf("chains: %+v", sp.Chains)
	}
	if sp.Secrets != nil || sp.RoundTableTemplate != nil || sp.KnightTemplates != nil || sp.Notify != nil ||
		sp.NATSPrefix != "" || sp.Briefing != "" || sp.GeneratedKnights != nil || sp.GeneratedChains != nil {
		t.Errorf("unexpected spec fields: %+v", sp)
	}
}

func TestStartMissionDefaults(t *testing.T) {
	e := newMissionEnv(t, Options{Missions: MissionOptions{MaxTTLSeconds: 1200}}, true)

	// planner {} picks the table's planner (#566); ttl/timeout default,
	// capped by the gateway maximum.
	body := validMission()
	body["planner"] = map[string]any{}
	body["costBudgetUSD"] = 0.25
	resp, out := e.do(t, http.MethodPost, "/v1/missions", testToken, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status %d: %v", resp.StatusCode, out)
	}
	m := e.mission(t, out["name"].(string))
	if aiv1alpha1.RefName(m.Spec.Planner.KnightRef) != "dev-planner" || !m.Spec.MetaMission {
		t.Errorf("planner: %+v", m.Spec.Planner)
	}
	if m.Spec.TTLSeconds != 1200 || m.Spec.TimeoutSeconds != defaultMissionTimeout || m.Spec.CostBudgetUSD != "0.25" {
		t.Errorf("spec: ttl %d timeout %d budget %s", m.Spec.TTLSeconds, m.Spec.TimeoutSeconds, m.Spec.CostBudgetUSD)
	}
}

func TestStartMissionRejects(t *testing.T) {
	e := newMissionEnv(t, Options{}, true)
	with := func(mut func(b map[string]any)) map[string]any {
		b := validMission()
		b["planner"] = map[string]any{"knight": "planner"}
		mut(b)
		return b
	}
	cases := map[string]map[string]any{
		// Fields that could pick an image, mount a Secret or define a pod.
		"ephemeralSpec":      with(func(b map[string]any) { b["ephemeralSpec"] = map[string]any{"image": "evil"} }),
		"secrets":            with(func(b map[string]any) { b["secrets"] = []map[string]any{{"name": "github-token"}} }),
		"roundTableTemplate": with(func(b map[string]any) { b["roundTableTemplate"] = map[string]any{"natsURL": "nats://x"} }),
		"knightTemplates": with(func(b map[string]any) {
			b["knightTemplates"] = []map[string]any{{"name": "x", "spec": map[string]any{}}}
		}),
		"image":            with(func(b map[string]any) { b["image"] = "evil:latest" }),
		"notify":           with(func(b map[string]any) { b["notify"] = map[string]any{} }),
		"spec":             with(func(b map[string]any) { b["spec"] = map[string]any{"objective": "x"} }),
		"generatedKnights": with(func(b map[string]any) { b["generatedKnights"] = []map[string]any{{"name": "x"}} }),
		"planner.ephemeralSpec": with(func(b map[string]any) {
			b["planner"] = map[string]any{"ephemeralSpec": map[string]any{"image": "evil"}}
		}),
		"planner.templateName": with(func(b map[string]any) { b["planner"] = map[string]any{"templateName": "anything"} }),
		"planner.knightRef":    with(func(b map[string]any) { b["planner"] = map[string]any{"knightRef": map[string]any{"name": "planner"}} }),
		"knight object": with(func(b map[string]any) {
			b["knights"] = []map[string]any{{"name": "x", "ephemeralSpec": map[string]any{}}}
		}),
		"chain inputOverride": with(func(b map[string]any) { b["chains"] = []map[string]any{{"name": "research", "steps": []any{}}} }),
		"skill generation":    with(func(b map[string]any) { b["planner"] = map[string]any{"allowSkillGeneration": true} }),
		// Budget.
		"over budget":   with(func(b map[string]any) { b["costBudgetUSD"] = 10.01 }),
		"zero budget":   with(func(b map[string]any) { b["costBudgetUSD"] = 0 }),
		"no budget":     with(func(b map[string]any) { delete(b, "costBudgetUSD") }),
		"string budget": with(func(b map[string]any) { b["costBudgetUSD"] = "5" }),
		// Required fields, limits and references.
		"no objective":       with(func(b map[string]any) { delete(b, "objective") }),
		"long objective":     with(func(b map[string]any) { b["objective"] = strings.Repeat("x", maxObjectiveBytes+1) }),
		"no roundTable":      with(func(b map[string]any) { delete(b, "roundTable") }),
		"unknown roundTable": with(func(b map[string]any) { b["roundTable"] = "nope" }),
		"nothing to do":      with(func(b map[string]any) { delete(b, "planner") }),
		"unknown knight":     with(func(b map[string]any) { b["knights"] = []string{"nobody"} }),
		"bad knight name":    with(func(b map[string]any) { b["knights"] = []string{"../x"} }),
		"duplicate knight":   with(func(b map[string]any) { b["knights"] = []string{"operator", "operator"} }),
		"unknown chain":      with(func(b map[string]any) { b["chains"] = []map[string]any{{"name": "nope"}} }),
		"unknown planner":    with(func(b map[string]any) { b["planner"] = map[string]any{"knight": "nobody"} }),
		"ttl over cap":       with(func(b map[string]any) { b["ttlSeconds"] = 86401 }),
		"ttl too short":      with(func(b map[string]any) { b["ttlSeconds"] = 59 }),
		"timeout over cap":   with(func(b map[string]any) { b["timeoutSeconds"] = 21601 }),
		"maxChains":          with(func(b map[string]any) { b["planner"] = map[string]any{"maxChains": 21} }),
		"maxKnights":         with(func(b map[string]any) { b["planner"] = map[string]any{"maxKnights": 51} }),
		"namespace":          with(func(b map[string]any) { b["namespace"] = "kube-system" }),
		"negative wait":      with(func(b map[string]any) { b["waitSeconds"] = -1 }),
	}
	for name, body := range cases {
		resp, out := e.do(t, http.MethodPost, "/v1/missions", testToken, body)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status %d: %v", name, resp.StatusCode, out)
		}
	}
	if n := len(e.missions(t)); n != 0 {
		t.Fatalf("%d missions created by rejected requests", n)
	}

	// Without a planner, at least one chain is required; with no planner
	// knight in the namespace, planner {} is rejected.
	resp, out := e.do(t, http.MethodPost, "/v1/missions", testToken, with(func(b map[string]any) {
		delete(b, "planner")
		b["chains"] = []map[string]any{{"name": "research"}}
	}))
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("chain-only mission: %d %v", resp.StatusCode, out)
	}
	if m := e.mission(t, out["name"].(string)); m.Spec.MetaMission || m.Spec.Planner != nil {
		t.Errorf("chain-only mission became a meta-mission: %+v", m.Spec)
	}
}

// disableTablePlanner turns off the planner of the test table "dev".
func disableTablePlanner(t *testing.T, e *missionEnv) {
	t.Helper()
	rt := &aiv1alpha1.RoundTable{}
	if err := e.k8s.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "dev"}, rt); err != nil {
		t.Fatal(err)
	}
	rt.Spec.Planner = &aiv1alpha1.RoundTablePlanner{Enabled: new(bool)}
	if err := e.k8s.Update(context.Background(), rt); err != nil {
		t.Fatal(err)
	}
}

// TestStartMissionRoleLabelledPlanner: with the table's planner disabled,
// planner {} falls back to the knight labelled role=planner, skipping
// another table's planner.
func TestStartMissionRoleLabelledPlanner(t *testing.T) {
	other := knight("a-other-planner", "planning", "", false)
	other.Labels[plannerRoleLabel] = "planner"
	other.Labels[aiv1alpha1.LabelRoundTable] = "other"
	e := newMissionEnv(t, Options{}, true, other)
	disableTablePlanner(t, e)

	body := validMission()
	body["planner"] = map[string]any{}
	resp, out := e.do(t, http.MethodPost, "/v1/missions", testToken, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status %d: %v", resp.StatusCode, out)
	}
	if m := e.mission(t, out["name"].(string)); aiv1alpha1.RefName(m.Spec.Planner.KnightRef) != "planner" {
		t.Errorf("planner: %+v", m.Spec.Planner)
	}
}

func TestStartMissionNoDefaultPlanner(t *testing.T) {
	e := newMissionEnv(t, Options{}, true)
	disableTablePlanner(t, e)
	p := &aiv1alpha1.Knight{}
	_ = e.k8s.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "planner"}, p)
	delete(p.Labels, plannerRoleLabel)
	if err := e.k8s.Update(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	body := validMission()
	body["planner"] = map[string]any{}
	resp, out := e.do(t, http.MethodPost, "/v1/missions", testToken, body)
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(out["error"].(string), "planner.knight") {
		t.Fatalf("status %d: %v", resp.StatusCode, out)
	}
}

func TestStartMissionSkillGenerationAllowed(t *testing.T) {
	e := newMissionEnv(t, Options{Missions: MissionOptions{AllowSkillGeneration: true, MaxBudgetUSD: 50}}, true)
	body := validMission()
	body["costBudgetUSD"] = 40
	body["planner"] = map[string]any{"allowSkillGeneration": true}
	resp, out := e.do(t, http.MethodPost, "/v1/missions", testToken, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status %d: %v", resp.StatusCode, out)
	}
	if m := e.mission(t, out["name"].(string)); !m.Spec.Planner.AllowSkillGeneration || m.Spec.CostBudgetUSD != "40" {
		t.Fatalf("spec: %+v", m.Spec)
	}
}

// operatorMission is a Mission written directly (as the dashboard or kubectl
// would), with every sensitive spec field set and a full status.
func operatorMission() (*aiv1alpha1.Mission, *aiv1alpha1.Chain) {
	now := metav1.NewTime(time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC))
	m := &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: "manual", Namespace: testNS},
		Spec: aiv1alpha1.MissionSpec{
			Objective: "secret-objective-text",
			Secrets:   []corev1.LocalObjectReference{{Name: "secret-name-xyz"}},
			Knights: []aiv1alpha1.MissionKnight{{
				Name: "eph", Ephemeral: true,
				EphemeralSpec: &aiv1alpha1.KnightSpec{Domain: "x", Image: "registry.example/evil:1",
					Env: []corev1.EnvVar{{Name: "API_KEY", Value: "env-value-abc"}}},
			}},
			Briefing: "briefing-text-def",
		},
		Status: aiv1alpha1.MissionStatus{
			Phase:          aiv1alpha1.MissionPhaseActive,
			StartedAt:      &now,
			Result:         "",
			TotalCost:      "1.25",
			KnightStatuses: []aiv1alpha1.MissionKnightStatus{{Name: "eph", Ready: true, TasksCompleted: 2, Ephemeral: true}},
			ChainStatuses:  []aiv1alpha1.MissionChainStatus{{Name: "research", ChainCRName: "manual-research", Phase: aiv1alpha1.ChainPhaseRunning}},
			PlanningResult: &aiv1alpha1.PlanningResult{ChainsGenerated: 1, KnightsGenerated: 1, RawOutput: strings.Repeat("p", 2*maxRawPlannerOutput)},
			Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Assembled",
				Message: "ok", LastTransitionTime: now}},
		},
	}
	ch := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: "manual-research", Namespace: testNS},
		Spec:       aiv1alpha1.ChainSpec{Input: "chain-input-ghi"},
		Status: aiv1alpha1.ChainStatus{Phase: aiv1alpha1.ChainPhaseRunning, StepStatuses: []aiv1alpha1.ChainStepStatus{
			{Name: "gather", Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: "step-output-jkl"},
			{Name: "write", Phase: aiv1alpha1.ChainStepPhaseRunning},
		}},
	}
	return m, ch
}

func TestGetMissionProjection(t *testing.T) {
	m, ch := operatorMission()
	e := newMissionEnv(t, Options{}, true, m, ch)

	req, _ := http.NewRequest(http.MethodGet, e.server.URL+"/v1/missions/"+testNS+"/manual", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var raw json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %v", resp.StatusCode, err)
	}
	for _, leak := range []string{"secret-objective-text", "secret-name-xyz", "registry.example", "env-value-abc",
		"API_KEY", "briefing-text-def", "chain-input-ghi", "step-output-jkl", `"spec"`, "ephemeralSpec", "objective"} {
		if strings.Contains(string(raw), leak) {
			t.Errorf("projection contains %q: %s", leak, raw)
		}
	}

	var v MissionView
	_ = json.Unmarshal(raw, &v)
	if v.Phase != "Active" || v.Done || v.CreatedByGateway || v.TotalCost != "1.25" || v.StartedAt == nil {
		t.Errorf("view: %+v", v)
	}
	if v.Planning == nil || !v.Planning.RawOutputTruncated || len(v.Planning.RawOutput) != maxRawPlannerOutput || v.Planning.ChainsGenerated != 1 {
		t.Errorf("planning: %+v", v.Planning)
	}
	if len(v.Chains) != 1 || v.Chains[0].Chain != "manual-research" || len(v.Chains[0].Steps) != 2 ||
		v.Chains[0].Steps[0] != (MissionStepView{Name: "gather", Phase: "Succeeded"}) {
		t.Errorf("chains: %+v", v.Chains)
	}
	if len(v.Knights) != 1 || v.Knights[0] != (MissionKnightView{Name: "eph", Ready: true, TasksCompleted: 2, Ephemeral: true}) {
		t.Errorf("knights: %+v", v.Knights)
	}
	if len(v.Conditions) != 1 || v.Conditions[0].Reason != "Assembled" {
		t.Errorf("conditions: %+v", v.Conditions)
	}

	if resp, _ := e.do(t, http.MethodGet, "/v1/missions/"+testNS+"/nope", testToken, nil); resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown mission: %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, http.MethodGet, "/v1/missions/kube-system/manual", testToken, nil); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("disallowed namespace: %d", resp.StatusCode)
	}
}

func TestMissionWaitAndList(t *testing.T) {
	m, ch := operatorMission()
	e := newMissionEnv(t, Options{}, true, m, ch)

	resp, out := e.do(t, http.MethodPost, "/v1/missions", testToken, func() map[string]any {
		b := validMission()
		b["planner"] = map[string]any{}
		return b
	}())
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("start: %d %v", resp.StatusCode, out)
	}
	name := out["name"].(string)

	// A wait that ends before the mission does returns it unfinished.
	start := time.Now()
	resp, out = e.do(t, http.MethodGet, "/v1/missions/"+testNS+"/"+name+"?waitSeconds=1", testToken, nil)
	if resp.StatusCode != http.StatusOK || out["done"] != false || time.Since(start) < 900*time.Millisecond {
		t.Fatalf("short wait: %d %v after %s", resp.StatusCode, out, time.Since(start))
	}

	// The operator finishes the mission while the caller waits.
	go func() {
		time.Sleep(100 * time.Millisecond)
		mm := e.mission(t, name)
		mm.Status.Phase = aiv1alpha1.MissionPhaseSucceeded
		mm.Status.Result = "KV TTLs are per bucket"
		if err := e.k8s.Status().Update(context.Background(), mm); err != nil {
			t.Errorf("status update: %v", err)
		}
	}()
	resp, out = e.do(t, http.MethodGet, "/v1/missions/"+testNS+"/"+name+"?waitSeconds=10", testToken, nil)
	if resp.StatusCode != http.StatusOK || out["done"] != true || out["phase"] != "Succeeded" || out["result"] != "KV TTLs are per bucket" {
		t.Fatalf("wait: %d %v", resp.StatusCode, out)
	}

	resp, out = e.do(t, http.MethodGet, "/v1/missions", testToken, nil)
	if resp.StatusCode != http.StatusOK || len(out["missions"].([]any)) != 2 {
		t.Fatalf("list: %d %v", resp.StatusCode, out)
	}
	_, out = e.do(t, http.MethodGet, "/v1/missions?phase=succeeded", testToken, nil)
	if ms := out["missions"].([]any); len(ms) != 1 || ms[0].(map[string]any)["name"] != name {
		t.Fatalf("list phase: %v", out)
	}
	_, out = e.do(t, http.MethodGet, "/v1/missions?limit=1", testToken, nil)
	if len(out["missions"].([]any)) != 1 {
		t.Fatalf("list limit: %v", out)
	}
	// List leaves out step details.
	_, out = e.do(t, http.MethodGet, "/v1/missions?phase=Active", testToken, nil)
	if c := out["missions"].([]any)[0].(map[string]any)["chains"].([]any)[0].(map[string]any); c["steps"] != nil {
		t.Fatalf("list has steps: %v", c)
	}
	// A mission the operator hasn't touched yet lists as Pending.
	_, out = e.do(t, http.MethodGet, "/v1/missions?phase=Pending", testToken, nil)
	if len(out["missions"].([]any)) != 0 {
		t.Fatalf("list pending: %v", out)
	}
	m2 := validMission()
	m2["planner"] = map[string]any{}
	e.do(t, http.MethodPost, "/v1/missions", testToken, m2)
	_, out = e.do(t, http.MethodGet, "/v1/missions?phase=Pending", testToken, nil)
	if len(out["missions"].([]any)) != 1 {
		t.Fatalf("list pending: %v", out)
	}
	if resp, _ := e.do(t, http.MethodGet, "/v1/missions?limit=x", testToken, nil); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad limit: %d", resp.StatusCode)
	}
}

func TestStartMissionWait(t *testing.T) {
	e := newMissionEnv(t, Options{}, true)
	body := validMission()
	body["planner"] = map[string]any{}
	body["waitSeconds"] = 1
	resp, out := e.do(t, http.MethodPost, "/v1/missions", testToken, body)
	if resp.StatusCode != http.StatusAccepted || out["done"] != false || resp.Header.Get("Location") == "" {
		t.Fatalf("status %d: %v", resp.StatusCode, out)
	}
}

func TestDeleteMission(t *testing.T) {
	m, ch := operatorMission()
	e := newMissionEnv(t, Options{}, true, m, ch)

	// Missions the gateway didn't create can't be deleted through it.
	resp, out := e.do(t, http.MethodDelete, "/v1/missions/"+testNS+"/manual", testToken, nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("delete manual: %d %v", resp.StatusCode, out)
	}
	body := validMission()
	body["planner"] = map[string]any{}
	_, out = e.do(t, http.MethodPost, "/v1/missions", testToken, body)
	name := out["name"].(string)
	resp, out = e.do(t, http.MethodDelete, "/v1/missions/"+testNS+"/"+name, testToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete: %d %v", resp.StatusCode, out)
	}
	if n := len(e.missions(t)); n != 1 {
		t.Fatalf("%d missions left, want 1", n)
	}
	if resp, _ := e.do(t, http.MethodDelete, "/v1/missions/"+testNS+"/"+name, testToken, nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("second delete: %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, http.MethodDelete, "/v1/missions/"+testNS+"/"+name, "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated delete: %d", resp.StatusCode)
	}
}

func TestMissionsDisabled(t *testing.T) {
	e := newMissionEnv(t, Options{}, false)
	body := validMission()
	body["planner"] = map[string]any{}
	if resp, _ := e.do(t, http.MethodPost, "/v1/missions", testToken, body); resp.StatusCode != http.StatusNotFound &&
		resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("start with missions disabled: %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, http.MethodGet, "/v1/missions", testToken, nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("list with missions disabled: %d", resp.StatusCode)
	}
	if len(e.missions(t)) != 0 {
		t.Fatal("mission created while disabled")
	}
}

func TestMCPMissions(t *testing.T) {
	m, ch := operatorMission()
	e := newMissionEnv(t, Options{}, true, m, ch)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "v0"}, nil)
	cs, err := c.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:   e.server.URL + "/mcp",
		HTTPClient: &http.Client{Transport: bearer{http.DefaultTransport}},
		MaxRetries: -1,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cs.Close() }()

	tools, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, tool := range tools.Tools {
		names[tool.Name] = true
	}
	for _, want := range []string{"start_mission", "get_mission", "list_missions"} {
		if !names[want] {
			t.Fatalf("missing tool %s in %v", want, names)
		}
	}
	call := func(name string, args map[string]any) (*mcp.CallToolResult, map[string]any, error) {
		res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
		if err != nil {
			return nil, nil, err
		}
		var sc map[string]any
		b, _ := json.Marshal(res.StructuredContent)
		_ = json.Unmarshal(b, &sc)
		return res, sc, nil
	}

	args := validMission()
	args["planner"] = map[string]any{"context": "be brief"}
	res, sc, err := call("start_mission", args)
	if err != nil || res.IsError || sc["phase"] != "Pending" {
		t.Fatalf("start_mission: %v %+v %v", err, res, sc)
	}
	name := sc["name"].(string)
	if mm := e.mission(t, name); mm.Labels[aiv1alpha1.LabelCreatedBy] != aiv1alpha1.CreatedByGateway || !mm.Spec.MetaMission {
		t.Fatalf("mission: %+v", mm)
	}

	// Smuggled fields fail the input schema (no additional properties).
	bad := validMission()
	bad["planner"] = map[string]any{"ephemeralSpec": map[string]any{"image": "evil"}}
	bad["secrets"] = []map[string]any{{"name": "github-token"}}
	if res, _, err := call("start_mission", bad); err == nil && !res.IsError {
		t.Fatal("start_mission accepted secrets and planner.ephemeralSpec")
	}
	if n := len(e.missions(t)); n != 2 {
		t.Fatalf("%d missions, want 2", n)
	}
	over := validMission()
	over["costBudgetUSD"] = 100
	over["planner"] = map[string]any{}
	if res, _, err := call("start_mission", over); err == nil && !res.IsError {
		t.Fatal("over-budget start_mission succeeded")
	}

	mm := e.mission(t, name)
	mm.Status.Phase = aiv1alpha1.MissionPhaseFailed
	mm.Status.Result = "planner gave up"
	if err := e.k8s.Status().Update(ctx, mm); err != nil {
		t.Fatal(err)
	}
	res, sc, err = call("get_mission", map[string]any{"name": name, "waitSeconds": 5})
	if err != nil || !res.IsError || sc["done"] != true {
		t.Fatalf("get_mission: %v %+v %v", err, res, sc)
	}
	if tc, ok := res.Content[0].(*mcp.TextContent); !ok || tc.Text != "planner gave up" {
		t.Fatalf("get_mission text: %+v", res.Content)
	}
	res, sc, err = call("get_mission", map[string]any{"name": "manual"})
	if err != nil || res.IsError || sc["done"] != false || !strings.Contains(res.Content[0].(*mcp.TextContent).Text, "get_mission") {
		t.Fatalf("get_mission running: %v %+v %v", err, res, sc)
	}

	res, sc, err = call("list_missions", map[string]any{"phase": "Failed"})
	if err != nil || res.IsError || len(sc["missions"].([]any)) != 1 {
		t.Fatalf("list_missions: %v %+v %v", err, res, sc)
	}
}
