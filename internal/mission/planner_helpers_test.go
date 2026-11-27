package mission

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func TestPlanningTimeout(t *testing.T) {
	tests := []struct {
		name    string
		planner *aiv1alpha1.MissionPlanner
		want    int32
	}{
		{name: "nil planner defaults to 300", planner: nil, want: 300},
		{name: "zero timeout defaults to 300", planner: &aiv1alpha1.MissionPlanner{}, want: 300},
		{name: "override is used", planner: &aiv1alpha1.MissionPlanner{TimeoutSeconds: 42}, want: 42},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &aiv1alpha1.Mission{Spec: aiv1alpha1.MissionSpec{Planner: tt.planner}}
			if got := planningTimeout(m); got != tt.want {
				t.Errorf("planningTimeout() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestPlanningTimedOut(t *testing.T) {
	started := func(ago time.Duration) *metav1.Time {
		return &metav1.Time{Time: time.Now().Add(-ago)}
	}
	tests := []struct {
		name      string
		planner   *aiv1alpha1.MissionPlanner
		startedAt *metav1.Time
		want      bool
	}{
		{name: "no start time is not timed out", startedAt: nil, want: false},
		{name: "default: 299s elapsed is not timed out", startedAt: started(299 * time.Second), want: false},
		{name: "default: 301s elapsed is timed out", startedAt: started(301 * time.Second), want: true},
		{
			name:      "override: 59s elapsed under 60s timeout is not timed out",
			planner:   &aiv1alpha1.MissionPlanner{TimeoutSeconds: 60},
			startedAt: started(59 * time.Second),
			want:      false,
		},
		{
			name:      "override: 61s elapsed over 60s timeout is timed out",
			planner:   &aiv1alpha1.MissionPlanner{TimeoutSeconds: 60},
			startedAt: started(61 * time.Second),
			want:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &aiv1alpha1.Mission{
				Spec:   aiv1alpha1.MissionSpec{Planner: tt.planner},
				Status: aiv1alpha1.MissionStatus{StartedAt: tt.startedAt},
			}
			if got := planningTimedOut(m); got != tt.want {
				t.Errorf("planningTimedOut() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPlannerHasSource(t *testing.T) {
	tests := []struct {
		name    string
		planner *aiv1alpha1.MissionPlanner
		want    bool
	}{
		{name: "empty planner has no source", planner: &aiv1alpha1.MissionPlanner{}, want: false},
		{name: "knightRef is a source", planner: &aiv1alpha1.MissionPlanner{KnightRef: &corev1.LocalObjectReference{Name: "p"}}, want: true},
		{name: "templateName is a source", planner: &aiv1alpha1.MissionPlanner{TemplateName: "base"}, want: true},
		{name: "ephemeralSpec is a source", planner: &aiv1alpha1.MissionPlanner{EphemeralSpec: &aiv1alpha1.KnightSpec{}}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := plannerHasSource(tt.planner); got != tt.want {
				t.Errorf("plannerHasSource() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTemplatesOnlyGuard(t *testing.T) {
	gateway := &aiv1alpha1.Mission{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
		aiv1alpha1.LabelCreatedBy: aiv1alpha1.CreatedByGateway,
	}}}
	other := &aiv1alpha1.Mission{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
		aiv1alpha1.LabelCreatedBy: "cli",
	}}}
	if !templatesOnly(gateway) {
		t.Error("templatesOnly(gateway mission) = false, want true")
	}
	if templatesOnly(other) {
		t.Error("templatesOnly(non-gateway mission) = true, want false")
	}
	if templatesOnly(&aiv1alpha1.Mission{}) {
		t.Error("templatesOnly(unlabelled mission) = true, want false")
	}

	everything := PlannerKnight{
		Name:          "k",
		EphemeralSpec: &aiv1alpha1.KnightSpec{},
		SpecOverrides: &aiv1alpha1.KnightSpecOverrides{
			Env:   []corev1.EnvVar{{Name: "A", Value: "b"}},
			Tools: &aiv1alpha1.KnightTools{},
		},
	}
	if err := checkTemplatesOnly(other, everything); err != nil {
		t.Errorf("checkTemplatesOnly(non-gateway) = %v, want nil", err)
	}

	// Env fires before tools, and the message names only the arm that fired.
	envAndTools := PlannerKnight{Name: "k", SpecOverrides: everything.SpecOverrides}
	err := checkTemplatesOnly(gateway, envAndTools)
	if err == nil || !strings.Contains(err.Error(), "specOverrides.env is not allowed") || strings.Contains(err.Error(), "specOverrides.tools") {
		t.Errorf("checkTemplatesOnly(env+tools) = %v, want the env error only", err)
	}

	tests := []struct {
		name    string
		k       PlannerKnight
		wantErr string
	}{
		{name: "ephemeralSpec rejected", k: PlannerKnight{Name: "k", EphemeralSpec: &aiv1alpha1.KnightSpec{}}, wantErr: "ephemeralSpec"},
		{
			name:    "specOverrides.env rejected",
			k:       PlannerKnight{Name: "k", SpecOverrides: &aiv1alpha1.KnightSpecOverrides{Env: []corev1.EnvVar{{Name: "A", Value: "b"}}}},
			wantErr: "specOverrides.env is not allowed",
		},
		{
			name:    "specOverrides.tools rejected",
			k:       PlannerKnight{Name: "k", SpecOverrides: &aiv1alpha1.KnightSpecOverrides{Tools: &aiv1alpha1.KnightTools{}}},
			wantErr: "specOverrides.tools is not allowed",
		},
		{name: "templateName allowed", k: PlannerKnight{Name: "k", TemplateName: "base"}},
		{
			name: "model, skills and prompt overrides allowed",
			k: PlannerKnight{Name: "k", TemplateName: "base", SpecOverrides: &aiv1alpha1.KnightSpecOverrides{
				Model:  "claude-haiku-5-5",
				Skills: []string{"s"},
				Prompt: &aiv1alpha1.KnightPrompt{},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkTemplatesOnly(gateway, tt.k)
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("checkTemplatesOnly() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("checkTemplatesOnly() = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestTruncateString(t *testing.T) {
	tests := []struct {
		name string
		s    string
		n    int
		want string
	}{
		{name: "shorter than n is unchanged", s: "abc", n: 5, want: "abc"},
		{name: "exactly n runes is unchanged", s: "abcde", n: 5, want: "abcde"},
		{name: "one over n is truncated with ellipsis", s: "abcdef", n: 5, want: "abcde..."},
		{name: "counts runes not bytes", s: "héllo", n: 5, want: "héllo"},
		{name: "multi-byte input cut on rune boundary", s: "日本語テキスト", n: 3, want: "日本語..."},
		{name: "empty string", s: "", n: 0, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := truncateString(tt.s, tt.n); got != tt.want {
				t.Errorf("truncateString(%q, %d) = %q, want %q", tt.s, tt.n, got, tt.want)
			}
		})
	}
}

func TestValidChainPhase(t *testing.T) {
	for _, p := range []string{"", "Setup", "Active", "Teardown"} {
		if !validChainPhase(p) {
			t.Errorf("validChainPhase(%q) = false, want true", p)
		}
	}
	for _, p := range []string{"Running", "Succeeded", "active", "Failed"} {
		if validChainPhase(p) {
			t.Errorf("validChainPhase(%q) = true, want false", p)
		}
	}
}
