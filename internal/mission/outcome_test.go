package mission

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func completeCond(status metav1.ConditionStatus, reason string) []metav1.Condition {
	return []metav1.Condition{{Type: aiv1alpha1.ConditionMissionComplete, Status: status, Reason: reason}}
}

func TestTerminalOutcome(t *testing.T) {
	tests := []struct {
		name   string
		status aiv1alpha1.MissionStatus
		want   aiv1alpha1.MissionPhase
	}{
		{
			name:   "terminal phase Succeeded is kept",
			status: aiv1alpha1.MissionStatus{Phase: aiv1alpha1.MissionPhaseSucceeded},
			want:   aiv1alpha1.MissionPhaseSucceeded,
		},
		{
			name:   "terminal phase Failed is kept",
			status: aiv1alpha1.MissionStatus{Phase: aiv1alpha1.MissionPhaseFailed},
			want:   aiv1alpha1.MissionPhaseFailed,
		},
		{
			name:   "terminal phase Expired is kept",
			status: aiv1alpha1.MissionStatus{Phase: aiv1alpha1.MissionPhaseExpired},
			want:   aiv1alpha1.MissionPhaseExpired,
		},
		{
			name: "CleaningUp reads Complete=Succeeded as Succeeded",
			status: aiv1alpha1.MissionStatus{
				Phase:      aiv1alpha1.MissionPhaseCleaningUp,
				Conditions: completeCond(metav1.ConditionTrue, aiv1alpha1.ReasonMissionSucceeded),
			},
			want: aiv1alpha1.MissionPhaseSucceeded,
		},
		{
			name: "CleaningUp reads Complete=Expired as Expired",
			status: aiv1alpha1.MissionStatus{
				Phase:      aiv1alpha1.MissionPhaseCleaningUp,
				Conditions: completeCond(metav1.ConditionTrue, aiv1alpha1.ReasonMissionExpired),
			},
			want: aiv1alpha1.MissionPhaseExpired,
		},
		{
			name: "CleaningUp reads Complete=Failed as Failed",
			status: aiv1alpha1.MissionStatus{
				Phase:      aiv1alpha1.MissionPhaseCleaningUp,
				Conditions: completeCond(metav1.ConditionTrue, aiv1alpha1.ReasonMissionFailed),
			},
			want: aiv1alpha1.MissionPhaseFailed,
		},
		{
			name: "CleaningUp reads Complete=ChainFailed as Failed",
			status: aiv1alpha1.MissionStatus{
				Phase:      aiv1alpha1.MissionPhaseCleaningUp,
				Conditions: completeCond(metav1.ConditionTrue, aiv1alpha1.ReasonMissionChainFailed),
			},
			want: aiv1alpha1.MissionPhaseFailed,
		},
		{
			name: "CleaningUp reads Complete=Timeout as Failed",
			status: aiv1alpha1.MissionStatus{
				Phase:      aiv1alpha1.MissionPhaseCleaningUp,
				Conditions: completeCond(metav1.ConditionTrue, aiv1alpha1.ReasonMissionTimeout),
			},
			want: aiv1alpha1.MissionPhaseFailed,
		},
		{
			name: "CleaningUp reads Complete=OverBudget as Failed",
			status: aiv1alpha1.MissionStatus{
				Phase:      aiv1alpha1.MissionPhaseCleaningUp,
				Conditions: completeCond(metav1.ConditionTrue, aiv1alpha1.ReasonOverBudget),
			},
			want: aiv1alpha1.MissionPhaseFailed,
		},
		{
			name: "condition wins over a contradicting chain status",
			status: aiv1alpha1.MissionStatus{
				Phase:         aiv1alpha1.MissionPhaseCleaningUp,
				Conditions:    completeCond(metav1.ConditionTrue, aiv1alpha1.ReasonMissionSucceeded),
				ChainStatuses: []aiv1alpha1.MissionChainStatus{{Name: "c", Phase: aiv1alpha1.ChainPhaseFailed}},
			},
			want: aiv1alpha1.MissionPhaseSucceeded,
		},
		{
			name: "Complete=False is ignored and falls back to inference",
			status: aiv1alpha1.MissionStatus{
				Phase:      aiv1alpha1.MissionPhaseCleaningUp,
				Conditions: completeCond(metav1.ConditionFalse, aiv1alpha1.ReasonMissionFailed),
			},
			want: aiv1alpha1.MissionPhaseSucceeded,
		},
		{
			name: "legacy: result containing failed is Failed",
			status: aiv1alpha1.MissionStatus{
				Phase:  aiv1alpha1.MissionPhaseCleaningUp,
				Result: "chain build failed: exit 1",
			},
			want: aiv1alpha1.MissionPhaseFailed,
		},
		{
			name: "legacy: result without failed is Succeeded",
			status: aiv1alpha1.MissionStatus{
				Phase:  aiv1alpha1.MissionPhaseCleaningUp,
				Result: "all good",
			},
			want: aiv1alpha1.MissionPhaseSucceeded,
		},
		{
			name: "legacy: a failed chain is Failed",
			status: aiv1alpha1.MissionStatus{
				Phase:         aiv1alpha1.MissionPhaseCleaningUp,
				ChainStatuses: []aiv1alpha1.MissionChainStatus{{Name: "a", Phase: aiv1alpha1.ChainPhaseSucceeded}, {Name: "b", Phase: aiv1alpha1.ChainPhaseFailed}},
			},
			want: aiv1alpha1.MissionPhaseFailed,
		},
		{
			name: "legacy: partially succeeded chains are Succeeded",
			status: aiv1alpha1.MissionStatus{
				Phase:         aiv1alpha1.MissionPhaseCleaningUp,
				ChainStatuses: []aiv1alpha1.MissionChainStatus{{Name: "a", Phase: aiv1alpha1.ChainPhasePartiallySucceeded}},
			},
			want: aiv1alpha1.MissionPhaseSucceeded,
		},
		{
			name:   "nothing set is Succeeded",
			status: aiv1alpha1.MissionStatus{},
			want:   aiv1alpha1.MissionPhaseSucceeded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &aiv1alpha1.Mission{Status: tt.status}
			if got := TerminalOutcome(m); got != tt.want {
				t.Errorf("TerminalOutcome() = %q, want %q", got, tt.want)
			}
		})
	}
}
