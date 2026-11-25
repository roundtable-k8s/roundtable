package mission

import (
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// TerminalOutcome returns the mission's terminal phase. The phase itself is
// overwritten to CleaningUp while cleanup runs, so the outcome is read from
// the Complete condition (recorded before cleanup starts); missions that
// reach cleanup without one fall back to inferring from result and chains.
func TerminalOutcome(mission *aiv1alpha1.Mission) aiv1alpha1.MissionPhase {
	switch mission.Status.Phase {
	case aiv1alpha1.MissionPhaseSucceeded, aiv1alpha1.MissionPhaseFailed, aiv1alpha1.MissionPhaseExpired:
		return mission.Status.Phase
	}

	if cond := meta.FindStatusCondition(mission.Status.Conditions, aiv1alpha1.ConditionMissionComplete); cond != nil &&
		cond.Status == metav1.ConditionTrue {
		switch cond.Reason {
		case aiv1alpha1.ReasonMissionSucceeded:
			return aiv1alpha1.MissionPhaseSucceeded
		case aiv1alpha1.ReasonMissionExpired:
			return aiv1alpha1.MissionPhaseExpired
		default: // Failed, ChainFailed, Timeout, OverBudget, ...
			return aiv1alpha1.MissionPhaseFailed
		}
	}

	// Legacy inference for missions completed before the condition was recorded.
	if mission.Status.Result != "" && strings.Contains(mission.Status.Result, "failed") {
		return aiv1alpha1.MissionPhaseFailed
	}
	for _, cs := range mission.Status.ChainStatuses {
		if cs.Phase == aiv1alpha1.ChainPhaseFailed {
			return aiv1alpha1.MissionPhaseFailed
		}
	}
	return aiv1alpha1.MissionPhaseSucceeded
}
