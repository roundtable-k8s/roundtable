package v1alpha1

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestValidateConcurrency(t *testing.T) {
	spec := func(p TriggerConcurrencyPolicy, tg TriggerTarget) *TriggerSpec {
		s := &TriggerSpec{Target: tg}
		s.Concurrency.Policy = p
		return s
	}
	chain := TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: "c"}}
	knight := TriggerTarget{Knight: &TriggerKnightTarget{}}
	mission := TriggerTarget{Mission: &TriggerMissionTarget{}}
	for name, tc := range map[string]struct {
		spec *TriggerSpec
		want string
	}{
		"knight replace":  {spec(TriggerConcurrencyReplace, knight), "spec.concurrency.policy Replace is not supported with target.knight"},
		"mission replace": {spec(TriggerConcurrencyReplace, mission), "spec.concurrency.policy Replace is not supported with target.mission"},
		"knight forbid":   {spec(TriggerConcurrencyForbid, knight), ""},
		"chain replace":   {spec(TriggerConcurrencyReplace, chain), ""},
	} {
		err := ValidateConcurrency(tc.spec)
		if tc.want == "" && err != nil {
			t.Errorf("%s: unexpected error %v", name, err)
		}
		if tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
			t.Errorf("%s: got %v, want %q", name, err, tc.want)
		}
	}
}
