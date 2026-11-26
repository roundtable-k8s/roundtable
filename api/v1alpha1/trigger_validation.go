package v1alpha1

import "fmt"

// ValidateConcurrency reports a concurrency policy that cannot work with the
// Trigger's target: Replace needs to withdraw the active run, which a sent
// knight task or a created Mission does not allow. The controller uses it for
// the Ready condition (InvalidSpec) and the gateway's schedules API (#584)
// for create and replace requests, so both report the same rule.
func ValidateConcurrency(spec *TriggerSpec) error {
	if spec.Concurrency.Policy != TriggerConcurrencyReplace {
		return nil
	}
	switch {
	case spec.Target.Knight != nil:
		return fmt.Errorf("spec.concurrency.policy Replace is not supported with target.knight: a sent task cannot be withdrawn")
	case spec.Target.Mission != nil:
		return fmt.Errorf("spec.concurrency.policy Replace is not supported with target.mission: a created Mission is not withdrawn")
	}
	return nil
}
