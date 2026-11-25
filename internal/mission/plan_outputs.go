package mission

import (
	"fmt"
	"regexp"
	"slices"
)

// Output passes only between steps of the same chain: the chain controller
// renders {{ .Steps.<name>.Output }} from that chain's own step statuses, and
// the chains of a plan start together. A step that reads another chain's
// output would get "<no value>" (#533), so the plan is rejected instead.

var (
	// templateActionRe matches one {{ ... }} template action.
	templateActionRe = regexp.MustCompile(`(?s)\{\{(.*?)\}\}`)
	// stepFieldRefRe matches .Steps.<name> inside an action.
	stepFieldRefRe = regexp.MustCompile(`\.Steps\.([A-Za-z0-9_-]+)`)
	// stepIndexRefRe matches index .Steps "<name>" inside an action.
	stepIndexRefRe = regexp.MustCompile(`index\s+\.Steps\s+"([^"]+)"`)
)

// stepOutputRefs returns the step names a task template reads from .Steps,
// in order of first appearance.
func stepOutputRefs(task string) []string {
	var refs []string
	for _, action := range templateActionRe.FindAllStringSubmatch(task, -1) {
		for _, re := range []*regexp.Regexp{stepFieldRefRe, stepIndexRefRe} {
			for _, m := range re.FindAllStringSubmatch(action[1], -1) {
				if !slices.Contains(refs, m[1]) {
					refs = append(refs, m[1])
				}
			}
		}
	}
	return refs
}

// validateStepOutputRefs checks that every step of a generated chain reads
// only the output of steps it depends on (directly or transitively) in the
// same chain. Names are compared after sanitizeStepName, as applyPlan does.
func validateStepOutputRefs(chains []PlannerChain) error {
	owner := make(map[string]string) // sanitized step name -> first chain that has it
	for _, c := range chains {
		for _, s := range c.Steps {
			if _, ok := owner[sanitizeStepName(s.Name)]; !ok {
				owner[sanitizeStepName(s.Name)] = c.Name
			}
		}
	}

	for _, c := range chains {
		deps := make(map[string][]string, len(c.Steps))
		for _, s := range c.Steps {
			names := make([]string, len(s.DependsOn))
			for i, d := range s.DependsOn {
				names[i] = sanitizeStepName(d)
			}
			deps[sanitizeStepName(s.Name)] = names
		}

		for _, s := range c.Steps {
			self := sanitizeStepName(s.Name)
			for _, raw := range stepOutputRefs(s.Task) {
				ref := sanitizeStepName(raw)
				if err := checkStepOutputRef(c.Name, s.Name, self, raw, ref, deps, owner); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func checkStepOutputRef(chain, step, self, raw, ref string, deps map[string][]string, owner map[string]string) error {
	prefix := fmt.Sprintf("chain %q step %q: task reads the output of step %q", chain, step, raw)
	switch {
	case ref == self:
		return fmt.Errorf("%s, its own output", prefix)
	case hasStep(deps, ref):
		if !dependsOnTransitively(deps, self, ref) {
			return fmt.Errorf("%s, which is not in its dependsOn; add %q to dependsOn so it runs first", prefix, raw)
		}
		return nil
	case owner[ref] != "":
		return fmt.Errorf("%s in chain %q; output passes only between steps of the same chain, so put both steps in one chain and add %q to dependsOn",
			prefix, owner[ref], raw)
	default:
		return fmt.Errorf("%s, which no chain in the plan has", prefix)
	}
}

func hasStep(deps map[string][]string, name string) bool {
	_, ok := deps[name]
	return ok
}

// dependsOnTransitively reports whether step from reaches step to through
// dependsOn edges. The DAG is validated before this runs, so it terminates.
func dependsOnTransitively(deps map[string][]string, from, to string) bool {
	seen := map[string]bool{}
	stack := slices.Clone(deps[from])
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if n == to {
			return true
		}
		if !seen[n] {
			seen[n] = true
			stack = append(stack, deps[n]...)
		}
	}
	return false
}
