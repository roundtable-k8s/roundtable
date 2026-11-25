package controller

import (
	"os"
	"testing"

	aiv1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"sigs.k8s.io/yaml"
)

// TestBacklogGroomingWhenCompiles checks the recipe's `when` is valid CEL.
func TestBacklogGroomingWhenCompiles(t *testing.T) {
	data, err := os.ReadFile("../../examples/recipes/backlog-grooming/chain.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var c aiv1.Chain
	if err := yaml.UnmarshalStrict(data, &c); err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, s := range c.Spec.Steps {
		if s.When == "" {
			continue
		}
		n++
		if _, err := compileCondition(s.When); err != nil {
			t.Errorf("step %s: %v", s.Name, err)
		}
	}
	if n == 0 {
		t.Error("no when conditions found")
	}
}
