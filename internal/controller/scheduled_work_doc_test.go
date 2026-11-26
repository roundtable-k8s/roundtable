package controller

import (
	"os"
	"regexp"
	"testing"

	aiv1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"sigs.k8s.io/yaml"
)

// TestScheduledWorkDocTriggers strictly decodes every Trigger YAML block in
// docs/scheduled-work.md and docs/migrating-from-kagent.md (unknown fields
// fail), and checks each is a schedule with a cron that parses.
func TestScheduledWorkDocTriggers(t *testing.T) {
	block := regexp.MustCompile("(?s)```yaml\n(.*?)```")
	for _, f := range []string{"../../docs/scheduled-work.md", "../../docs/migrating-from-kagent.md"} {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, m := range block.FindAllSubmatch(data, -1) {
			if !regexp.MustCompile(`(?m)^kind: Trigger$`).Match(m[1]) {
				continue
			}
			var trg aiv1.Trigger
			if err := yaml.UnmarshalStrict(m[1], &trg); err != nil {
				t.Errorf("%s: %v", f, err)
				continue
			}
			n++
			src := trg.Spec.Source.Schedule
			if src == nil {
				t.Errorf("%s: Trigger %s has no source.schedule", f, trg.Name)
				continue
			}
			if _, err := parseCron(src.Cron, src.TimeZone, "source.schedule.timeZone"); err != nil {
				t.Errorf("%s: Trigger %s: %v", f, trg.Name, err)
			}
		}
		if n == 0 {
			t.Errorf("%s: no Trigger blocks found", f)
		}
	}
}
