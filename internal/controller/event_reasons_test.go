package controller

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// TestEventReasonMarkers asserts that each reasons marker comment (read by
// scripts/check-events-doc.sh) names exactly the reasons its variable-reason
// Warning path can emit, taken from the Reason* constants. A new reason on a
// marked path must be added both here and in the marker, so the marker cannot
// silently omit one.
func TestEventReasonMarkers(t *testing.T) {
	want := map[string][][]string{
		"knight_controller.go": {
			{aiv1alpha1.ReasonRuntimeClassNotFound, aiv1alpha1.ReasonSandboxUnavailable},
			{eventReasonMaxDeliverIgnored},
		},
		"knight_egress.go": {
			{aiv1alpha1.ReasonEgressServiceNotFound},
			{eventReasonInClusterHost},
			{eventReasonHostNetworkPortUnresolved},
		},
		"knight_memory.go":  {{aiv1alpha1.ReasonMemoryNotRWX}},
		"knight_scaling.go": {{aiv1alpha1.ReasonAutoscalingUnsupported, aiv1alpha1.ReasonKEDAUnavailable}},
		"roundtable_controller.go": {
			{aiv1alpha1.ReasonRoundTableDeprecatedFieldsUsed},
			{aiv1alpha1.ReasonPlannerKnightMissing, aiv1alpha1.ReasonPlannerKnightNotInTable},
		},
	}
	// The marker text is split so the guard script does not read this file as one.
	re := regexp.MustCompile(`// event-` + `reasons:(.*)`)
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		name := f.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		var got [][]string
		for _, m := range re.FindAllStringSubmatch(string(src), -1) {
			r := strings.Fields(m[1])
			sort.Strings(r)
			got = append(got, r)
		}
		exp := want[name]
		if len(got) != len(exp) {
			t.Errorf("%s: %d markers, want %d", name, len(got), len(exp))
			continue
		}
		for i := range exp {
			e := append([]string(nil), exp[i]...)
			sort.Strings(e)
			if strings.Join(got[i], " ") != strings.Join(e, " ") {
				t.Errorf("%s marker %d: got %v, want %v", name, i, got[i], e)
			}
		}
	}
}
