package mission

import (
	"context"
	"testing"

	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func newURLAssembler(t *testing.T, objs ...*aiv1alpha1.RoundTable) *KnightAssembler {
	t.Helper()
	s := k8sruntime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	b := fake.NewClientBuilder().WithScheme(s)
	for _, o := range objs {
		b = b.WithObjects(o)
	}
	return &KnightAssembler{Client: b.Build()}
}

func TestDefaultMissionNATSURL(t *testing.T) {
	t.Setenv(EnvDefaultNATSURL, "")
	if got := DefaultMissionNATSURL(); got != builtinDefaultNATSURL {
		t.Errorf("unset: got %q, want %q", got, builtinDefaultNATSURL)
	}
	t.Setenv(EnvDefaultNATSURL, "nats://nats.messaging.svc:4222")
	if got := DefaultMissionNATSURL(); got != "nats://nats.messaging.svc:4222" {
		t.Errorf("set: got %q", got)
	}
}

func TestResolveMissionNATSURLFallbackAndOverrides(t *testing.T) {
	const envURL = "nats://nats.messaging.svc:4222"
	t.Setenv(EnvDefaultNATSURL, envURL)
	mission, _, parent := ephemeralFixtures()
	a := newURLAssembler(t, parent)
	ctx := context.Background()

	noRef := mission.DeepCopy()
	noRef.Spec.RoundTableRef = nil
	if got := a.resolveMissionNATSURL(ctx, noRef); got != envURL {
		t.Errorf("no parent: got %q, want env default", got)
	}
	rt := a.BuildEphemeralRoundTable(ctx, noRef, "msn", "msn", "t", "r")
	if rt.Spec.NATS.URL != envURL {
		t.Errorf("ephemeral RT url = %q, want env default", rt.Spec.NATS.URL)
	}

	if got := a.resolveMissionNATSURL(ctx, mission); got != parent.Spec.NATS.URL {
		t.Errorf("parent: got %q, want %q", got, parent.Spec.NATS.URL)
	}

	mission.Spec.RoundTableTemplate = &aiv1alpha1.MissionRoundTableTemplate{NATSURL: "nats://tmpl:4222"}
	if got := a.resolveMissionNATSURL(ctx, mission); got != "nats://tmpl:4222" {
		t.Errorf("template: got %q", got)
	}
}
