package mission

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// Briefer publishes mission briefings to knights over NATS.
type Briefer struct {
	Client   client.Client
	Recorder record.EventRecorder
	NATS     *natspkg.Provider

	// TracerProvider makes the briefing spans; nil uses the global one.
	TracerProvider trace.TracerProvider
}

// natsClient returns the shared NATS client, or an error if the provider is not configured.
func (b *Briefer) natsClient() (natspkg.Client, error) {
	if b.NATS == nil {
		return nil, fmt.Errorf("NATS provider not configured")
	}
	return b.NATS.Client()
}

// PublishBriefing delivers the mission briefing to each named knight's task subject.
//
// There is deliberately no broadcast publish to "<prefix>.briefing": no JetStream
// stream covers that subject on any provisioning path (RoundTable streams only
// capture "<prefix>.tasks.>" and "<prefix>.results.>") and nothing subscribes to
// it, so a JetStream publish there can never be acked. It wedged every briefed
// mission at BriefingPublished=False/PublishFailed.
func (b *Briefer) PublishBriefing(ctx context.Context, mission *aiv1alpha1.Mission) error {
	log := logf.FromContext(ctx)

	natsCli, err := b.natsClient()
	if err != nil {
		return err
	}

	// Fallback subject prefix for knights whose own subjects can't be parsed:
	// prefer the referenced RoundTable's prefix (covered by its tasks stream)
	// over the mission-scoped prefix, which only exists for ephemeral tables.
	fallbackPrefix := natsPrefix(mission)
	if aiv1alpha1.RefName(mission.Spec.RoundTableRef) != "" {
		rt := &aiv1alpha1.RoundTable{}
		if err := b.Client.Get(ctx, types.NamespacedName{
			Name:      aiv1alpha1.RefName(mission.Spec.RoundTableRef),
			Namespace: mission.Namespace,
		}, rt); err == nil && rt.Spec.NATS.SubjectPrefix != "" {
			fallbackPrefix = rt.Spec.NATS.SubjectPrefix
		}
	}

	attempted := 0
	published := 0
	for _, mk := range mission.Spec.Knights {
		if mk.Ephemeral {
			continue
		}
		attempted++

		knight := &aiv1alpha1.Knight{}
		if err := b.Client.Get(ctx, types.NamespacedName{
			Name:      mk.Name,
			Namespace: mission.Namespace,
		}, knight); err != nil {
			log.Error(err, "Failed to get knight for briefing", "knight", mk.Name)
			continue
		}

		taskPayload := natspkg.TaskPayload{
			// Generation-based TaskID so a retried publish carries the same ID
			// (same idempotency pattern as the planner's dispatchPlanningTask).
			TaskID:    fmt.Sprintf("mission-%s-briefing-%s-gen%d", mission.Name, mk.Name, mission.Generation),
			ChainName: fmt.Sprintf("mission-%s", mission.Name),
			StepName:  "briefing",
			Task:      fmt.Sprintf("[Mission: %s]\nObjective: %s\n\n%s", mission.Name, mission.Spec.Objective, mission.Spec.Briefing),
		}

		// Derive subject prefix from the knight's NATS config
		briefingPrefix := fallbackPrefix
		if len(knight.Spec.NATS.Subjects) > 0 {
			parts := strings.SplitN(knight.Spec.NATS.Subjects[0], ".tasks.", 2)
			if len(parts) == 2 {
				briefingPrefix = parts[0]
			}
		}
		taskSubject := natspkg.TaskSubject(briefingPrefix, knight.Spec.Domain, mk.Name)
		// Each briefing task is a span under the mission, and the knight's
		// task a child of it.
		span := BriefingSpanKey(mk.Name)
		started := time.Now()
		if err := natsCli.PublishJSONCtx(WithSpan(ctx, mission, span), taskSubject, taskPayload, natspkg.WithTable(aiv1alpha1.RefName(mission.Spec.RoundTableRef))); err != nil {
			log.Error(err, "Failed to publish briefing to knight", "knight", mk.Name, "subject", taskSubject)
			continue
		}
		published++
		EmitSpan(ctx, b.TracerProvider, mission, span, "briefing "+mk.Name, SpanEnd{
			Start: started, End: time.Now(),
			Attrs: []attribute.KeyValue{KnightAttr(mk.Name), TaskAttr(taskPayload.TaskID)},
		})
	}

	if attempted > 0 && published == 0 {
		return fmt.Errorf("briefing not delivered to any of %d knights", attempted)
	}
	if published < attempted {
		b.Recorder.Eventf(mission, corev1.EventTypeWarning, "BriefingPartialDelivery",
			"Briefing delivered to %d of %d knights", published, attempted)
	}
	return nil
}
