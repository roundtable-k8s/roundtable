package mission

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

const (
	// cleanupPolicyDelete is the Mission.Spec.CleanupPolicy value that deletes resources.
	cleanupPolicyDelete = "Delete"
	// resultsKVBucket is the NATS KV bucket holding retained mission results.
	resultsKVBucket = natspkg.ResultsBucket
)

// Cleaner owns mission teardown (ephemeral knights, NATS streams/consumers,
// ephemeral RoundTable) and retained-results storage in NATS KV.
type Cleaner struct {
	Client client.Client
	NATS   *natspkg.Provider
}

// natsClient returns the NATS client from the provider.
func (c *Cleaner) natsClient() (natspkg.Client, error) {
	if c.NATS == nil {
		return nil, fmt.Errorf("NATS provider not configured")
	}
	return c.NATS.Client()
}

// ShouldDeleteResources determines if resources should be deleted based on cleanup policy and mission outcome.
func (c *Cleaner) ShouldDeleteResources(mission *aiv1alpha1.Mission) bool {
	switch mission.Spec.CleanupPolicy {
	case cleanupPolicyDelete:
		return true
	case "Retain":
		return false
	case "OnSuccess":
		return TerminalOutcome(mission) == aiv1alpha1.MissionPhaseSucceeded
	case "OnFailure":
		return TerminalOutcome(mission) == aiv1alpha1.MissionPhaseFailed
	default:
		return true // Default to Delete
	}
}

// DeleteEphemeralKnights deletes all ephemeral Knight CRs owned by this mission.
func (c *Cleaner) DeleteEphemeralKnights(ctx context.Context, mission *aiv1alpha1.Mission) error {
	for _, ks := range mission.Status.KnightStatuses {
		if !ks.Ephemeral {
			continue
		}

		knightName := fmt.Sprintf("%s-%s", mission.Name, ks.Name)
		knight := &aiv1alpha1.Knight{}
		if err := c.Client.Get(ctx, types.NamespacedName{
			Name:      knightName,
			Namespace: mission.Namespace,
		}, knight); err != nil {
			if client.IgnoreNotFound(err) == nil {
				continue // Already deleted
			}
			return err
		}

		if err := c.Client.Delete(ctx, knight); err != nil {
			return fmt.Errorf("failed to delete knight %s: %w", knightName, err)
		}
	}
	return nil
}

// DeleteNATSConsumers deletes all NATS consumers for this mission's streams (best effort).
//
//nolint:unparam // error return kept so failures can propagate without a caller change
func (c *Cleaner) DeleteNATSConsumers(ctx context.Context, mission *aiv1alpha1.Mission) error {
	natsCli, err := c.natsClient()
	if err != nil {
		return nil // Gracefully skip if no NATS client
	}

	tasksStream := mission.Status.NATSTasksStream
	resultsStream := mission.Status.NATSResultsStream

	for _, ks := range mission.Status.KnightStatuses {
		if !ks.Ephemeral {
			continue
		}

		consumerName := fmt.Sprintf("msn-%s-%s", mission.Name, ks.Name)

		// Delete from tasks stream (best effort)
		if tasksStream != "" {
			_ = natsCli.DeleteConsumer(ctx, tasksStream, consumerName)
		}

		// Delete from results stream (best effort)
		if resultsStream != "" {
			_ = natsCli.DeleteConsumer(ctx, resultsStream, consumerName)
		}
	}

	return nil
}

// DeleteNATSStreams deletes the mission's task and result streams. A stream
// that does not exist (already deleted) is not an error.
func (c *Cleaner) DeleteNATSStreams(ctx context.Context, mission *aiv1alpha1.Mission) error {
	natsCli, err := c.natsClient()
	if err != nil {
		return nil // Gracefully skip if no NATS client
	}

	// Delete tasks stream
	if mission.Status.NATSTasksStream != "" {
		if err := natsCli.DeleteStream(ctx, mission.Status.NATSTasksStream); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
			return fmt.Errorf("failed to delete tasks stream: %w", err)
		}
	}

	// Delete results stream
	if mission.Status.NATSResultsStream != "" {
		if err := natsCli.DeleteStream(ctx, mission.Status.NATSResultsStream); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
			return fmt.Errorf("failed to delete results stream: %w", err)
		}
	}

	return nil
}

// DeleteEphemeralRoundTable deletes the mission's ephemeral RoundTable CR.
func (c *Cleaner) DeleteEphemeralRoundTable(ctx context.Context, mission *aiv1alpha1.Mission) error {
	if mission.Status.RoundTableName == "" {
		return nil // No RoundTable to delete
	}

	rt := &aiv1alpha1.RoundTable{}
	if err := c.Client.Get(ctx, types.NamespacedName{
		Name:      mission.Status.RoundTableName,
		Namespace: mission.Namespace,
	}, rt); err != nil {
		if client.IgnoreNotFound(err) == nil {
			return nil // Already deleted
		}
		return err
	}

	return c.Client.Delete(ctx, rt)
}

// SetResultsRef records where the retained results are: status.resultsRef and
// its deprecated "<bucket>.<key>" alias status.resultsConfigMap.
func SetResultsRef(mission *aiv1alpha1.Mission, bucket, key string) {
	mission.Status.ResultsRef = &aiv1alpha1.MissionResultsRef{Bucket: bucket, Key: key}
	mission.Status.ResultsConfigMap = fmt.Sprintf("%s.%s", bucket, key)
}

// ResultsKey returns the KV key a mission's results are stored under.
func ResultsKey(mission *aiv1alpha1.Mission) string { return mission.Name }

// ResultsBucketName returns the KV bucket retained results are stored in.
func ResultsBucketName() string { return resultsKVBucket }

// retainedSkill is a generated skill in the retained results document.
type retainedSkill struct {
	Name    string `json:"name"`
	Knight  string `json:"knight"`
	Content string `json:"content"`
}

// generatedChains returns the full specs of the chains the planner generated.
func generatedChains(mission *aiv1alpha1.Mission) []aiv1alpha1.GeneratedChain {
	if len(mission.Spec.GeneratedChains) == 0 {
		return []aiv1alpha1.GeneratedChain{}
	}
	return mission.Spec.GeneratedChains
}

// generatedSkills returns the skills generated for the mission's knights.
func generatedSkills(mission *aiv1alpha1.Mission) []retainedSkill {
	out := []retainedSkill{}
	for _, mk := range mission.Spec.GeneratedKnights {
		if mk.EphemeralSpec == nil {
			continue
		}
		for _, sk := range mk.EphemeralSpec.GeneratedSkills {
			out = append(out, retainedSkill{Name: sk.Name, Knight: mk.Name, Content: sk.Content})
		}
	}
	return out
}

// StoreResultsToKV stores mission results in a NATS KV bucket for retention.
// Bucket: "mission-results", Key: mission name, Value: JSON with all results.
// KV Put is idempotent — no "already exists" problem like ConfigMaps.
func (c *Cleaner) StoreResultsToKV(ctx context.Context, mission *aiv1alpha1.Mission) error {
	log := logf.FromContext(ctx)

	natsCli, err := c.natsClient()
	if err != nil || !natsCli.IsConnected() {
		log.Info("NATS not available, skipping KV results storage")
		return fmt.Errorf("NATS client not available: %w", err)
	}

	kvKey := ResultsKey(mission)
	const kvBucket = resultsKVBucket

	// Build duration string
	duration := ""
	if mission.Status.StartedAt != nil && mission.Status.CompletedAt != nil {
		dur := mission.Status.CompletedAt.Sub(mission.Status.StartedAt.Time)
		duration = fmt.Sprintf("%.0fm%.0fs", dur.Minutes(), dur.Seconds()-dur.Minutes()*60)
	}

	// Build complete results document
	results := map[string]any{
		"summary": map[string]any{
			"mission":   mission.Name,
			"objective": mission.Spec.Objective,
			"phase":     string(mission.Status.Phase),
			"result":    mission.Status.Result,
			"duration":  duration,
			"totalCost": mission.Status.TotalCost,
		},
		"timeline": map[string]any{
			"started":   "",
			"completed": "",
			"phases":    []map[string]string{},
		},
		"chains":  map[string]any{},
		"knights": []map[string]any{},
		// What the planner generated, so it can be copied out and reused.
		"generatedChains": generatedChains(mission),
		"generatedSkills": generatedSkills(mission),
	}

	// Timeline
	timeline := results["timeline"].(map[string]any)
	if mission.Status.StartedAt != nil {
		timeline["started"] = mission.Status.StartedAt.Format(time.RFC3339)
	}
	if mission.Status.CompletedAt != nil {
		timeline["completed"] = mission.Status.CompletedAt.Format(time.RFC3339)
	}
	for _, cond := range mission.Status.Conditions {
		timeline["phases"] = append(timeline["phases"].([]map[string]string), map[string]string{
			"type":      cond.Type,
			"status":    string(cond.Status),
			"reason":    cond.Reason,
			"timestamp": cond.LastTransitionTime.Format(time.RFC3339),
		})
	}

	// Chain results
	chains := results["chains"].(map[string]any)
	for _, cs := range mission.Status.ChainStatuses {
		chain := &aiv1alpha1.Chain{}
		if err := c.Client.Get(ctx, types.NamespacedName{
			Name:      cs.ChainCRName,
			Namespace: mission.Namespace,
		}, chain); err != nil {
			log.Info("Failed to fetch chain for results", "chain", cs.ChainCRName, "error", err.Error())
			continue
		}

		chainResult := map[string]any{
			"name":   cs.Name,
			"phase":  string(cs.Phase),
			"steps":  []map[string]any{},
			"output": "",
		}
		for _, stepStatus := range chain.Status.StepStatuses {
			chainResult["steps"] = append(chainResult["steps"].([]map[string]any), map[string]any{
				"name":   stepStatus.Name,
				"output": stepStatus.Output,
				"error":  stepStatus.Error,
			})
			if stepStatus.Output != "" {
				chainResult["output"] = stepStatus.Output
			}
		}
		chains[cs.Name] = chainResult
	}

	// Knight statuses
	for _, ks := range mission.Status.KnightStatuses {
		results["knights"] = append(results["knights"].([]map[string]any), map[string]any{
			"name":      ks.Name,
			"ready":     ks.Ready,
			"ephemeral": ks.Ephemeral,
		})
	}

	// Cost breakdown
	if len(mission.Status.CostBreakdown) > 0 {
		results["costs"] = map[string]any{
			"breakdown": mission.Status.CostBreakdown,
			"total":     mission.Status.TotalCost,
		}
	}

	// Marshal and store — KVPut is idempotent (upsert)
	resultsJSON, err := json.Marshal(results)
	if err != nil {
		return fmt.Errorf("failed to marshal results: %w", err)
	}

	if err := natsCli.KVPut(ctx, kvBucket, kvKey, resultsJSON); err != nil {
		return fmt.Errorf("failed to store results in NATS KV: %w", err)
	}

	SetResultsRef(mission, kvBucket, kvKey)
	log.Info("Stored mission results to NATS KV", "bucket", kvBucket, "key", kvKey,
		"chains", len(mission.Status.ChainStatuses), "bytes", len(resultsJSON))
	return nil
}
