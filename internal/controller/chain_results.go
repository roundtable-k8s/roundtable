/*
Copyright 2026 dapperdivers.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// defaultMaxQueueWait bounds how long a task may sit unclaimed by its knight.
const defaultMaxQueueWait = 1800 * time.Second

func maxQueueWait(chain *aiv1alpha1.Chain) time.Duration {
	if chain.Spec.MaxQueueWaitSeconds > 0 {
		return time.Duration(chain.Spec.MaxQueueWaitSeconds) * time.Second
	}
	return defaultMaxQueueWait
}

// detectPickup records ss.PickedUpAt once the knight has picked the task up.
//
// Pickup signal: knights consume the tasks stream through a durable
// "<Knight>-consumer" and keep each message un-acked while they work on it
// (in-progress acks), acking only after the result is published. The ack floor
// is therefore a completion signal; the consumer's delivered stream sequence
// reaching the task's sequence is the pickup signal (a pull consumer on a
// WorkQueue stream delivers in stream order). No knight runtime change is
// needed. A step without a recorded sequence (published by an older operator)
// falls back to the legacy behaviour: the clock starts at publish time.
func (r *ChainReconciler) detectPickup(ctx context.Context, nc natsConfig, ss *aiv1alpha1.ChainStepStatus, spec *aiv1alpha1.ChainStep) {
	if ss.PickedUpAt != nil || ss.StartedAt == nil {
		return
	}
	if ss.TaskSeq == 0 || spec == nil {
		ss.PickedUpAt = ss.StartedAt
		return
	}
	// The task may be re-running on the fallback knight (#321); track the
	// knight it was actually published to.
	knight := spec.KnightRef.Name
	if ss.Knight != "" {
		knight = ss.Knight
	}
	if r.taskDelivered(ctx, nc, knight, ss.TaskSeq) {
		now := metav1.Now()
		ss.PickedUpAt = &now
		logf.FromContext(ctx).Info("Knight picked up step task", "step", ss.Name, "queuedFor", now.Sub(ss.StartedAt.Time).Round(time.Second).String())
	}
}

// taskDelivered reports whether the knight's task consumer has been delivered
// the tasks-stream message with sequence seq (i.e. picked the task up).
func (r *ChainReconciler) taskDelivered(ctx context.Context, nc natsConfig, knightRef string, seq uint64) bool {
	natsCli, err := r.natsClient()
	if err != nil {
		return false
	}
	delivered, err := natsCli.ConsumerDeliveredSeq(ctx, nc.TasksStream, natspkg.KnightTaskConsumerName(knightRef))
	if err != nil {
		logf.FromContext(ctx).V(1).Info("Could not read knight consumer state", "knight", knightRef, "error", err.Error())
		return false
	}
	return delivered >= seq
}

// pollResult checks for a result message for a given chain step.
// If taskID is provided, polls for that exact result subject; otherwise falls back to wildcard.
func (r *ChainReconciler) pollResult(ctx context.Context, nc natsConfig, chainName, stepName, taskID string) (*natspkg.TaskResult, error) {
	log := logf.FromContext(ctx)

	natsCli, err := r.natsClient()
	if err != nil {
		return nil, err
	}

	// Use exact taskID subject when available (prevents stale result replay)
	var subject string
	if taskID != "" {
		subject = natspkg.ResultSubject(nc.SubjectPrefix, taskID)
	} else {
		taskPrefix := fmt.Sprintf("chain-%s-%s", chainName, stepName)
		subject = natspkg.ResultSubjectWildcard(nc.SubjectPrefix, taskPrefix)
	}

	// Durable consumer with explicit ack (compatible with both Limits and
	// WorkQueue retention), deleted again after the poll. FetchOne acks the
	// result (required for WorkQueue retention).
	consumerName := natspkg.ChainConsumerName(chainName, stepName)

	msg, err := natsCli.FetchOne(ctx, nc.ResultsStream,
		natspkg.ResultConsumerConfig(consumerName, subject), 2*time.Second)

	// Clean up the consumer
	defer func() {
		_ = natsCli.DeleteConsumer(ctx, nc.ResultsStream, consumerName)
	}()

	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil // Timeout, no result yet
	}

	_, consumeSpan := natspkg.StartConsumeSpan(ctx, msg, natspkg.WithTable(nc.Table), natspkg.WithTaskID(taskID))
	consumeSpan.End()

	log.Info("Received result message", "subject", msg.Subject(), "dataLen", len(msg.Data()))

	var result natspkg.TaskResult
	if err := json.Unmarshal(msg.Data(), &result); err != nil {
		log.Error(err, "Failed to unmarshal result", "subject", msg.Subject(), "rawPreview", string(msg.Data()[:min(200, len(msg.Data()))]))
		return nil, fmt.Errorf("unmarshal result: %w", err)
	}

	log.Info("Parsed result", "taskId", result.GetTaskID(), "outputLen", len(result.GetOutput()), "error", result.GetError())

	return &result, nil
}
