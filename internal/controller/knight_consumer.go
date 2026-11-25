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
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	rtmetrics "github.com/roundtable-k8s/roundtable/pkg/metrics"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

const (
	// consumerOpTimeout bounds one consumer create/update/delete call.
	consumerOpTimeout = 10 * time.Second

	// ConsumerDeleteGiveUp is how long after a Knight's deletion the
	// finalizer keeps retrying the consumer delete (e.g. through a NATS
	// outage) before it gives up, emits a Warning event and lets the Knight go.
	ConsumerDeleteGiveUp = 5 * time.Minute

	// consumerDeleteRetry is how often a failed consumer delete is retried.
	consumerDeleteRetry = 10 * time.Second
)

// KnightConsumerClient is what the Knight controller needs from NATS to own
// each knight's durable task consumer.
type KnightConsumerClient interface {
	EnsureConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (natspkg.ConsumerAction, error)
	RemoveConsumer(ctx context.Context, stream, consumer string) error
}

// consumerClient returns the client the knight consumer is managed with:
// Consumers if set (tests), else the shared NATS client. (nil, nil) means the
// operator runs without NATS and leaves consumers alone.
func (r *KnightReconciler) consumerClient() (KnightConsumerClient, error) {
	if r.Consumers != nil {
		return r.Consumers, nil
	}
	if r.NATS == nil {
		return nil, nil
	}
	cli, err := r.NATS.Client()
	if err != nil {
		return nil, err
	}
	return cli, nil
}

// reconcileConsumer creates, updates or adopts the knight's durable task
// consumer (natspkg.KnightTaskConsumerConfig), sets the ConsumerReady
// condition and gauge, and reports whether the condition changed. A
// consumer that already matches (e.g. one a runtime created) is left as it
// is. The error is non-nil when the consumer is not ready; the caller
// retries.
func (r *KnightReconciler) reconcileConsumer(ctx context.Context, k *aiv1alpha1.Knight) (bool, error) {
	cli, err := r.consumerClient()
	if cli == nil && err == nil {
		return false, nil
	}
	name := knightpkg.TaskConsumerName(k)
	stream := k.Spec.NATS.Stream
	if err != nil {
		return r.setConsumerNotReady(k, aiv1alpha1.ReasonConsumerNATSUnavailable,
			fmt.Sprintf("Cannot reach NATS to reconcile consumer %s: %v", name, err), err)
	}

	cfg := natspkg.KnightTaskConsumerConfig(k.Name, k.Spec.NATS.Subjects)
	if stream == "" || len(cfg.FilterSubjects) == 0 {
		err := errors.New("spec.nats.stream and spec.nats.subjects must be set")
		return r.setConsumerNotReady(k, aiv1alpha1.ReasonConsumerFailed,
			fmt.Sprintf("Consumer %s not reconciled: %v", name, err), err)
	}

	opCtx, cancel := context.WithTimeout(ctx, consumerOpTimeout)
	defer cancel()
	action, err := cli.EnsureConsumer(opCtx, stream, cfg)
	if err != nil {
		reason := aiv1alpha1.ReasonConsumerFailed
		switch {
		case errors.Is(err, jetstream.ErrStreamNotFound):
			reason = aiv1alpha1.ReasonConsumerStreamNotFound
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, nats.ErrTimeout),
			errors.Is(err, nats.ErrNoResponders), errors.Is(err, nats.ErrConnectionClosed),
			errors.Is(err, nats.ErrNoServers), errors.Is(err, jetstream.ErrJetStreamNotEnabled):
			reason = aiv1alpha1.ReasonConsumerNATSUnavailable
		}
		return r.setConsumerNotReady(k, reason,
			fmt.Sprintf("Consumer %s on stream %s: %v", name, stream, err), err)
	}

	switch action {
	case natspkg.ConsumerCreated, natspkg.ConsumerUpdated:
		r.Recorder.Eventf(k, corev1.EventTypeNormal, "Consumer"+string(action),
			"%s consumer %s on stream %s", action, name, stream)
	case natspkg.ConsumerRecreated:
		r.Recorder.Eventf(k, corev1.EventTypeWarning, "ConsumerRecreated",
			"Recreated consumer %s on stream %s: a field NATS cannot update differed", name, stream)
	}
	setConsumerMetric(k, true)
	return meta.SetStatusCondition(&k.Status.Conditions, metav1.Condition{
		Type:   aiv1alpha1.ConditionKnightConsumerReady,
		Status: metav1.ConditionTrue,
		Reason: aiv1alpha1.ReasonConsumerReady,
		Message: fmt.Sprintf("Consumer %s on stream %s has the expected config (filters %v)",
			name, stream, cfg.FilterSubjects),
		ObservedGeneration: k.Generation,
	}), nil
}

// setConsumerNotReady sets ConsumerReady=False and the gauge to 0, and
// returns whether the condition changed along with err.
func (r *KnightReconciler) setConsumerNotReady(k *aiv1alpha1.Knight, reason, msg string, err error) (bool, error) {
	setConsumerMetric(k, false)
	return meta.SetStatusCondition(&k.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightConsumerReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: k.Generation,
	}), err
}

// finalizeConsumer deletes the knight's task consumer when the Knight is
// deleted. It reports done once the consumer is gone (or was never managed),
// or once ConsumerDeleteGiveUp has passed since the deletion started: a NATS
// outage must not block the Knight's deletion forever. Giving up emits a
// Warning event naming the consumer to delete by hand. Otherwise it returns
// the delay to retry after.
func (r *KnightReconciler) finalizeConsumer(ctx context.Context, k *aiv1alpha1.Knight) (bool, time.Duration) {
	cli, err := r.consumerClient()
	if cli == nil && err == nil {
		return true, 0
	}
	name := knightpkg.TaskConsumerName(k)
	stream := k.Spec.NATS.Stream
	if stream == "" {
		return true, 0
	}
	if err == nil {
		opCtx, cancel := context.WithTimeout(ctx, consumerOpTimeout)
		err = cli.RemoveConsumer(opCtx, stream, name)
		cancel()
	}
	if err == nil {
		logf.FromContext(ctx).Info("Deleted knight consumer", "consumer", name, "stream", stream)
		return true, 0
	}

	var since time.Duration
	if k.DeletionTimestamp != nil {
		since = r.now().Sub(k.DeletionTimestamp.Time)
	}
	if since >= ConsumerDeleteGiveUp {
		r.Recorder.Eventf(k, corev1.EventTypeWarning, "ConsumerDeleteAbandoned",
			"Could not delete consumer %s on stream %s for %s; giving up so the Knight can be deleted. "+
				"Delete it by hand (nats consumer rm %s %s): %v",
			name, stream, ConsumerDeleteGiveUp, stream, name, err)
		return true, 0
	}
	logf.FromContext(ctx).Info("Could not delete knight consumer; retrying",
		"consumer", name, "stream", stream, "error", err.Error(), "givesUpIn", ConsumerDeleteGiveUp-since)
	return false, consumerDeleteRetry
}

// consumerMetricLabels selects the knight's series of the consumer gauge.
func consumerMetricLabels(k *aiv1alpha1.Knight) prometheus.Labels {
	return prometheus.Labels{"table": knightpkg.KnightTable(k), "knight": k.Name}
}

// setConsumerMetric sets the knight's consumer gauge to 1 (ready) or 0.
func setConsumerMetric(k *aiv1alpha1.Knight, ready bool) {
	v := 0.0
	if ready {
		v = 1
	}
	rtmetrics.KnightConsumerReady.With(consumerMetricLabels(k)).Set(v)
}

// deleteConsumerMetric drops the knight's consumer gauge series.
func deleteConsumerMetric(k *aiv1alpha1.Knight) {
	rtmetrics.KnightConsumerReady.DeletePartialMatch(consumerMetricLabels(k))
}

// retryConsumer makes a reconcile result come back within RequeueSlow while
// the consumer is not ready (consumerErr != nil), for the reconcile paths
// that return early.
func retryConsumer(consumerErr error) func(ctrl.Result, error) (ctrl.Result, error) {
	return func(res ctrl.Result, err error) (ctrl.Result, error) {
		if consumerErr != nil && err == nil && (res.RequeueAfter == 0 || res.RequeueAfter > RequeueSlow) {
			res.RequeueAfter = RequeueSlow
		}
		return res, err
	}
}
