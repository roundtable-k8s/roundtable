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
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/chainrun"
	"github.com/roundtable-k8s/roundtable/internal/deadletter"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

const (
	// triggerRequeueError is how soon a Trigger that is not Ready is retried.
	triggerRequeueError = 30 * time.Second
	// triggerNakDelay is how long a NATS event that could not be handled
	// (API error, missing target) waits before it is redelivered.
	triggerNakDelay = 15 * time.Second
	// triggerMaxCatchUpSteps bounds the walk over missed schedule times.
	triggerMaxCatchUpSteps = 100000
	// triggerKeyAnnotationMax bounds the dedupe key stored on a run.
	triggerKeyAnnotationMax = 1024
)

// TriggerReconciler runs Triggers: for a NATS source it keeps one durable
// consumer on the RoundTable's events stream and a fetch loop that starts a
// run per message; for a schedule source it fires at each cron time. Runs are
// copies of the target Chain started through chainrun.Start.
//
// It only runs in the elected leader (controllers need leader election by
// default), so fetch loops, which are started from Reconcile, do too.
type TriggerReconciler struct {
	client.Client
	// APIReader reads runs uncached for dedupe and concurrency checks, so a
	// run created a moment ago is never missed. nil uses Client.
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Recorder  record.EventRecorder
	NATS      *natspkg.Provider

	// Now returns the current time; nil means time.Now (tests override it).
	Now func() time.Time

	initOnce  sync.Once
	consumers *triggerConsumers
	fireLocks sync.Map // namespace/name -> *sync.Mutex
}

// +kubebuilder:rbac:groups=ai.roundtable.io,resources=triggers,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=triggers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=triggers/finalizers,verbs=update
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=chains,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=roundtables,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// Reconcile keeps a Trigger's consumer or schedule in line with its spec.
func (r *TriggerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.init()
	trg := &aiv1alpha1.Trigger{}
	if err := r.Get(ctx, req.NamespacedName, trg); err != nil {
		if apierrors.IsNotFound(err) {
			r.consumers.stop(req.String())
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !trg.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.finalize(ctx, trg)
	}
	if trg.Spec.Source.NATS != nil && !controllerutil.ContainsFinalizer(trg, aiv1alpha1.FinalizerTrigger) {
		controllerutil.AddFinalizer(trg, aiv1alpha1.FinalizerTrigger)
		if err := r.Update(ctx, trg); err != nil {
			return ctrl.Result{}, err
		}
	}

	now := r.now()
	var res ctrl.Result
	var cond metav1.Condition
	var err error
	if trg.Spec.Suspend {
		cond, err = r.suspend(ctx, trg)
	} else {
		res, cond, err = r.reconcileSource(ctx, trg, now)
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	// A schedule firing writes status itself: start from the latest object.
	latest := &aiv1alpha1.Trigger{}
	if err := r.reader().Get(ctx, req.NamespacedName, latest); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	orig := latest.DeepCopy()
	active, gcAfter, err := r.collectRuns(ctx, latest, now)
	if err != nil {
		return ctrl.Result{}, err
	}
	latest.Status.ActiveRuns = active
	latest.Status.Stream = trg.Status.Stream
	latest.Status.Consumer = trg.Status.Consumer
	latest.Status.NextScheduledAt = trg.Status.NextScheduledAt
	latest.Status.ObservedGeneration = latest.Generation
	cond.ObservedGeneration = latest.Generation
	meta.SetStatusCondition(&latest.Status.Conditions, cond)
	if err := r.Status().Patch(ctx, latest, client.MergeFrom(orig)); err != nil {
		return ctrl.Result{}, err
	}

	if gcAfter > 0 && (res.RequeueAfter == 0 || gcAfter < res.RequeueAfter) {
		res.RequeueAfter = gcAfter
	}
	return res, nil
}

func (r *TriggerReconciler) init() {
	r.initOnce.Do(func() {
		if r.consumers == nil {
			r.consumers = newTriggerConsumers(logf.Log.WithName("trigger-consumers"))
		}
	})
}

func (r *TriggerReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *TriggerReconciler) reader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *TriggerReconciler) event(obj runtime.Object, eventType, reason, format string, args ...any) {
	if r.Recorder != nil {
		r.Recorder.Eventf(obj, eventType, reason, format, args...)
	}
}

func triggerCondition(status metav1.ConditionStatus, reason, msg string) metav1.Condition {
	return metav1.Condition{Type: aiv1alpha1.ConditionTriggerReady, Status: status, Reason: reason, Message: msg}
}

// natsConn returns the operator's raw NATS connection.
func (r *TriggerReconciler) natsConn() (*nats.Conn, error) {
	if r.NATS == nil {
		return nil, errors.New("NATS provider not configured")
	}
	cli, err := r.NATS.Client()
	if err != nil {
		return nil, err
	}
	nc := deadletter.Connection(cli)
	if nc == nil {
		return nil, errors.New("NATS client exposes no connection")
	}
	return nc, nil
}

// releaseConsumer stops the fetch loop and deletes the durable consumer
// recorded in status, if any.
func (r *TriggerReconciler) releaseConsumer(ctx context.Context, trg *aiv1alpha1.Trigger) error {
	r.consumers.stop(client.ObjectKeyFromObject(trg).String())
	if trg.Status.Consumer == "" || trg.Status.Stream == "" {
		trg.Status.Stream, trg.Status.Consumer = "", ""
		return nil
	}
	nc, err := r.natsConn()
	if err != nil {
		return err
	}
	if err := deleteTriggerConsumer(ctx, nc, trg.Status.Stream, trg.Status.Consumer); err != nil {
		return err
	}
	logf.FromContext(ctx).Info("Deleted Trigger consumer", "stream", trg.Status.Stream, "consumer", trg.Status.Consumer)
	trg.Status.Stream, trg.Status.Consumer = "", ""
	return nil
}

// finalize deletes the Trigger's consumer and releases the finalizer.
func (r *TriggerReconciler) finalize(ctx context.Context, trg *aiv1alpha1.Trigger) error {
	if !controllerutil.ContainsFinalizer(trg, aiv1alpha1.FinalizerTrigger) {
		r.consumers.stop(client.ObjectKeyFromObject(trg).String())
		return nil
	}
	if err := r.releaseConsumer(ctx, trg); err != nil {
		return err
	}
	controllerutil.RemoveFinalizer(trg, aiv1alpha1.FinalizerTrigger)
	return client.IgnoreNotFound(r.Update(ctx, trg))
}

// suspend stops the Trigger: its consumer is deleted and no schedule is due.
func (r *TriggerReconciler) suspend(ctx context.Context, trg *aiv1alpha1.Trigger) (metav1.Condition, error) {
	if err := r.releaseConsumer(ctx, trg); err != nil {
		return metav1.Condition{}, err
	}
	trg.Status.NextScheduledAt = nil
	return triggerCondition(metav1.ConditionFalse, aiv1alpha1.ReasonTriggerSuspended, "spec.suspend is set"), nil
}

// reconcileSource validates the spec and runs the configured source.
func (r *TriggerReconciler) reconcileSource(ctx context.Context, trg *aiv1alpha1.Trigger, now time.Time) (ctrl.Result, metav1.Condition, error) {
	key := client.ObjectKeyFromObject(trg).String()
	notReady := func(reason, format string, args ...any) (ctrl.Result, metav1.Condition, error) {
		// Events wait in the durable consumer until the Trigger is fixed.
		r.consumers.stop(key)
		trg.Status.NextScheduledAt = nil
		return ctrl.Result{RequeueAfter: triggerRequeueError},
			triggerCondition(metav1.ConditionFalse, reason, fmt.Sprintf(format, args...)), nil
	}

	if _, err := parseTriggerTemplate("input", trg.Spec.Input); err != nil {
		return notReady(aiv1alpha1.ReasonTriggerInvalidSpec, "spec.input: %v", err)
	}
	if _, err := parseTriggerTemplate("dedupeKey", trg.Spec.DedupeKey); err != nil {
		return notReady(aiv1alpha1.ReasonTriggerInvalidSpec, "spec.dedupeKey: %v", err)
	}
	chain := &aiv1alpha1.Chain{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: trg.Namespace, Name: trg.Spec.Target.ChainRef.Name}, chain); err != nil {
		if apierrors.IsNotFound(err) {
			return notReady(aiv1alpha1.ReasonTriggerTargetNotFound, "Chain %q not found", trg.Spec.Target.ChainRef.Name)
		}
		return ctrl.Result{}, metav1.Condition{}, err
	}

	switch src := trg.Spec.Source; {
	case src.NATS != nil:
		return r.reconcileNATS(ctx, trg, src.NATS, notReady)
	case src.Schedule != nil:
		if err := r.releaseConsumer(ctx, trg); err != nil {
			return ctrl.Result{}, metav1.Condition{}, err
		}
		return r.reconcileSchedule(ctx, trg, src.Schedule, now, notReady)
	default:
		return notReady(aiv1alpha1.ReasonTriggerInvalidSpec, "no supported source is set")
	}
}

type notReadyFunc func(reason, format string, args ...any) (ctrl.Result, metav1.Condition, error)

// reconcileNATS ensures the durable consumer and its fetch loop.
func (r *TriggerReconciler) reconcileNATS(ctx context.Context, trg *aiv1alpha1.Trigger, src *aiv1alpha1.TriggerNATSSource, notReady notReadyFunc) (ctrl.Result, metav1.Condition, error) {
	rtName := aiv1alpha1.RefName(trg.Spec.RoundTableRef)
	rt := &aiv1alpha1.RoundTable{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: trg.Namespace, Name: rtName}, rt); err != nil {
		if apierrors.IsNotFound(err) {
			return notReady(aiv1alpha1.ReasonTriggerRoundTableNotFound, "RoundTable %q not found", rtName)
		}
		return ctrl.Result{}, metav1.Condition{}, err
	}
	prefix := natspkg.EventsSubjectPrefix(rt.Spec.NATS.SubjectPrefix)
	if !strings.HasPrefix(src.Subject, prefix) || len(src.Subject) == len(prefix) {
		return notReady(aiv1alpha1.ReasonTriggerInvalidSpec, "source.nats.subject %q must sit under %q", src.Subject, prefix)
	}

	stream := rt.Spec.NATS.EventsStreamName()
	durable := natspkg.TriggerConsumerName(trg.Namespace, trg.Name)
	if trg.Status.Consumer != "" && (trg.Status.Stream != stream || trg.Status.Consumer != durable) {
		// The RoundTable (or its events stream) changed: drop the old consumer.
		if err := r.releaseConsumer(ctx, trg); err != nil {
			return ctrl.Result{}, metav1.Condition{}, err
		}
	}
	nc, err := r.natsConn()
	if err != nil {
		return notReady(aiv1alpha1.ReasonTriggerConsumerError, "%v", err)
	}
	if err := ensureTriggerConsumer(ctx, nc, stream, durable, src.Subject); err != nil {
		return notReady(aiv1alpha1.ReasonTriggerConsumerError, "%v", err)
	}
	trg.Status.Stream, trg.Status.Consumer = stream, durable
	trg.Status.NextScheduledAt = nil

	nn := client.ObjectKeyFromObject(trg)
	cfg := triggerLoopConfig{stream: stream, durable: durable, subject: src.Subject}
	if err := r.consumers.ensure(nc, nn.String(), cfg, r.messageHandler(nn)); err != nil {
		return notReady(aiv1alpha1.ReasonTriggerConsumerError, "%v", err)
	}
	// Re-check periodically: the fetch loop exits if its consumer is deleted
	// (events stream recreated, JetStream storage lost); the next reconcile
	// recreates the consumer and restarts the loop.
	return ctrl.Result{RequeueAfter: RequeueVerySlow}, triggerCondition(metav1.ConditionTrue, aiv1alpha1.ReasonTriggerListening,
		fmt.Sprintf("Consumer %s on stream %s reads %s", durable, stream, src.Subject)), nil
}

// reconcileSchedule fires when a schedule time has passed since the last one
// handled (or since the Trigger was created) and requeues for the next one.
// Like Chain.spec.schedule, missed times fire at most once, and only within
// startingDeadlineSeconds.
func (r *TriggerReconciler) reconcileSchedule(ctx context.Context, trg *aiv1alpha1.Trigger, src *aiv1alpha1.TriggerScheduleSource, now time.Time, notReady notReadyFunc) (ctrl.Result, metav1.Condition, error) {
	sched, err := parseCron(src.Cron, src.TimeZone, "source.schedule.timeZone")
	if err != nil {
		return notReady(aiv1alpha1.ReasonTriggerInvalidSpec, "%v", err)
	}

	// Read the last schedule time uncached: a fire a moment ago must not be
	// seen as due again.
	fresh := &aiv1alpha1.Trigger{}
	if err := r.reader().Get(ctx, client.ObjectKeyFromObject(trg), fresh); err != nil {
		return ctrl.Result{}, metav1.Condition{}, err
	}
	base := fresh.CreationTimestamp.Time
	if fresh.Status.LastScheduledAt != nil {
		base = fresh.Status.LastScheduledAt.Time
	}
	var due time.Time
	for t, i := sched.Next(base), 0; !t.IsZero() && !t.After(now) && i < triggerMaxCatchUpSteps; t, i = sched.Next(t), i+1 {
		due = t
	}
	if !due.IsZero() {
		nn := client.ObjectKeyFromObject(trg)
		markDue := func(st *aiv1alpha1.TriggerStatus) {
			at := metav1.NewTime(due)
			st.LastScheduledAt = &at
		}
		key := due.UTC().Format(time.RFC3339)
		if dl := src.StartingDeadlineSeconds; dl != nil && now.Sub(due) > time.Duration(*dl)*time.Second {
			firing := aiv1alpha1.TriggerFiring{Time: metav1.NewTime(now), Key: key,
				SkipReason: fmt.Sprintf("missed schedule time %s by more than startingDeadlineSeconds", key)}
			if err := r.recordFiring(ctx, nn, firing, markDue); err != nil {
				return ctrl.Result{}, metav1.Condition{}, err
			}
		} else {
			out, err := r.fire(ctx, nn, TriggerEvent{Time: due, Headers: map[string]string{}}, key, "", markDue)
			if out == triggerRetry {
				if err == nil {
					err = errors.New("trigger is not ready to fire")
				}
				return ctrl.Result{}, metav1.Condition{}, err
			}
		}
	}

	next := sched.Next(now)
	nextAt := metav1.NewTime(next)
	trg.Status.NextScheduledAt = &nextAt
	return ctrl.Result{RequeueAfter: next.Sub(now) + time.Second},
		triggerCondition(metav1.ConditionTrue, aiv1alpha1.ReasonTriggerListening, "Next fire at "+next.UTC().Format(time.RFC3339)), nil
}

// messageHandler handles one NATS event: ack once a run started or the event
// was deliberately skipped, term when the templates can't render it, and nak
// with a delay on errors worth retrying.
func (r *TriggerReconciler) messageHandler(nn types.NamespacedName) triggerMsgHandler {
	return func(ctx context.Context, msg jetstream.Msg) {
		log := logf.Log.WithName("trigger").WithValues("trigger", nn.String(), "subject", msg.Subject())
		ev := TriggerEvent{
			Subject: msg.Subject(),
			Data:    eventData(msg.Data()),
			Headers: eventHeaders(msg.Headers()),
		}
		key := msg.Headers().Get(nats.MsgIdHdr)
		if md, err := msg.Metadata(); err == nil {
			ev.Time = md.Timestamp
			if key == "" {
				key = "seq-" + strconv.FormatUint(md.Sequence.Stream, 10)
			}
		}
		if ev.Time.IsZero() {
			ev.Time = r.now()
		}
		out, err := r.fire(ctx, nn, ev, key, msg.Subject(), nil)
		switch out {
		case triggerHandled:
			if err := msg.DoubleAck(ctx); err != nil {
				log.Error(err, "Failed to ack Trigger event")
			}
		case triggerTerm:
			if err := msg.Term(); err != nil {
				log.Error(err, "Failed to terminate Trigger event")
			}
		default:
			if err != nil {
				log.Error(err, "Trigger event not handled, will retry")
			}
			_ = msg.NakWithDelay(triggerNakDelay)
		}
	}
}

type triggerOutcome int

const (
	// triggerHandled: a run started, or the event was deliberately skipped.
	triggerHandled triggerOutcome = iota
	// triggerTerm: the event can never fire (its templates fail to render).
	triggerTerm
	// triggerRetry: try the event again later.
	triggerRetry
)

func (r *TriggerReconciler) fireLock(nn types.NamespacedName) *sync.Mutex {
	mu, _ := r.fireLocks.LoadOrStore(nn.String(), &sync.Mutex{})
	return mu.(*sync.Mutex)
}

// fire handles one event: render the input and dedupe key, apply dedupe and
// concurrency, start the run, and record the firing. mutate, if set, also
// changes the status written with the firing.
func (r *TriggerReconciler) fire(ctx context.Context, nn types.NamespacedName, ev TriggerEvent, defaultKey, subject string, mutate func(*aiv1alpha1.TriggerStatus)) (triggerOutcome, error) {
	mu := r.fireLock(nn)
	mu.Lock()
	defer mu.Unlock()

	trg := &aiv1alpha1.Trigger{}
	if err := r.reader().Get(ctx, nn, trg); err != nil {
		return triggerRetry, err
	}
	if trg.Spec.Suspend || !trg.DeletionTimestamp.IsZero() {
		return triggerRetry, nil
	}
	now := r.now()
	firing := aiv1alpha1.TriggerFiring{Time: metav1.NewTime(now)}
	skip := func(outcome triggerOutcome, format string, args ...any) (triggerOutcome, error) {
		firing.SkipReason = fmt.Sprintf(format, args...)
		if err := r.recordFiring(ctx, nn, firing, mutate); err != nil {
			return triggerRetry, err
		}
		if outcome == triggerTerm {
			r.event(trg, corev1.EventTypeWarning, "EventRejected", "%s", firing.SkipReason)
		}
		return outcome, nil
	}

	data := triggerTemplateData{Event: ev, Trigger: triggerRef{Name: trg.Name, Namespace: trg.Namespace}}
	input, err := renderTriggerTemplate("input", trg.Spec.Input, data)
	if err != nil {
		return skip(triggerTerm, "input template: %v", err)
	}
	key := defaultKey
	if trg.Spec.DedupeKey != "" {
		rendered, err := renderTriggerTemplate("dedupeKey", trg.Spec.DedupeKey, data)
		if err != nil {
			return skip(triggerTerm, "dedupeKey template: %v", err)
		}
		if rendered = strings.TrimSpace(rendered); rendered != "" {
			key = rendered
		}
	}
	if key == "" {
		key = strconv.FormatInt(now.UnixNano(), 10)
	}
	firing.Key = truncate(key, 253)

	tmpl := &aiv1alpha1.Chain{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: nn.Namespace, Name: trg.Spec.Target.ChainRef.Name}, tmpl); err != nil {
		return triggerRetry, fmt.Errorf("get target chain %q: %w", trg.Spec.Target.ChainRef.Name, err)
	}
	name := chainrun.Name(tmpl.Name, trg.Name, dedupeHash(key))

	existing := &aiv1alpha1.Chain{}
	err = r.reader().Get(ctx, types.NamespacedName{Namespace: nn.Namespace, Name: name}, existing)
	switch {
	case err == nil:
		return skip(triggerHandled, "duplicate dedupe key: run %s exists", name)
	case !apierrors.IsNotFound(err):
		return triggerRetry, err
	}

	active, err := r.activeRuns(ctx, trg)
	if err != nil {
		return triggerRetry, err
	}
	switch trg.Spec.Concurrency.Policy {
	case aiv1alpha1.TriggerConcurrencyForbid:
		if len(active) > 0 {
			return skip(triggerHandled, "concurrency Forbid: run %s is active", active[0].Name)
		}
	case aiv1alpha1.TriggerConcurrencyReplace:
		for i := range active {
			if err := r.Delete(ctx, &active[i]); client.IgnoreNotFound(err) != nil {
				return triggerRetry, fmt.Errorf("replace run %s: %w", active[i].Name, err)
			}
			r.event(trg, corev1.EventTypeNormal, "RunReplaced", "Deleted active run %s", active[i].Name)
		}
	default:
		if n := trg.Spec.Concurrency.MaxActive; n > 0 && int32(len(active)) >= n {
			return skip(triggerHandled, "concurrency Allow: maxActive %d reached", n)
		}
	}

	if trg.Spec.Input == "" {
		input = tmpl.Spec.Input
	}
	annotations := map[string]string{aiv1alpha1.AnnotationTriggerKey: truncate(key, triggerKeyAnnotationMax)}
	if subject != "" {
		annotations[aiv1alpha1.AnnotationTriggerSubject] = subject
	}
	_, err = chainrun.Start(ctx, r.Client, r.Scheme, tmpl, chainrun.StartOptions{
		Name:        name,
		Namespace:   nn.Namespace,
		Input:       input,
		Trigger:     strconv.FormatInt(now.UnixNano(), 10),
		Source:      chainrun.SourceTrigger,
		Labels:      map[string]string{aiv1alpha1.LabelTrigger: trg.Name},
		Annotations: annotations,
		Owner:       trg,
	})
	if apierrors.IsAlreadyExists(err) {
		return skip(triggerHandled, "duplicate dedupe key: run %s exists", name)
	}
	if err != nil {
		return triggerRetry, fmt.Errorf("start run %s: %w", name, err)
	}

	firing.Run = name
	if err := r.recordFiring(ctx, nn, firing, func(st *aiv1alpha1.TriggerStatus) {
		at := metav1.NewTime(now)
		st.LastFiredAt = &at
		if !slices.Contains(st.ActiveRuns, name) {
			st.ActiveRuns = append(st.ActiveRuns, name)
		}
		if mutate != nil {
			mutate(st)
		}
	}); err != nil {
		// The run started; a redelivery finds it by name and skips.
		logf.FromContext(ctx).Error(err, "Failed to record Trigger firing", "trigger", nn.String(), "run", name)
	}
	logf.Log.WithName("trigger").Info("Started Chain run", "trigger", nn.String(), "run", name, "key", firing.Key)
	r.event(trg, corev1.EventTypeNormal, "Fired", "Started run %s", name)
	return triggerHandled, nil
}

// recordFiring prepends firing to status.recentFirings (bounded) and applies
// mutate, retrying on conflicts.
func (r *TriggerReconciler) recordFiring(ctx context.Context, nn types.NamespacedName, firing aiv1alpha1.TriggerFiring, mutate func(*aiv1alpha1.TriggerStatus)) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		trg := &aiv1alpha1.Trigger{}
		if err := r.reader().Get(ctx, nn, trg); err != nil {
			return err
		}
		firings := append([]aiv1alpha1.TriggerFiring{firing}, trg.Status.RecentFirings...)
		if len(firings) > aiv1alpha1.TriggerMaxRecentFirings {
			firings = firings[:aiv1alpha1.TriggerMaxRecentFirings]
		}
		trg.Status.RecentFirings = firings
		if mutate != nil {
			mutate(&trg.Status)
		}
		return r.Status().Update(ctx, trg)
	})
}

// listRuns returns the Trigger's runs, oldest first.
func (r *TriggerReconciler) listRuns(ctx context.Context, trg *aiv1alpha1.Trigger) ([]aiv1alpha1.Chain, error) {
	runs := &aiv1alpha1.ChainList{}
	if err := r.reader().List(ctx, runs, client.InNamespace(trg.Namespace),
		client.MatchingLabels{aiv1alpha1.LabelTrigger: trg.Name}); err != nil {
		return nil, fmt.Errorf("list trigger runs: %w", err)
	}
	items := slices.DeleteFunc(runs.Items, func(c aiv1alpha1.Chain) bool {
		return !metav1.IsControlledBy(&c, trg) || !c.DeletionTimestamp.IsZero()
	})
	slices.SortFunc(items, func(a, b aiv1alpha1.Chain) int {
		if c := a.CreationTimestamp.Compare(b.CreationTimestamp.Time); c != 0 {
			return c
		}
		return strings.Compare(a.Name, b.Name)
	})
	return items, nil
}

// activeRuns returns the Trigger's running or pending runs, oldest first.
func (r *TriggerReconciler) activeRuns(ctx context.Context, trg *aiv1alpha1.Trigger) ([]aiv1alpha1.Chain, error) {
	runs, err := r.listRuns(ctx, trg)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(runs, func(c aiv1alpha1.Chain) bool { return !chainrun.Active(&c) }), nil
}

// collectRuns deletes finished runs past ttlSecondsAfterFinished and returns
// the active run names and when the next finished run expires (0: none).
func (r *TriggerReconciler) collectRuns(ctx context.Context, trg *aiv1alpha1.Trigger, now time.Time) ([]string, time.Duration, error) {
	runs, err := r.listRuns(ctx, trg)
	if err != nil {
		return nil, 0, err
	}
	ttl := trg.Spec.TTLSecondsAfterFinished
	var active []string
	var next time.Duration
	for i := range runs {
		c := &runs[i]
		if chainrun.Active(c) {
			active = append(active, c.Name)
			continue
		}
		if ttl == nil || *ttl <= 0 || c.Status.CompletedAt == nil {
			continue
		}
		if chainrun.Expired(c, ttl, now) {
			if err := r.Delete(ctx, c); client.IgnoreNotFound(err) != nil {
				return nil, 0, fmt.Errorf("delete finished run %s: %w", c.Name, err)
			}
			logf.FromContext(ctx).Info("Deleted finished Trigger run", "run", c.Name)
			continue
		}
		left := c.Status.CompletedAt.Add(time.Duration(*ttl) * time.Second).Sub(now)
		if next == 0 || left < next {
			next = left
		}
	}
	return active, next, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *TriggerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.consumers = newTriggerConsumers(mgr.GetLogger().WithName("trigger-consumers"))
	r.init()
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	// Stop the fetch loops when the manager stops (or loses leadership).
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		<-ctx.Done()
		r.consumers.stopAll()
		return nil
	})); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiv1alpha1.Trigger{}).
		Owns(&aiv1alpha1.Chain{}).
		Named("trigger").
		Complete(r)
}
