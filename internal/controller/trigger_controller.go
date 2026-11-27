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
	"github.com/prometheus/client_golang/prometheus"
	batchv1 "k8s.io/api/batch/v1"
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
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/chainrun"
	"github.com/roundtable-k8s/roundtable/internal/deadletter"
	"github.com/roundtable-k8s/roundtable/internal/gateway"
	"github.com/roundtable-k8s/roundtable/internal/github"
	"github.com/roundtable-k8s/roundtable/pkg/metrics"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

const (
	// triggerRequeueError is how soon a Trigger that is not Ready is retried.
	triggerRequeueError = 30 * time.Second
	// triggerNakDelay is how long a NATS event that could not be handled
	// (API error, missing target) waits before it is redelivered.
	triggerNakDelay = 15 * time.Second
	// triggerKeyAnnotationMax bounds the dedupe key stored on a run.
	triggerKeyAnnotationMax = 1024
)

// TriggerReconciler runs Triggers: for a NATS source it keeps one durable
// consumer on the RoundTable's events stream and a fetch loop that starts a
// run per message. Runs are copies of the target Chain started through
// chainrun.Start. A github source is a NATS source fed by a poll loop that
// publishes matching items to the Trigger's own subject; a schedule source
// one fed by an owned CronJob whose pod asks the gateway to publish each
// firing to the Trigger's own subject.
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
	// GitHub builds GitHub clients for github sources; nil means
	// api.github.com.
	GitHub *github.ClientFactory

	// TaskBucket and TaskTTL are the gateway's task KV bucket and record
	// TTL: knight-target firings record their tasks there. Empty/0 use the
	// gateway's defaults.
	TaskBucket string
	TaskTTL    time.Duration
	// ScheduleRetention keeps the finished runs of a schedule source at
	// least this long (chart value gateway.scheduleRuns.resultRetention),
	// whatever its ttlSecondsAfterFinished. 0 leaves the Trigger's own.
	ScheduleRetention time.Duration
	// DefaultKnightImage infers a knight's runtime to validate a knight
	// target's model (the operator's DEFAULT_KNIGHT_IMAGE).
	DefaultKnightImage string
	// Schedule renders the CronJob of schedule-source Triggers.
	Schedule TriggerScheduleConfig

	// Now returns the current time; nil means time.Now (tests override it).
	Now func() time.Time

	initOnce  sync.Once
	consumers *triggerConsumers
	pollers   *triggerPollers
	// manualGitHubPolls keeps Reconcile from starting poll loops (tests
	// call pollGitHub themselves).
	manualGitHubPolls bool
	fireLocks         sync.Map // namespace/name -> *sync.Mutex
	holdSince         sync.Map // namespace/name/repository -> time.Time the repository's hold began

	tasksMu   sync.Mutex
	tasks     *gateway.Service
	tasksConn *nats.Conn
}

// +kubebuilder:rbac:groups=ai.roundtable.io,resources=triggers,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=triggers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=triggers/finalizers,verbs=update
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=chains,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=roundtables,verbs=get;list;watch
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=knights,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete

// Reconcile keeps a Trigger's consumer or schedule in line with its spec.
func (r *TriggerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.init()
	trg := &aiv1alpha1.Trigger{}
	if err := r.Get(ctx, req.NamespacedName, trg); err != nil {
		if apierrors.IsNotFound(err) {
			r.consumers.stop(req.String())
			r.pollers.stop(req.String())
			deleteTriggerLastFired(req.Namespace, req.Name)
			r.clearRepositoryHolds(req.Namespace, req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !trg.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.finalize(ctx, trg)
	}
	if trg.Spec.Source.Schedule == nil {
		// The schedule source was removed (or never set): its CronJob and
		// ServiceAccount go.
		if err := r.deleteScheduleObjects(ctx, trg); err != nil {
			return ctrl.Result{}, err
		}
		trg.Status.LastScheduledAt = nil
	}
	if usesConsumer(trg) && !controllerutil.ContainsFinalizer(trg, aiv1alpha1.FinalizerTrigger) {
		controllerutil.AddFinalizer(trg, aiv1alpha1.FinalizerTrigger)
		if err := r.Update(ctx, trg); err != nil {
			return ctrl.Result{}, err
		}
	}

	runNowRetry := false
	if id := trg.Annotations[aiv1alpha1.AnnotationTriggerRunNow]; id != "" {
		// A request that cannot fire yet must not block the Ready
		// condition and suspendedBy: keep it, carry on, and requeue.
		if err := r.runNow(ctx, trg, id); err != nil {
			logf.FromContext(ctx).Error(err, "run-now request not fired, will retry", "request", id)
			runNowRetry = true
		}
	}

	now := r.now()
	var res ctrl.Result
	var cond metav1.Condition
	var err error
	if trg.Suspended() {
		cond, err = r.suspend(ctx, trg)
	} else {
		res, cond, err = r.reconcileSource(ctx, trg, now)
	}
	if err != nil {
		return ctrl.Result{}, err
	}

	// A firing writes status itself: start from the latest object.
	latest := &aiv1alpha1.Trigger{}
	if err := r.reader().Get(ctx, req.NamespacedName, latest); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	orig := latest.DeepCopy()
	active, gcAfter, err := r.collectRuns(ctx, latest, now)
	if err != nil {
		return ctrl.Result{}, err
	}
	if latest.Spec.Target.Mission != nil {
		// A mission target's active runs are its unfinished Missions.
		active, pollAfter, err := r.collectMissions(ctx, latest, now)
		if err != nil {
			return ctrl.Result{}, err
		}
		latest.Status.ActiveRuns = active
		if pollAfter > 0 && (gcAfter == 0 || pollAfter < gcAfter) {
			gcAfter = pollAfter
		}
	} else if latest.Spec.Target.Knight != nil {
		// A knight target's active runs are its tasks without a result.
		pollAfter := r.collectTasks(ctx, latest, now)
		if pollAfter > 0 && (gcAfter == 0 || pollAfter < gcAfter) {
			gcAfter = pollAfter
		}
	} else {
		latest.Status.ActiveRuns = active
	}
	latest.Status.Stream = trg.Status.Stream
	latest.Status.Consumer = trg.Status.Consumer
	latest.Status.NextScheduledAt = trg.Status.NextScheduledAt
	latest.Status.LastScheduledAt = trg.Status.LastScheduledAt
	latest.Status.LastSuccessfulScheduledAt = trg.Status.LastSuccessfulScheduledAt
	if latest.Spec.Source.Schedule == nil {
		meta.RemoveStatusCondition(&latest.Status.Conditions, aiv1alpha1.ConditionTriggerScheduleHealthy)
		latest.Status.LastSuccessfulScheduledAt = nil
	} else if c := meta.FindStatusCondition(trg.Status.Conditions, aiv1alpha1.ConditionTriggerScheduleHealthy); c != nil {
		c.ObservedGeneration = latest.Generation
		meta.SetStatusCondition(&latest.Status.Conditions, *c)
	}
	latest.Status.ObservedGeneration = latest.Generation
	latest.Status.SuspendedBy = trg.SuspendedBy()
	cond.ObservedGeneration = latest.Generation
	meta.SetStatusCondition(&latest.Status.Conditions, cond)
	if latest.Spec.Source.GitHub == nil || trg.Suspended() {
		meta.RemoveStatusCondition(&latest.Status.Conditions, aiv1alpha1.ConditionTriggerGitHubReady)
		latest.Status.LastPolledAt = nil
		// A stopped (or absent) poller never clears its holds: drop the
		// series, start times and condition here.
		r.clearRepositoryHolds(latest.Namespace, latest.Name)
		meta.RemoveStatusCondition(&latest.Status.Conditions, aiv1alpha1.ConditionTriggerRepositoryHeld)
	}
	patch := client.MergeFrom(orig)
	if latest.Spec.Source.GitHub != nil || latest.Spec.Target.Knight != nil || latest.Spec.Target.Mission != nil {
		// The poll loop writes GitHubReady concurrently, and a firing adds
		// a knight target's task to activeRuns: don't overwrite them with a
		// stale list (a conflict requeues).
		patch = client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})
	}
	if err := r.Status().Patch(ctx, latest, patch); err != nil {
		return ctrl.Result{}, err
	}
	// Count after the patch so a conflict-retried reconcile cannot count a
	// firing twice: the outcome is persisted exactly once.
	if latest.Spec.Target.Knight != nil || latest.Spec.Target.Mission != nil {
		r.countFinishedRuns(ctx, orig, latest)
	}

	if gcAfter > 0 && (res.RequeueAfter == 0 || gcAfter < res.RequeueAfter) {
		res.RequeueAfter = gcAfter
	}
	if runNowRetry && (res.RequeueAfter == 0 || runNowRetryDelay < res.RequeueAfter) {
		res.RequeueAfter = runNowRetryDelay
	}
	return res, nil
}

// runNowRetryDelay is how long a run-now request that could not fire waits
// before the next attempt.
const runNowRetryDelay = 10 * time.Second

func (r *TriggerReconciler) init() {
	r.initOnce.Do(func() {
		if r.consumers == nil {
			r.consumers = newTriggerConsumers(logf.Log.WithName("trigger-consumers"))
		}
		if r.pollers == nil {
			r.pollers = newTriggerPollers(logf.Log.WithName("trigger-github"))
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

// releaseConsumer stops the fetch and poll loops and deletes the durable
// consumer recorded in status, if any.
func (r *TriggerReconciler) releaseConsumer(ctx context.Context, trg *aiv1alpha1.Trigger) error {
	r.pollers.stop(client.ObjectKeyFromObject(trg).String())
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
		r.pollers.stop(client.ObjectKeyFromObject(trg).String())
		r.consumers.stop(client.ObjectKeyFromObject(trg).String())
		return nil
	}
	if err := r.releaseConsumer(ctx, trg); err != nil {
		return err
	}
	controllerutil.RemoveFinalizer(trg, aiv1alpha1.FinalizerTrigger)
	return client.IgnoreNotFound(r.Update(ctx, trg))
}

// suspend stops the Trigger: its consumer is deleted and a schedule
// source's CronJob is suspended.
func (r *TriggerReconciler) suspend(ctx context.Context, trg *aiv1alpha1.Trigger) (metav1.Condition, error) {
	if err := r.releaseConsumer(ctx, trg); err != nil {
		return metav1.Condition{}, err
	}
	trg.Status.NextScheduledAt = nil
	msg := suspendedMessage(trg)
	if src := trg.Spec.Source.Schedule; src != nil {
		if _, err := parseCron(src.Cron, src.TimeZone, "source.schedule.timeZone"); err == nil && r.scheduleConfigured() == nil {
			cj, err := r.ensureScheduleObjects(ctx, trg, src)
			var notOwned errNotOwned
			switch {
			case errors.As(err, &notOwned):
				msg += "; " + notOwned.Error()
			case err != nil:
				return metav1.Condition{}, err
			default:
				trg.Status.LastScheduledAt = cj.Status.LastScheduleTime
				if err := r.observeFireJobs(ctx, trg, cj); err != nil {
					return metav1.Condition{}, err
				}
				msg += "; CronJob " + cj.Name + " is suspended"
			}
		}
	}
	return triggerCondition(metav1.ConditionFalse, aiv1alpha1.ReasonTriggerSuspended, msg), nil
}

// suspendedMessage says what suspends the Trigger.
func suspendedMessage(trg *aiv1alpha1.Trigger) string {
	var why []string
	if trg.Spec.Suspend {
		why = append(why, "spec.suspend is set")
	}
	if trg.SuspendedByAnnotation() {
		msg := "annotation " + aiv1alpha1.AnnotationTriggerSuspended + " is set"
		if by := trg.Annotations[aiv1alpha1.AnnotationTriggerSuspended]; by != "" && by != labelValueTrue {
			msg += " (by " + by + ")"
		}
		why = append(why, msg)
	}
	return strings.Join(why, "; ")
}

// triggerManualFiring is a firing asked for with the run-now annotation.
type triggerManualFiring struct {
	// caller asked for it; recorded on the run.
	caller string
}

// runNow fires the Trigger once for the ai.roundtable.io/run-now annotation
// (request id), through the normal firing path, then removes the
// annotation. The firing's key is "manual:<id>", so a repeated reconcile
// finds the run by name and starts nothing twice.
func (r *TriggerReconciler) runNow(ctx context.Context, trg *aiv1alpha1.Trigger, id string) error {
	nn := client.ObjectKeyFromObject(trg)
	manual := &triggerManualFiring{caller: trg.Annotations[aiv1alpha1.AnnotationTriggerRunNowCaller]}
	ev := TriggerEvent{Time: r.now(), Headers: map[string]string{}}
	out, err := r.fire(ctx, nn, ev, aiv1alpha1.TriggerRunNowKey(id), "", nil, manual)
	if out == triggerRetry {
		if err == nil {
			err = errors.New("trigger is not ready to fire")
		}
		return err
	}
	// Remove the request only if it is still this one: a newer request
	// (conflict) is handled on the next reconcile.
	latest := &aiv1alpha1.Trigger{}
	if err := r.reader().Get(ctx, nn, latest); err != nil {
		return client.IgnoreNotFound(err)
	}
	if latest.Annotations[aiv1alpha1.AnnotationTriggerRunNow] != id {
		return nil
	}
	patch := client.MergeFromWithOptions(latest.DeepCopy(), client.MergeFromWithOptimisticLock{})
	delete(latest.Annotations, aiv1alpha1.AnnotationTriggerRunNow)
	delete(latest.Annotations, aiv1alpha1.AnnotationTriggerRunNowCaller)
	if err := r.Patch(ctx, latest, patch); err != nil {
		return client.IgnoreNotFound(err)
	}
	trg.Annotations = latest.Annotations
	trg.ResourceVersion = latest.ResourceVersion
	return nil
}

// reconcileSource validates the spec and runs the configured source.
func (r *TriggerReconciler) reconcileSource(ctx context.Context, trg *aiv1alpha1.Trigger, now time.Time) (ctrl.Result, metav1.Condition, error) {
	key := client.ObjectKeyFromObject(trg).String()
	notReady := func(reason, format string, args ...any) (ctrl.Result, metav1.Condition, error) {
		// Events wait in the durable consumer until the Trigger is fixed.
		r.consumers.stop(key)
		r.pollers.stop(key)
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
	if err := validateGitHubSource(trg.Spec.Source.GitHub); err != nil {
		return notReady(aiv1alpha1.ReasonTriggerInvalidSpec, "source.github: %v", err)
	}
	if err := aiv1alpha1.ValidateConcurrency(&trg.Spec); err != nil {
		return notReady(aiv1alpha1.ReasonTriggerInvalidSpec, "%v", err)
	}
	switch target := trg.Spec.Target; {
	case target.Knight != nil:
		if _, err := parseTriggerTemplate("task", target.Knight.Task); err != nil {
			return notReady(aiv1alpha1.ReasonTriggerInvalidSpec, "target.knight.task: %v", err)
		}
		if trg.Spec.Source.GitHub != nil || trg.Spec.Input != "" {
			return notReady(aiv1alpha1.ReasonTriggerInvalidSpec,
				"target.knight does not support source.github or spec.input")
		}
		reason, msg, err := r.checkKnightTarget(ctx, trg)
		if err != nil {
			return ctrl.Result{}, metav1.Condition{}, err
		}
		if reason != "" {
			return notReady(reason, "%s", msg)
		}
	case target.Mission != nil:
		if _, err := parseTriggerTemplate("objective", target.Mission.Objective); err != nil {
			return notReady(aiv1alpha1.ReasonTriggerInvalidSpec, "target.mission.objective: %v", err)
		}
		if _, err := parseTriggerTemplate("successCriteria", target.Mission.SuccessCriteria); err != nil {
			return notReady(aiv1alpha1.ReasonTriggerInvalidSpec, "target.mission.successCriteria: %v", err)
		}
		if trg.Spec.Source.GitHub != nil || trg.Spec.Input != "" || trg.Spec.RoundTableRef == nil {
			return notReady(aiv1alpha1.ReasonTriggerInvalidSpec,
				"target.mission needs roundTableRef and does not support source.github or spec.input")
		}
		rt := &aiv1alpha1.RoundTable{}
		if err := r.reader().Get(ctx, types.NamespacedName{Namespace: trg.Namespace, Name: trg.Spec.RoundTableRef.Name}, rt); err != nil {
			if apierrors.IsNotFound(err) {
				return notReady(aiv1alpha1.ReasonTriggerTargetNotFound, "RoundTable %q not found", trg.Spec.RoundTableRef.Name)
			}
			return ctrl.Result{}, metav1.Condition{}, err
		}
	case target.ChainRef != nil:
		chain := &aiv1alpha1.Chain{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: trg.Namespace, Name: target.ChainRef.Name}, chain); err != nil {
			if apierrors.IsNotFound(err) {
				return notReady(aiv1alpha1.ReasonTriggerTargetNotFound, "Chain %q not found", target.ChainRef.Name)
			}
			return ctrl.Result{}, metav1.Condition{}, err
		}
	default:
		return notReady(aiv1alpha1.ReasonTriggerInvalidSpec, "set target.chainRef, target.knight or target.mission")
	}

	switch src := trg.Spec.Source; {
	case src.NATS != nil, src.Webhook != nil, src.GitHub != nil:
		return r.reconcileNATS(ctx, trg, notReady)
	case src.Schedule != nil:
		return r.reconcileSchedule(ctx, trg, src.Schedule, now, notReady)
	default:
		return notReady(aiv1alpha1.ReasonTriggerInvalidSpec, "no supported source is set")
	}
}

type notReadyFunc func(reason, format string, args ...any) (ctrl.Result, metav1.Condition, error)

// usesConsumer reports whether the Trigger reads the events stream through a
// durable consumer (all sources: a schedule source's firings arrive there
// from the gateway).
func usesConsumer(trg *aiv1alpha1.Trigger) bool {
	src := trg.Spec.Source
	return src.NATS != nil || src.Webhook != nil || src.GitHub != nil || src.Schedule != nil
}

// reconcileNATS ensures the durable consumer and its fetch loop. A webhook
// source is a NATS source on the fixed subject the gateway publishes its
// deliveries to; a github source one on the subject its poll loop publishes
// matching items to; a schedule source one on the subject the gateway
// publishes its CronJob's firings to.
func (r *TriggerReconciler) reconcileNATS(ctx context.Context, trg *aiv1alpha1.Trigger, notReady notReadyFunc) (ctrl.Result, metav1.Condition, error) {
	rtName := aiv1alpha1.RefName(trg.Spec.RoundTableRef)
	if trg.Spec.Source.Schedule != nil {
		name, err := r.knightResolver().TriggerTable(ctx, trg)
		if errors.Is(err, gateway.ErrNotFound) {
			return notReady(aiv1alpha1.ReasonTriggerTargetNotFound, "%v", err)
		}
		if err != nil {
			return ctrl.Result{}, metav1.Condition{}, err
		}
		rtName = name
	}
	rt := &aiv1alpha1.RoundTable{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: trg.Namespace, Name: rtName}, rt); err != nil {
		if apierrors.IsNotFound(err) {
			return notReady(aiv1alpha1.ReasonTriggerRoundTableNotFound, "RoundTable %q not found", rtName)
		}
		return ctrl.Result{}, metav1.Condition{}, err
	}
	var subject string
	switch src := trg.Spec.Source; {
	case src.Webhook != nil, src.GitHub != nil, src.Schedule != nil:
		if rt.Spec.NATS.SubjectPrefix == "" {
			return notReady(aiv1alpha1.ReasonTriggerInvalidSpec, "RoundTable %q has no spec.nats.subjectPrefix", rtName)
		}
		switch {
		case src.GitHub != nil:
			subject = natspkg.GitHubEventSubject(rt.Spec.NATS.SubjectPrefix, trg.Namespace, trg.Name)
		case src.Schedule != nil:
			subject = natspkg.ScheduleEventSubject(rt.Spec.NATS.SubjectPrefix, trg.Namespace, trg.Name)
		default:
			subject = natspkg.WebhookEventSubject(rt.Spec.NATS.SubjectPrefix, trg.Namespace, trg.Name)
		}
	default:
		subject = trg.Spec.Source.NATS.Subject
		prefix := natspkg.EventsSubjectPrefix(rt.Spec.NATS.SubjectPrefix)
		if !strings.HasPrefix(subject, prefix) || len(subject) == len(prefix) {
			return notReady(aiv1alpha1.ReasonTriggerInvalidSpec, "source.nats.subject %q must sit under %q", subject, prefix)
		}
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
	if err := ensureTriggerConsumer(ctx, nc, stream, durable, subject); err != nil {
		return notReady(aiv1alpha1.ReasonTriggerConsumerError, "%v", err)
	}
	trg.Status.Stream, trg.Status.Consumer = stream, durable
	trg.Status.NextScheduledAt = nil

	nn := client.ObjectKeyFromObject(trg)
	cfg := triggerLoopConfig{stream: stream, durable: durable, subject: subject}
	if err := r.consumers.ensure(nc, nn.String(), cfg, r.messageHandler(nn)); err != nil {
		return notReady(aiv1alpha1.ReasonTriggerConsumerError, "%v", err)
	}
	msg := fmt.Sprintf("Consumer %s on stream %s reads %s", durable, stream, subject)
	if gh := trg.Spec.Source.GitHub; gh != nil {
		if !r.manualGitHubPolls {
			r.pollers.ensure(nn.String(), trg.Generation, func(ctx context.Context) time.Duration {
				return r.pollGitHub(ctx, nn)
			})
		}
		msg += fmt.Sprintf("; polling %d repositories every %s", len(gh.Repositories), githubPollInterval(gh))
	} else {
		r.pollers.stop(nn.String())
	}
	// Re-check periodically: the fetch loop exits if its consumer is deleted
	// (events stream recreated, JetStream storage lost); the next reconcile
	// recreates the consumer and restarts the loop.
	return ctrl.Result{RequeueAfter: RequeueVerySlow}, triggerCondition(metav1.ConditionTrue, aiv1alpha1.ReasonTriggerListening, msg), nil
}

// scheduleConfigured reports why the operator cannot render a schedule
// source's CronJob (nil when it can).
func (r *TriggerReconciler) scheduleConfigured() error {
	switch {
	case r.Schedule.Image == "":
		return errors.New("the operator has no image for the firing pod (--trigger-image)")
	case r.Schedule.GatewayURL == "":
		return errors.New("the operator has no gateway URL for the firing pod (--trigger-gateway-url): " +
			"scheduled Triggers need the gateway")
	}
	return nil
}

// reconcileSchedule renders the Trigger's CronJob and ServiceAccount and
// reads the firings the CronJob's pod publishes through the gateway. The
// cluster's CronJob controller decides when to fire (time zone, missed runs
// within startingDeadlineSeconds, concurrency); the operator only reports the
// CronJob's last schedule time and computes the next one for display.
func (r *TriggerReconciler) reconcileSchedule(ctx context.Context, trg *aiv1alpha1.Trigger, src *aiv1alpha1.TriggerScheduleSource, now time.Time, notReady notReadyFunc) (ctrl.Result, metav1.Condition, error) {
	sched, err := parseCron(src.Cron, src.TimeZone, "source.schedule.timeZone")
	if err != nil {
		return notReady(aiv1alpha1.ReasonTriggerInvalidSpec, "%v", err)
	}
	if err := r.scheduleConfigured(); err != nil {
		return notReady(aiv1alpha1.ReasonTriggerCronJobError, "%v", err)
	}
	cj, err := r.ensureScheduleObjects(ctx, trg, src)
	var notOwned errNotOwned
	if errors.As(err, &notOwned) {
		return notReady(aiv1alpha1.ReasonTriggerCronJobError, "%v", err)
	}
	if err != nil {
		if apierrors.IsInvalid(err) {
			return notReady(aiv1alpha1.ReasonTriggerCronJobError, "%v", err)
		}
		return ctrl.Result{}, metav1.Condition{}, err
	}
	trg.Status.LastScheduledAt = cj.Status.LastScheduleTime
	if err := r.observeFireJobs(ctx, trg, cj); err != nil {
		return ctrl.Result{}, metav1.Condition{}, err
	}

	res, cond, err := r.reconcileNATS(ctx, trg, notReady)
	if err != nil || cond.Status != metav1.ConditionTrue {
		return res, cond, err
	}
	next := sched.Next(now)
	nextAt := metav1.NewTime(next)
	trg.Status.NextScheduledAt = &nextAt
	cond.Message = fmt.Sprintf("CronJob %s fires %q (next at %s); %s",
		cj.Name, src.Cron, next.UTC().Format(time.RFC3339), cond.Message)
	return res, cond, nil
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
		key := msg.Headers().Get(natspkg.EventKeyHeader)
		if key == "" {
			key = msg.Headers().Get(nats.MsgIdHdr)
		}
		if md, err := msg.Metadata(); err == nil {
			ev.Time = md.Timestamp
			if key == "" {
				var err error
				if key, ev.streamCreated, err = r.streamSeqKey(ctx, md); err != nil {
					log.Error(err, "Failed to build the default dedupe key, will retry")
					_ = msg.NakWithDelay(triggerNakDelay)
					return
				}
				ev.legacyKey = "seq-" + strconv.FormatUint(md.Sequence.Stream, 10)
			}
		}
		if at, err := time.Parse(time.RFC3339, msg.Headers().Get(natspkg.ScheduledTimeHeader)); err == nil {
			// A scheduled firing: .Event.Time is the schedule time.
			ev.Time = at
		}
		if ev.Time.IsZero() {
			ev.Time = r.now()
		}
		out, err := r.fire(ctx, nn, ev, key, msg.Subject(), nil, nil)
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

// streamSeqKey is the default dedupe key of a message with no key header:
// the stream's creation time and the message's sequence. A recreated stream
// restarts its sequence at 1, so the sequence alone would match runs from
// before the reset.
func (r *TriggerReconciler) streamSeqKey(ctx context.Context, md *jetstream.MsgMetadata) (string, time.Time, error) {
	nc, err := r.natsConn()
	if err != nil {
		return "", time.Time{}, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return "", time.Time{}, err
	}
	s, err := js.Stream(ctx, md.Stream)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("stream %s: %w", md.Stream, err)
	}
	created := s.CachedInfo().Created
	return streamSeqKeyOf(created, md.Sequence.Stream), created, nil
}

func streamSeqKeyOf(created time.Time, seq uint64) string {
	return strconv.FormatInt(created.UnixNano(), 10) + "/" + strconv.FormatUint(seq, 10)
}

// legacyKeyFired reports whether a run already exists under the pre-stream-
// creation default key ("seq-<n>"), so a message delivered but not acked
// before an upgrade is not run twice. Only a run created at or after the
// stream counts: an older one belongs to the stream's previous incarnation
// (the sequence restarted), so the new message must run. A knight target has no run object to
// look up and is not checked.
func (r *TriggerReconciler) legacyKeyFired(ctx context.Context, trg *aiv1alpha1.Trigger, key string, streamCreated time.Time) (string, error) {
	var obj client.Object
	var name string
	switch {
	case trg.Spec.Target.Mission != nil:
		name, obj = triggerMissionName(trg, key), &aiv1alpha1.Mission{}
	case trg.Spec.Target.ChainRef != nil:
		name, obj = chainrun.TriggerRunName(aiv1alpha1.RefName(trg.Spec.Target.ChainRef), trg.Name, key), &aiv1alpha1.Chain{}
	default:
		return "", nil
	}
	err := r.reader().Get(ctx, types.NamespacedName{Namespace: trg.Namespace, Name: name}, obj)
	switch {
	case err == nil:
		// creationTimestamp has one-second resolution.
		if obj.GetCreationTimestamp().Time.Before(streamCreated.Truncate(time.Second)) {
			return "", nil
		}
		return name, nil
	case apierrors.IsNotFound(err):
		return "", nil
	}
	return "", err
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
// changes the status written with the firing. A manual firing (run-now) fires
// while the Trigger is suspended and keeps defaultKey: spec.dedupeKey is not
// rendered, so the run's name is known to whoever asked for it.
func (r *TriggerReconciler) fire(ctx context.Context, nn types.NamespacedName, ev TriggerEvent, defaultKey, subject string, mutate func(*aiv1alpha1.TriggerStatus), manual *triggerManualFiring) (_ triggerOutcome, retErr error) {
	mu := r.fireLock(nn)
	mu.Lock()
	defer mu.Unlock()

	trg := &aiv1alpha1.Trigger{}
	if err := r.reader().Get(ctx, nn, trg); err != nil {
		return triggerRetry, err
	}
	if (trg.Suspended() && manual == nil) || !trg.DeletionTimestamp.IsZero() {
		return triggerRetry, nil
	}
	if wh := trg.Spec.Source.Webhook; manual == nil && wh != nil && wh.EventType != "" && webhookEventType(ev.Headers) != wh.EventType {
		// Filtered out: the event stays on the stream, nothing fires and the
		// status is not churned by events the Trigger never wanted.
		return triggerHandled, nil
	}
	now := r.now()
	firing := aiv1alpha1.TriggerFiring{Time: metav1.NewTime(now)}
	table := r.triggerTable(ctx, trg)
	// A failed attempt is retried (and counted again) until it succeeds.
	defer func() {
		if retErr != nil {
			countTriggerFiring(table, trg, triggerOutcomeError)
		}
	}()
	// metricOutcome overrides how skip counts a firing (deduped).
	metricOutcome := ""
	skip := func(outcome triggerOutcome, format string, args ...any) (triggerOutcome, error) {
		firing.SkipReason = fmt.Sprintf(format, args...)
		if err := r.recordFiring(ctx, nn, firing, mutate); err != nil {
			return triggerRetry, err
		}
		switch {
		case metricOutcome != "":
			countTriggerFiring(table, trg, metricOutcome)
		case outcome == triggerTerm:
			countTriggerFiring(table, trg, triggerOutcomeError)
		default:
			countTriggerFiring(table, trg, triggerOutcomeSkipped)
		}
		if outcome == triggerTerm {
			r.event(trg, corev1.EventTypeWarning, "EventRejected", "%s", firing.SkipReason)
		}
		if trg.Spec.Source.Schedule != nil && outcome == triggerHandled && metricOutcome == "" && manual == nil {
			// A retried slot (dedupe) is not reported: only a skipped one.
			r.event(trg, corev1.EventTypeNormal, "FiringSkipped", "%s (schedule time %s)",
				firing.SkipReason, ev.Time.UTC().Format(time.RFC3339))
		}
		return outcome, nil
	}

	var item *githubItem
	if trg.Spec.Source.GitHub != nil {
		if it, err := githubEventItem(ev); err == nil {
			item = &it
		}
	}
	if item != nil && !githubRepoListed(trg.Spec.Source.GitHub, item.Repo) {
		// The events subject is writable by any NATS client, and repositories
		// may have been edited since the event was published.
		return skip(triggerHandled, "%s", githubRepoNotListedReason(item.Repo))
	}
	stateItem := item != nil && githubStateSource(trg.Spec.Source.GitHub)
	feedbackTable := ""
	if item != nil {
		feedbackTable = prFeedbackTable(trg)
	}
	if feedbackTable != "" {
		// PR feedback: the input names the author knight and its run.
		extra, reason, err := r.prFeedbackAuthor(ctx, trg, feedbackTable, *item)
		if err != nil {
			return triggerRetry, err
		}
		if reason != "" {
			firing.Key = truncate(defaultKey, 253)
			return skip(triggerHandled, "%s", reason)
		}
		ev.Data = withEventFields(ev.Data, extra)
	}

	data := triggerTemplateData{Event: ev, Trigger: triggerRef{Name: trg.Name, Namespace: trg.Namespace}}
	input, err := renderTriggerTemplate("input", trg.Spec.Input, data)
	if err != nil {
		return skip(triggerTerm, "input template: %v", err)
	}
	key := defaultKey
	if trg.Spec.DedupeKey != "" && manual == nil {
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

	if ev.legacyKey != "" && key == defaultKey && manual == nil {
		run, err := r.legacyKeyFired(ctx, trg, ev.legacyKey, ev.streamCreated)
		if err != nil {
			return triggerRetry, err
		}
		if run != "" {
			metricOutcome = triggerOutcomeDeduped
			return skip(triggerHandled, "duplicate dedupe key: run %s exists under the legacy key %s", run, ev.legacyKey)
		}
	}

	// A schedule firing consumed after startingDeadlineSeconds is stale (the
	// operator was down, or the Trigger was not Ready): it is skipped, so a
	// backlog does not start a burst of runs. ev.Time is the schedule time.
	if src := trg.Spec.Source.Schedule; src != nil && manual == nil && src.StartingDeadlineSeconds != nil &&
		now.Sub(ev.Time) > time.Duration(*src.StartingDeadlineSeconds)*time.Second {
		return skip(triggerHandled, "missed schedule time %s by more than startingDeadlineSeconds", ev.Time.UTC().Format(time.RFC3339))
	}

	if trg.Spec.Target.Mission != nil {
		return r.fireMission(ctx, missionFiring{
			nn: nn, trg: trg, data: data, key: key, firing: &firing, now: now, table: table, mutate: mutate, manual: manual,
			skip: skip,
			deduped: func(format string, args ...any) (triggerOutcome, error) {
				metricOutcome = triggerOutcomeDeduped
				defer func() { metricOutcome = "" }()
				return skip(triggerHandled, format, args...)
			},
		})
	}
	if trg.Spec.Target.Knight != nil {
		return r.fireKnight(ctx, knightFiring{
			nn: nn, trg: trg, data: data, key: key, firing: &firing, now: now, table: table, mutate: mutate, manual: manual,
			skip: skip,
			deduped: func(format string, args ...any) (triggerOutcome, error) {
				metricOutcome = triggerOutcomeDeduped
				defer func() { metricOutcome = "" }()
				return skip(triggerHandled, format, args...)
			},
		})
	}
	chainName := aiv1alpha1.RefName(trg.Spec.Target.ChainRef)
	tmpl := &aiv1alpha1.Chain{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: nn.Namespace, Name: chainName}, tmpl); err != nil {
		if manual != nil && apierrors.IsNotFound(err) {
			// A run-now request can never fire without its Chain: record
			// it as skipped and let the request be cleared.
			return skip(triggerHandled, "target chain %q not found", chainName)
		}
		return triggerRetry, fmt.Errorf("get target chain %q: %w", chainName, err)
	}
	name := triggerRunName(trg, tmpl.Name, key, item)

	// duplicate handles an event whose run already exists. A github item
	// whose claim failed when its run started is published again by the
	// next poll: apply the claim now.
	duplicate := func() (triggerOutcome, error) {
		if pendingGitHubClaim(trg, ev) == nil {
			metricOutcome = triggerOutcomeDeduped
			defer func() { metricOutcome = "" }()
			return skip(triggerHandled, "duplicate dedupe key: run %s exists", name)
		}
		if reason := r.githubAfterRun(ctx, trg, ev, name); reason != "" {
			firing.Run = name
			return skip(triggerHandled, "%s", reason)
		}
		return triggerHandled, nil
	}

	// rerun is a finished run of a github item whose claim was applied and
	// that is selected again (its selecting label was put back): it runs
	// again under the same name.
	// A state-selector item has one run, started again for each new
	// failure: attempt counts its runs.
	var rerun *aiv1alpha1.Chain
	attempt := int32(1)
	existing := &aiv1alpha1.Chain{}
	err = r.reader().Get(ctx, types.NamespacedName{Namespace: nn.Namespace, Name: name}, existing)
	switch {
	case err == nil && stateItem:
		state, runs := githubItemRun(trg, existing, key)
		switch state {
		case githubItemRunDuplicate:
			return duplicate()
		case githubItemRunBusy:
			return skip(triggerHandled, "run %s of %s is active", name, item.key())
		case githubItemRunExhausted:
			return skip(triggerHandled, "maxRunsPerItem %d reached for %s: the poller hands it back", runs, item.key())
		}
		rerun, attempt = existing, runs+1
	case err == nil:
		if owner := githubRunRepo(existing); item != nil && pickupTable(trg) != "" && owner != "" && owner != item.Repo {
			return skip(triggerHandled, "run %s already belongs to %s", name, owner)
		}
		if !githubRerun(trg, existing, ev) {
			return duplicate()
		}
		rerun = existing
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
		if n := githubMaxPerRepo(trg); n > 0 && item != nil && githubActiveIn(active, item.Repo) >= n {
			return skip(triggerHandled, "maxActivePerRepository %d reached for %s", n, item.Repo)
		}
	}

	if stateItem {
		if feedbackTable != "" {
			// Fetched only now that a run starts: it costs API calls.
			ev.Data = withEventFields(ev.Data, map[string]any{"log": r.prFeedbackLog(ctx, trg, *item)})
			data.Event = ev
		}
		data.Run = triggerRunRef{Name: name, Attempt: attempt}
		if input, err = renderTriggerTemplate("input", trg.Spec.Input, data); err != nil {
			return skip(triggerTerm, "input template: %v", err)
		}
	}
	if trg.Spec.Input == "" {
		input = tmpl.Spec.Input
	}
	opts := chainrun.StartOptions{
		Name:        name,
		Namespace:   nn.Namespace,
		Input:       input,
		Trigger:     strconv.FormatInt(now.UnixNano(), 10),
		Source:      chainrun.SourceTrigger,
		Labels:      map[string]string{aiv1alpha1.LabelTrigger: trg.Name},
		Annotations: map[string]string{aiv1alpha1.AnnotationTriggerKey: truncate(key, triggerKeyAnnotationMax)},
		Owner:       trg,
		Existing:    rerun,
	}
	if subject != "" {
		opts.Annotations[aiv1alpha1.AnnotationTriggerSubject] = subject
	}
	if manual != nil && manual.caller != "" {
		opts.Annotations[aiv1alpha1.AnnotationCaller] = manual.caller
	}
	if item != nil {
		if err := r.githubRunOptions(ctx, trg, *item, &opts); err != nil {
			return triggerRetry, err
		}
	}
	if stateItem {
		opts.Annotations[aiv1alpha1.AnnotationTriggerItemRuns] = strconv.Itoa(int(attempt))
		opts.Annotations[aiv1alpha1.AnnotationTriggerHandedBack] = ""
	}
	_, err = chainrun.Start(ctx, r.Client, r.Scheme, tmpl, opts)
	if apierrors.IsAlreadyExists(err) {
		return duplicate()
	}
	if err != nil {
		return triggerRetry, fmt.Errorf("start run %s: %w", name, err)
	}

	firing.Run = name
	countTriggerFiring(table, trg, triggerOutcomeStarted)
	// Claim only once the run exists: a failed claim leaves the item
	// selected, and the next poll retries the claim (the run is found by
	// name, so nothing starts twice).
	firing.SkipReason = r.githubAfterRun(ctx, trg, ev, name)
	if err := r.recordFiring(ctx, nn, firing, func(st *aiv1alpha1.TriggerStatus) {
		at := metav1.NewTime(now)
		st.LastFiredAt = &at
		metrics.TriggerLastFiredTimestampSeconds.WithLabelValues(trg.Namespace, table, trg.Name).Set(float64(now.Unix()))
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
	if rerun != nil {
		r.event(trg, corev1.EventTypeNormal, "Fired", "Started run %s again", name)
	} else {
		r.event(trg, corev1.EventTypeNormal, "Fired", "Started run %s", name)
	}
	return triggerHandled, nil
}

// noTableLabel is the table label of a signal with no table.
const noTableLabel = "none"

// deleteTriggerLastFired drops a deleted Trigger's last-fired gauge series, so
// an alert on its age does not fire forever. The counter is left alone: a
// counter series that stops growing is normal.
func deleteTriggerLastFired(namespace, trigger string) {
	metrics.TriggerLastFiredTimestampSeconds.DeletePartialMatch(prometheus.Labels{"namespace": namespace, "trigger": trigger})
}

// Outcomes of roundtable_trigger_firings_total.
const (
	triggerOutcomeStarted = "started"
	triggerOutcomeDeduped = "deduped"
	triggerOutcomeSkipped = "skipped"
	triggerOutcomeError   = "error"
)

// triggerTable is the table label of a Trigger's metrics: its target chain's
// roundTableRef or target knight's RoundTable, else the Trigger's own, else
// none.
func (r *TriggerReconciler) triggerTable(ctx context.Context, trg *aiv1alpha1.Trigger) string {
	switch target := trg.Spec.Target; {
	case target.Knight != nil:
		if name, err := r.knightResolver().KnightTable(ctx, trg.Namespace, target.Knight.Name); err == nil && name != "" {
			return name
		}
	case target.ChainRef != nil:
		chain := &aiv1alpha1.Chain{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: trg.Namespace, Name: target.ChainRef.Name}, chain); err == nil {
			if name := aiv1alpha1.RefName(chain.Spec.RoundTableRef); name != "" {
				return name
			}
		}
	}
	if name := aiv1alpha1.RefName(trg.Spec.RoundTableRef); name != "" {
		return name
	}
	return noTableLabel
}

// triggerSource names the Trigger's source for the source label.
func triggerSource(trg *aiv1alpha1.Trigger) string {
	switch src := trg.Spec.Source; {
	case src.Schedule != nil:
		return "schedule"
	case src.Webhook != nil:
		return "webhook"
	case src.GitHub != nil:
		return "github"
	case src.NATS != nil:
		return "nats"
	}
	return "unknown"
}

// countFinishedRuns counts the firings of a knight or mission target whose
// outcome went from empty to a final value between before and after.
func (r *TriggerReconciler) countFinishedRuns(ctx context.Context, before, after *aiv1alpha1.Trigger) {
	pending := map[string]bool{}
	for _, f := range before.Status.RecentFirings {
		if f.Run != "" && f.Outcome == "" && f.SkipReason == "" {
			pending[f.Run] = true
		}
	}
	if len(pending) == 0 {
		return
	}
	table := ""
	for _, f := range after.Status.RecentFirings {
		if !pending[f.Run] || f.Outcome == "" {
			continue
		}
		if table == "" {
			table = r.triggerTable(ctx, after)
		}
		countTriggerRunFinished(table, after, string(f.Outcome))
		delete(pending, f.Run)
	}
}

// countTriggerRunFinished counts a knight or mission firing that reached a
// final outcome (succeeded, failed, lost).
func countTriggerRunFinished(table string, trg *aiv1alpha1.Trigger, outcome string) {
	metrics.TriggerRunsFinishedTotal.WithLabelValues(table, trg.Name, triggerSource(trg), outcome).Inc()
}

func countTriggerFiring(table string, trg *aiv1alpha1.Trigger, outcome string) {
	metrics.TriggerFiringsTotal.WithLabelValues(table, trg.Name, triggerSource(trg), outcome).Inc()
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

// listRuns returns the Trigger's runs, oldest first. The runs of a Trigger
// generated by a RoundTable's issue pickup are all of the table's pickup
// Chains, whichever engine started them, so caps and dedupe hold across an
// engine switch.
func (r *TriggerReconciler) listRuns(ctx context.Context, trg *aiv1alpha1.Trigger) ([]aiv1alpha1.Chain, error) {
	runs := &aiv1alpha1.ChainList{}
	sel := client.MatchingLabels{aiv1alpha1.LabelTrigger: trg.Name}
	table := pickupTable(trg)
	if table != "" {
		sel = client.MatchingLabels{aiv1alpha1.LabelPickupRoundTable: table}
	}
	if err := r.reader().List(ctx, runs, client.InNamespace(trg.Namespace), sel); err != nil {
		return nil, fmt.Errorf("list trigger runs: %w", err)
	}
	items := slices.DeleteFunc(runs.Items, func(c aiv1alpha1.Chain) bool {
		return (table == "" && !metav1.IsControlledBy(&c, trg)) || !c.DeletionTimestamp.IsZero()
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

// scheduleRunTTL raises the TTL of a schedule source's finished runs to the
// result retention, so their results stay readable that long. 0 (keep
// forever) and other sources are left as they are.
func scheduleRunTTL(trg *aiv1alpha1.Trigger, ttl *int32, retention time.Duration) *int32 {
	if retention <= 0 || trg.Spec.Source.Schedule == nil || ttl == nil || *ttl <= 0 {
		return ttl
	}
	return new(max(*ttl, int32(min(retention/time.Second, 1<<31-1))))
}

// collectRuns deletes finished runs past ttlSecondsAfterFinished and returns
// the active run names and when the next finished run expires (0: none).
func (r *TriggerReconciler) collectRuns(ctx context.Context, trg *aiv1alpha1.Trigger, now time.Time) ([]string, time.Duration, error) {
	runs, err := r.listRuns(ctx, trg)
	if err != nil {
		return nil, 0, err
	}
	ttl := trg.Spec.TTLSecondsAfterFinished
	if pickupTable(trg) != "" {
		// Issue pickup deletes its finished runs once their post-run
		// checks are done (ttlSecondsAfterFinished of spec.issuePickup).
		ttl = nil
	}
	ttl = scheduleRunTTL(trg, ttl, r.ScheduleRetention)
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
	r.pollers = newTriggerPollers(mgr.GetLogger().WithName("trigger-github"))
	r.init()
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	// Stop the fetch loops when the manager stops (or loses leadership).
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		<-ctx.Done()
		r.pollers.stopAll()
		r.consumers.stopAll()
		return nil
	})); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiv1alpha1.Trigger{}).
		Owns(&aiv1alpha1.Chain{}).
		Owns(&aiv1alpha1.Mission{}).
		// A CronJob's status.lastScheduleTime is the Trigger's lastScheduledAt.
		Owns(&batchv1.CronJob{}).
		// The CronJob's firing Jobs carry the Trigger's label; a failed one
		// turns ScheduleHealthy False.
		Watches(&batchv1.Job{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
			name := obj.GetLabels()[aiv1alpha1.LabelTrigger]
			if name == "" || obj.GetLabels()[labelManagedBy] != triggerManagedBy {
				return nil
			}
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: name}}}
		})).
		Named("trigger").
		Complete(r)
}
