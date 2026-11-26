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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/gateway"
)

// DefaultTriggerGatewayAudience is the audience of the projected token a
// firing pod sends: the gateway's default ServiceAccount token audience.
const DefaultTriggerGatewayAudience = gateway.DefaultServiceAccountAudience

const (
	// triggerObjectPrefix prefixes the CronJob and ServiceAccount of a
	// scheduled Trigger: trigger-<name>.
	triggerObjectPrefix = "trigger-"
	// cronJobNameMax is the longest CronJob name (the Jobs it creates append
	// an 11-character suffix to a 63-character label value).
	cronJobNameMax = 52
	// triggerTokenDir and triggerTokenFile locate the projected
	// ServiceAccount token the firing pod sends to the gateway.
	triggerTokenDir  = "/var/run/secrets/roundtable/gateway"
	triggerTokenFile = "token"
	// triggerJobNameEnv carries the Job's name (the pod's
	// batch.kubernetes.io/job-name label) into the firing pod.
	triggerJobNameEnv = "ROUNDTABLE_JOB_NAME"
	// triggerTokenExpiry is the projected token's lifetime (the shortest the
	// API server accepts).
	triggerTokenExpiry int64 = 600
	// triggerJobDeadline bounds one firing pod: it makes a single call.
	triggerJobDeadline int64 = 120
	// triggerJobBackoff retries a failed call (the firing key makes a retry
	// fire at most once).
	triggerJobBackoff int32 = 2
	// triggerDefaultTimeZone is a schedule's zone when timeZone is unset.
	triggerDefaultTimeZone = "Etc/UTC"
	// triggerManagedBy is the app.kubernetes.io/managed-by value on the
	// objects the operator renders for a Trigger.
	triggerManagedBy = "roundtable-operator"
)

// TriggerScheduleConfig is how the operator renders the CronJob of a
// scheduled Trigger.
type TriggerScheduleConfig struct {
	// Image is the operator's own image; the firing pod runs its
	// fire-trigger subcommand.
	Image string
	// ImagePullPolicy of the firing pod ("" = the cluster default).
	ImagePullPolicy corev1.PullPolicy
	// ImagePullSecrets of the firing pod; they must exist in the Trigger's
	// namespace.
	ImagePullSecrets []corev1.LocalObjectReference
	// NodeSelector, Tolerations and PriorityClassName place the firing pod.
	NodeSelector      map[string]string
	Tolerations       []corev1.Toleration
	PriorityClassName string
	// GatewayURL is the gateway base URL the firing pod calls.
	GatewayURL string
	// Audience is the gateway's ServiceAccount token audience.
	Audience string
}

// TriggerServiceAccountName is the ServiceAccount a scheduled Trigger's
// CronJob runs as: trigger-<name>. The gateway accepts a firing of the
// Trigger from this ServiceAccount.
func TriggerServiceAccountName(trigger string) string {
	return triggerObjectPrefix + trigger
}

// triggerCronJobName is trigger-<name>, shortened with a hash suffix when
// longer than a CronJob name may be.
func triggerCronJobName(trigger string) string {
	name := triggerObjectPrefix + trigger
	if len(name) <= cronJobNameMax {
		return name
	}
	sum := sha256.Sum256([]byte(trigger))
	return name[:cronJobNameMax-9] + "-" + hex.EncodeToString(sum[:])[:8]
}

// cronConcurrencyPolicy maps a Trigger's concurrency policy onto the
// CronJob's.
func cronConcurrencyPolicy(p aiv1alpha1.TriggerConcurrencyPolicy) batchv1.ConcurrencyPolicy {
	switch p {
	case aiv1alpha1.TriggerConcurrencyForbid:
		return batchv1.ForbidConcurrent
	case aiv1alpha1.TriggerConcurrencyReplace:
		return batchv1.ReplaceConcurrent
	default:
		return batchv1.AllowConcurrent
	}
}

func triggerObjectLabels(trg *aiv1alpha1.Trigger) map[string]string {
	return map[string]string{
		aiv1alpha1.LabelTrigger:        trg.Name,
		"app.kubernetes.io/managed-by": triggerManagedBy,
		"app.kubernetes.io/component":  "trigger-schedule",
	}
}

// desiredTriggerCronJob renders the CronJob of a scheduled Trigger.
func desiredTriggerCronJob(trg *aiv1alpha1.Trigger, src *aiv1alpha1.TriggerScheduleSource, cfg TriggerScheduleConfig) *batchv1.CronJob {
	tz := src.TimeZone
	if tz == "" {
		tz = triggerDefaultTimeZone
	}
	labels := triggerObjectLabels(trg)
	tokenVolume := "gateway-token"
	pod := corev1.PodSpec{
		ServiceAccountName:           TriggerServiceAccountName(trg.Name),
		AutomountServiceAccountToken: new(false),
		RestartPolicy:                corev1.RestartPolicyNever,
		ImagePullSecrets:             cfg.ImagePullSecrets,
		NodeSelector:                 cfg.NodeSelector,
		Tolerations:                  cfg.Tolerations,
		PriorityClassName:            cfg.PriorityClassName,
		SecurityContext: &corev1.PodSecurityContext{
			RunAsNonRoot:   new(true),
			SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
		Containers: []corev1.Container{{
			Name:            "fire",
			Image:           cfg.Image,
			ImagePullPolicy: cfg.ImagePullPolicy,
			Command:         []string{"/manager"},
			Args: []string{
				"fire-trigger",
				"--gateway=" + cfg.GatewayURL,
				"--trigger=" + trg.Namespace + "/" + trg.Name,
				"--token-file=" + path.Join(triggerTokenDir, triggerTokenFile),
				// The CronJob names each Job <cronjob>-<scheduled unix minutes>;
				// fire-trigger reads the schedule time from it, so a late or
				// retried run keeps its slot's firing key.
				"--job-name=$(" + triggerJobNameEnv + ")",
			},
			Env: []corev1.EnvVar{{
				Name: triggerJobNameEnv,
				ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.labels['batch.kubernetes.io/job-name']"}},
			}},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("10m"),
					corev1.ResourceMemory: resource.MustParse("16Mi"),
				},
				Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("64Mi")},
			},
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: new(false),
				ReadOnlyRootFilesystem:   new(true),
				RunAsNonRoot:             new(true),
				Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			},
			VolumeMounts: []corev1.VolumeMount{{Name: tokenVolume, MountPath: triggerTokenDir, ReadOnly: true}},
		}},
		Volumes: []corev1.Volume{{
			Name: tokenVolume,
			VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
				Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
					Audience:          cfg.Audience,
					ExpirationSeconds: new(triggerTokenExpiry),
					Path:              triggerTokenFile,
				}}},
			}},
		}},
	}
	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: triggerCronJobName(trg.Name), Namespace: trg.Namespace, Labels: labels},
		Spec: batchv1.CronJobSpec{
			Schedule:                   src.Cron,
			TimeZone:                   new(tz),
			StartingDeadlineSeconds:    src.StartingDeadlineSeconds,
			ConcurrencyPolicy:          cronConcurrencyPolicy(trg.Spec.Concurrency.Policy),
			Suspend:                    new(trg.Suspended()),
			SuccessfulJobsHistoryLimit: ptr.To[int32](1),
			FailedJobsHistoryLimit:     ptr.To[int32](2),
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: batchv1.JobSpec{
					BackoffLimit:          new(triggerJobBackoff),
					ActiveDeadlineSeconds: new(triggerJobDeadline),
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: labels},
						Spec:       pod,
					},
				},
			},
		},
	}
}

// errNotOwned reports an object of a scheduled Trigger's name that the
// Trigger does not control.
type errNotOwned struct{ kind, name string }

func (e errNotOwned) Error() string {
	return fmt.Sprintf("%s %q exists and is not owned by this Trigger", e.kind, e.name)
}

// ensureScheduleObjects creates or updates the ServiceAccount and CronJob of
// a scheduled Trigger and returns the CronJob as stored. An object of the
// same name that the Trigger does not control is left alone (errNotOwned).
func (r *TriggerReconciler) ensureScheduleObjects(ctx context.Context, trg *aiv1alpha1.Trigger, src *aiv1alpha1.TriggerScheduleSource) (*batchv1.CronJob, error) {
	log := logf.FromContext(ctx)

	sa := &corev1.ServiceAccount{}
	saKey := types.NamespacedName{Namespace: trg.Namespace, Name: TriggerServiceAccountName(trg.Name)}
	switch err := r.Get(ctx, saKey, sa); {
	case apierrors.IsNotFound(err):
		sa = &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{Name: saKey.Name, Namespace: saKey.Namespace, Labels: triggerObjectLabels(trg)},
			// The firing pod mounts only its projected gateway token.
			AutomountServiceAccountToken: new(false),
		}
		if err := controllerutil.SetControllerReference(trg, sa, r.Scheme); err != nil {
			return nil, err
		}
		if err := r.Create(ctx, sa); err != nil {
			return nil, err
		}
		log.Info("Created ServiceAccount", "name", sa.Name)
	case err != nil:
		return nil, err
	case !metav1.IsControlledBy(sa, trg):
		return nil, errNotOwned{kind: "ServiceAccount", name: sa.Name}
	}

	want := desiredTriggerCronJob(trg, src, r.Schedule)
	cj := &batchv1.CronJob{}
	switch err := r.Get(ctx, client.ObjectKeyFromObject(want), cj); {
	case apierrors.IsNotFound(err):
		if err := controllerutil.SetControllerReference(trg, want, r.Scheme); err != nil {
			return nil, err
		}
		if err := r.Create(ctx, want); err != nil {
			return nil, err
		}
		log.Info("Created CronJob", "name", want.Name, "schedule", want.Spec.Schedule)
		return want, nil
	case err != nil:
		return nil, err
	case !metav1.IsControlledBy(cj, trg):
		return nil, errNotOwned{kind: "CronJob", name: cj.Name}
	}
	// Compare only what the operator sets: the API server fills in defaults
	// that must not count as drift.
	// DeepDerivative ignores fields empty in the desired spec, so the fields
	// the operator may clear (pod placement, startingDeadlineSeconds, the
	// firing container's pull policy) are compared explicitly: clearing one
	// must propagate.
	wantPod := want.Spec.JobTemplate.Spec.Template.Spec
	gotPod := cj.Spec.JobTemplate.Spec.Template.Spec
	if equality.Semantic.DeepDerivative(want.Spec, cj.Spec) && equality.Semantic.DeepDerivative(want.Labels, cj.Labels) &&
		podPlacementEqual(wantPod, gotPod) &&
		startingDeadlineEqual(want.Spec.StartingDeadlineSeconds, cj.Spec.StartingDeadlineSeconds) &&
		pullPolicyEqual(firePullPolicy(wantPod), firePullPolicy(gotPod), firePullImage(wantPod)) {
		return cj, nil
	}
	cj.Spec = want.Spec
	cj.Labels = want.Labels
	if err := r.Update(ctx, cj); err != nil {
		return nil, err
	}
	log.Info("Updated CronJob", "name", cj.Name, "schedule", cj.Spec.Schedule, "suspend", *cj.Spec.Suspend)
	return cj, nil
}

// podPlacementEqual compares the operator-configured scheduling fields of two
// firing pod specs, treating nil and empty as equal.
func podPlacementEqual(a, b corev1.PodSpec) bool {
	return equality.Semantic.DeepEqual(a.Tolerations, b.Tolerations) &&
		equality.Semantic.DeepEqual(a.ImagePullSecrets, b.ImagePullSecrets) &&
		a.PriorityClassName == b.PriorityClassName &&
		len(a.NodeSelector) == len(b.NodeSelector) &&
		(len(a.NodeSelector) == 0 || equality.Semantic.DeepEqual(a.NodeSelector, b.NodeSelector))
}

// startingDeadlineEqual compares startingDeadlineSeconds exactly: nil means
// unset on both sides, so clearing the field on the Trigger reaches the CronJob.
func startingDeadlineEqual(a, b *int64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// firePullPolicy returns the pull policy of the firing container, or "" when
// the container is absent.
func firePullPolicy(pod corev1.PodSpec) corev1.PullPolicy {
	for _, c := range pod.Containers {
		if c.Name == "fire" {
			return c.ImagePullPolicy
		}
	}
	return ""
}

// firePullImage returns the image of the firing container, or "" when absent.
func firePullImage(pod corev1.PodSpec) string {
	for _, c := range pod.Containers {
		if c.Name == "fire" {
			return c.Image
		}
	}
	return ""
}

// pullPolicyEqual compares a desired and an observed pull policy. An unset
// desired policy matches the value the API server defaults it to for image,
// so that defaulting does not cause an update on every reconcile.
func pullPolicyEqual(want, got corev1.PullPolicy, image string) bool {
	if want != "" {
		return want == got
	}
	return got == "" || got == defaultImagePullPolicy(image)
}

// defaultImagePullPolicy mirrors the API server's defaulting of an empty
// container pull policy: an image whose tag is "latest" (or that has no tag)
// is pulled Always, any other image IfNotPresent.
func defaultImagePullPolicy(image string) corev1.PullPolicy {
	ref := image
	if i := strings.Index(ref, "@"); i >= 0 {
		ref = ref[:i]
	}
	last := ref[strings.LastIndex(ref, "/")+1:]
	if i := strings.LastIndex(last, ":"); i >= 0 && last[i+1:] != "latest" {
		return corev1.PullIfNotPresent
	}
	return corev1.PullAlways
}

// deleteScheduleObjects deletes the CronJob and ServiceAccount a Trigger
// owns once its schedule source is gone. Objects it does not control are
// left alone.
func (r *TriggerReconciler) deleteScheduleObjects(ctx context.Context, trg *aiv1alpha1.Trigger) error {
	objs := map[string]client.Object{
		"CronJob":        &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Namespace: trg.Namespace, Name: triggerCronJobName(trg.Name)}},
		"ServiceAccount": &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: trg.Namespace, Name: TriggerServiceAccountName(trg.Name)}},
	}
	for kind, obj := range objs {
		if err := r.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if !metav1.IsControlledBy(obj, trg) {
			continue
		}
		if err := r.Delete(ctx, obj, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
			return err
		}
		logf.FromContext(ctx).Info("Deleted "+kind+" of removed schedule source", "name", obj.GetName())
	}
	return nil
}

// observeFireJobs records the schedule's health on trg.Status: the CronJob's
// last successful time and the ScheduleHealthy condition, which is False
// while the CronJob's most recent Job failed. A Warning event FireJobFailed
// is emitted once per failed Job.
func (r *TriggerReconciler) observeFireJobs(ctx context.Context, trg *aiv1alpha1.Trigger, cj *batchv1.CronJob) error {
	trg.Status.LastSuccessfulScheduledAt = cj.Status.LastSuccessfulTime
	// Only this Trigger's firing Jobs carry its LabelTrigger (set on the
	// CronJob's JobTemplate), so list those from the cache instead of every
	// Job in the namespace from the API server. IsControlledBy stays the
	// authority on ownership below.
	jobs := &batchv1.JobList{}
	if err := r.List(ctx, jobs, client.InNamespace(trg.Namespace),
		client.MatchingLabels{aiv1alpha1.LabelTrigger: trg.Name}); err != nil {
		return err
	}
	var latest *batchv1.Job
	for i := range jobs.Items {
		j := &jobs.Items[i]
		if !metav1.IsControlledBy(j, cj) {
			continue
		}
		if latest == nil || latest.CreationTimestamp.Before(&j.CreationTimestamp) ||
			(latest.CreationTimestamp.Equal(&j.CreationTimestamp) && latest.Name < j.Name) {
			latest = j
		}
	}
	cond := metav1.Condition{
		Type: aiv1alpha1.ConditionTriggerScheduleHealthy, Status: metav1.ConditionTrue,
		Reason: aiv1alpha1.ReasonTriggerFireJobsHealthy, Message: "the CronJob's latest firing Job did not fail",
	}
	if latest != nil {
		for _, c := range latest.Status.Conditions {
			if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
				cond.Status = metav1.ConditionFalse
				cond.Reason = aiv1alpha1.ReasonTriggerFireJobFailing
				cond.Message = fmt.Sprintf("Job %s failed: %s", latest.Name, c.Reason)
				if c.Message != "" {
					cond.Message += ": " + c.Message
				}
				break
			}
		}
	}
	if cond.Status == metav1.ConditionFalse {
		prev := meta.FindStatusCondition(trg.Status.Conditions, cond.Type)
		if prev == nil || prev.Status != metav1.ConditionFalse || !strings.HasPrefix(prev.Message, "Job "+latest.Name+" failed") {
			r.event(trg, corev1.EventTypeWarning, "FireJobFailed", "%s", cond.Message)
		}
	}
	meta.SetStatusCondition(&trg.Status.Conditions, cond)
	return nil
}
