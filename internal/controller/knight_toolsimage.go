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
	"fmt"
	"net/http"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	rtruntime "github.com/roundtable-k8s/roundtable/pkg/runtime"
)

const (
	// toolsImageFlakeKey is the knight ConfigMap key holding the tools image
	// flake the build Job consumes.
	toolsImageFlakeKey = "tools-image-flake.nix"

	// toolsImageJobTTL keeps finished build Jobs around briefly for log
	// inspection. A failed Job is retried once this expires and it is removed.
	toolsImageJobTTL int32 = 3600

	// toolsImageVerifyInterval is how often the operator re-checks that a
	// published tools image is still in the registry.
	toolsImageVerifyInterval = 10 * time.Minute

	// toolsImageProbeTimeout bounds one registry check.
	toolsImageProbeTimeout = 5 * time.Second
)

// probeToolsImageMissing asks the registry for the image manifest and reports
// whether the registry positively says it is gone (404). Anything else — the
// registry being down, a timeout, an unexpected status — counts as "not known
// to be missing", so an outage never triggers a rebuild of every image.
//
// It is a GET, not a HEAD, on purpose: a registry that expires tags nobody
// pulls (zot's pulledWithin retention) counts a GET as a pull but not a HEAD.
// A knight's pod can run for months without its node pulling the image again,
// so this periodic check is what keeps an image in use from expiring.
func probeToolsImageMissing(ctx context.Context, manifestURL string) bool {
	ctx, cancel := context.WithTimeout(ctx, toolsImageProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("Accept", "application/vnd.oci.image.manifest.v1+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusNotFound
}

// toolsImageLost reports whether the knight's published tools image has
// disappeared from the registry. Checks are throttled per image reference to
// one every toolsImageVerifyInterval (knights sharing an image share the
// check).
func (r *KnightReconciler) toolsImageLost(ctx context.Context, knight *aiv1alpha1.Knight, ref string) bool {
	if at, ok := r.toolsImageVerifiedAt.Load(ref); ok && time.Since(at.(time.Time)) < toolsImageVerifyInterval {
		return false
	}
	probe := r.ToolsImageMissing
	if probe == nil {
		probe = probeToolsImageMissing
	}
	if probe(ctx, knightpkg.ToolsImageManifestURL(knight)) {
		r.toolsImageVerifiedAt.Delete(ref)
		return true
	}
	r.toolsImageVerifiedAt.Store(ref, time.Now())
	return false
}

// toolsImageBuildScript is what the build Job runs. It skips the build when
// the tag is already in the registry (another knight with the same tool list
// got there first), otherwise builds the flake with nix2container and pushes
// the image straight from the Nix store.
const toolsImageBuildScript = `set -eu
if curl -fsS -o /dev/null -I -H "Accept: application/vnd.oci.image.manifest.v1+json" "$TOOLS_IMAGE_MANIFEST_URL" 2>/dev/null; then
  echo "Tools image $TOOLS_IMAGE_PUSH_REF already published; nothing to build"
  exit 0
fi
mkdir -p /work/flake
cp /flake-src/flake.nix /work/flake/flake.nix
cd /work/flake
export NIX_CONFIG="experimental-features = nix-command flakes"
echo "Building and pushing $TOOLS_IMAGE_PUSH_REF"
# shellcheck disable=SC2086
nix run .#tools.copyTo -- $TOOLS_IMAGE_COPY_ARGS "docker://$TOOLS_IMAGE_PUSH_REF"
echo "Published $TOOLS_IMAGE_PUSH_REF"
`

// reconcileNixTools provisions the knight's Nix tools (spec.tools.nix and spec.tools.flakes) as a
// tools image. It returns an interval to come back on while an image is
// published, so the operator notices if the registry loses it. Without a
// tools image repository configured there is nowhere to build to: the knight
// runs without its Nix tools and gets a warning event saying so.
func (r *KnightReconciler) reconcileNixTools(ctx context.Context, knight *aiv1alpha1.Knight) (time.Duration, error) {
	if knight.Spec.Tools != nil {
		for _, f := range knight.Spec.Tools.Flakes {
			if err := aiv1alpha1.ValidateFlakeRefDir(f.Ref); err != nil {
				r.Recorder.Event(knight, corev1.EventTypeWarning, "InvalidFlakeRef", err.Error())
				r.setToolsImageCondition(knight, metav1.ConditionFalse, aiv1alpha1.ReasonInvalidFlakeRef, err.Error())
				return 0, fmt.Errorf("spec.tools.flakes: %w", err)
			}
		}
	}
	if !knightpkg.ToolsImageEnabled() {
		if knightpkg.HasToolsImage(knight) {
			r.Recorder.Event(knight, corev1.EventTypeWarning, "ToolsImageNotConfigured",
				"spec.tools.nix or spec.tools.flakes is set but the operator has no tools image repository (chart value toolsImage.repository); Nix tools are not installed")
		}
		return 0, nil
	}
	if err := r.reconcileToolsImage(ctx, knight); err != nil {
		return 0, err
	}
	if knight.Status.ToolsImage != "" {
		return toolsImageVerifyInterval, nil
	}
	return 0, nil
}

// toolsImageJobName names the build Job after the knight and the image tag, so
// a changed tool list gets a fresh Job.
func toolsImageJobName(knight *aiv1alpha1.Knight) string {
	return fmt.Sprintf("%s-toolsimg-%s", knight.Name, knightpkg.ToolsImageTag(knight))
}

// reconcileToolsImage ensures the knight's tools image is in the registry. It
// records the published reference in status.toolsImage, which is what stops it
// from building again and what the knight's pod mounts. While a changed tool
// list builds, the status still names the previous image, so the pod keeps
// running on it and rolls only when the new one is published. A new knight has
// no previous image: its pod waits in image pull, never starting without its
// declared tools.
func (r *KnightReconciler) reconcileToolsImage(ctx context.Context, knight *aiv1alpha1.Knight) error {
	log := logf.FromContext(ctx)

	ref := knightpkg.ToolsImageRef(knight)
	if ref == "" {
		knight.Status.ToolsImage = "" // no Nix tools; persisted by updateStatus
		meta.RemoveStatusCondition(&knight.Status.Conditions, aiv1alpha1.ConditionKnightToolsImageReady)
		return nil
	}
	jobName := toolsImageJobName(knight)
	job := &batchv1.Job{}

	if knight.Status.ToolsImage == ref {
		if !r.toolsImageLost(ctx, knight, ref) {
			r.setToolsImageCondition(knight, metav1.ConditionTrue, aiv1alpha1.ReasonToolsImagePublished,
				fmt.Sprintf("Tools image %s is published", ref))
			return nil // already published
		}
		// The registry no longer has the image (wiped volume, garbage
		// collection, manual delete). Forget that it was published and build
		// it again. The pod spec does not change, so a running knight keeps
		// going on the copy its node has cached.
		log.Info("Published tools image is missing from the registry", "image", ref)
		r.Recorder.Eventf(knight, corev1.EventTypeWarning, "ToolsImageMissing",
			"Tools image %s is no longer in the registry; rebuilding", ref)
		knight.Status.ToolsImage = "" // persisted by updateStatus
		// A finished Job from the original build would read as "published";
		// remove it so a fresh one is created on the requeue its deletion causes.
		err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: knight.Namespace}, job)
		if err == nil {
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("stale tools image Job delete failed: %w", err)
			}
			return nil
		} else if !apierrors.IsNotFound(err) {
			return fmt.Errorf("tools image Job get failed: %w", err)
		}
	}

	err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: knight.Namespace}, job)
	if apierrors.IsNotFound(err) {
		newJob := r.buildToolsImageJob(knight)
		if err := controllerutil.SetControllerReference(knight, newJob, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, newJob); err != nil {
			return fmt.Errorf("tools image Job create failed: %w", err)
		}
		log.Info("Created tools image Job", "job", jobName, "image", ref)
		r.setToolsImageBuilding(knight, ref)
		r.Recorder.Eventf(knight, corev1.EventTypeNormal, "ToolsImageBuildStarted", "Building tools image %s", ref)
		return nil
	} else if err != nil {
		return fmt.Errorf("tools image Job get failed: %w", err)
	}

	// Job exists — react to its terminal state (the owner watch requeues us).
	switch {
	case job.DeletionTimestamp != nil:
		// Being removed (see the missing-image path above); wait for it to go.
		r.setToolsImageBuilding(knight, ref)
	case job.Status.Succeeded > 0:
		knight.Status.ToolsImage = ref // persisted by updateStatus
		r.setToolsImageCondition(knight, metav1.ConditionTrue, aiv1alpha1.ReasonToolsImagePublished,
			fmt.Sprintf("Tools image %s is published", ref))
		r.toolsImageVerifiedAt.Store(ref, time.Now())
		log.Info("Published tools image", "job", jobName, "image", ref)
		r.Recorder.Eventf(knight, corev1.EventTypeNormal, "ToolsImagePublished", "Tools image %s published", ref)
	case job.Spec.BackoffLimit != nil && job.Status.Failed > *job.Spec.BackoffLimit:
		r.Recorder.Eventf(knight, corev1.EventTypeWarning, "ToolsImageBuildFailed",
			"Tools image Job %s failed; image %s not published", jobName, ref)
		r.setToolsImageCondition(knight, metav1.ConditionFalse, aiv1alpha1.ReasonToolsImageBuildFailed,
			fmt.Sprintf("Tools image Job %s failed; image %s not published and the pod rollout is held", jobName, ref))
	default:
		r.setToolsImageBuilding(knight, ref)
	}
	return nil
}

func (r *KnightReconciler) setToolsImageBuilding(knight *aiv1alpha1.Knight, ref string) {
	r.setToolsImageCondition(knight, metav1.ConditionFalse, aiv1alpha1.ReasonToolsImageBuilding,
		fmt.Sprintf("Tools image %s is building; the pod keeps its current template until it is published", ref))
}

func (r *KnightReconciler) setToolsImageCondition(knight *aiv1alpha1.Knight, status metav1.ConditionStatus, reason, msg string) {
	meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightToolsImageReady,
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: knight.Generation,
	})
}

// toolsImagePending reports whether the knight's desired tools image is not
// published yet.
func toolsImagePending(knight *aiv1alpha1.Knight) bool {
	ref := knightpkg.ToolsImageRef(knight)
	return ref != "" && knight.Status.ToolsImage != ref
}

// holdRollout reports whether the runtime workload must be left as it is
// because the tools image for the current spec is not published. Rolling the
// whole pod template (env, mounts, other spec changes) before the image exists
// would run the new spec on the old tools. A knight with no workload yet is not
// held: there is nothing to roll, and its pod waits in image pull.
func (r *KnightReconciler) holdRollout(ctx context.Context, knight *aiv1alpha1.Knight) (bool, error) {
	if !toolsImagePending(knight) {
		return false, nil
	}
	nn := types.NamespacedName{Name: knight.Name, Namespace: knight.Namespace}
	var obj client.Object = &appsv1.Deployment{}
	if knight.Spec.Runtime == runtimeSandbox {
		obj = rtruntime.NewSandboxObject(knight.Name, knight.Namespace)
	}
	if err := r.Get(ctx, nn, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// buildToolsImageJob constructs the Job that builds and pushes the knight's
// tools image. It needs no PVC: the Nix store lives in the container and is
// thrown away, because the registry is the cache.
func (r *KnightReconciler) buildToolsImageJob(knight *aiv1alpha1.Knight) *batchv1.Job {
	labels := map[string]string{
		"app.kubernetes.io/name":       "knight-toolsimage",
		"app.kubernetes.io/instance":   knight.Name,
		"app.kubernetes.io/managed-by": "roundtable-operator",
		"roundtable.io/purpose":        "tools-image-build",
	}

	copyArgs := ""
	if knightpkg.ToolsImagePushInsecure() {
		copyArgs = "--dest-tls-verify=false"
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      toolsImageJobName(knight),
			Namespace: knight.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            new(int32(2)),
			TTLSecondsAfterFinished: new(toolsImageJobTTL),
			ActiveDeadlineSeconds:   new(int64(1800)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: new(false),
					// Root, like the shared-store builder: the stock Nix image
					// owns /nix as root, and source builds expect to chown and
					// chmod. The pod is short-lived and mounts nothing shared.
					SecurityContext: &corev1.PodSecurityContext{
						RunAsUser:  new(int64(0)),
						RunAsGroup: new(int64(0)),
					},
					Containers: []corev1.Container{
						{
							Name:    "build",
							Image:   knightpkg.ToolsBuilderImage(),
							Command: []string{"sh", "-c", toolsImageBuildScript},
							Env: []corev1.EnvVar{
								{Name: "TOOLS_IMAGE_PUSH_REF", Value: knightpkg.ToolsImagePushRef(knight)},
								{Name: "TOOLS_IMAGE_MANIFEST_URL", Value: knightpkg.ToolsImageManifestURL(knight)},
								{Name: "TOOLS_IMAGE_COPY_ARGS", Value: copyArgs},
								{Name: "TMPDIR", Value: "/work"},
							},
							SecurityContext: &corev1.SecurityContext{
								AllowPrivilegeEscalation: new(true),
								RunAsNonRoot:             new(false),
								SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "flake", MountPath: "/flake-src", ReadOnly: true},
								{Name: "work", MountPath: "/work"},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "flake",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: fmt.Sprintf("knight-%s-config", knight.Name),
									},
									Items: []corev1.KeyToPath{{Key: toolsImageFlakeKey, Path: "flake.nix"}},
								},
							},
						},
						{
							Name:         "work",
							VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
						},
					},
				},
			},
		},
	}
}
