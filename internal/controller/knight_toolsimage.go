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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
)

const (
	// toolsImageFlakeKey is the knight ConfigMap key holding the tools image
	// flake the build Job consumes.
	toolsImageFlakeKey = "tools-image-flake.nix"

	// toolsImageJobTTL keeps finished build Jobs around briefly for log
	// inspection. A failed Job is retried once this expires and it is removed.
	toolsImageJobTTL int32 = 3600
)

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

// toolsImageJobName names the build Job after the knight and the image tag, so
// a changed tool list gets a fresh Job.
func toolsImageJobName(knight *aiv1alpha1.Knight) string {
	return fmt.Sprintf("%s-toolsimg-%s", knight.Name, knightpkg.ToolsImageTag(knight))
}

// reconcileToolsImage ensures the knight's tools image is in the registry. It
// records the published reference in status.toolsImage, which is what stops it
// from building again. The knight's pod references the image regardless, so it
// simply waits in image pull until the build lands — a knight never starts
// without its declared tools.
func (r *KnightReconciler) reconcileToolsImage(ctx context.Context, knight *aiv1alpha1.Knight) error {
	log := logf.FromContext(ctx)

	ref := knightpkg.ToolsImageRef(knight)
	if ref == "" {
		knight.Status.ToolsImage = "" // no Nix tools; persisted by updateStatus
		return nil
	}
	if knight.Status.ToolsImage == ref {
		return nil // already published
	}

	jobName := toolsImageJobName(knight)
	job := &batchv1.Job{}
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
		r.Recorder.Eventf(knight, corev1.EventTypeNormal, "ToolsImageBuildStarted", "Building tools image %s", ref)
		return nil
	} else if err != nil {
		return fmt.Errorf("tools image Job get failed: %w", err)
	}

	// Job exists — react to its terminal state (the owner watch requeues us).
	switch {
	case job.Status.Succeeded > 0:
		knight.Status.ToolsImage = ref // persisted by updateStatus
		log.Info("Published tools image", "job", jobName, "image", ref)
		r.Recorder.Eventf(knight, corev1.EventTypeNormal, "ToolsImagePublished", "Tools image %s published", ref)
	case job.Spec.BackoffLimit != nil && job.Status.Failed > *job.Spec.BackoffLimit:
		r.Recorder.Eventf(knight, corev1.EventTypeWarning, "ToolsImageBuildFailed",
			"Tools image Job %s failed; image %s not published", jobName, ref)
	}
	return nil
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
			BackoffLimit:            ptr.To(int32(2)),
			TTLSecondsAfterFinished: ptr.To(toolsImageJobTTL),
			ActiveDeadlineSeconds:   ptr.To(int64(1800)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: ptr.To(false),
					// Root, like the shared-store builder: the stock Nix image
					// owns /nix as root, and source builds expect to chown and
					// chmod. The pod is short-lived and mounts nothing shared.
					SecurityContext: &corev1.PodSecurityContext{
						RunAsUser:  ptr.To(int64(0)),
						RunAsGroup: ptr.To(int64(0)),
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
								AllowPrivilegeEscalation: ptr.To(true),
								RunAsNonRoot:             ptr.To(false),
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
