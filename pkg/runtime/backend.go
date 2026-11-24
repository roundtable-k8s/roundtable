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

package runtime

import (
	"context"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// RuntimeBackend abstracts how Knight pods are created and managed.
// Implementations include DeploymentBackend (always-on) and SandboxBackend (scale-to-zero).
//
// Contract (enforced for every backend by conformance_test.go):
//   - Reconcile creates one workload named after the Knight, in its namespace,
//     controlled by the Knight, and updates it when the pod spec changes. The
//     pod template carries the app.kubernetes.io/{name,instance,managed-by}
//     and roundtable.io/domain labels, the roundtable.io/{model,skills,domain}
//     annotations, and the injected pod spec unchanged. Reconcile on a
//     knight that is no longer suspended returns the workload to running.
//   - IsReady is false when the workload is missing or not ready.
//   - Suspend keeps the workload object but stops it running; Resume starts
//     it again. Both are idempotent; Suspend on a missing workload is a no-op.
//   - Cleanup removes the workload and is a no-op when it is already gone.
//
// The one allowed difference: only DeploymentBackend supports more than one
// replica and KEDA autoscaling; SandboxBackend is a singleton.
type RuntimeBackend interface {
	// Reconcile ensures runtime resources exist and match the Knight spec.
	Reconcile(ctx context.Context, knight *aiv1alpha1.Knight) error

	// Cleanup removes runtime resources when a Knight is deleted.
	Cleanup(ctx context.Context, knight *aiv1alpha1.Knight) error

	// IsReady returns whether the knight's runtime is ready to accept tasks.
	IsReady(ctx context.Context, knight *aiv1alpha1.Knight) (bool, error)

	// Suspend pauses the knight's runtime (e.g., scale to zero).
	Suspend(ctx context.Context, knight *aiv1alpha1.Knight) error

	// Resume wakes the knight's runtime.
	Resume(ctx context.Context, knight *aiv1alpha1.Knight) error
}
