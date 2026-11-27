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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/roundtable-k8s/roundtable/internal/status"
)

// writeStatus is the one status write of every controller: it retries an
// optimistic-lock conflict on the latest resourceVersion (see status.Update).
func writeStatus(ctx context.Context, c client.Client, obj client.Object) error {
	return status.Update(ctx, c, obj)
}

// quietConflict turns a conflict that escaped a reconcile into a quiet
// requeue: neither an ERROR line nor a counted reconcile error.
func quietConflict(ctx context.Context, res ctrl.Result, err error) (ctrl.Result, error) {
	if err != nil && apierrors.IsConflict(err) {
		logf.FromContext(ctx).V(1).Info("Conflict writing status; requeueing", "error", err.Error())
		return ctrl.Result{RequeueAfter: RequeueFast}, nil
	}
	return res, err
}
