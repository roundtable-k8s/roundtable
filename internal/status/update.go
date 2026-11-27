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

package status

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Update writes obj's status through the status subresource. A conflict means
// another writer changed the object between the read and this write; it is
// expected and harmless, so the write is retried against the latest
// resourceVersion with the computed status kept. Only the status subresource
// is written, so a concurrent spec or metadata change is not touched. A
// conflict that survives the retries is returned for the caller to requeue.
func Update(ctx context.Context, c client.Client, obj client.Object) error {
	first := true
	return retry.OnError(retry.DefaultRetry, apierrors.IsConflict, func() error {
		if !first {
			latest, ok := obj.DeepCopyObject().(client.Object)
			if !ok {
				return apierrors.NewConflict(schema.GroupResource{}, obj.GetName(), nil)
			}
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), latest); err != nil {
				return err
			}
			obj.SetResourceVersion(latest.GetResourceVersion())
		}
		first = false
		return c.Status().Update(ctx, obj)
	})
}
