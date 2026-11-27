package status

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func TestUpdateRetriesConflictOnFreshVersion(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := aiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	k := &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: "k", Namespace: "ns"}}
	conflicts := 0
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(k).WithStatusSubresource(k).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if conflicts < 2 {
					conflicts++
					return apierrors.NewConflict(schema.GroupResource{Resource: "knights"}, obj.GetName(), nil)
				}
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).Build()

	mine := &aiv1alpha1.Knight{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(k), mine); err != nil {
		t.Fatal(err)
	}
	// Another writer changes the object after our read.
	other := mine.DeepCopy()
	other.Labels = map[string]string{"a": "b"}
	if err := c.Update(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	mine.Status.Phase = aiv1alpha1.KnightPhaseProvisioning

	if err := Update(context.Background(), c, mine); err != nil {
		t.Fatalf("conflict must be retried, got %v", err)
	}
	got := &aiv1alpha1.Knight{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(k), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != aiv1alpha1.KnightPhaseProvisioning || got.Labels["a"] != "b" || conflicts != 2 {
		t.Fatalf("phase=%q labels=%v conflicts=%d", got.Status.Phase, got.Labels, conflicts)
	}
}

func TestUpdateStaleVersionSucceeds(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = aiv1alpha1.AddToScheme(scheme)
	k := &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: "k", Namespace: "ns"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(k).WithStatusSubresource(k).Build()
	mine := &aiv1alpha1.Knight{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(k), mine)
	other := mine.DeepCopy()
	other.Labels = map[string]string{"a": "b"}
	if err := c.Update(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	mine.Status.Ready = true
	if err := Update(context.Background(), c, mine); err != nil {
		t.Fatal(err)
	}
}
