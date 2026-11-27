package controller

import (
	"context"
	"errors"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
)

func TestQuietConflict(t *testing.T) {
	conflict := apierrors.NewConflict(schema.GroupResource{Resource: "knights"}, "k", errors.New("modified"))
	res, err := quietConflict(context.Background(), ctrl.Result{}, conflict)
	if err != nil || !res.Requeue {
		t.Fatalf("conflict must requeue without error, got %v %v", res, err)
	}
	other := errors.New("boom")
	if _, err := quietConflict(context.Background(), ctrl.Result{}, other); !errors.Is(err, other) {
		t.Fatalf("other errors pass through, got %v", err)
	}
}
