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

package knight

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func TestWithSharedWorkspaceTableLabels(t *testing.T) {
	s := runtime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	rt := &aiv1alpha1.RoundTable{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "default"},
		Spec: aiv1alpha1.RoundTableSpec{
			SharedWorkspace: &aiv1alpha1.SharedWorkspaceConfig{ClaimName: "shared-pvc", MountPath: "/work"},
		},
	}
	reader := fake.NewClientBuilder().WithScheme(s).WithObjects(rt).Build()

	cases := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{"current label only", map[string]string{aiv1alpha1.LabelRoundTable: "dev"}, true},
		{"legacy label only", map[string]string{LegacyTableLabel: "dev"}, true},
		{"both labels", map[string]string{aiv1alpha1.LabelRoundTable: "dev", LegacyTableLabel: "dev"}, true},
		{"neither label", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k := &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: "k", Namespace: "default", Labels: tc.labels}}
			spec := NewPodBuilder(k, "img").WithReader(reader).WithSharedWorkspace(context.Background()).Build(context.Background())
			var vol, mount bool
			for _, v := range spec.Volumes {
				if v.Name == "shared-workspace" && v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == "shared-pvc" {
					vol = true
				}
			}
			for _, m := range spec.Containers[0].VolumeMounts {
				if m.Name == "shared-workspace" && m.MountPath == "/work" {
					mount = true
				}
			}
			if vol != tc.want || mount != tc.want {
				t.Fatalf("volume=%v mount=%v, want both %v", vol, mount, tc.want)
			}
		})
	}
}
