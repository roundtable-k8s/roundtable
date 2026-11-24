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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

var _ = Describe("RuntimeClassName", func() {
	ctx := context.Background()

	newKnight := func(rc *string) *aiv1alpha1.Knight {
		return &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{
				Name: "rc-knight", Namespace: "default",
				Labels: map[string]string{aiv1alpha1.LabelRoundTable: "rc-table"},
			},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "d", RuntimeClassName: rc,
				NATS: aiv1alpha1.KnightNATS{URL: "nats://x:4222", Stream: "s", ResultsStream: "r", Subjects: []string{"a.>"}},
			},
		}
	}
	tableWith := func(rc *string) *aiv1alpha1.RoundTable {
		return &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: "rc-table", Namespace: "default"},
			Spec:       aiv1alpha1.RoundTableSpec{Defaults: &aiv1alpha1.RoundTableDefaults{RuntimeClassName: rc}},
		}
	}
	build := func(k *aiv1alpha1.Knight, rt *aiv1alpha1.RoundTable) *string {
		s := runtime.NewScheme()
		Expect(aiv1alpha1.AddToScheme(s)).To(Succeed())
		r := fake.NewClientBuilder().WithScheme(s).WithObjects(rt).Build()
		ApplyTableDefaults(ctx, r, k)
		return NewPodBuilder(k, "img").Build(ctx).RuntimeClassName
	}
	str := func(s string) *string { return &s }

	It("prefers the knight's value over the table default", func() {
		Expect(build(newKnight(str("kata")), tableWith(str("gvisor")))).To(HaveValue(Equal("kata")))
	})
	It("applies the table default when the knight leaves it unset", func() {
		Expect(build(newKnight(nil), tableWith(str("gvisor")))).To(HaveValue(Equal("gvisor")))
	})
	It("leaves RuntimeClassName nil when nothing is set", func() {
		Expect(build(newKnight(nil), tableWith(nil))).To(BeNil())
	})
	It("treats an empty name as unset", func() {
		Expect(build(newKnight(str("")), tableWith(nil))).To(BeNil())
	})
})
