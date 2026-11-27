package controller

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// The CRD schema, not the controller, rejects a bad spec.telemetry; removing
// the Enum or Pattern marker from RoundTableTelemetry fails these.
var _ = Describe("RoundTable spec.telemetry validation", func() {
	create := func(name string, tel aiv1alpha1.RoundTableTelemetry) error {
		rt := &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec:       aiv1alpha1.RoundTableSpec{Telemetry: &tel},
		}
		err := k8sClient.Create(ctx, rt)
		if err == nil {
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, rt) })
		}
		return err
	}
	ratio := func(s string) *string { return &s }

	DescribeTable("rejects",
		func(name string, tel aiv1alpha1.RoundTableTelemetry) {
			err := create(name, tel)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err)).To(BeTrue(), "got %v", err)
		},
		Entry("an unknown protocol", "tel-bad-proto", aiv1alpha1.RoundTableTelemetry{OTLPProtocol: "bogus"}),
		Entry("a ratio above 1", "tel-ratio-high", aiv1alpha1.RoundTableTelemetry{SamplingRatio: ratio("1.5")}),
		Entry("a negative ratio", "tel-ratio-neg", aiv1alpha1.RoundTableTelemetry{SamplingRatio: ratio("-0.1")}),
	)

	DescribeTable("accepts",
		func(name string, tel aiv1alpha1.RoundTableTelemetry) {
			Expect(create(name, tel)).To(Succeed())
		},
		Entry("grpc", "tel-grpc", aiv1alpha1.RoundTableTelemetry{OTLPProtocol: "grpc"}),
		Entry("http/protobuf", "tel-http", aiv1alpha1.RoundTableTelemetry{OTLPProtocol: "http/protobuf"}),
		Entry("ratio 0", "tel-ratio-0", aiv1alpha1.RoundTableTelemetry{SamplingRatio: ratio("0")}),
		Entry("ratio 0.25", "tel-ratio-025", aiv1alpha1.RoundTableTelemetry{SamplingRatio: ratio("0.25")}),
		Entry("ratio 1", "tel-ratio-1", aiv1alpha1.RoundTableTelemetry{SamplingRatio: ratio("1")}),
	)
})
