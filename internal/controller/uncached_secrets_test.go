package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
)

var _ = Describe("Uncached Secret reads", func() {
	It("reads tokens without a Secret informer and sees updates on the next read", func() {
		c, err := cluster.New(cfg, func(o *cluster.Options) {
			o.Scheme = scheme.Scheme
			o.Client = ManagerClientOptions()
		})
		Expect(err).NotTo(HaveOccurred())
		cctx, ccancel := context.WithCancel(context.Background())
		defer ccancel()
		go func() { defer GinkgoRecover(); _ = c.Start(cctx) }()

		sec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "uncached-token", Namespace: "default"},
			Data:       map[string][]byte{"token": []byte("v1")},
		}
		Expect(k8sClient.Create(cctx, sec)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(context.Background(), sec) })

		key := types.NamespacedName{Name: sec.Name, Namespace: sec.Namespace}
		got := &corev1.Secret{}
		Expect(c.GetClient().Get(cctx, key, got)).To(Succeed())
		Expect(string(got.Data["token"])).To(Equal("v1"))

		got.Data["token"] = []byte("v2")
		Expect(k8sClient.Update(cctx, got)).To(Succeed())

		// Immediately visible: a cached client would lag behind the update.
		again := &corev1.Secret{}
		Expect(c.GetClient().Get(cctx, key, again)).To(Succeed())
		Expect(string(again.Data["token"])).To(Equal("v2"))

	})
})
