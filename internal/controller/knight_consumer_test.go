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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	rtmetrics "github.com/roundtable-k8s/roundtable/pkg/metrics"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

var _ = Describe("Knight task consumer", func() {
	ctx := context.Background()
	const (
		name     = "consumer-knight"
		consumer = "Consumer-knight-consumer"
		stream   = "ck_tasks"
	)
	key := types.NamespacedName{Name: name, Namespace: "default"}
	var (
		ns         *natsserver.Server
		js         jetstream.JetStream
		reconciler *KnightReconciler
		recorder   *record.FakeRecorder
	)

	newKnight := func() *aiv1alpha1.Knight {
		return &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "code", Skills: []string{"shared"},
				NATS: aiv1alpha1.KnightNATS{
					URL: "nats://nats.test:4222", Subjects: []string{"ck.tasks.code.>"},
					Stream: stream, ResultsStream: "ck_results",
				},
			},
		}
	}
	reconcileKnight := func() {
		GinkgoHelper()
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
	}
	consumerInfo := func() (*jetstream.ConsumerInfo, error) {
		c, err := js.Consumer(ctx, stream, consumer)
		if err != nil {
			return nil, err
		}
		return c.Info(ctx)
	}
	events := func() []string {
		var out []string
		for {
			select {
			case e := <-recorder.Events:
				out = append(out, e)
			default:
				return out
			}
		}
	}

	BeforeEach(func() {
		opts := natstest.DefaultTestOptions
		opts.Port = -1
		opts.JetStream = true
		opts.StoreDir = GinkgoT().TempDir()
		ns = natstest.RunServer(&opts)

		cli := natspkg.NewClient(natspkg.Config{URL: ns.ClientURL()}, logr.Discard())
		Expect(cli.Connect()).To(Succeed())
		Expect(cli.CreateStream(ctx, natspkg.StreamConfig{
			Name: stream, Subjects: []string{"ck.tasks.>"},
			Retention: natspkg.RetentionWorkQueue, Storage: natspkg.StorageMemory,
		})).To(Succeed())
		var err error
		js, err = jetstream.New(cli.(natspkg.ConnProvider).Conn())
		Expect(err).NotTo(HaveOccurred())

		recorder = record.NewFakeRecorder(100)
		reconciler = &KnightReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: recorder,
			NATS: natspkg.NewProviderWithClient(cli, logr.Discard()),
		}
	})

	AfterEach(func() {
		k := &aiv1alpha1.Knight{}
		if err := k8sClient.Get(ctx, key, k); err == nil {
			k.Finalizers = nil
			_ = k8sClient.Update(ctx, k)
			_ = k8sClient.Delete(ctx, k)
		}
		_ = k8sClient.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}})
		_ = k8sClient.Delete(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}})
		ns.Shutdown()
	})

	It("creates the consumer with the runtimes' config and reports ConsumerReady", func() {
		Expect(k8sClient.Create(ctx, newKnight())).To(Succeed())
		reconcileKnight()
		reconcileKnight()

		info, err := consumerInfo()
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Config.AckPolicy).To(Equal(jetstream.AckExplicitPolicy))
		Expect(info.Config.DeliverPolicy).To(Equal(jetstream.DeliverAllPolicy))
		Expect(info.Config.MaxDeliver).To(Equal(1))
		Expect(info.Config.AckWait).To(Equal(30 * time.Second))
		filters := append([]string{info.Config.FilterSubject}, info.Config.FilterSubjects...)
		Expect(filters).To(ContainElement("ck.tasks.code.>"))

		k := &aiv1alpha1.Knight{}
		Expect(k8sClient.Get(ctx, key, k)).To(Succeed())
		c := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightConsumerReady)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionTrue))
		Expect(c.Reason).To(Equal(aiv1alpha1.ReasonConsumerReady))
		Expect(testutil.ToFloat64(rtmetrics.KnightConsumerReady.WithLabelValues("", name))).To(Equal(1.0))

		Expect(strings.Join(events(), "\n")).To(ContainSubstring("ConsumerCreated"))
	})

	It("adopts a consumer a runtime already created without recreating it", func() {
		// As packages/core subscribe() creates it, with a task in flight.
		_, err := js.CreateConsumer(ctx, stream, jetstream.ConsumerConfig{
			Durable: consumer, FilterSubjects: []string{"ck.tasks.code.>"},
			AckPolicy: jetstream.AckExplicitPolicy, DeliverPolicy: jetstream.DeliverAllPolicy,
			MaxDeliver: 1, AckWait: 30 * time.Second,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = js.Publish(ctx, "ck.tasks.code.t1", []byte(`{"taskId":"t1","task":"x"}`))
		Expect(err).NotTo(HaveOccurred())
		cons, err := js.Consumer(ctx, stream, consumer)
		Expect(err).NotTo(HaveOccurred())
		_, err = cons.Next(jetstream.FetchMaxWait(2 * time.Second))
		Expect(err).NotTo(HaveOccurred())
		before, err := consumerInfo()
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Create(ctx, newKnight())).To(Succeed())
		reconcileKnight()
		reconcileKnight()

		after, err := consumerInfo()
		Expect(err).NotTo(HaveOccurred())
		Expect(after.Created).To(BeTemporally("==", before.Created))
		Expect(after.NumAckPending).To(Equal(1))
		Expect(strings.Join(events(), "\n")).NotTo(Or(
			ContainSubstring("ConsumerCreated"), ContainSubstring("ConsumerRecreated"), ContainSubstring("ConsumerUpdated")))

		k := &aiv1alpha1.Knight{}
		Expect(k8sClient.Get(ctx, key, k)).To(Succeed())
		Expect(meta.IsStatusConditionTrue(k.Status.Conditions, aiv1alpha1.ConditionKnightConsumerReady)).To(BeTrue())
	})

	It("deletes the consumer when the Knight is deleted", func() {
		Expect(k8sClient.Create(ctx, newKnight())).To(Succeed())
		reconcileKnight()
		_, err := consumerInfo()
		Expect(err).NotTo(HaveOccurred())

		k := &aiv1alpha1.Knight{}
		Expect(k8sClient.Get(ctx, key, k)).To(Succeed())
		Expect(k8sClient.Delete(ctx, k)).To(Succeed())
		reconcileKnight()

		_, err = consumerInfo()
		Expect(errors.Is(err, jetstream.ErrConsumerNotFound)).To(BeTrue(), "consumer still there: %v", err)
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, key, &aiv1alpha1.Knight{}))).To(BeTrue())
	})

	It("reports StreamNotFound when spec.nats.stream does not exist", func() {
		k := newKnight()
		k.Spec.NATS.Stream = "ck_missing"
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		reconcileKnight()
		res, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(BeNumerically(">", 0))

		Expect(k8sClient.Get(ctx, key, k)).To(Succeed())
		c := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightConsumerReady)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(aiv1alpha1.ReasonConsumerStreamNotFound))
		Expect(testutil.ToFloat64(rtmetrics.KnightConsumerReady.WithLabelValues("", name))).To(Equal(0.0))
	})
})

// failingConsumers is a KnightConsumerClient whose NATS is down.
type failingConsumers struct{ removes int }

func (f *failingConsumers) EnsureConsumer(context.Context, string, jetstream.ConsumerConfig) (natspkg.ConsumerAction, error) {
	return "", fmt.Errorf("consumer info s/A-consumer: %w", nats.ErrNoServers)
}

func (f *failingConsumers) RemoveConsumer(context.Context, string, string) error {
	f.removes++
	return fmt.Errorf("delete consumer: %w", nats.ErrNoServers)
}

// A NATS outage holds a Knight's deletion only for ConsumerDeleteGiveUp; then
// the finalizer goes with a Warning event.
func TestKnightConsumerDeleteGivesUpAfterOutage(t *testing.T) {
	s := k8sruntime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	k := &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{
			Name: "outage", Namespace: "default", Finalizers: []string{knightFinalizer},
		},
		Spec: aiv1alpha1.KnightSpec{NATS: aiv1alpha1.KnightNATS{Stream: "s", Subjects: []string{"a.>"}}},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(k).Build()
	ctx := context.Background()
	if err := c.Delete(ctx, k); err != nil {
		t.Fatal(err)
	}
	got := &aiv1alpha1.Knight{}
	if err := c.Get(ctx, types.NamespacedName{Name: "outage", Namespace: "default"}, got); err != nil {
		t.Fatal(err)
	}
	deleted := got.DeletionTimestamp.Time

	cons := &failingConsumers{}
	rec := record.NewFakeRecorder(10)
	clock := deleted.Add(time.Minute)
	r := &KnightReconciler{Client: c, Scheme: s, Recorder: rec, Consumers: cons, Now: func() time.Time { return clock }}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: "outage", Namespace: "default"}}

	res, err := r.Reconcile(ctx, req)
	if err != nil || res.RequeueAfter == 0 {
		t.Fatalf("Reconcile during outage = %+v, %v; want a requeue", res, err)
	}
	if err := c.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatalf("Knight gone before the give-up time: %v", err)
	}

	clock = deleted.Add(ConsumerDeleteGiveUp)
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, req.NamespacedName, got); !apierrors.IsNotFound(err) {
		t.Fatalf("Knight still there after the give-up time: %v (finalizers %v)", err, got.Finalizers)
	}
	if cons.removes != 2 {
		t.Fatalf("RemoveConsumer called %d times, want 2", cons.removes)
	}
	select {
	case e := <-rec.Events:
		if !strings.Contains(e, "Warning ConsumerDeleteAbandoned") || !strings.Contains(e, "Outage-consumer") {
			t.Fatalf("event = %q", e)
		}
	default:
		t.Fatal("no ConsumerDeleteAbandoned event")
	}
}

// A NATS outage sets ConsumerReady=False (NATSUnavailable) and the gauge to 0
// without failing the reconcile.
func TestKnightConsumerNotReadyDuringOutage(t *testing.T) {
	k := &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "outage2", Namespace: "default", Generation: 3},
		Spec:       aiv1alpha1.KnightSpec{NATS: aiv1alpha1.KnightNATS{Stream: "s", Subjects: []string{"a.>"}}},
	}
	r := &KnightReconciler{Recorder: record.NewFakeRecorder(10), Consumers: &failingConsumers{}}
	changed, err := r.reconcileConsumer(context.Background(), k)
	if err == nil || !changed {
		t.Fatalf("reconcileConsumer = %v, %v; want changed and an error", changed, err)
	}
	c := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightConsumerReady)
	if c == nil || c.Status != metav1.ConditionFalse || c.ObservedGeneration != 3 ||
		c.Reason != aiv1alpha1.ReasonConsumerNATSUnavailable {
		t.Fatalf("condition = %+v", c)
	}
	if v := testutil.ToFloat64(rtmetrics.KnightConsumerReady.WithLabelValues("", "outage2")); v != 0 {
		t.Fatalf("gauge = %v, want 0", v)
	}
}
