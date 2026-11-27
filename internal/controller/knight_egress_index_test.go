package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// spyClient counts the Knights returned by List and the Pod reads.
type spyClient struct {
	client.Client
	knightsListed int
	podGets       int
}

func (s *spyClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := s.Client.List(ctx, list, opts...); err != nil {
		return err
	}
	if kl, ok := list.(*aiv1alpha1.KnightList); ok {
		s.knightsListed += len(kl.Items)
	}
	return nil
}

func (s *spyClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Pod); ok {
		s.podGets++
	}
	return s.Client.Get(ctx, key, obj, opts...)
}

func indexTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	sc := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, discoveryv1.AddToScheme, aiv1alpha1.AddToScheme} {
		if err := add(sc); err != nil {
			t.Fatal(err)
		}
	}
	return sc
}

func TestServiceKnightRequestsUsesIndexes(t *testing.T) {
	mk := func(name, ns string, egress *aiv1alpha1.KnightEgress, labels map[string]string) *aiv1alpha1.Knight {
		return &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: labels},
			Spec:       aiv1alpha1.KnightSpec{Egress: egress},
		}
	}
	svcEgress := &aiv1alpha1.KnightEgress{AllowedServices: []aiv1alpha1.EgressService{{Name: "hass", Ports: []int32{80}}}}
	rt := &aiv1alpha1.RoundTable{
		ObjectMeta: metav1.ObjectMeta{Name: "tbl", Namespace: "apps"},
		Spec: aiv1alpha1.RoundTableSpec{Defaults: &aiv1alpha1.RoundTableDefaults{Egress: &aiv1alpha1.KnightEgress{
			AllowedServices: []aiv1alpha1.EgressService{{Name: "tbl-svc", Ports: []int32{80}}},
		}}},
	}
	fc := fake.NewClientBuilder().WithScheme(indexTestScheme(t)).
		WithIndex(&aiv1alpha1.Knight{}, knightEgressServiceIndex, knightEgressIndexValues).
		WithIndex(&aiv1alpha1.RoundTable{}, tableEgressServiceIndex, tableEgressIndexValues).
		WithObjects(rt,
			mk("own", "apps", svcEgress, nil),
			mk("member", "apps", nil, map[string]string{aiv1alpha1.LabelRoundTable: "tbl"}),
			mk("other", "other", &aiv1alpha1.KnightEgress{}, nil),
		).Build()
	spy := &spyClient{Client: fc}
	r := &KnightReconciler{Client: spy, serviceIndexed: true}
	svc := func(ns, name string) *corev1.Service {
		return &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	}

	// A Service no knight names lists no Knights.
	if got := r.serviceKnightRequests(context.Background(), svc("unrelated", "x")); len(got) != 0 || spy.knightsListed != 0 {
		t.Fatalf("unrelated Service: requests=%v knightsListed=%d", got, spy.knightsListed)
	}
	// A Service one knight names lists only that knight.
	got := r.serviceKnightRequests(context.Background(), svc("apps", "hass"))
	if len(got) != 1 || got[0].Name != "own" || spy.knightsListed != 1 {
		t.Fatalf("named Service: requests=%v knightsListed=%d", got, spy.knightsListed)
	}
	// A table default Service reaches the table's knight without a full list.
	spy.knightsListed = 0
	got = r.serviceKnightRequests(context.Background(), svc("apps", "tbl-svc"))
	if len(got) != 1 || got[0].Name != "member" || spy.knightsListed != 1 {
		t.Fatalf("table Service: requests=%v knightsListed=%d", got, spy.knightsListed)
	}
}

func TestHostNetworkEndpointsReadsEachPodOnce(t *testing.T) {
	pod := func(name string, uid types.UID) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: uid},
			Spec: corev1.PodSpec{HostNetwork: true}}
	}
	ref := func(name string, uid types.UID) *corev1.ObjectReference {
		return &corev1.ObjectReference{Kind: "Pod", Name: name, Namespace: "ns", UID: uid}
	}
	es := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "s-1", Namespace: "ns", Labels: map[string]string{discoveryv1.LabelServiceName: "s"}},
		Endpoints: []discoveryv1.Endpoint{
			{Addresses: []string{"10.0.0.1"}, TargetRef: ref("a", "uid-a"), Conditions: discoveryv1.EndpointConditions{Ready: new(true)}},
			{Addresses: []string{"10.0.0.2"}, TargetRef: ref("b", "uid-b"), Conditions: discoveryv1.EndpointConditions{Ready: new(true)}},
		},
	}
	fc := fake.NewClientBuilder().WithScheme(indexTestScheme(t)).WithObjects(es, pod("a", "uid-a"), pod("b", "uid-b")).Build()
	spy := &spyClient{Client: fc}
	r := &KnightReconciler{Client: spy}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"}}
	for i := range 3 {
		hostNet, addrs, _, err := r.hostNetworkEndpoints(context.Background(), svc)
		if err != nil || !hostNet || len(addrs) != 2 {
			t.Fatalf("run %d: hostNet=%v addrs=%v err=%v", i, hostNet, addrs, err)
		}
	}
	if spy.podGets != 2 {
		t.Fatalf("pod reads = %d, want 2 (one per pod across three reconciles)", spy.podGets)
	}
}

type fakeFieldIndexer struct{ fields map[string]bool }

func (f *fakeFieldIndexer) IndexField(_ context.Context, obj client.Object, field string, _ client.IndexerFunc) error {
	f.fields[fmt.Sprintf("%T/%s", obj, field)] = true
	return nil
}

func TestRegisterEgressIndexes(t *testing.T) {
	idx := &fakeFieldIndexer{fields: map[string]bool{}}
	r := &KnightReconciler{}
	require.NoError(t, r.registerEgressIndexes(context.Background(), idx))
	assert.True(t, r.serviceIndexed)
	assert.True(t, idx.fields["*v1alpha1.Knight/"+knightEgressServiceIndex])
	assert.True(t, idx.fields["*v1alpha1.RoundTable/"+tableEgressServiceIndex])
}

func TestRememberHostNetPodKeepsRecentOnOverflow(t *testing.T) {
	r := &KnightReconciler{}
	for i := 0; i < hostNetPodCacheMax+1; i++ {
		r.rememberHostNetPod(types.UID(fmt.Sprintf("uid-%d", i)), true)
	}
	_, oldest := r.hostNetPods.Load(types.UID("uid-0"))
	_, newest := r.hostNetPods.Load(types.UID(fmt.Sprintf("uid-%d", hostNetPodCacheMax)))
	_, recent := r.hostNetPods.Load(types.UID(fmt.Sprintf("uid-%d", hostNetPodCacheMax-1)))
	assert.False(t, oldest)
	assert.True(t, newest)
	assert.True(t, recent)
	assert.LessOrEqual(t, r.hostNetPodCount.Load(), int64(hostNetPodCacheMax))
	assert.Greater(t, r.hostNetPodCount.Load(), int64(hostNetPodCacheMax/2))
}
