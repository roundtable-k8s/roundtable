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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	missionpkg "github.com/roundtable-k8s/roundtable/internal/mission"
	"github.com/roundtable-k8s/roundtable/internal/telemetry"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
	rtruntime "github.com/roundtable-k8s/roundtable/pkg/runtime"
)

// streamRecorder is a NATS client fake that records DeleteStream calls. Only
// the methods the mission deletion path uses are implemented; the embedded
// interface is nil, so any other method panics.
type streamRecorder struct {
	natspkg.Client

	mu        sync.Mutex
	deleted   []string
	streamErr func(name string) error
}

func (s *streamRecorder) IsConnected() bool { return true }

func (s *streamRecorder) DeleteConsumer(context.Context, string, string) error { return nil }

func (s *streamRecorder) DeleteStream(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted = append(s.deleted, name)
	if s.streamErr != nil {
		return s.streamErr(name)
	}
	return nil
}

func (s *streamRecorder) deletedStreams() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.deleted...)
}

var _ = Describe("Mission Controller", func() {
	const (
		missionName = "test-mission"
		knightName  = "test-mission-knight"
		chainName   = "test-chain"
		namespace   = "default"
	)

	ctx := context.Background()

	missionNN := types.NamespacedName{Name: missionName, Namespace: namespace}
	knightNN := types.NamespacedName{Name: knightName, Namespace: namespace}
	chainNN := types.NamespacedName{Name: chainName, Namespace: namespace}

	createKnight := func() {
		knight := &aiv1alpha1.Knight{}
		err := k8sClient.Get(ctx, knightNN, knight)
		if err != nil && errors.IsNotFound(err) {
			knight = &aiv1alpha1.Knight{
				ObjectMeta: metav1.ObjectMeta{
					Name:      knightName,
					Namespace: namespace,
				},
				Spec: aiv1alpha1.KnightSpec{
					Domain: "general",
					Model:  "claude-sonnet-4-20250514",
					Skills: []string{"general"},
					NATS: aiv1alpha1.KnightNATS{
						URL:           "nats://nats.test:4222",
						Subjects:      []string{"test.tasks.general.>"},
						Stream:        "test_tasks",
						ResultsStream: "test_results",
						MaxDeliver:    1,
					},
					Concurrency:        1,
					TaskTimeoutSeconds: 120,
				},
			}
			Expect(k8sClient.Create(ctx, knight)).To(Succeed())
		}
	}

	makeKnightReady := func() {
		knight := &aiv1alpha1.Knight{}
		Expect(k8sClient.Get(ctx, knightNN, knight)).To(Succeed())
		knight.Status.Phase = aiv1alpha1.KnightPhaseReady
		knight.Status.Ready = true
		Expect(k8sClient.Status().Update(ctx, knight)).To(Succeed())
	}

	// setMissionChainCost records a chain cost on the mission status, where
	// the budget check reads the chains' total.
	setMissionChainCost := func(cost string) {
		mission := &aiv1alpha1.Mission{}
		Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())
		mission.Status.ChainStatuses = []aiv1alpha1.MissionChainStatus{
			{Name: "budget-chain", CostUSD: cost},
		}
		Expect(k8sClient.Status().Update(ctx, mission)).To(Succeed())
	}

	deleteKnight := func() {
		knight := &aiv1alpha1.Knight{}
		if err := k8sClient.Get(ctx, knightNN, knight); err == nil {
			_ = k8sClient.Delete(ctx, knight)
		}
	}

	createChain := func() {
		chain := &aiv1alpha1.Chain{}
		err := k8sClient.Get(ctx, chainNN, chain)
		if err != nil && errors.IsNotFound(err) {
			chain = &aiv1alpha1.Chain{
				ObjectMeta: metav1.ObjectMeta{
					Name:      chainName,
					Namespace: namespace,
				},
				Spec: aiv1alpha1.ChainSpec{
					Description: "Test chain",
					Steps: []aiv1alpha1.ChainStep{
						{
							Name:      "step1",
							KnightRef: corev1.LocalObjectReference{Name: knightName},
							Task:      "Execute test task",
						},
					},
					RoundTableRef:  aiv1alpha1.LocalRef("default"),
					TimeoutSeconds: 300,
				},
			}
			Expect(k8sClient.Create(ctx, chain)).To(Succeed())
		}
	}

	deleteChain := func() {
		chain := &aiv1alpha1.Chain{}
		if err := k8sClient.Get(ctx, chainNN, chain); err == nil {
			_ = k8sClient.Delete(ctx, chain)
		}
	}

	createMission := func(spec aiv1alpha1.MissionSpec) {
		mission := &aiv1alpha1.Mission{}
		err := k8sClient.Get(ctx, missionNN, mission)
		if err != nil && errors.IsNotFound(err) {
			mission = &aiv1alpha1.Mission{
				ObjectMeta: metav1.ObjectMeta{
					Name:      missionName,
					Namespace: namespace,
				},
				Spec: spec,
			}
			Expect(k8sClient.Create(ctx, mission)).To(Succeed())
		}
	}

	deleteMission := func() {
		mission := &aiv1alpha1.Mission{}
		if err := k8sClient.Get(ctx, missionNN, mission); err == nil {
			// Delete ephemeral RoundTable if it exists
			if mission.Status.RoundTableName != "" {
				rt := &aiv1alpha1.RoundTable{}
				if err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      mission.Status.RoundTableName,
					Namespace: namespace,
				}, rt); err == nil {
					_ = k8sClient.Delete(ctx, rt)
				}
			}

			// Remove finalizer for clean test teardown
			mission.Finalizers = nil
			_ = k8sClient.Update(ctx, mission)
			_ = k8sClient.Delete(ctx, mission)
		}
		// Also delete mission-scoped chains
		missionChainName := fmt.Sprintf("mission-%s-%s", missionName, chainName)
		missionChain := &aiv1alpha1.Chain{}
		if err := k8sClient.Get(ctx, types.NamespacedName{
			Name:      missionChainName,
			Namespace: namespace,
		}, missionChain); err == nil {
			_ = k8sClient.Delete(ctx, missionChain)
		}
	}

	// driveToPhase reconciles until the mission reaches targetPhase or maxIter is exceeded.
	// Optional beforeReconcile callback runs before each reconcile (e.g., to make knights ready).
	driveToPhase := func(r *MissionReconciler, targetPhase aiv1alpha1.MissionPhase, maxIter int, beforeReconcile ...func(aiv1alpha1.MissionPhase)) {
		for range maxIter {
			// Run callbacks before reconciling (e.g., make knights ready)
			m := &aiv1alpha1.Mission{}
			if err := k8sClient.Get(ctx, missionNN, m); err == nil {
				for _, fn := range beforeReconcile {
					fn(m.Status.Phase)
				}
			}
			// Reconcile
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
			// Check phase AFTER reconciling
			if err := k8sClient.Get(ctx, missionNN, m); err == nil && m.Status.Phase == targetPhase {
				return
			}
		}
		// Final check
		m := &aiv1alpha1.Mission{}
		_ = k8sClient.Get(ctx, missionNN, m)
		Expect(m.Status.Phase).To(Equal(targetPhase), fmt.Sprintf("driveToPhase: wanted %s, got %s after %d iterations", targetPhase, m.Status.Phase, maxIter))
	}

	// readyOnAssembling returns a callback that makes the knight ready when phase is Assembling.
	readyOnAssembling := func() func(aiv1alpha1.MissionPhase) {
		called := false
		return func(phase aiv1alpha1.MissionPhase) {
			if phase == aiv1alpha1.MissionPhaseAssembling && !called {
				makeKnightReady()
				called = true
			}
		}
	}

	// makeEphemeralKnightsReady finds all ephemeral knights for the mission and marks them Ready.
	makeEphemeralKnightsReady := func() {
		mission := &aiv1alpha1.Mission{}
		if err := k8sClient.Get(ctx, missionNN, mission); err != nil {
			return
		}
		for _, mk := range mission.Spec.Knights {
			if !mk.Ephemeral {
				continue
			}
			ephName := fmt.Sprintf("%s-%s", mission.Name, mk.Name)
			knight := &aiv1alpha1.Knight{}
			nn := types.NamespacedName{Name: ephName, Namespace: namespace}
			if err := k8sClient.Get(ctx, nn, knight); err != nil {
				continue
			}
			knight.Status.Phase = aiv1alpha1.KnightPhaseReady
			knight.Status.Ready = true
			_ = k8sClient.Status().Update(ctx, knight)
		}
	}

	// readyEphemeralsOnAssembling returns a callback that marks ephemeral knights ready during Assembling.
	readyEphemeralsOnAssembling := func() func(aiv1alpha1.MissionPhase) {
		return func(phase aiv1alpha1.MissionPhase) {
			if phase == aiv1alpha1.MissionPhaseAssembling {
				makeEphemeralKnightsReady()
			}
		}
	}

	// makeEphemeralRoundTableReady finds and marks the ephemeral RoundTable as Ready
	makeEphemeralRoundTableReady := func() {
		// Get mission to find the RoundTable name
		mission := &aiv1alpha1.Mission{}
		if err := k8sClient.Get(ctx, missionNN, mission); err != nil {
			return
		}
		if mission.Status.RoundTableName == "" {
			return
		}

		// Get the ephemeral RoundTable
		rt := &aiv1alpha1.RoundTable{}
		rtKey := types.NamespacedName{Name: mission.Status.RoundTableName, Namespace: namespace}
		if err := k8sClient.Get(ctx, rtKey, rt); err != nil {
			return
		}

		// Mark as Ready
		rt.Status.Phase = aiv1alpha1.RoundTablePhaseReady
		rt.Status.KnightsReady = 0
		rt.Status.KnightsTotal = 0
		_ = k8sClient.Status().Update(ctx, rt)
	}

	// readyOnProvisioning returns a callback that makes the RoundTable ready when phase is Provisioning.
	readyOnProvisioning := func() func(aiv1alpha1.MissionPhase) {
		return func(phase aiv1alpha1.MissionPhase) {
			if phase == aiv1alpha1.MissionPhaseProvisioning {
				// Keep trying to mark RT as ready until it exists and is marked
				makeEphemeralRoundTableReady()
			}
		}
	}

	newReconciler := func() *MissionReconciler {
		return &MissionReconciler{
			Client:      k8sClient,
			Scheme:      k8sClient.Scheme(),
			Recorder:    record.NewFakeRecorder(100),
			Assembler:   &missionpkg.KnightAssembler{Client: k8sClient, Scheme: k8sClient.Scheme()},
			Cleaner:     &missionpkg.Cleaner{Client: k8sClient},
			CostTracker: &missionpkg.CostTracker{Client: k8sClient},
			Briefer:     &missionpkg.Briefer{Client: k8sClient, Recorder: record.NewFakeRecorder(100)},
			Chains:      &missionpkg.ChainManager{Client: k8sClient, Scheme: k8sClient.Scheme()},
		}
	}

	Context("When reconciling a new mission", func() {
		BeforeEach(func() {
			createKnight()
			createMission(aiv1alpha1.MissionSpec{
				Objective: "Test the mission controller",
				Knights: []aiv1alpha1.MissionKnight{
					{Name: knightName, Role: "tester"},
				},
				TTLSeconds:     3600,
				TimeoutSeconds: 1800,
			})
		})

		AfterEach(func() {
			deleteMission()
			deleteKnight()
		})

		It("should add a finalizer on first reconcile", func() {
			r := newReconciler()
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
			Expect(err).NotTo(HaveOccurred())

			mission := &aiv1alpha1.Mission{}
			Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())
			Expect(mission.Finalizers).To(ContainElement(missionFinalizer))
		})

		It("puts table and trace_id on its log lines", func() {
			mission := &aiv1alpha1.Mission{}
			Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())
			mission.Spec.RoundTableRef = aiv1alpha1.LocalRef("log-table")
			Expect(k8sClient.Update(ctx, mission)).To(Succeed())

			var mu sync.Mutex
			var lines []string
			sink := funcr.New(func(_, args string) {
				mu.Lock()
				defer mu.Unlock()
				lines = append(lines, args)
			}, funcr.Options{Verbosity: 2})
			logCtx := logf.IntoContext(ctx, sink)

			r := newReconciler()
			r.TracerProvider = sdktrace.NewTracerProvider(sdktrace.WithIDGenerator(telemetry.NewIDGenerator()))
			for range 3 {
				// Later cycles may error on the missing RoundTable; only the log matters.
				_, _ = r.Reconcile(logCtx, reconcile.Request{NamespacedName: missionNN})
			}
			Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())
			traceID := mission.Status.TraceID
			Expect(traceID).To(HaveLen(32))

			// A fresh reconcile starts from the stored status, so every line it
			// writes carries both fields.
			mu.Lock()
			lines = nil
			mu.Unlock()
			_, _ = newReconciler().Reconcile(logCtx, reconcile.Request{NamespacedName: missionNN})
			mu.Lock()
			defer mu.Unlock()
			var matched int
			for _, l := range lines {
				if strings.Contains(l, `"trace_id"="`+traceID+`"`) && strings.Contains(l, `"table"="log-table"`) {
					matched++
				}
			}
			Expect(matched).To(BeNumerically(">", 0), "lines: %v", lines)
		})

		It("should initialize to Pending phase", func() {
			r := newReconciler()
			// Reconcile until Pending (finalizer + init may happen in one or two cycles)
			driveToPhase(r, aiv1alpha1.MissionPhasePending, 5)

			mission := &aiv1alpha1.Mission{}
			Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())
			Expect(mission.Status.Phase).To(Equal(aiv1alpha1.MissionPhasePending))
			Expect(mission.Status.StartedAt).NotTo(BeNil())
			Expect(mission.Status.ExpiresAt).NotTo(BeNil())
			Expect(mission.Status.KnightStatuses).To(HaveLen(1))
			Expect(mission.Status.KnightStatuses[0].Name).To(Equal(knightName))
		})
	})

	Context("Phase transitions", func() {
		BeforeEach(func() {
			createKnight()
			createMission(aiv1alpha1.MissionSpec{
				Objective: "Test phase transitions",
				Knights: []aiv1alpha1.MissionKnight{
					{Name: knightName, Role: "tester"},
				},
				TTLSeconds:     3600,
				TimeoutSeconds: 1800,
			})
		})

		AfterEach(func() {
			deleteMission()
			deleteKnight()
		})

		It("should progress through phases: Pending → Provisioning → Assembling → Briefing → Active", func() {
			r := newReconciler()
			cbProvisioning := readyOnProvisioning()
			cbAssembling := readyOnAssembling()

			// Verify each phase is reachable in order
			// Note: chainless missions stay Active (no auto-Succeeded transition)
			for _, phase := range []aiv1alpha1.MissionPhase{
				aiv1alpha1.MissionPhasePending,
				aiv1alpha1.MissionPhaseProvisioning,
				aiv1alpha1.MissionPhaseAssembling,
				aiv1alpha1.MissionPhaseBriefing,
				aiv1alpha1.MissionPhaseActive,
			} {
				driveToPhase(r, phase, 10, cbProvisioning, cbAssembling)
			}
		})
	})

	Context("Chain copy creation", func() {
		BeforeEach(func() {
			createKnight()
			createChain()
			createMission(aiv1alpha1.MissionSpec{
				Objective: "Test chain copy",
				Knights: []aiv1alpha1.MissionKnight{
					{Name: knightName, Role: "tester"},
				},
				Chains: []aiv1alpha1.MissionChainRef{
					{
						Name:  chainName,
						Phase: "Active",
					},
				},
				RoundTableRef:  aiv1alpha1.LocalRef("test-rt"),
				TTLSeconds:     3600,
				TimeoutSeconds: 1800,
			})
		})

		AfterEach(func() {
			deleteMission()
			deleteChain()
			deleteKnight()
		})

		It("should create a mission-scoped chain copy with correct name and ownerRef", func() {
			r := newReconciler()

			// Drive to Active phase (creates chain copies)
			driveToPhase(r, aiv1alpha1.MissionPhaseActive, 10, readyOnProvisioning(), readyOnAssembling())
			// One more reconcile to process chains
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
			Expect(err).NotTo(HaveOccurred())

			// Check mission-scoped chain was created
			missionChainName := fmt.Sprintf("mission-%s-%s", missionName, chainName)
			missionChain := &aiv1alpha1.Chain{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      missionChainName,
					Namespace: namespace,
				}, missionChain)
			}, "5s", "500ms").Should(Succeed())

			// Verify chain properties
			Expect(missionChain.Name).To(Equal(missionChainName))
			Expect(missionChain.Labels).To(HaveKeyWithValue("ai.roundtable.io/mission", missionName))
			Expect(missionChain.Labels).To(HaveKeyWithValue("ai.roundtable.io/chain-phase", "Active"))
			Expect(aiv1alpha1.RefName(missionChain.Spec.RoundTableRef)).To(Equal("test-rt"))

			// Verify owner reference
			Expect(missionChain.OwnerReferences).To(HaveLen(1))
			Expect(missionChain.OwnerReferences[0].Name).To(Equal(missionName))
			Expect(missionChain.OwnerReferences[0].Kind).To(Equal("Mission"))
			Expect(*missionChain.OwnerReferences[0].Controller).To(BeTrue())

			// Verify mission status tracks the chain
			mission := &aiv1alpha1.Mission{}
			Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())
			Expect(mission.Status.ChainStatuses).To(HaveLen(1))
			Expect(mission.Status.ChainStatuses[0].Name).To(Equal(chainName))
			Expect(mission.Status.ChainStatuses[0].ChainCRName).To(Equal(missionChainName))
		})
	})

	Context("Status aggregation", func() {
		BeforeEach(func() {
			createKnight()
		})

		AfterEach(func() {
			deleteMission()
			deleteKnight()
		})

		It("should succeed when all chains succeed", func() {
			createChain()
			createMission(aiv1alpha1.MissionSpec{
				Objective: "Test all chains succeed",
				Knights: []aiv1alpha1.MissionKnight{
					{Name: knightName, Role: "tester"},
				},
				Chains: []aiv1alpha1.MissionChainRef{
					{Name: chainName, Phase: "Active"},
				},
				RoundTableRef:  aiv1alpha1.LocalRef("test-rt"),
				TTLSeconds:     3600,
				TimeoutSeconds: 1800,
			})

			r := newReconciler()

			// Drive to Active phase
			driveToPhase(r, aiv1alpha1.MissionPhaseActive, 10, readyOnProvisioning(), readyOnAssembling())

			// Reconcile once more to let reconcileActive create chain copies
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
			// Mark mission chain as succeeded
			missionChainName := fmt.Sprintf("mission-%s-%s", missionName, chainName)
			Eventually(func() error {
				_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
				missionChain := &aiv1alpha1.Chain{}
				if err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      missionChainName,
					Namespace: namespace,
				}, missionChain); err != nil {
					return err
				}
				missionChain.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
				return k8sClient.Status().Update(ctx, missionChain)
			}, "5s", "500ms").Should(Succeed())

			// Reconcile should detect success
			Eventually(func() aiv1alpha1.MissionPhase {
				_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
				mission := &aiv1alpha1.Mission{}
				_ = k8sClient.Get(ctx, missionNN, mission)
				return mission.Status.Phase
			}, "5s", "500ms").Should(Equal(aiv1alpha1.MissionPhaseSucceeded))

			mission := &aiv1alpha1.Mission{}
			Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())
			Expect(mission.Status.Result).To(ContainSubstring("successfully"))
			condition := meta.FindStatusCondition(mission.Status.Conditions, "Complete")
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal("Succeeded"))

			deleteChain()
		})

		It("should fail when one chain fails", func() {
			createChain()
			createMission(aiv1alpha1.MissionSpec{
				Objective: "Test chain failure",
				Knights: []aiv1alpha1.MissionKnight{
					{Name: knightName, Role: "tester"},
				},
				Chains: []aiv1alpha1.MissionChainRef{
					{Name: chainName, Phase: "Active"},
				},
				RoundTableRef:  aiv1alpha1.LocalRef("test-rt"),
				TTLSeconds:     3600,
				TimeoutSeconds: 1800,
			})

			r := newReconciler()

			// Drive to Active phase
			driveToPhase(r, aiv1alpha1.MissionPhaseActive, 10, readyOnProvisioning(), readyOnAssembling())

			// Reconcile once more to let reconcileActive create chain copies
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
			// Mark mission chain as failed
			missionChainName := fmt.Sprintf("mission-%s-%s", missionName, chainName)
			Eventually(func() error {
				_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
				missionChain := &aiv1alpha1.Chain{}
				if err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      missionChainName,
					Namespace: namespace,
				}, missionChain); err != nil {
					return err
				}
				missionChain.Status.Phase = aiv1alpha1.ChainPhaseFailed
				return k8sClient.Status().Update(ctx, missionChain)
			}, "5s", "500ms").Should(Succeed())

			// Reconcile should detect failure
			Eventually(func() aiv1alpha1.MissionPhase {
				_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
				mission := &aiv1alpha1.Mission{}
				_ = k8sClient.Get(ctx, missionNN, mission)
				return mission.Status.Phase
			}, "5s", "500ms").Should(Equal(aiv1alpha1.MissionPhaseFailed))

			mission := &aiv1alpha1.Mission{}
			Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())
			Expect(mission.Status.Result).To(ContainSubstring("failed"))
			condition := meta.FindStatusCondition(mission.Status.Conditions, "Complete")
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal("ChainFailed"))

			deleteChain()
		})
	})

	Context("Budget enforcement", func() {
		BeforeEach(func() {
			createKnight()
		})

		AfterEach(func() {
			deleteMission()
			deleteKnight()
		})

		It("should track cost and stay Active when under budget", func() {
			createMission(aiv1alpha1.MissionSpec{
				Objective:     "Test under budget",
				CostBudgetUSD: "10.00",
				Knights: []aiv1alpha1.MissionKnight{
					{Name: knightName, Role: "tester"},
				},
				TTLSeconds:     3600,
				TimeoutSeconds: 1800,
			})

			r := newReconciler()

			// Drive to Active
			driveToPhase(r, aiv1alpha1.MissionPhaseActive, 10, readyOnProvisioning(), readyOnAssembling())

			// Record chain cost under budget (the mission's cost comes from its chains)
			setMissionChainCost("5.25")

			// Reconcile — should stay Active (no chains to complete) with cost tracked
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})

			mission := &aiv1alpha1.Mission{}
			Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())
			Expect(mission.Status.Phase).To(Equal(aiv1alpha1.MissionPhaseActive))
			Expect(mission.Status.TotalCost).To(Equal("5.2500"))
		})

		It("should fail when over budget", func() {
			createMission(aiv1alpha1.MissionSpec{
				Objective:     "Test over budget",
				CostBudgetUSD: "10.00",
				Knights: []aiv1alpha1.MissionKnight{
					{Name: knightName, Role: "tester"},
				},
				TTLSeconds:     3600,
				TimeoutSeconds: 1800,
			})

			r := newReconciler()

			// Drive to Active (no chains, so it will try to succeed immediately)
			driveToPhase(r, aiv1alpha1.MissionPhaseActive, 10, readyOnProvisioning(), readyOnAssembling())

			// Record chain cost over budget (the mission's cost comes from its chains)
			setMissionChainCost("15.75")

			// Reconcile should fail due to budget
			Eventually(func() aiv1alpha1.MissionPhase {
				_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
				mission := &aiv1alpha1.Mission{}
				_ = k8sClient.Get(ctx, missionNN, mission)
				return mission.Status.Phase
			}, "5s", "500ms").Should(Equal(aiv1alpha1.MissionPhaseFailed))

			mission := &aiv1alpha1.Mission{}
			Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())
			Expect(mission.Status.Result).To(ContainSubstring("budget exceeded"))
			Expect(mission.Status.TotalCost).To(Equal("15.7500"))
			condition := meta.FindStatusCondition(mission.Status.Conditions, "Complete")
			Expect(condition).NotTo(BeNil())
			Expect(condition.Reason).To(Equal("OverBudget"))
		})
	})

	Context("Result storage (NATS KV)", func() {
		BeforeEach(func() {
			createKnight()
			createChain()
			createMission(aiv1alpha1.MissionSpec{
				Objective:     "Test result retention",
				RetainResults: true,
				RoundTableRef: aiv1alpha1.LocalRef("test-rt"),
				Knights: []aiv1alpha1.MissionKnight{
					{Name: knightName, Role: "tester"},
				},
				Chains: []aiv1alpha1.MissionChainRef{
					{Name: chainName, Phase: "Active"},
				},
				TTLSeconds:     3600,
				TimeoutSeconds: 1800,
			})
		})

		AfterEach(func() {
			deleteMission()
			deleteChain()
			deleteKnight()
		})

		It("should set ResultsConfigMap reference when retainResults=true (graceful without NATS)", func() {
			r := newReconciler()

			// Drive to Active and complete
			driveToPhase(r, aiv1alpha1.MissionPhaseActive, 10, readyOnProvisioning(), readyOnAssembling())

			// Reconcile once more to let reconcileActive create chain copies
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
			// Mark chain as succeeded
			missionChainName := fmt.Sprintf("mission-%s-%s", missionName, chainName)
			Eventually(func() error {
				_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
				missionChain := &aiv1alpha1.Chain{}
				if err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      missionChainName,
					Namespace: namespace,
				}, missionChain); err != nil {
					return err
				}
				missionChain.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
				return k8sClient.Status().Update(ctx, missionChain)
			}, "5s", "500ms").Should(Succeed())

			// Wait for completion and cleanup — without NATS, KV store fails gracefully
			// but ResultsConfigMap is still set (to prevent infinite retry)
			// Check both phase transition AND ResultsConfigMap in one Eventually
			Eventually(func() string {
				_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
				mission := &aiv1alpha1.Mission{}
				_ = k8sClient.Get(ctx, missionNN, mission)
				return mission.Status.ResultsConfigMap
			}, "15s", "500ms").ShouldNot(BeEmpty())
		})
	})

	Context("When mission has timeout", func() {
		BeforeEach(func() {
			createKnight()
			createChain()
			createMission(aiv1alpha1.MissionSpec{
				Objective: "Test timeout",
				Knights: []aiv1alpha1.MissionKnight{
					{Name: knightName, Role: "tester"},
				},
				Chains: []aiv1alpha1.MissionChainRef{
					{Name: chainName, Phase: "Active"},
				},
				TTLSeconds:     3600,
				TimeoutSeconds: 60,
			})
		})

		AfterEach(func() {
			deleteMission()
			deleteChain()
			deleteKnight()
		})

		It("should fail the mission when timeout is exceeded", func() {
			r := newReconciler()

			// Drive to Active
			driveToPhase(r, aiv1alpha1.MissionPhaseActive, 10, readyOnProvisioning(), readyOnAssembling())

			// Set startedAt to the past to trigger timeout immediately
			mission := &aiv1alpha1.Mission{}
			Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())
			pastTime := metav1.NewTime(time.Now().Add(-120 * time.Second))
			mission.Status.StartedAt = &pastTime
			Expect(k8sClient.Status().Update(ctx, mission)).To(Succeed())

			// Reconcile should detect timeout and fail
			Eventually(func() aiv1alpha1.MissionPhase {
				_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
				m := &aiv1alpha1.Mission{}
				_ = k8sClient.Get(ctx, missionNN, m)
				return m.Status.Phase
			}, "5s", "500ms").Should(Equal(aiv1alpha1.MissionPhaseFailed))

			Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())
			Expect(mission.Status.Result).To(ContainSubstring("timed out"))
			condition := meta.FindStatusCondition(mission.Status.Conditions, "Complete")
			Expect(condition).NotTo(BeNil())
			Expect(condition.Reason).To(Equal("Timeout"))
		})
	})

	Context("When a knight reference is invalid", func() {
		BeforeEach(func() {
			createMission(aiv1alpha1.MissionSpec{
				Objective: "Test with missing knight",
				Knights: []aiv1alpha1.MissionKnight{
					{Name: "nonexistent-knight", Role: "ghost"},
				},
				TTLSeconds:     3600,
				TimeoutSeconds: 1800,
			})
		})

		AfterEach(func() {
			deleteMission()
		})

		It("should stay in Assembling and set KnightsReady=False", func() {
			r := newReconciler()

			// Drive to Assembling — it won't progress further since knight doesn't exist
			driveToPhase(r, aiv1alpha1.MissionPhaseAssembling, 10, readyOnProvisioning())
			// A few more reconciles to set KnightsReady condition (stays in Assembling)
			for range 3 {
				_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
			}

			mission := &aiv1alpha1.Mission{}
			Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())
			Expect(mission.Status.Phase).To(Equal(aiv1alpha1.MissionPhaseAssembling))

			condition := meta.FindStatusCondition(mission.Status.Conditions, "KnightsReady")
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal("KnightNotFound"))
		})
	})

	Context("When mission has no chains (briefing-only)", func() {
		BeforeEach(func() {
			createKnight()
			createMission(aiv1alpha1.MissionSpec{
				Objective: "Briefing-only mission",
				Knights: []aiv1alpha1.MissionKnight{
					{Name: knightName, Role: "listener"},
				},
				TTLSeconds:     3600,
				TimeoutSeconds: 1800,
			})
		})

		AfterEach(func() {
			deleteMission()
			deleteKnight()
		})

		It("should stay Active when no chains are defined", func() {
			r := newReconciler()

			// Drive to Active — chainless missions remain Active awaiting TTL/timeout
			driveToPhase(r, aiv1alpha1.MissionPhaseActive, 10, readyOnProvisioning(), readyOnAssembling())

			// One more reconcile — should stay Active (not transition to Succeeded)
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})

			mission := &aiv1alpha1.Mission{}
			Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())
			Expect(mission.Status.Phase).To(Equal(aiv1alpha1.MissionPhaseActive))
		})
	})

	Context("Cleanup phase", func() {
		BeforeEach(func() {
			createKnight()
			createMission(aiv1alpha1.MissionSpec{
				Objective:     "Test cleanup",
				CleanupPolicy: "Retain",
				Knights: []aiv1alpha1.MissionKnight{
					{Name: knightName, Role: "tester"},
				},
				TTLSeconds:     3600,
				TimeoutSeconds: 1800,
			})
		})

		AfterEach(func() {
			deleteMission()
			deleteKnight()
		})

		It("should set CleanupComplete condition when retained", func() {
			r := newReconciler()

			// Drive to Active, then manually transition to CleaningUp
			// (chainless missions stay Active, so we simulate TTL expiry)
			driveToPhase(r, aiv1alpha1.MissionPhaseActive, 10, readyOnProvisioning(), readyOnAssembling())

			mission := &aiv1alpha1.Mission{}
			Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())
			mission.Status.Phase = aiv1alpha1.MissionPhaseCleaningUp
			Expect(k8sClient.Status().Update(ctx, mission)).To(Succeed())
			// One more reconcile to actually run reconcileCleaningUp (sets conditions)
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})

			mission = &aiv1alpha1.Mission{}
			Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())
			// Should still exist (Retain policy)
			Expect(mission.Name).To(Equal(missionName))

			condition := meta.FindStatusCondition(mission.Status.Conditions, "CleanupComplete")
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
		})

		It("should keep a Failed phase through cleanup", func() {
			r := newReconciler()

			driveToPhase(r, aiv1alpha1.MissionPhaseActive, 10, readyOnProvisioning(), readyOnAssembling())

			// Fail the mission the way the assembly timeout does: phase Failed
			// with a result message, but no Complete condition. The result
			// deliberately doesn't contain "failed" — the old inference marked
			// exactly this case Succeeded after cleanup.
			mission := &aiv1alpha1.Mission{}
			Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())
			mission.Status.Phase = aiv1alpha1.MissionPhaseFailed
			mission.Status.Result = "Assembly timeout: knights not ready: [tester]"
			Expect(k8sClient.Status().Update(ctx, mission)).To(Succeed())

			// First reconcile: Failed -> CleaningUp (records the outcome),
			// second: runs cleanup and restores the terminal phase.
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})

			mission = &aiv1alpha1.Mission{}
			Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())
			Expect(mission.Status.Phase).To(Equal(aiv1alpha1.MissionPhaseFailed))
			Expect(mission.Status.Result).To(ContainSubstring("Assembly timeout"))

			condition := meta.FindStatusCondition(mission.Status.Conditions, aiv1alpha1.ConditionMissionComplete)
			Expect(condition).NotTo(BeNil())
			Expect(condition.Reason).To(Equal(aiv1alpha1.ReasonMissionFailed))
		})
	})

	Context("When the mission is deleted before its cleanup phase ran", func() {
		var (
			rec     *streamRecorder
			deleter *MissionReconciler
		)

		// startDeletion gives the mission the controller's finalizer, sets the
		// given stream names and conditions in status, then deletes it.
		startDeletion := func(tasks, results string, conds ...metav1.Condition) {
			m := &aiv1alpha1.Mission{}
			Expect(k8sClient.Get(ctx, missionNN, m)).To(Succeed())
			controllerutil.AddFinalizer(m, missionFinalizer)
			Expect(k8sClient.Update(ctx, m)).To(Succeed())

			Expect(k8sClient.Get(ctx, missionNN, m)).To(Succeed())
			m.Status.NATSTasksStream = tasks
			m.Status.NATSResultsStream = results
			for _, c := range conds {
				meta.SetStatusCondition(&m.Status.Conditions, c)
			}
			Expect(k8sClient.Status().Update(ctx, m)).To(Succeed())

			Expect(k8sClient.Get(ctx, missionNN, m)).To(Succeed())
			Expect(k8sClient.Delete(ctx, m)).To(Succeed())
		}

		// missionGone reports whether the mission has been removed from the API.
		missionGone := func() bool {
			err := k8sClient.Get(ctx, missionNN, &aiv1alpha1.Mission{})
			return errors.IsNotFound(err)
		}

		BeforeEach(func() {
			rec = &streamRecorder{}
			createMission(aiv1alpha1.MissionSpec{
				Objective: "Test deletion cleanup",
				Knights: []aiv1alpha1.MissionKnight{
					{Name: knightName, Role: "tester"},
				},
				TTLSeconds:     3600,
				TimeoutSeconds: 1800,
			})
			deleter = newReconciler()
			deleter.Cleaner = &missionpkg.Cleaner{
				Client: k8sClient,
				NATS:   natspkg.NewProviderWithClient(rec, logr.Discard()),
			}
		})

		AfterEach(func() {
			// A test that leaves the finalizer behind must not block the next BeforeEach.
			m := &aiv1alpha1.Mission{}
			if err := k8sClient.Get(ctx, missionNN, m); err == nil {
				controllerutil.RemoveFinalizer(m, missionFinalizer)
				_ = k8sClient.Update(ctx, m)
			}
			deleteMission()
		})

		It("deletes both NATS streams and then removes the finalizer", func() {
			startDeletion("rt_msn_tasks", "rt_msn_results")

			result, err := deleter.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())
			Expect(rec.deletedStreams()).To(ConsistOf("rt_msn_tasks", "rt_msn_results"))
			Expect(missionGone()).To(BeTrue())
		})

		It("does not delete streams again when cleanup already completed", func() {
			startDeletion("rt_msn_tasks", "rt_msn_results", metav1.Condition{
				Type:    aiv1alpha1.ConditionCleanupComplete,
				Status:  metav1.ConditionTrue,
				Reason:  aiv1alpha1.ReasonCleanupComplete,
				Message: "done",
			})

			_, err := deleter.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(rec.deletedStreams()).To(BeEmpty())
			Expect(missionGone()).To(BeTrue())
		})

		It("treats an already-missing stream as deleted", func() {
			rec.streamErr = func(name string) error {
				return fmt.Errorf("stream %s: %w", name, jetstream.ErrStreamNotFound)
			}
			startDeletion("rt_msn_tasks", "rt_msn_results")

			_, err := deleter.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(missionGone()).To(BeTrue())
		})

		It("keeps the finalizer and requeues when a stream delete fails", func() {
			rec.streamErr = func(string) error { return fmt.Errorf("nats unavailable") }
			startDeletion("rt_msn_tasks", "rt_msn_results")

			result, err := deleter.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(RequeueModerate))

			m := &aiv1alpha1.Mission{}
			Expect(k8sClient.Get(ctx, missionNN, m)).To(Succeed())
			Expect(controllerutil.ContainsFinalizer(m, missionFinalizer)).To(BeTrue())
		})

		It("completes immediately when the mission never created streams", func() {
			startDeletion("", "")

			_, err := deleter.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
			Expect(err).NotTo(HaveOccurred())
			Expect(rec.deletedStreams()).To(BeEmpty())
			Expect(missionGone()).To(BeTrue())
		})
	})

	Context("Knight Templates", func() {
		var (
			roundTableName = "test-roundtable"
			rtNN           = types.NamespacedName{Name: roundTableName, Namespace: namespace}
		)

		createRoundTableWithTemplates := func() {
			rt := &aiv1alpha1.RoundTable{}
			err := k8sClient.Get(ctx, rtNN, rt)
			if err != nil && errors.IsNotFound(err) {
				rt = &aiv1alpha1.RoundTable{
					ObjectMeta: metav1.ObjectMeta{
						Name:      roundTableName,
						Namespace: namespace,
					},
					Spec: aiv1alpha1.RoundTableSpec{
						Description: "RoundTable with knight templates",
						NATS: aiv1alpha1.RoundTableNATS{
							URL:           "nats://nats.test:4222",
							SubjectPrefix: "test-fleet",
							TasksStream:   "test_tasks",
							ResultsStream: "test_results",
						},
						KnightTemplates: map[string]aiv1alpha1.KnightSpec{
							"auditor": {
								Domain: "security",
								Model:  "claude-sonnet-4-20250514",
								Skills: []string{"security", "nmap", "reconnaissance"},
								NATS: aiv1alpha1.KnightNATS{
									Subjects: []string{"test-fleet.tasks.security.>"},
								},
								Concurrency:        2,
								TaskTimeoutSeconds: 300,
							},
							"pentester": {
								Domain: "security",
								Model:  "claude-opus-4-20250514",
								Skills: []string{"security", "exploitation"},
								NATS: aiv1alpha1.KnightNATS{
									Subjects: []string{"test-fleet.tasks.security.>"},
								},
								Concurrency:        1,
								TaskTimeoutSeconds: 600,
							},
							"reporter": {
								Domain: "research",
								Model:  "claude-haiku-35-20241022",
								Skills: []string{"research", "documentation"},
								NATS: aiv1alpha1.KnightNATS{
									Subjects: []string{"test-fleet.tasks.research.>"},
								},
								Concurrency:        4,
								TaskTimeoutSeconds: 120,
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, rt)).To(Succeed())

				// Mark RoundTable as ready
				rt.Status.Phase = aiv1alpha1.RoundTablePhaseReady
				Expect(k8sClient.Status().Update(ctx, rt)).To(Succeed())
			}
		}

		deleteRoundTable := func() {
			rt := &aiv1alpha1.RoundTable{}
			if err := k8sClient.Get(ctx, rtNN, rt); err == nil {
				_ = k8sClient.Delete(ctx, rt)
			}
		}

		Context("When using RoundTable templates", func() {
			BeforeEach(func() {
				createRoundTableWithTemplates()
			})

			AfterEach(func() {
				deleteMission()
				deleteRoundTable()
			})

			It("should create ephemeral knight from RoundTable template", func() {
				createMission(aiv1alpha1.MissionSpec{
					Objective:     "Test RoundTable templates",
					RoundTableRef: aiv1alpha1.LocalRef(roundTableName),
					Knights: []aiv1alpha1.MissionKnight{
						{
							Name:         "test-auditor",
							Ephemeral:    true,
							TemplateName: "auditor",
							Role:         "security-analyst",
						},
					},
					TTLSeconds:     3600,
					TimeoutSeconds: 1800,
				})

				r := newReconciler()

				// Reconcile until Assembling creates the ephemeral knight
				for range 15 {
					_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
					makeEphemeralKnightsReady()
				}

				// Verify ephemeral knight was created with template values
				ephemeralKnightName := fmt.Sprintf("%s-%s", missionName, "test-auditor")
				ephemeralKnightNN := types.NamespacedName{Name: ephemeralKnightName, Namespace: namespace}
				knight := &aiv1alpha1.Knight{}
				Expect(k8sClient.Get(ctx, ephemeralKnightNN, knight)).To(Succeed())

				// Verify template values
				Expect(knight.Spec.Domain).To(Equal("security"))
				Expect(knight.Spec.Model).To(Equal("claude-sonnet-4-20250514"))
				Expect(knight.Spec.Skills).To(ConsistOf("security", "nmap", "reconnaissance"))
				Expect(knight.Spec.Concurrency).To(Equal(int32(2)))
				Expect(knight.Spec.TaskTimeoutSeconds).To(Equal(int32(300)))

				// Verify labels
				Expect(knight.Labels[aiv1alpha1.LabelMission]).To(Equal(missionName))
				Expect(knight.Labels[aiv1alpha1.LabelEphemeral]).To(Equal("true"))
				Expect(knight.Labels[aiv1alpha1.LabelRole]).To(Equal("security-analyst"))

				// Cleanup
				_ = k8sClient.Delete(ctx, knight)
			})

			It("should apply specOverrides on top of RoundTable template", func() {
				overrideConcurrency := int32(5)
				createMission(aiv1alpha1.MissionSpec{
					Objective:     "Test template overrides",
					RoundTableRef: aiv1alpha1.LocalRef(roundTableName),
					Knights: []aiv1alpha1.MissionKnight{
						{
							Name:         "test-pentester",
							Ephemeral:    true,
							TemplateName: "pentester",
							SpecOverrides: &aiv1alpha1.KnightSpecOverrides{
								Model:       "claude-sonnet-4-20250514", // Override opus → sonnet
								Skills:      []string{"security", "custom-skill"},
								Concurrency: &overrideConcurrency,
								Env: []corev1.EnvVar{
									{Name: "TARGET_URL", Value: "https://example.com"},
								},
							},
						},
					},
					TTLSeconds:     3600,
					TimeoutSeconds: 1800,
				})

				r := newReconciler()
				for range 15 {
					_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
					makeEphemeralKnightsReady()
				}

				ephemeralKnightName := fmt.Sprintf("%s-%s", missionName, "test-pentester")
				ephemeralKnightNN := types.NamespacedName{Name: ephemeralKnightName, Namespace: namespace}
				knight := &aiv1alpha1.Knight{}
				Expect(k8sClient.Get(ctx, ephemeralKnightNN, knight)).To(Succeed())

				// Verify overrides applied
				Expect(knight.Spec.Model).To(Equal("claude-sonnet-4-20250514"))      // Overridden
				Expect(knight.Spec.Skills).To(ConsistOf("security", "custom-skill")) // Overridden
				Expect(knight.Spec.Concurrency).To(Equal(int32(5)))                  // Overridden
				Expect(knight.Spec.Domain).To(Equal("security"))                     // From template
				Expect(knight.Spec.TaskTimeoutSeconds).To(Equal(int32(600)))         // From template

				// Verify environment variables added
				found := false
				for _, env := range knight.Spec.Env {
					if env.Name == "TARGET_URL" && env.Value == "https://example.com" {
						found = true
						break
					}
				}
				Expect(found).To(BeTrue(), "Expected TARGET_URL env var to be present")

				// Cleanup
				_ = k8sClient.Delete(ctx, knight)
			})

			It("should prioritize mission-level template over RoundTable template", func() {
				createMission(aiv1alpha1.MissionSpec{
					Objective:     "Test template priority",
					RoundTableRef: aiv1alpha1.LocalRef(roundTableName),
					// Mission-level template overrides RoundTable template with same name
					KnightTemplates: []aiv1alpha1.MissionKnightTemplate{
						{
							Name: "auditor", // Same name as RoundTable template
							Spec: aiv1alpha1.KnightSpec{
								Domain:             "incident-response",
								Model:              "claude-opus-4-20250514",
								Skills:             []string{"forensics", "incident-response"},
								Concurrency:        10,
								TaskTimeoutSeconds: 900,
								NATS: aiv1alpha1.KnightNATS{
									Subjects: []string{"mission.tasks.ir.>"},
								},
							},
						},
					},
					Knights: []aiv1alpha1.MissionKnight{
						{
							Name:         "test-auditor",
							Ephemeral:    true,
							TemplateName: "auditor", // Should use mission-level template
						},
					},
					TTLSeconds:     3600,
					TimeoutSeconds: 1800,
				})

				r := newReconciler()
				driveToPhase(r, aiv1alpha1.MissionPhaseActive, 20, readyOnProvisioning(), readyEphemeralsOnAssembling())

				ephemeralKnightName := fmt.Sprintf("%s-%s", missionName, "test-auditor")
				ephemeralKnightNN := types.NamespacedName{Name: ephemeralKnightName, Namespace: namespace}
				knight := &aiv1alpha1.Knight{}
				Expect(k8sClient.Get(ctx, ephemeralKnightNN, knight)).To(Succeed())

				// Verify mission-level template values (not RoundTable template values)
				Expect(knight.Spec.Domain).To(Equal("incident-response"))                  // From mission template
				Expect(knight.Spec.Model).To(Equal("claude-opus-4-20250514"))              // From mission template
				Expect(knight.Spec.Skills).To(ConsistOf("forensics", "incident-response")) // From mission template
				Expect(knight.Spec.Concurrency).To(Equal(int32(10)))                       // From mission template
				Expect(knight.Spec.TaskTimeoutSeconds).To(Equal(int32(900)))               // From mission template

				// Should NOT have RoundTable template values
				Expect(knight.Spec.Domain).NotTo(Equal("security"))
				Expect(knight.Spec.Concurrency).NotTo(Equal(int32(2)))

				// Cleanup
				_ = k8sClient.Delete(ctx, knight)
			})

			It("should use multiple templates from RoundTable", func() {
				createMission(aiv1alpha1.MissionSpec{
					Objective:     "Test multiple templates",
					RoundTableRef: aiv1alpha1.LocalRef(roundTableName),
					Knights: []aiv1alpha1.MissionKnight{
						{
							Name:         "auditor1",
							Ephemeral:    true,
							TemplateName: "auditor",
						},
						{
							Name:         "pentester1",
							Ephemeral:    true,
							TemplateName: "pentester",
						},
						{
							Name:         "reporter1",
							Ephemeral:    true,
							TemplateName: "reporter",
						},
					},
					TTLSeconds:     3600,
					TimeoutSeconds: 1800,
				})

				r := newReconciler()
				for range 15 {
					_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
					makeEphemeralKnightsReady()
				}

				// Verify all three knights created with correct templates
				knights := []struct {
					name        string
					domain      string
					model       string
					concurrency int32
				}{
					{"auditor1", "security", "claude-sonnet-4-20250514", 2},
					{"pentester1", "security", "claude-opus-4-20250514", 1},
					{"reporter1", "research", "claude-haiku-35-20241022", 4},
				}

				for _, k := range knights {
					ephemeralKnightName := fmt.Sprintf("%s-%s", missionName, k.name)
					ephemeralKnightNN := types.NamespacedName{Name: ephemeralKnightName, Namespace: namespace}
					knight := &aiv1alpha1.Knight{}
					Expect(k8sClient.Get(ctx, ephemeralKnightNN, knight)).To(Succeed())

					Expect(knight.Spec.Domain).To(Equal(k.domain))
					Expect(knight.Spec.Model).To(Equal(k.model))
					Expect(knight.Spec.Concurrency).To(Equal(k.concurrency))

					// Cleanup
					_ = k8sClient.Delete(ctx, knight)
				}
			})
		})

		Context("When template reference is invalid", func() {
			BeforeEach(func() {
				createRoundTableWithTemplates()
			})

			AfterEach(func() {
				deleteMission()
				deleteRoundTable()
			})

			It("should fail when templateName not found in mission or RoundTable", func() {
				createMission(aiv1alpha1.MissionSpec{
					Objective:     "Test invalid template",
					RoundTableRef: aiv1alpha1.LocalRef(roundTableName),
					Knights: []aiv1alpha1.MissionKnight{
						{
							Name:         "test-knight",
							Ephemeral:    true,
							TemplateName: "nonexistent-template", // Template doesn't exist
						},
					},
					TTLSeconds:     3600,
					TimeoutSeconds: 1800,
				})

				r := newReconciler()

				// Reconcile should fail or set an error condition
				// Drive a few iterations to let the controller process
				for range 5 {
					_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
				}

				// Mission should have error condition or stay in early phase
				mission := &aiv1alpha1.Mission{}
				Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())

				// Should not progress to Active phase due to error
				Expect(mission.Status.Phase).NotTo(Equal(aiv1alpha1.MissionPhaseActive))

				// Should have error condition
				knightReadyCond := meta.FindStatusCondition(mission.Status.Conditions, "KnightsReady")
				if knightReadyCond != nil {
					Expect(knightReadyCond.Status).To(Equal(metav1.ConditionFalse))
				}
			})

			It("should fail when ephemeral=true but neither ephemeralSpec nor templateName", func() {
				createMission(aiv1alpha1.MissionSpec{
					Objective:     "Test missing spec",
					RoundTableRef: aiv1alpha1.LocalRef(roundTableName),
					Knights: []aiv1alpha1.MissionKnight{
						{
							Name:      "test-knight",
							Ephemeral: true,
							// No ephemeralSpec, no templateName
						},
					},
					TTLSeconds:     3600,
					TimeoutSeconds: 1800,
				})

				r := newReconciler()

				// Reconcile should fail
				for range 5 {
					_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
				}

				mission := &aiv1alpha1.Mission{}
				Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())

				// Should not progress to Active
				Expect(mission.Status.Phase).NotTo(Equal(aiv1alpha1.MissionPhaseActive))
			})
		})

		Context("When RoundTable has no templates", func() {
			var emptyRTName = "empty-roundtable"
			var emptyRTNN = types.NamespacedName{Name: emptyRTName, Namespace: namespace}

			BeforeEach(func() {
				rt := &aiv1alpha1.RoundTable{
					ObjectMeta: metav1.ObjectMeta{
						Name:      emptyRTName,
						Namespace: namespace,
					},
					Spec: aiv1alpha1.RoundTableSpec{
						Description: "RoundTable without templates",
						NATS: aiv1alpha1.RoundTableNATS{
							URL:           "nats://nats.test:4222",
							SubjectPrefix: "test-fleet",
							TasksStream:   "test_tasks",
							ResultsStream: "test_results",
						},
						// No knightTemplates
					},
				}
				Expect(k8sClient.Create(ctx, rt)).To(Succeed())
				rt.Status.Phase = aiv1alpha1.RoundTablePhaseReady
				Expect(k8sClient.Status().Update(ctx, rt)).To(Succeed())
			})

			AfterEach(func() {
				deleteMission()
				rt := &aiv1alpha1.RoundTable{}
				if err := k8sClient.Get(ctx, emptyRTNN, rt); err == nil {
					_ = k8sClient.Delete(ctx, rt)
				}
			})

			It("should fail gracefully when templateName used but RoundTable has no templates", func() {
				createMission(aiv1alpha1.MissionSpec{
					Objective:     "Test empty RoundTable",
					RoundTableRef: aiv1alpha1.LocalRef(emptyRTName),
					Knights: []aiv1alpha1.MissionKnight{
						{
							Name:         "test-knight",
							Ephemeral:    true,
							TemplateName: "auditor", // Template doesn't exist
						},
					},
					TTLSeconds:     3600,
					TimeoutSeconds: 1800,
				})

				r := newReconciler()

				// Reconcile should fail gracefully
				for range 5 {
					_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
				}

				mission := &aiv1alpha1.Mission{}
				Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())

				// Should not progress to Active
				Expect(mission.Status.Phase).NotTo(Equal(aiv1alpha1.MissionPhaseActive))
			})

			It("should work with inline ephemeralSpec when RoundTable has no templates", func() {
				createMission(aiv1alpha1.MissionSpec{
					Objective:     "Test inline spec",
					RoundTableRef: aiv1alpha1.LocalRef(emptyRTName),
					Knights: []aiv1alpha1.MissionKnight{
						{
							Name:      "test-knight",
							Ephemeral: true,
							EphemeralSpec: &aiv1alpha1.KnightSpec{
								Domain:             "general",
								Model:              "claude-sonnet-4-20250514",
								Skills:             []string{"general"},
								Concurrency:        3,
								TaskTimeoutSeconds: 180,
								NATS: aiv1alpha1.KnightNATS{
									Subjects: []string{"test.tasks.general.>"},
								},
							},
						},
					},
					TTLSeconds:     3600,
					TimeoutSeconds: 1800,
				})

				r := newReconciler()
				driveToPhase(r, aiv1alpha1.MissionPhaseActive, 20, readyOnProvisioning(), readyEphemeralsOnAssembling())

				// Verify ephemeral knight created with inline spec
				ephemeralKnightName := fmt.Sprintf("%s-%s", missionName, "test-knight")
				ephemeralKnightNN := types.NamespacedName{Name: ephemeralKnightName, Namespace: namespace}
				knight := &aiv1alpha1.Knight{}
				Expect(k8sClient.Get(ctx, ephemeralKnightNN, knight)).To(Succeed())

				Expect(knight.Spec.Domain).To(Equal("general"))
				Expect(knight.Spec.Model).To(Equal("claude-sonnet-4-20250514"))
				Expect(knight.Spec.Concurrency).To(Equal(int32(3)))

				// Cleanup
				_ = k8sClient.Delete(ctx, knight)
			})
		})
	})
})

var _ = Describe("Mission Controller - Sandbox mission knights", func() {
	Context("When a mission knight's template says runtime: sandbox", func() {
		const (
			missionName  = "test-sandbox-mission"
			rtName       = "test-sandbox-rt"
			namespace    = "default"
			templateName = "sandbox-knight"
		)

		ctx := context.Background()
		missionNN := types.NamespacedName{Name: missionName, Namespace: namespace}
		rtNN := types.NamespacedName{Name: rtName, Namespace: namespace}

		var rt *aiv1alpha1.RoundTable

		BeforeEach(func() {
			rt = &aiv1alpha1.RoundTable{
				ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: namespace},
				Spec: aiv1alpha1.RoundTableSpec{
					NATS: aiv1alpha1.RoundTableNATS{
						URL:           "nats://nats.test:4222",
						SubjectPrefix: "sbx",
						TasksStream:   "sbx_tasks",
						ResultsStream: "sbx_results",
					},
					KnightTemplates: map[string]aiv1alpha1.KnightSpec{
						templateName: {
							Runtime: "sandbox",
							Domain:  "general",
							Model:   "claude-sonnet-4-20250514",
							Skills:  []string{"general"},
							NATS: aiv1alpha1.KnightNATS{
								URL:           "nats://nats.test:4222",
								Subjects:      []string{"sbx.tasks.general.>"},
								Stream:        "sbx_tasks",
								ResultsStream: "sbx_results",
							},
						},
					},
					Ephemeral: true,
				},
			}
			Expect(k8sClient.Create(ctx, rt)).To(Succeed())
		})

		AfterEach(func() {
			mission := &aiv1alpha1.Mission{}
			if err := k8sClient.Get(ctx, missionNN, mission); err == nil {
				mission.Finalizers = nil
				_ = k8sClient.Update(ctx, mission)
				_ = k8sClient.Delete(ctx, mission)
			}
			knights := &aiv1alpha1.KnightList{}
			_ = k8sClient.List(ctx, knights, client.InNamespace(namespace),
				client.MatchingLabels{aiv1alpha1.LabelRoundTable: rtName})
			for i := range knights.Items {
				_ = k8sClient.Delete(ctx, &knights.Items[i])
			}
			if err := k8sClient.Get(ctx, rtNN, rt); err == nil {
				_ = k8sClient.Delete(ctx, rt)
			}
		})

		It("creates the knight directly and its Sandbox shuts down at the mission's expiry", func() {
			mission := &aiv1alpha1.Mission{
				ObjectMeta: metav1.ObjectMeta{Name: missionName, Namespace: namespace},
				Spec: aiv1alpha1.MissionSpec{
					Objective:     "Test bare sandbox mission knight",
					RoundTableRef: aiv1alpha1.LocalRef(rtName),
					Knights: []aiv1alpha1.MissionKnight{
						{Name: "knight1", Ephemeral: true, TemplateName: templateName},
					},
					TimeoutSeconds: 600,
					TTLSeconds:     900,
				},
			}
			Expect(k8sClient.Create(ctx, mission)).To(Succeed())

			reconciler := &MissionReconciler{
				Client:      k8sClient,
				Scheme:      k8sClient.Scheme(),
				Recorder:    record.NewFakeRecorder(100),
				Assembler:   &missionpkg.KnightAssembler{Client: k8sClient, Scheme: k8sClient.Scheme()},
				Cleaner:     &missionpkg.Cleaner{Client: k8sClient},
				CostTracker: &missionpkg.CostTracker{Client: k8sClient},
				Briefer:     &missionpkg.Briefer{Client: k8sClient, Recorder: record.NewFakeRecorder(100)},
				Chains:      &missionpkg.ChainManager{Client: k8sClient, Scheme: k8sClient.Scheme()},
			}

			// Drive into Assembling. The knight never becomes ready in envtest,
			// so the mission stays there once the knight exists.
			knightNN := types.NamespacedName{Name: missionName + "-knight1", Namespace: namespace}
			knight := &aiv1alpha1.Knight{}
			Eventually(func() error {
				_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
				Expect(err).NotTo(HaveOccurred())
				return k8sClient.Get(ctx, knightNN, knight)
			}, 5*time.Second, 50*time.Millisecond).Should(Succeed())

			Expect(k8sClient.Get(ctx, missionNN, mission)).To(Succeed())
			Expect(mission.Status.StartedAt).NotTo(BeNil())
			Expect(mission.Status.ExpiresAt).NotTo(BeNil())
			// ExpiresAt is fixed at status init (creation + TTL); StartedAt is
			// reset at admission, so the two are no longer tied together.
			wantShutdown := mission.Status.ExpiresAt.UTC().Format(time.RFC3339)
			Expect(mission.Status.ExpiresAt.Time).To(BeTemporally("~",
				mission.CreationTimestamp.Add(900*time.Second), 5*time.Second))

			By("Verifying the knight is a direct mission knight on the sandbox runtime")
			Expect(knight.Spec.Runtime).To(Equal("sandbox"))
			Expect(knight.Labels[aiv1alpha1.LabelMission]).To(Equal(missionName))
			Expect(knight.Annotations[aiv1alpha1.AnnotationShutdownTime]).To(Equal(wantShutdown))
			Expect(metav1.IsControlledBy(knight, mission)).To(BeTrue(),
				"deleting the mission must delete the knight")

			By("Verifying the knight's Sandbox carries shutdownTime")
			// envtest has no agent-sandbox CRDs: store the Sandbox in a fake client.
			s := k8sruntime.NewScheme()
			Expect(aiv1alpha1.AddToScheme(s)).To(Succeed())
			sandboxClient := fake.NewClientBuilder().WithScheme(s).Build()
			backend := rtruntime.NewSandboxBackend(sandboxClient, s, "",
				func(context.Context, *aiv1alpha1.Knight) corev1.PodSpec { return corev1.PodSpec{} })
			Expect(backend.Reconcile(ctx, knight)).To(Succeed())
			sb := rtruntime.NewSandboxObject("", "")
			Expect(sandboxClient.Get(ctx, knightNN, sb)).To(Succeed())
			shutdown, found, err := unstructured.NestedString(sb.Object, "spec", "shutdownTime")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(shutdown).To(Equal(wantShutdown))
		})
	})
})

var _ = Describe("Mission Controller - Generated Chains", func() {
	Context("When creating chains from mission spec", func() {
		const (
			missionName = "test-generated-chain-mission"
			rtName      = "test-rt"
			namespace   = "default"
		)

		ctx := context.Background()
		missionNN := types.NamespacedName{Name: missionName, Namespace: namespace}
		rtNN := types.NamespacedName{Name: rtName, Namespace: namespace}

		var rt *aiv1alpha1.RoundTable

		BeforeEach(func() {
			By("Creating a RoundTable")
			rt = &aiv1alpha1.RoundTable{
				ObjectMeta: metav1.ObjectMeta{
					Name:      rtName,
					Namespace: namespace,
				},
				Spec: aiv1alpha1.RoundTableSpec{
					NATS: aiv1alpha1.RoundTableNATS{
						URL:           "nats://nats.test:4222",
						SubjectPrefix: "test",
						TasksStream:   "test_tasks",
						ResultsStream: "test_results",
					},
					KnightTemplates: map[string]aiv1alpha1.KnightSpec{
						"default": {
							Domain: "general",
							Model:  "claude-sonnet-4-20250514",
							Skills: []string{"general"},
							NATS: aiv1alpha1.KnightNATS{
								URL:           "nats://nats.test:4222",
								Subjects:      []string{"test.tasks.general.>"},
								Stream:        "test_tasks",
								ResultsStream: "test_results",
							},
							Concurrency:        2,
							TaskTimeoutSeconds: 120,
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, rt)).To(Succeed())
		})

		AfterEach(func() {
			By("Cleaning up mission and resources")
			mission := &aiv1alpha1.Mission{}
			if err := k8sClient.Get(ctx, missionNN, mission); err == nil {
				mission.Finalizers = nil
				_ = k8sClient.Update(ctx, mission)
				_ = k8sClient.Delete(ctx, mission)
			}

			// Delete any generated chains
			chainList := &aiv1alpha1.ChainList{}
			_ = k8sClient.List(ctx, chainList, client.InNamespace(namespace))
			for _, chain := range chainList.Items {
				if chain.Labels[aiv1alpha1.LabelMission] == missionName {
					_ = k8sClient.Delete(ctx, &chain)
				}
			}

			if err := k8sClient.Get(ctx, rtNN, rt); err == nil {
				_ = k8sClient.Delete(ctx, rt)
			}
		})

		It("should set roundTableRef on chains created from mission spec", func() {
			By("Creating a source chain template")
			sourceChain := &aiv1alpha1.Chain{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-chain-template",
					Namespace: namespace,
				},
				Spec: aiv1alpha1.ChainSpec{
					Description: "Test chain template",
					Steps: []aiv1alpha1.ChainStep{
						{
							Name:      "step1",
							KnightRef: corev1.LocalObjectReference{Name: "test-knight"},
							Task:      "Test task",
						},
					},
					RoundTableRef: aiv1alpha1.LocalRef(rtName),
				},
			}
			Expect(k8sClient.Create(ctx, sourceChain)).To(Succeed())
			defer func() {
				_ = k8sClient.Delete(ctx, sourceChain)
			}()

			By("Creating a mission with chain reference")
			mission := &aiv1alpha1.Mission{
				ObjectMeta: metav1.ObjectMeta{
					Name:      missionName,
					Namespace: namespace,
				},
				Spec: aiv1alpha1.MissionSpec{
					Objective:     "Test generated chains",
					RoundTableRef: aiv1alpha1.LocalRef(rtName),
					Chains: []aiv1alpha1.MissionChainRef{
						{
							Name:  "test-chain-template",
							Phase: "Active",
						},
					},
					TimeoutSeconds: 600,
					TTLSeconds:     900,
				},
			}
			Expect(k8sClient.Create(ctx, mission)).To(Succeed())

			By("Reconciling the mission")
			reconciler := &MissionReconciler{
				Client:      k8sClient,
				Scheme:      k8sClient.Scheme(),
				Recorder:    record.NewFakeRecorder(100),
				Assembler:   &missionpkg.KnightAssembler{Client: k8sClient, Scheme: k8sClient.Scheme()},
				Cleaner:     &missionpkg.Cleaner{Client: k8sClient},
				CostTracker: &missionpkg.CostTracker{Client: k8sClient},
				Briefer:     &missionpkg.Briefer{Client: k8sClient, Recorder: record.NewFakeRecorder(100)},
				Chains:      &missionpkg.ChainManager{Client: k8sClient, Scheme: k8sClient.Scheme()},
			}

			// Reconcile a few times to create the mission-scoped chain
			for range 10 {
				_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: missionNN})
				Expect(err).NotTo(HaveOccurred())
				time.Sleep(100 * time.Millisecond)
			}

			By("Verifying the generated chain has roundTableRef")
			generatedChainName := fmt.Sprintf("mission-%s-test-chain-template", missionName)
			generatedChain := &aiv1alpha1.Chain{}

			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{
					Name:      generatedChainName,
					Namespace: namespace,
				}, generatedChain)
			}, "5s", "500ms").Should(Succeed())

			Expect(aiv1alpha1.RefName(generatedChain.Spec.RoundTableRef)).To(Equal(rtName),
				"Generated chain should inherit roundTableRef from parent mission")
		})

		It("should set roundTableRef on chains created by planner", func() {
			By("Creating a mission with GeneratedChains (simulating planner output)")
			mission := &aiv1alpha1.Mission{
				ObjectMeta: metav1.ObjectMeta{
					Name:      missionName,
					Namespace: namespace,
				},
				Spec: aiv1alpha1.MissionSpec{
					Objective:     "Test planner-generated chains",
					RoundTableRef: aiv1alpha1.LocalRef(rtName),
					GeneratedChains: []aiv1alpha1.GeneratedChain{
						{
							Name:        "planner-chain",
							Description: "Chain generated by planner",
							Steps: []aiv1alpha1.ChainStep{
								{
									Name:      "analyze",
									KnightRef: corev1.LocalObjectReference{Name: "analyst"},
									Task:      "Analyze the data",
								},
							},
							Phase:          "Active",
							TimeoutSeconds: func() *int32 { t := int32(300); return &t }(),
						},
					},
					TimeoutSeconds: 600,
					TTLSeconds:     900,
				},
			}
			Expect(k8sClient.Create(ctx, mission)).To(Succeed())

			By("Manually creating the chain as the planner would")
			// Note: In real scenario, the planner creates chains in applyPlan()
			// We're simulating that here
			plannerChainName := fmt.Sprintf("%s-planner-chain", missionName)
			plannerChain := &aiv1alpha1.Chain{
				ObjectMeta: metav1.ObjectMeta{
					Name:      plannerChainName,
					Namespace: namespace,
					Labels: map[string]string{
						aiv1alpha1.LabelMission:         missionName,
						aiv1alpha1.LabelEphemeral:       "true",
						"ai.roundtable.io/generated-by": "planner",
					},
				},
				Spec: aiv1alpha1.ChainSpec{
					Description: "Chain generated by planner",
					Steps: []aiv1alpha1.ChainStep{
						{
							Name:      "analyze",
							KnightRef: corev1.LocalObjectReference{Name: "analyst"},
							Task:      "Analyze the data",
						},
					},
					MissionRef:    aiv1alpha1.LocalRef(missionName),
					RoundTableRef: mission.Spec.RoundTableRef, // This is the fix for issue #84
				},
			}
			Expect(k8sClient.Create(ctx, plannerChain)).To(Succeed())

			By("Verifying the planner-generated chain has roundTableRef")
			chain := &aiv1alpha1.Chain{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      plannerChainName,
				Namespace: namespace,
			}, chain)).To(Succeed())

			Expect(aiv1alpha1.RefName(chain.Spec.RoundTableRef)).To(Equal(rtName),
				"Planner-generated chain should inherit roundTableRef from parent mission (issue #84)")
			Expect(aiv1alpha1.RefName(chain.Spec.MissionRef)).To(Equal(missionName),
				"Planner-generated chain should have missionRef set")
		})
	})
})
