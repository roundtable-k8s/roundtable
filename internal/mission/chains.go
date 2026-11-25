package mission

import (
	"context"
	"fmt"
	"strconv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// defaultRoundTableRef is the RoundTable used when a mission does not name one.
const defaultRoundTableRef = "default"

// chainPhaseActive is the default MissionChainRef phase.
const chainPhaseActive = "Active"

// ChainManager creates, triggers and tracks the Chain CRs of a mission.
type ChainManager struct {
	Client client.Client
	Scheme *runtime.Scheme
}

// ReconcileChains creates and monitors Chain CRs for the mission.
// Returns (allComplete, anyFailed, error).
func (m *ChainManager) ReconcileChains(ctx context.Context, mission *aiv1alpha1.Mission) (bool, bool, error) {
	log := logf.FromContext(ctx)

	// Determine which phase chains to run based on mission state
	// Setup chains run first, then Active, Teardown runs during cleanup
	activePhases := []string{"Setup", chainPhaseActive}

	allComplete := true
	anyFailed := false

	for _, chainRef := range mission.Spec.Chains {
		// Only process chains for current active phases
		phaseMatch := false
		for _, p := range activePhases {
			if chainRef.Phase == p || (chainRef.Phase == "" && p == chainPhaseActive) { // default is Active
				phaseMatch = true
				break
			}
		}
		if !phaseMatch {
			continue
		}

		// Create mission-scoped chain copy if it doesn't exist
		if err := m.EnsureMissionChain(ctx, mission, chainRef); err != nil {
			log.Error(err, "Failed to create mission chain", "chain", chainRef.Name)
			anyFailed = true
			continue
		}

		// Monitor the mission-scoped chain copy
		missionChainName := fmt.Sprintf("mission-%s-%s", mission.Name, chainRef.Name)
		chain := &aiv1alpha1.Chain{}
		err := m.Client.Get(ctx, types.NamespacedName{
			Name:      missionChainName,
			Namespace: mission.Namespace,
		}, chain)
		if err != nil {
			if client.IgnoreNotFound(err) == nil {
				log.Info("Mission chain not yet created", "chain", missionChainName)
				allComplete = false
				continue
			}
			return false, false, err
		}

		// Trigger idle chains — mission-scoped chains have no schedule,
		// so the mission controller must kick them to Running.
		if chain.Status.Phase == aiv1alpha1.ChainPhaseIdle || chain.Status.Phase == "" {
			// Re-fetch to avoid conflict errors (chain controller may have reconciled)
			if err := m.Client.Get(ctx, types.NamespacedName{
				Name:      missionChainName,
				Namespace: mission.Namespace,
			}, chain); err != nil {
				log.Error(err, "Failed to re-fetch chain for trigger", "chain", missionChainName)
				allComplete = false
				continue
			}
			now := metav1.Now()
			chain.Status.Phase = aiv1alpha1.ChainPhaseRunning
			chain.Status.StartedAt = &now
			if err := m.Client.Status().Update(ctx, chain); err != nil {
				if apierrors.IsConflict(err) {
					log.Info("Conflict triggering chain, will retry", "chain", missionChainName)
					allComplete = false
					continue
				}
				return false, false, fmt.Errorf("failed to trigger chain %s: %w", missionChainName, err)
			}
			log.Info("Triggered mission chain", "chain", missionChainName)
			m.UpdateChainStatus(mission, chainRef.Name, missionChainName, aiv1alpha1.ChainPhaseRunning)
			allComplete = false
			continue
		}

		// Update mission.status.chainStatuses
		m.UpdateChainStatus(mission, chainRef.Name, missionChainName, chain.Status.Phase)
		m.UpdateChainUsage(mission, chainRef.Name, chain)

		// Check chain status
		switch chain.Status.Phase {
		case aiv1alpha1.ChainPhaseSucceeded:
			// OK
		case aiv1alpha1.ChainPhaseFailed:
			anyFailed = true
		default:
			allComplete = false
		}
	}

	return allComplete, anyFailed, nil
}

// EnsureMissionChain creates a mission-scoped chain copy if it doesn't already exist.
func (m *ChainManager) EnsureMissionChain(ctx context.Context, mission *aiv1alpha1.Mission, chainRef aiv1alpha1.MissionChainRef) error {
	log := logf.FromContext(ctx)

	// Build the mission-scoped chain name
	missionChainName := fmt.Sprintf("mission-%s-%s", mission.Name, chainRef.Name)

	// If the mission-scoped chain already exists, there is nothing to do.
	// Meta-mission chains are created directly by the planner (applyPlan), so
	// there is no standalone source-chain template to copy from — this
	// short-circuits before the source lookup below, which would otherwise
	// fail with "source chain not found" and mark the mission Failed even
	// though its chain ran successfully.
	existingChain := &aiv1alpha1.Chain{}
	err := m.Client.Get(ctx, types.NamespacedName{
		Name:      missionChainName,
		Namespace: mission.Namespace,
	}, existingChain)
	if err == nil {
		// Chain already exists
		return nil
	}
	if client.IgnoreNotFound(err) != nil {
		return err
	}

	// Fetch the source chain template (template-based missions only)
	sourceChain := &aiv1alpha1.Chain{}
	if err := m.Client.Get(ctx, types.NamespacedName{
		Name:      chainRef.Name,
		Namespace: mission.Namespace,
	}, sourceChain); err != nil {
		return fmt.Errorf("source chain %q not found: %w", chainRef.Name, err)
	}

	// Get RoundTable reference for this mission
	rtRef := aiv1alpha1.RefName(mission.Spec.RoundTableRef)
	if rtRef == "" {
		rtRef = defaultRoundTableRef // fallback to default if not specified
	}

	// Create the mission-scoped chain
	missionChain := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{
			Name:      missionChainName,
			Namespace: mission.Namespace,
			Labels: map[string]string{
				"ai.roundtable.io/mission":     mission.Name,
				"ai.roundtable.io/chain-phase": chainRef.Phase,
			},
			Annotations: ParentAnnotations(mission),
		},
		Spec: aiv1alpha1.ChainSpec{
			Description:     fmt.Sprintf("Mission %s: %s", mission.Name, sourceChain.Spec.Description),
			Steps:           sourceChain.Spec.Steps,
			TimeoutSeconds:  sourceChain.Spec.TimeoutSeconds,
			RoundTableRef:   aiv1alpha1.LocalRef(rtRef),
			OutputKnightRef: sourceChain.Spec.OutputKnightRef,
			RetryPolicy:     sourceChain.Spec.RetryPolicy,
		},
	}

	// Override input if specified
	if chainRef.InputOverride != "" {
		missionChain.Spec.Input = chainRef.InputOverride
	} else {
		missionChain.Spec.Input = sourceChain.Spec.Input
	}

	// Set owner reference for garbage collection
	if err := controllerutil.SetControllerReference(mission, missionChain, m.Scheme); err != nil {
		return fmt.Errorf("failed to set owner reference: %w", err)
	}

	// Create the chain CR
	if err := m.Client.Create(ctx, missionChain); err != nil {
		return fmt.Errorf("failed to create mission chain: %w", err)
	}

	log.Info("Created mission-scoped chain", "chain", missionChainName, "sourceChain", chainRef.Name)
	return nil
}

// TriggerGeneratedChains transitions all mission-generated chains from Idle to Running.
// This is necessary because the chain controller only triggers chains via cron schedule,
// and mission-generated chains have no schedule.
func (m *ChainManager) TriggerGeneratedChains(ctx context.Context, mission *aiv1alpha1.Mission) error {
	log := logf.FromContext(ctx)

	// Iterate all chains owned by this mission
	chainList := &aiv1alpha1.ChainList{}
	if err := m.Client.List(ctx, chainList,
		client.InNamespace(mission.Namespace),
		client.MatchingLabels{aiv1alpha1.LabelMission: mission.Name},
	); err != nil {
		return fmt.Errorf("failed to list mission chains: %w", err)
	}

	for _, chain := range chainList.Items {
		// Only trigger chains that are in Idle phase
		if chain.Status.Phase != aiv1alpha1.ChainPhaseIdle && chain.Status.Phase != "" {
			continue
		}

		// Transition to Running
		log.Info("Triggering generated chain", "chain", chain.Name)
		now := metav1.Now()
		chain.Status.Phase = aiv1alpha1.ChainPhaseRunning
		chain.Status.StartedAt = &now
		if err := m.Client.Status().Update(ctx, &chain); err != nil {
			// Log but don't fail the mission - the chain controller will eventually reconcile
			log.Error(err, "Failed to trigger chain (will retry)", "chain", chain.Name)
			return fmt.Errorf("failed to trigger chain %s: %w", chain.Name, err)
		}
	}

	return nil
}

// UpdateChainStatus updates the mission's chainStatuses array with the latest chain status.
func (m *ChainManager) UpdateChainStatus(mission *aiv1alpha1.Mission, chainRefName, chainCRName string, phase aiv1alpha1.ChainPhase) {
	// Find existing status entry
	for i := range mission.Status.ChainStatuses {
		if mission.Status.ChainStatuses[i].Name == chainRefName {
			mission.Status.ChainStatuses[i].ChainCRName = chainCRName
			mission.Status.ChainStatuses[i].Phase = phase
			return
		}
	}

	// Add new status entry
	mission.Status.ChainStatuses = append(mission.Status.ChainStatuses, aiv1alpha1.MissionChainStatus{
		Name:        chainRefName,
		ChainCRName: chainCRName,
		Phase:       phase,
	})
}

// UpdateChainUsage copies a chain's run cost and token totals into the
// mission's chainStatuses entry for it.
func (m *ChainManager) UpdateChainUsage(mission *aiv1alpha1.Mission, chainRefName string, chain *aiv1alpha1.Chain) {
	for i := range mission.Status.ChainStatuses {
		cs := &mission.Status.ChainStatuses[i]
		if cs.Name != chainRefName {
			continue
		}
		cs.CostUSD = chain.Status.CostUSD
		cs.Tokens = chain.Status.Tokens.DeepCopy()
		return
	}
}

// SumChainUsage totals the cost (USD) and tokens of the mission's chains as
// last copied into status.chainStatuses by UpdateChainUsage.
func SumChainUsage(mission *aiv1alpha1.Mission) (cost float64, tokens *aiv1alpha1.TokenTotals) {
	for _, cs := range mission.Status.ChainStatuses {
		if v, err := strconv.ParseFloat(cs.CostUSD, 64); err == nil {
			cost += v
		}
		if cs.Tokens != nil {
			if tokens == nil {
				tokens = &aiv1alpha1.TokenTotals{}
			}
			tokens.Input += cs.Tokens.Input
			tokens.Output += cs.Tokens.Output
		}
	}
	return cost, tokens
}
