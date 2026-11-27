package mission

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

const (
	unknownKnight = "unknown"
	// costEpsilon absorbs float noise and the 6-decimal rounding of costUSD
	// and knightCosts when comparing a step's costUSD with the sum of its
	// knightCosts: each rounded value is within 5e-7 of its true value, so the
	// difference can be off by up to 1e-6.
	costEpsilon = 2e-6
)

// CostTracker aggregates per-knight cost for a mission and suspends mission
// chains when the budget is exceeded.
type CostTracker struct {
	Client client.Client
}

// SuspendMissionChains suspends all mission-owned chains to prevent further cost accumulation.
//
//nolint:unparam // error return kept so failures can propagate without a caller change
func (c *CostTracker) SuspendMissionChains(ctx context.Context, mission *aiv1alpha1.Mission) error {
	log := logf.FromContext(ctx)

	for _, cs := range mission.Status.ChainStatuses {
		if cs.ChainCRName == "" {
			continue
		}

		chain := &aiv1alpha1.Chain{}
		if err := c.Client.Get(ctx, types.NamespacedName{
			Name:      cs.ChainCRName,
			Namespace: mission.Namespace,
		}, chain); err != nil {
			if client.IgnoreNotFound(err) == nil {
				continue // Chain not found, skip
			}
			log.Error(err, "Failed to fetch chain for suspension", "chain", cs.ChainCRName)
			continue
		}

		// Set suspended flag
		if !chain.Spec.Suspend {
			chain.Spec.Suspend = true
			if err := c.Client.Update(ctx, chain); err != nil {
				log.Error(err, "Failed to suspend chain", "chain", cs.ChainCRName)
				continue
			}
			log.Info("Suspended chain due to budget exceeded", "chain", cs.ChainCRName)
		}
	}

	return nil
}

// AggregateMissionCost sums the cost of the mission's chains per knight, sets
// Status.CostBreakdown from it and returns the total. A step's knightCosts
// entries are charged to the knight each names ("unknown" for an empty name).
// Any part of the step's costUSD (already summed over its retries, fan-out
// items and loop rounds) that knightCosts does not cover (all of it for steps
// recorded before knightCosts existed) is charged to the step's knight, the
// chain step's knight, a loop body's single knight, or "unknown". A remainder
// within costEpsilon of zero is rounding noise and is not charged. The
// breakdown has one line per knight in Spec.Knights (zero for a knight that
// ran nothing), then one line per other knight that ran a chain step (e.g. a
// fallback), by name.
func (c *CostTracker) AggregateMissionCost(ctx context.Context, mission *aiv1alpha1.Mission) (float64, error) {
	perKnight := map[string]float64{}
	for _, cs := range mission.Status.ChainStatuses {
		if cs.ChainCRName == "" {
			continue
		}
		chain := &aiv1alpha1.Chain{}
		if err := c.Client.Get(ctx, types.NamespacedName{
			Name:      cs.ChainCRName,
			Namespace: mission.Namespace,
		}, chain); err != nil {
			if client.IgnoreNotFound(err) == nil {
				continue
			}
			return 0, err
		}
		for i, ss := range chain.Status.StepStatuses {
			var charged float64
			for _, kc := range ss.KnightCosts {
				v, err := strconv.ParseFloat(kc.CostUSD, 64)
				if err != nil || v <= 0 {
					continue
				}
				charged += v
				name := kc.Name
				if name == "" {
					name = unknownKnight
				}
				perKnight[name] += v
			}
			// Legacy path: whatever part of the step's costUSD knightCosts
			// does not cover (all of it for steps recorded before knightCosts
			// existed) is charged to the step's current knight.
			v, err := strconv.ParseFloat(ss.CostUSD, 64)
			if err != nil {
				continue
			}
			rest := v - charged
			if rest <= costEpsilon {
				continue
			}
			knight := ss.Knight
			if knight == "" && i < len(chain.Spec.Steps) {
				knight = chain.Spec.Steps[i].KnightRef.Name
			}
			if knight == "" && i < len(chain.Spec.Steps) {
				knight = loopBodyKnight(chain.Spec.Steps[i])
			}
			if knight == "" {
				knight = unknownKnight
			}
			perKnight[knight] += rest
		}
	}

	var total float64
	costBreakdown := make([]aiv1alpha1.MissionKnightCost, 0, len(mission.Spec.Knights))
	listed := make(map[string]bool, len(mission.Spec.Knights))
	for _, mk := range mission.Spec.Knights {
		listed[mk.Name] = true
		cost := perKnight[mk.Name]
		total += cost
		costBreakdown = append(costBreakdown, aiv1alpha1.MissionKnightCost{
			Name:      mk.Name,
			CostUSD:   fmt.Sprintf("%.4f", cost),
			Ephemeral: mk.Ephemeral,
		})
	}
	var others []string
	for name := range perKnight {
		if !listed[name] {
			others = append(others, name)
		}
	}
	sort.Strings(others)
	for _, name := range others {
		total += perKnight[name]
		costBreakdown = append(costBreakdown, aiv1alpha1.MissionKnightCost{
			Name:    name,
			CostUSD: fmt.Sprintf("%.4f", perKnight[name]),
		})
	}

	mission.Status.CostBreakdown = costBreakdown
	return total, nil
}

// BudgetStatus is the result of a cost-budget check.
type BudgetStatus struct {
	Total    float64
	Budget   float64
	Exceeded bool
	// Aggregated is true when CheckBudget ran AggregateMissionCost, so
	// Status.CostBreakdown reflects the chain statuses at that moment.
	Aggregated bool
}

// CheckBudget aggregates the mission's cost (the larger of the per-knight
// total and the chains' own total), records it in Status.TotalCost and reports
// whether it exceeds Spec.CostBudgetUSD. A mission with no budget ("" or "0")
// is not touched; an unparseable budget is never exceeded.
func (c *CostTracker) CheckBudget(ctx context.Context, mission *aiv1alpha1.Mission) (BudgetStatus, error) {
	if mission.Spec.CostBudgetUSD == "" || mission.Spec.CostBudgetUSD == "0" {
		return BudgetStatus{}, nil
	}

	total, err := c.AggregateMissionCost(ctx, mission)
	if err != nil {
		return BudgetStatus{}, err
	}
	// The chains' own totals count too: take the larger, so the budget and
	// status agree with the chain numbers.
	if chainCost, _ := SumChainUsage(mission); chainCost > total {
		total = chainCost
	}
	mission.Status.TotalCost = fmt.Sprintf("%.4f", total)

	var budget float64
	if _, err := fmt.Sscanf(mission.Spec.CostBudgetUSD, "%f", &budget); err != nil {
		return BudgetStatus{Total: total, Aggregated: true}, nil
	}
	return BudgetStatus{Total: total, Budget: budget, Exceeded: total > budget, Aggregated: true}, nil
}

// ApplyChainUsage exposes the chains' tokens and cost on the mission status.
// Status.TotalCost is only raised, never lowered.
func ApplyChainUsage(mission *aiv1alpha1.Mission) {
	chainCost, chainTokens := SumChainUsage(mission)
	mission.Status.Tokens = chainTokens
	if chainCost > 0 {
		var current float64
		_, _ = fmt.Sscanf(mission.Status.TotalCost, "%f", &current)
		if chainCost > current {
			mission.Status.TotalCost = fmt.Sprintf("%.4f", chainCost)
		}
	}
}

// loopBodyKnight is the one knight every body step of a loop step names, or ""
// when the step is not a loop or its body spans several knights.
func loopBodyKnight(st aiv1alpha1.ChainStep) string {
	if st.Loop == nil {
		return ""
	}
	name := ""
	for _, b := range st.Loop.Steps {
		if name != "" && b.KnightRef.Name != name {
			return ""
		}
		name = b.KnightRef.Name
	}
	return name
}
