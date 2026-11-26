package mission

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
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

// AggregateMissionCost calculates the total cost across all mission knights and updates costBreakdown.
func (c *CostTracker) AggregateMissionCost(ctx context.Context, mission *aiv1alpha1.Mission) (float64, error) {
	var totalCost float64
	costBreakdown := make([]aiv1alpha1.MissionKnightCost, 0, len(mission.Spec.Knights))

	for _, mk := range mission.Spec.Knights {
		knightName := mk.Name
		knight := &aiv1alpha1.Knight{}
		if err := c.Client.Get(ctx, types.NamespacedName{
			Name:      knightName,
			Namespace: mission.Namespace,
		}, knight); err != nil {
			if client.IgnoreNotFound(err) == nil {
				// Knight not found, add to breakdown with zero cost
				costBreakdown = append(costBreakdown, aiv1alpha1.MissionKnightCost{
					Name:      knightName,
					CostUSD:   "0.0000",
					Ephemeral: mk.Ephemeral,
				})
				continue
			}
			return 0, err
		}

		// Parse knight's total cost
		knightCost := "0.0000"
		if knight.Status.TotalCost != "" {
			var cost float64
			if _, err := fmt.Sscanf(knight.Status.TotalCost, "%f", &cost); err == nil {
				totalCost += cost
				knightCost = knight.Status.TotalCost
			}
		}

		// Add to breakdown
		costBreakdown = append(costBreakdown, aiv1alpha1.MissionKnightCost{
			Name:      knightName,
			CostUSD:   knightCost,
			Ephemeral: mk.Ephemeral,
		})
	}

	// Update mission status with breakdown
	mission.Status.CostBreakdown = costBreakdown

	return totalCost, nil
}

// BudgetStatus is the result of a cost-budget check.
type BudgetStatus struct {
	Total    float64
	Budget   float64
	Exceeded bool
}

// CheckBudget aggregates the mission's cost (the larger of the knights' total
// and the chains' own total), records it in Status.TotalCost and reports
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
		return BudgetStatus{Total: total}, nil
	}
	return BudgetStatus{Total: total, Budget: budget, Exceeded: total > budget}, nil
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
