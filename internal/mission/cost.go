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
