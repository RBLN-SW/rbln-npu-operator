package upgrade

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/RBLN-SW/rbln-npu-operator/api/v1beta1"
	"github.com/RBLN-SW/rbln-npu-operator/internal/metrics"
)

func logKeyForNodeState(state string) string {
	if state == UpgradeStateUnknown {
		return "Unknown"
	}
	return state
}

func recordNodeStateMetrics(s *ClusterUpgradeState) {
	counts := make(map[string]int, len(managedUpgradeStates))
	forEachUniqueNode(s, func(n *NodeUpgradeState) {
		state := n.Node.Labels[UpgradeStateLabelKey]
		if !IsManagedUpgradeState(state) {
			state = UpgradeStateUnknown
		}
		counts[state]++
	})
	for _, state := range managedUpgradeStates {
		metrics.DriverUpgradeNodes.WithLabelValues(strings.ToLower(logKeyForNodeState(state))).Set(float64(counts[state]))
	}
}

func (m *ClusterUpgradeStateManagerImpl) ApplyState(ctx context.Context, s *ClusterUpgradeState, policy *v1beta1.DriverUpgradePolicySpec, deviceClass string, validation ValidationOptions) error {
	if s == nil {
		return errors.New("currentState should not be empty")
	}
	if policy == nil || !policy.AutoUpgrade {
		return nil
	}
	// BuildState already prunes departed nodes. Also handle snapshots supplied
	// directly by a caller, keeping the caller's status view consistent.
	s.excludeDeparted()
	defer recordNodeStateMetrics(s)
	steps := []struct {
		name string
		run  func() error
	}{
		{"departed", func() error { return m.ProcessDepartedNodes(ctx, s) }},
		{"unknown", func() error { return m.ProcessUnknownNodes(ctx, s) }},
		{"done", func() error { return m.ProcessDoneNodes(ctx, s) }},
		{"skipped", func() error { return m.ProcessUpgradeSkippedNodes(ctx, s) }},
		{"failed", func() error { return m.ProcessUpgradeFailedNodes(ctx, s) }},
		{"admission", func() error { return m.ProcessUpgradeRequiredNodes(ctx, s, policy) }},
		{"cordon", func() error { return m.ProcessCordonRequiredNodes(ctx, s) }},
		{"completion", func() error { return m.ProcessWaitForJobsRequiredNodes(ctx, s, policy.WaitForCompletion) }},
		{"eviction", func() error { return m.ProcessPodDeletionRequiredNodes(ctx, s, policy.PodDeletion, deviceClass) }},
		{"restart", func() error { return m.ProcessPodRestartNodes(ctx, s, int64(policy.PodRestartTimeoutSeconds)) }},
		{"validation", func() error { return m.ProcessValidationRequiredNodes(ctx, s, validation) }},
		{"uncordon", func() error { return m.ProcessUncordonRequiredNodes(ctx, s) }},
	}
	var result error
	for _, step := range steps {
		if err := step.run(); err != nil {
			// The reconcile boundary logs returned errors once. Retain phase
			// and node context without logging the same failure at each layer.
			result = errors.Join(result, fmt.Errorf("%s: %w", step.name, err))
		}
	}
	return result
}
