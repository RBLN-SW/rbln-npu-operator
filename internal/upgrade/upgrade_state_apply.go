/*
Copyright 2026 Rebellions Inc.

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

func (m *ClusterUpgradeStateManagerImpl) ApplyState(ctx context.Context, s *ClusterUpgradeState, policy *v1beta1.DriverUpgradePolicySpec, deviceClass string) error {
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
		{"validation", func() error { return m.ProcessValidationRequiredNodes(ctx, s) }},
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
