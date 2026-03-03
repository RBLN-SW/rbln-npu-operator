package upgrade

import (
	"context"
	"fmt"

	"github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

type applyStateStep struct {
	name      string
	errorMsg  string
	errorArgs []any
	run       func() error
}

func logKeyForNodeState(state string) string {
	if state == UpgradeStateUnknown {
		return "Unknown"
	}
	return state
}

func (m *ClusterUpgradeStateManagerImpl) logNodeStates(currentState *ClusterUpgradeState) {
	logArgs := make([]any, 0, len(managedUpgradeStates)*2)
	for _, state := range managedUpgradeStates {
		logArgs = append(logArgs, logKeyForNodeState(state), len(currentState.NodeStates[state]))
	}
	m.Log.Info("Node states:", logArgs...)
}

func (m *ClusterUpgradeStateManagerImpl) runApplyStateStep(step applyStateStep) error {
	if err := step.run(); err != nil {
		errorArgs := append([]any{}, step.errorArgs...)
		errorArgs = append(errorArgs, "step", step.name)
		m.Log.V(consts.LogLevelError).Error(err, step.errorMsg, errorArgs...)
		return fmt.Errorf("apply state step %q failed: %w", step.name, err)
	}

	return nil
}

func (m *ClusterUpgradeStateManagerImpl) ApplyState(ctx context.Context,
	namespace string, currentState *ClusterUpgradeState, upgradePolicy *v1beta1.DriverUpgradePolicySpec,
) error {
	m.Log.Info("State Manager, got state update")

	if currentState == nil {
		return fmt.Errorf("currentState should not be empty")
	}

	if upgradePolicy == nil || !upgradePolicy.AutoUpgrade {
		m.Log.Info("Driver auto upgrade is disabled, skipping")
		return nil
	}

	drainEnabled := upgradePolicy.DrainSpec != nil && upgradePolicy.DrainSpec.Enable
	rebootConfig := upgradePolicy.Reboot
	rebootRequired := rebootConfig != nil && rebootConfig.Enable

	m.logNodeStates(currentState)

	steps := []applyStateStep{
		{
			name:      UpgradeStateUnknown,
			errorMsg:  "Failed to process nodes",
			errorArgs: []any{"state", UpgradeStateUnknown},
			run: func() error {
				return m.ProcessUnknownNodes(ctx, currentState)
			},
		},
		{
			name:      UpgradeStateDone,
			errorMsg:  "Failed to process nodes",
			errorArgs: []any{"state", UpgradeStateDone},
			run: func() error {
				return m.ProcessDoneNodes(ctx, currentState)
			},
		},
		{
			name:      UpgradeStateUpgradeRequired,
			errorMsg:  "Failed to process nodes",
			errorArgs: []any{"state", UpgradeStateUpgradeRequired},
			run: func() error {
				return m.ProcessUpgradeRequiredNodes(ctx, currentState, upgradePolicy)
			},
		},
		{
			name:     UpgradeStateCordonRequired,
			errorMsg: "Failed to cordon nodes",
			run: func() error {
				return m.ProcessCordonRequiredNodes(ctx, currentState)
			},
		},
		{
			name:     UpgradeStateWaitForJobsRequired,
			errorMsg: "Failed to waiting for required jobs to complete",
			run: func() error {
				return m.ProcessWaitForJobsRequiredNodes(ctx, currentState, upgradePolicy.WaitForCompletion)
			},
		},
		{
			name:     UpgradeStatePodDeletionRequired,
			errorMsg: "Failed to delete pods",
			run: func() error {
				return m.ProcessPodDeletionRequiredNodes(
					ctx,
					currentState,
					upgradePolicy.PodDeletion,
					drainEnabled,
					rebootRequired,
				)
			},
		},
		{
			name:     UpgradeStateDrainRequired,
			errorMsg: "Failed to schedule nodes drain",
			run: func() error {
				return m.ProcessDrainNodes(ctx, currentState, upgradePolicy.DrainSpec)
			},
		},
		{
			name:     UpgradeStatePodRestartRequired,
			errorMsg: "Failed for 'pod-restart-required' state",
			run: func() error {
				return m.ProcessPodRestartNodes(ctx, currentState, rebootRequired)
			},
		},
		{
			name:     UpgradeStateRebootRequired,
			errorMsg: "Failed for 'reboot-required' state",
			run: func() error {
				return m.ProcessRebootRequiredNodes(ctx, namespace, currentState, rebootConfig)
			},
		},
		{
			name:     UpgradeStateRebootValidationRequired,
			errorMsg: "Failed for 'reboot-validation-required' state",
			run: func() error {
				return m.ProcessRebootValidationRequiredNodes(ctx, namespace, currentState, rebootConfig)
			},
		},
		{
			name:     UpgradeStateRebootPostRequired,
			errorMsg: "Failed for 'reboot-post-required' state",
			run: func() error {
				return m.ProcessRebootPostRequiredNodes(ctx, namespace, currentState, rebootConfig)
			},
		},
		{
			name:     UpgradeStateFailed,
			errorMsg: "Failed to process nodes in 'upgrade-failed' state",
			run: func() error {
				return m.ProcessUpgradeFailedNodes(ctx, currentState)
			},
		},
		{
			name:     UpgradeStateValidationRequired,
			errorMsg: "Failed to validate driver upgrade",
			run: func() error {
				return m.ProcessValidationRequiredNodes(ctx, currentState)
			},
		},
		{
			name:     UpgradeStateUncordonRequired,
			errorMsg: "Failed to uncordon nodes",
			run: func() error {
				return m.ProcessUncordonRequiredNodes(ctx, currentState)
			},
		},
	}

	for _, step := range steps {
		if err := m.runApplyStateStep(step); err != nil {
			return err
		}
	}

	m.Log.V(consts.LogLevelInfo).Info("State Manager, finished processing")
	return nil
}
