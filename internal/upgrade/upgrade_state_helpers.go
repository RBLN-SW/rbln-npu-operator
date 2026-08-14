package upgrade

import (
	"slices"

	corev1 "k8s.io/api/core/v1"
)

func NewClusterUpgradeState() ClusterUpgradeState {
	return ClusterUpgradeState{NodeStates: make(map[string][]*NodeUpgradeState)}
}

func IsNodeUnschedulable(node *corev1.Node) bool {
	return node.Spec.Unschedulable
}

func IsNodeInRequestorMode(node *corev1.Node) bool {
	_, ok := node.Annotations[UpgradeRequestorModeAnnotationKey]
	return ok
}

func IsManagedUpgradeState(state string) bool {
	return slices.Contains(managedUpgradeStates, state)
}

var inProgressUpgradeStates = func() map[string]struct{} {
	excluded := map[string]struct{}{
		UpgradeStateUnknown:         {},
		UpgradeStateDone:            {},
		UpgradeStateUpgradeRequired: {},
	}
	states := make(map[string]struct{})
	for _, s := range managedUpgradeStates {
		if _, skip := excluded[s]; skip {
			continue
		}
		states[s] = struct{}{}
	}
	return states
}()

func IsInProgressUpgradeState(state string) bool {
	_, ok := inProgressUpgradeStates[state]
	return ok
}
