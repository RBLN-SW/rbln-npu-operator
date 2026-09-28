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
		UpgradeStateSkipped:         {},
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

// ShouldReleaseCordonOnTeardown reports whether letting go of a node — turning
// autoUpgrade off, or the node leaving the driver's scope — must lift its
// cordon. Only a cordon the rollout owns is lifted (OwnsCordon): an
// administrator's, k8s-driver-manager's (which with autoUpgrade off may be
// holding it legitimately) and a requestor's stay. upgrade-failed keeps its
// cordon on purpose: the node's driver did not come up, and the teardown leaves
// such a node alone altogether (see removeNodeUpgradeState).
//
// The verdict never consults Spec.Unschedulable: the node may come from the
// informer cache, which may not have seen a cordon written moments ago through
// the direct clientset, and the release is an idempotent patch.
func ShouldReleaseCordonOnTeardown(node *corev1.Node) bool {
	if node.Labels[UpgradeStateLabelKey] == UpgradeStateFailed {
		return false
	}
	return OwnsCordon(node) && !IsNodeInRequestorMode(node)
}
