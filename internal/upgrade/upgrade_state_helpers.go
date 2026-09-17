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

// ShouldReleaseCordonOnTeardown reports whether tearing the upgrade workflow
// down must lift this node's cordon. Turning autoUpgrade off is the documented
// way to pause a rollout, so a node caught mid-flight has to go back into
// service instead of staying unschedulable with the state label that explained
// it gone.
//
// The verdict comes from the node's bookkeeping alone, never from
// Spec.Unschedulable: the node is read from the informer cache, which may not
// have seen a cordon written moments ago through the direct clientset, and the
// release is an idempotent patch. Only the rollout's own cordon is lifted:
//
//   - upgrade-skipped counts as in flight here. markNodeUpgradeSkipped only
//     labels the node; the uncordon happens on its next pass, which a teardown
//     pre-empts.
//   - upgrade-failed keeps its cordon on purpose. The node's driver did not come
//     up. The teardown leaves such a node alone altogether (see
//     removeNodeUpgradeState); it is excluded here too so no caller lifts it.
//   - a node already unschedulable when the rollout admitted it carries
//     UpgradeInitialStateAnnotationKey; that cordon is the administrator's.
//   - requestor mode means an external requestor owns the cordon.
func ShouldReleaseCordonOnTeardown(node *corev1.Node) bool {
	state := node.Labels[UpgradeStateLabelKey]
	if state == UpgradeStateFailed {
		return false
	}
	if !IsInProgressUpgradeState(state) && state != UpgradeStateSkipped {
		return false
	}
	if _, wasUnschedulable := node.Annotations[UpgradeInitialStateAnnotationKey]; wasUnschedulable {
		return false
	}
	return !IsNodeInRequestorMode(node)
}
