package upgrade

import (
	"slices"

	corev1 "k8s.io/api/core/v1"
)

var inProgressUpgradeStates = []string{UpgradeStateCordonRequired, UpgradeStateWaitForJobsRequired, UpgradeStatePodDeletionRequired, UpgradeStatePodRestartRequired, UpgradeStateValidationRequired, UpgradeStateUncordonRequired, UpgradeStateFailed}

func (s *ClusterUpgradeState) addNode(phase string, node *NodeUpgradeState) {
	s.nodes = append(s.nodes, observedNode{phase: phase, node: node})
}

func (s *ClusterUpgradeState) nodesAt(phase string) []*NodeUpgradeState {
	var result []*NodeUpgradeState
	for _, observed := range s.nodes {
		if observed.phase == phase {
			result = append(result, observed.node)
		}
	}
	return result
}

// Prune the canonical snapshot, not a private execution copy. A lingering
// driver pod on a departed node must not reappear in status after cleanup.
func (s *ClusterUpgradeState) excludeDeparted() {
	if len(s.DepartedNodes) == 0 {
		return
	}
	departed := make(map[string]bool, len(s.DepartedNodes))
	for _, node := range s.DepartedNodes {
		departed[node.Name] = true
	}
	s.nodes = slices.DeleteFunc(s.nodes, func(observed observedNode) bool {
		return departed[observed.node.Node.Name]
	})
}
func IsNodeUnschedulable(n *corev1.Node) bool { return n != nil && n.Spec.Unschedulable }
func IsNodeInRequestorMode(n *corev1.Node) bool {
	if n == nil {
		return false
	}
	_, present := n.Annotations[UpgradeRequestorModeAnnotationKey]
	return present
}
func IsManagedUpgradeState(state string) bool { return slices.Contains(managedUpgradeStates, state) }
func IsInProgressUpgradeState(state string) bool {
	return slices.Contains(inProgressUpgradeStates, state)
}

func ShouldReleaseCordonOnTeardown(n *corev1.Node) bool {
	if n == nil || IsNodeInRequestorMode(n) || n.Labels[UpgradeStateLabelKey] == UpgradeStateFailed {
		return false
	}
	return OwnsCordon(n)
}
