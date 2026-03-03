package upgrade

import (
	"slices"

	corev1 "k8s.io/api/core/v1"
)

func NewClusterUpgradeState() ClusterUpgradeState {
	return ClusterUpgradeState{NodeStates: make(map[string][]*NodeUpgradeState)}
}

func IsOrphanedPod(pod *corev1.Pod) bool {
	return len(pod.OwnerReferences) < 1
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
