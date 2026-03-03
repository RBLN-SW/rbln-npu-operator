package upgrade

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (m *ClusterUpgradeStateManagerImpl) BuildState(ctx context.Context, namespace string,
	driverLabels map[string]string,
) (*ClusterUpgradeState, error) {
	m.Log.Info("Building state")

	upgradeState := NewClusterUpgradeState()

	daemonSets, err := m.GetDriverDaemonSets(ctx, namespace, driverLabels)
	if err != nil {
		m.Log.Error(err, "Failed to get driver DaemonSet list")
		return nil, err
	}

	m.Log.Info("Got driver DaemonSets", "length", len(daemonSets))

	podList := &corev1.PodList{}

	err = m.K8sClient.List(ctx, podList,
		client.InNamespace(namespace),
		client.MatchingLabels(driverLabels),
	)
	if err != nil {
		return nil, err
	}

	podsByDaemonSet, orphanedPods := m.partitionDriverPods(podList.Items, daemonSets)
	if err := m.validateDaemonSetScheduling(daemonSets, podsByDaemonSet); err != nil {
		return nil, err
	}

	for uid, dsPods := range podsByDaemonSet {
		ownerDaemonSet, found := daemonSets[uid]
		if !found {
			return nil, fmt.Errorf("managed daemonset not found for uid %q", uid)
		}
		if err := m.addOwnedPodsToUpgradeState(ctx, &upgradeState, dsPods, ownerDaemonSet); err != nil {
			return nil, err
		}
	}

	if err := m.addOrphanedPodsToUpgradeState(ctx, &upgradeState, orphanedPods); err != nil {
		return nil, err
	}

	return &upgradeState, nil
}

func (m *ClusterUpgradeStateManagerImpl) GetDriverDaemonSets(ctx context.Context, namespace string,
	labels map[string]string,
) (map[types.UID]*appsv1.DaemonSet, error) {
	daemonSetList := &appsv1.DaemonSetList{}

	err := m.K8sClient.List(ctx, daemonSetList,
		client.InNamespace(namespace),
		client.MatchingLabels(labels))
	if err != nil {
		return nil, fmt.Errorf("error getting DaemonSet list: %v", err)
	}

	daemonSetMap := make(map[types.UID]*appsv1.DaemonSet)
	for i := range daemonSetList.Items {
		daemonSet := &daemonSetList.Items[i]
		daemonSetMap[daemonSet.UID] = daemonSet
	}

	return daemonSetMap, nil
}

func (m *ClusterUpgradeStateManagerImpl) partitionDriverPods(
	pods []corev1.Pod,
	daemonSets map[types.UID]*appsv1.DaemonSet,
) (map[types.UID][]corev1.Pod, []corev1.Pod) {
	podsByDaemonSet := map[types.UID][]corev1.Pod{}
	orphanedPods := []corev1.Pod{}

	for i := range pods {
		pod := &pods[i]
		if IsOrphanedPod(pod) {
			m.Log.Info("Driver Pod has no owner DaemonSet", "pod", pod.Name)
			orphanedPods = append(orphanedPods, *pod)
			continue
		}

		ownerUID := pod.OwnerReferences[0].UID
		if _, found := daemonSets[ownerUID]; !found {
			m.Log.Info("Driver Pod is not owned by a managed Driver DaemonSet",
				"pod", pod.Name, "ownerUID", ownerUID)
			continue
		}

		podsByDaemonSet[ownerUID] = append(podsByDaemonSet[ownerUID], *pod)
	}
	m.Log.Info("Total orphaned Pods found:", "count", len(orphanedPods))
	return podsByDaemonSet, orphanedPods
}

func (m *ClusterUpgradeStateManagerImpl) validateDaemonSetScheduling(
	daemonSets map[types.UID]*appsv1.DaemonSet,
	podsByDaemonSet map[types.UID][]corev1.Pod,
) error {
	for uid, ds := range daemonSets {
		if int(ds.Status.DesiredNumberScheduled) != len(podsByDaemonSet[uid]) {
			m.Log.Info("Driver DaemonSet has Unscheduled pods",
				"name", ds.Name,
				"desiredNumberScheduled", ds.Status.DesiredNumberScheduled,
				"runningPods", len(podsByDaemonSet[uid]))
			return fmt.Errorf("%w: daemonset=%s desiredNumberScheduled=%d runningPods=%d",
				ErrDriverDaemonSetHasUnscheduledPods,
				ds.Name,
				ds.Status.DesiredNumberScheduled,
				len(podsByDaemonSet[uid]),
			)
		}
	}
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) buildNodeUpgradeState(
	ctx context.Context, pod *corev1.Pod, ds *appsv1.DaemonSet,
) (*NodeUpgradeState, error) {
	node, err := m.NodeUpgradeStateProvider.GetNode(ctx, pod.Spec.NodeName)
	if err != nil {
		return nil, fmt.Errorf("unable to get node %s: %v", pod.Spec.NodeName, err)
	}

	m.Log.Info("Node hosting a driver pod",
		"node", node.Name, "state", node.Labels[UpgradeStateLabelKey])

	return &NodeUpgradeState{Node: node, DriverPod: pod, DriverDaemonSet: ds}, nil
}

func (m *ClusterUpgradeStateManagerImpl) addOwnedPodsToUpgradeState(
	ctx context.Context,
	upgradeState *ClusterUpgradeState,
	pods []corev1.Pod,
	ownerDaemonSet *appsv1.DaemonSet,
) error {
	for i := range pods {
		if err := m.addPodToUpgradeState(ctx, upgradeState, &pods[i], ownerDaemonSet); err != nil {
			return err
		}
	}
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) addOrphanedPodsToUpgradeState(
	ctx context.Context,
	upgradeState *ClusterUpgradeState,
	pods []corev1.Pod,
) error {
	for i := range pods {
		if err := m.addPodToUpgradeState(ctx, upgradeState, &pods[i], nil); err != nil {
			return err
		}
	}
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) addPodToUpgradeState(
	ctx context.Context,
	upgradeState *ClusterUpgradeState,
	pod *corev1.Pod,
	ownerDaemonSet *appsv1.DaemonSet,
) error {
	if pod.Spec.NodeName == "" && pod.Status.Phase == corev1.PodPending {
		m.Log.Info("Driver Pod has no NodeName, skipping", "pod", pod.Name)
		return nil
	}

	nodeState, err := m.buildNodeUpgradeState(ctx, pod, ownerDaemonSet)
	if err != nil {
		m.Log.Error(err, "Failed to build node upgrade state for pod", "pod", pod)
		return err
	}

	nodeStateLabel := nodeState.Node.Labels[UpgradeStateLabelKey]
	if nodeStateLabel == "" {
		nodeStateLabel = UpgradeStateUnknown
	}
	if !IsManagedUpgradeState(nodeStateLabel) {
		m.Log.Info("Unknown node upgrade state label; falling back to unknown",
			"node", nodeState.Node.Name,
			"state", nodeStateLabel)
		nodeStateLabel = UpgradeStateUnknown
	}
	upgradeState.NodeStates[nodeStateLabel] = append(upgradeState.NodeStates[nodeStateLabel], nodeState)
	return nil
}
