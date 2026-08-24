package upgrade

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func (m *ClusterUpgradeStateManagerImpl) BuildState(ctx context.Context, namespace string,
	driverLabels map[string]string,
) (*ClusterUpgradeState, error) {
	m.log.V(consts.VDebug).Info("Building state")

	upgradeState := NewClusterUpgradeState()

	daemonSets, err := m.GetDriverDaemonSets(ctx, namespace, driverLabels)
	if err != nil {
		m.log.Error(err, "Failed to get driver DaemonSet list")
		return nil, err
	}

	m.log.V(consts.VDebug).Info("Got driver DaemonSets", "length", len(daemonSets))

	podList := &corev1.PodList{}

	err = m.k8sClient.List(ctx, podList,
		client.InNamespace(namespace),
		client.MatchingLabels(driverLabels),
	)
	if err != nil {
		return nil, err
	}

	podsByDaemonSet, orphanedPods := m.partitionDriverPods(podList.Items, daemonSets)
	m.filterStableDaemonSets(daemonSets, podsByDaemonSet)

	nodeCache := make(map[string]*corev1.Node)

	for uid, dsPods := range podsByDaemonSet {
		ownerDaemonSet, found := daemonSets[uid]
		if !found {
			return nil, fmt.Errorf("managed daemonset not found for uid %q", uid)
		}
		if err := m.addOwnedPodsToUpgradeState(ctx, &upgradeState, dsPods, ownerDaemonSet, nodeCache); err != nil {
			return nil, err
		}
	}

	if err := m.addOrphanedPodsToUpgradeState(ctx, &upgradeState, orphanedPods, nodeCache); err != nil {
		return nil, err
	}

	return &upgradeState, nil
}

func (m *ClusterUpgradeStateManagerImpl) GetDriverDaemonSets(ctx context.Context, namespace string,
	labels map[string]string,
) (map[types.UID]*appsv1.DaemonSet, error) {
	daemonSetList := &appsv1.DaemonSetList{}

	err := m.k8sClient.List(ctx, daemonSetList,
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
		controllerRef := metav1.GetControllerOf(pod)
		if controllerRef == nil {
			m.log.Info("Driver Pod has no controller owner", "pod", pod.Name)
			orphanedPods = append(orphanedPods, *pod)
			continue
		}

		ownerUID := controllerRef.UID
		if _, found := daemonSets[ownerUID]; !found {
			m.log.Info("Driver Pod is not owned by a managed Driver DaemonSet",
				"pod", pod.Name, "ownerUID", ownerUID)
			continue
		}

		podsByDaemonSet[ownerUID] = append(podsByDaemonSet[ownerUID], *pod)
	}
	m.log.V(consts.VDebug).Info("Total orphaned Pods found", "count", len(orphanedPods))
	return podsByDaemonSet, orphanedPods
}

// filterStableDaemonSets removes DaemonSets with scheduling inconsistencies from
// both daemonSets and podsByDaemonSet so that stable DaemonSets can proceed with
// upgrade processing while transiently inconsistent ones are skipped.
func (m *ClusterUpgradeStateManagerImpl) filterStableDaemonSets(
	daemonSets map[types.UID]*appsv1.DaemonSet,
	podsByDaemonSet map[types.UID][]corev1.Pod,
) {
	for uid, ds := range daemonSets {
		if int(ds.Status.DesiredNumberScheduled) != len(podsByDaemonSet[uid]) {
			m.log.Info("Driver DaemonSet has scheduling inconsistency; skipping until stable",
				"name", ds.Name,
				"desiredNumberScheduled", ds.Status.DesiredNumberScheduled,
				"observedPods", len(podsByDaemonSet[uid]))
			delete(daemonSets, uid)
			delete(podsByDaemonSet, uid)
		}
	}
}

func (m *ClusterUpgradeStateManagerImpl) buildNodeUpgradeState(
	ctx context.Context, pod *corev1.Pod, ds *appsv1.DaemonSet, nodeCache map[string]*corev1.Node,
) (*NodeUpgradeState, error) {
	nodeName := pod.Spec.NodeName
	node, ok := nodeCache[nodeName]
	if !ok {
		var err error
		node, err = m.nodeUpgradeStateProvider.GetNode(ctx, nodeName)
		if err != nil {
			return nil, fmt.Errorf("unable to get node %s: %v", nodeName, err)
		}
		nodeCache[nodeName] = node
	}

	m.log.V(consts.VDebug).Info("Node hosting a driver pod",
		"node", node.Name, "state", node.Labels[UpgradeStateLabelKey])

	return &NodeUpgradeState{Node: node, DriverPod: pod, DriverDaemonSet: ds}, nil
}

func (m *ClusterUpgradeStateManagerImpl) addOwnedPodsToUpgradeState(
	ctx context.Context,
	upgradeState *ClusterUpgradeState,
	pods []corev1.Pod,
	ownerDaemonSet *appsv1.DaemonSet,
	nodeCache map[string]*corev1.Node,
) error {
	for i := range pods {
		if err := m.addPodToUpgradeState(ctx, upgradeState, &pods[i], ownerDaemonSet, nodeCache); err != nil {
			return err
		}
	}
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) addOrphanedPodsToUpgradeState(
	ctx context.Context,
	upgradeState *ClusterUpgradeState,
	pods []corev1.Pod,
	nodeCache map[string]*corev1.Node,
) error {
	for i := range pods {
		if err := m.addPodToUpgradeState(ctx, upgradeState, &pods[i], nil, nodeCache); err != nil {
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
	nodeCache map[string]*corev1.Node,
) error {
	if pod.Spec.NodeName == "" && pod.Status.Phase == corev1.PodPending {
		m.log.Info("Driver Pod has no NodeName, skipping", "pod", pod.Name)
		return nil
	}

	nodeState, err := m.buildNodeUpgradeState(ctx, pod, ownerDaemonSet, nodeCache)
	if err != nil {
		m.log.Error(err, "Failed to build node upgrade state for pod", "pod", pod)
		return err
	}

	nodeStateLabel := nodeState.Node.Labels[UpgradeStateLabelKey]
	if !IsManagedUpgradeState(nodeStateLabel) {
		m.log.Info("Unknown node upgrade state label; falling back to unknown",
			"node", nodeState.Node.Name,
			"state", nodeStateLabel)
		nodeStateLabel = UpgradeStateUnknown
	}
	upgradeState.NodeStates[nodeStateLabel] = append(upgradeState.NodeStates[nodeStateLabel], nodeState)
	return nil
}
