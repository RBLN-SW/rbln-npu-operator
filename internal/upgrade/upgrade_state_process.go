package upgrade

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

const defaultRebootPostTimeoutSeconds int64 = 600

func (m *ClusterUpgradeStateManagerImpl) podInSyncWithDS(ctx context.Context,
	nodeState *NodeUpgradeState,
) (isPodSynced, isOrphened bool, err error) {
	if isOrphened = nodeState.IsOrphanedPod(); isOrphened {
		return isPodSynced, isOrphened, nil
	}
	podRevisionHash, err := m.PodManager.GetPodControllerRevisionHash(nodeState.DriverPod)
	if err != nil {
		m.Log.Error(
			err, "Failed to get pod template revision hash", "pod", nodeState.DriverPod)
		return isPodSynced, isOrphened, err
	}
	m.Log.V(consts.LogLevelDebug).Info("pod template revision hash", "hash", podRevisionHash)
	daemonsetRevisionHash, err := m.PodManager.GetDaemonsetControllerRevisionHash(ctx, nodeState.DriverDaemonSet)
	if err != nil {
		m.Log.Error(
			err, "Failed to get daemonset template revision hash", "daemonset", nodeState.DriverDaemonSet)
		return isPodSynced, isOrphened, err
	}
	m.Log.Info("daemonset template revision hash", "hash", daemonsetRevisionHash)
	isPodSynced = podRevisionHash == daemonsetRevisionHash
	return isPodSynced, isOrphened, nil
}

func (m *ClusterUpgradeStateManagerImpl) ProcessDoneOrUnknownNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState, nodeStateName string,
) error {
	return m.processDoneOrUnknownNodes(ctx, currentClusterState, nodeStateName)
}

func (m *ClusterUpgradeStateManagerImpl) ProcessUnknownNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState,
) error {
	return m.processDoneOrUnknownNodes(ctx, currentClusterState, UpgradeStateUnknown)
}

func (m *ClusterUpgradeStateManagerImpl) ProcessDoneNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState,
) error {
	return m.processDoneOrUnknownNodes(ctx, currentClusterState, UpgradeStateDone)
}

func (m *ClusterUpgradeStateManagerImpl) processDoneOrUnknownNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState, nodeStateName string,
) error {
	m.Log.Info("ProcessDoneOrUnknownNodes")

	for _, nodeState := range currentClusterState.NodeStates[nodeStateName] {
		requireUpgrade, err := m.shouldRequireUpgradeForDoneOrUnknownNode(ctx, nodeState)
		if err != nil {
			return err
		}
		if requireUpgrade {
			err = m.transitionDoneOrUnknownNodeToUpgradeRequired(ctx, nodeState)
			if err != nil {
				return err
			}
			continue
		}

		if nodeStateName == UpgradeStateUnknown {
			err = m.transitionUnknownNodeToDone(ctx, nodeState)
			if err != nil {
				return err
			}
			continue
		}
		m.Log.V(consts.LogLevelDebug).Info("Node in UpgradeDone state, upgrade not required",
			"node", nodeState.Node.Name)
	}
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) shouldRequireUpgradeForDoneOrUnknownNode(
	ctx context.Context, nodeState *NodeUpgradeState,
) (bool, error) {
	isPodSynced, isOrphaned, err := m.podInSyncWithDS(ctx, nodeState)
	if err != nil {
		m.Log.Error(err, "Failed to get daemonset template/pod revision hash")
		return false, err
	}

	isUpgradeRequested := m.IsUpgradeRequested(nodeState.Node)
	isWaitingForSafeDriverLoad := m.SafeDriverLoadManager.IsWaitingForSafeDriverLoad(ctx, nodeState.Node)
	if isWaitingForSafeDriverLoad {
		m.Log.Info("Node is waiting for safe driver load, initialize upgrade",
			"node", nodeState.Node.Name)
	}

	return (!isPodSynced && !isOrphaned) || isWaitingForSafeDriverLoad || isUpgradeRequested, nil
}

func (m *ClusterUpgradeStateManagerImpl) transitionDoneOrUnknownNodeToUpgradeRequired(
	ctx context.Context, nodeState *NodeUpgradeState,
) error {
	if IsNodeUnschedulable(nodeState.Node) {
		m.Log.V(consts.LogLevelInfo).Info(
			"Node is unschedulable, adding annotation to track initial state of the node",
			"node", nodeState.Node.Name, "annotation", UpgradeInitialStateAnnotationKey)
		err := m.NodeUpgradeStateProvider.SetNodeUpgradeAnnotation(ctx, nodeState.Node, UpgradeInitialStateAnnotationKey,
			trueString)
		if err != nil {
			return err
		}
	}

	err := m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, UpgradeStateUpgradeRequired)
	if err != nil {
		m.Log.Error(
			err, "Failed to change node upgrade state", "state", UpgradeStateUpgradeRequired, "node:", nodeState.Node)
		return err
	}

	m.Log.Info("Node requires upgrade, changed its state to UpgradeRequired",
		"node", nodeState.Node.Name)
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) transitionUnknownNodeToDone(
	ctx context.Context, nodeState *NodeUpgradeState,
) error {
	err := m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, UpgradeStateDone)
	if err != nil {
		m.Log.V(consts.LogLevelError).Error(
			err, "Failed to change node upgrade state", "state", UpgradeStateDone)
		return err
	}

	m.Log.V(consts.LogLevelInfo).Info("Changed node state to UpgradeDone",
		"node", nodeState.Node.Name)
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) ProcessUpgradeRequiredNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState,
	upgradePolicy *v1beta1.DriverUpgradePolicySpec,
) error {
	upgradesInProgress := m.GetUpgradesInProgress(currentClusterState)
	upgradesAvailable := len(currentClusterState.NodeStates[UpgradeStateUpgradeRequired])
	if upgradePolicy.MaxParallelUpgrades != 0 {
		upgradesAvailable = upgradePolicy.MaxParallelUpgrades - upgradesInProgress
	}
	m.Log.Info("Upgrades in progress",
		"currently in progress", upgradesInProgress,
		"max parallel upgrades", upgradePolicy.MaxParallelUpgrades,
		"upgrade slots available", upgradesAvailable)

	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateUpgradeRequired] {
		if m.IsUpgradeRequested(nodeState.Node) {
			err := m.NodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, nodeState.Node, UpgradeRequestedAnnotationKey)
			if err != nil {
				m.Log.Error(
					err, "Failed to delete node upgrade-requested annotation")
				return err
			}
		}
		if m.SkipNodeUpgrade(nodeState.Node) {
			m.Log.V(consts.LogLevelInfo).Info("Node is marked for skipping upgrades", "node", nodeState.Node.Name)
			continue
		}

		if upgradesAvailable <= 0 {
			// Already cordoned nodes are allowed to continue to avoid stalling upgrades that have already progressed.
			if m.IsNodeUnschedulable(nodeState.Node) {
				m.Log.Info("Node is already cordoned, progressing for driver upgrade",
					"node", nodeState.Node.Name)
			} else {
				m.Log.Info("Node upgrade limit reached, pausing further upgrades",
					"node", nodeState.Node.Name)
				continue
			}
		}

		err := m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, UpgradeStateCordonRequired)
		if err != nil {
			m.Log.Error(
				err, "Failed to change node upgrade state", "state", UpgradeStateCordonRequired)
			return err
		}
		upgradesAvailable--
		m.Log.Info("Node waiting for cordon",
			"node", nodeState.Node.Name)
	}

	return nil
}

func (m *ClusterUpgradeStateManagerImpl) IsUpgradeRequested(node *corev1.Node) bool {
	return node.Annotations[UpgradeRequestedAnnotationKey] == trueString
}

func (m *ClusterUpgradeStateManagerImpl) GetTotalManagedNodes(
	currentState *ClusterUpgradeState,
) int {
	totalNodes := 0
	for _, state := range managedUpgradeStates {
		totalNodes += len(currentState.NodeStates[state])
	}
	return totalNodes
}

func (m *ClusterUpgradeStateManagerImpl) GetUpgradesInProgress(
	currentState *ClusterUpgradeState,
) int {
	totalNodes := m.GetTotalManagedNodes(currentState)
	return totalNodes - (len(currentState.NodeStates[UpgradeStateUnknown]) +
		len(currentState.NodeStates[UpgradeStateDone]) +
		len(currentState.NodeStates[UpgradeStateUpgradeRequired]))
}

func (m *ClusterUpgradeStateManagerImpl) GetUpgradesDone(
	currentState *ClusterUpgradeState,
) int {
	return len(currentState.NodeStates[UpgradeStateDone])
}

func (m *ClusterUpgradeStateManagerImpl) GetUpgradesAvailable(
	currentState *ClusterUpgradeState, maxParallelUpgrades int,
) int {
	upgradesInProgress := m.GetUpgradesInProgress(currentState)

	var upgradesAvailable int
	if maxParallelUpgrades == 0 {
		upgradesAvailable = len(currentState.NodeStates[UpgradeStateUpgradeRequired])
	} else {
		upgradesAvailable = maxParallelUpgrades - upgradesInProgress
	}
	return upgradesAvailable
}

func (m *ClusterUpgradeStateManagerImpl) GetUpgradesFailed(
	currentState *ClusterUpgradeState,
) int {
	return len(currentState.NodeStates[UpgradeStateFailed])
}

func (m *ClusterUpgradeStateManagerImpl) GetUpgradesPending(
	currentState *ClusterUpgradeState,
) int {
	return len(currentState.NodeStates[UpgradeStateUpgradeRequired])
}

func (m *ClusterUpgradeStateManagerImpl) GetCurrentUnavailableNodes(
	currentState *ClusterUpgradeState,
) int {
	unavailableNodes := 0
	for _, nodeUpgradeStateList := range currentState.NodeStates {
		for _, nodeUpgradeState := range nodeUpgradeStateList {
			if m.IsNodeUnschedulable(nodeUpgradeState.Node) {
				m.Log.V(consts.LogLevelDebug).Info("Node is cordoned", "node", nodeUpgradeState.Node.Name)
				unavailableNodes++
				continue
			}
			if !m.isNodeConditionReady(nodeUpgradeState.Node) {
				m.Log.V(consts.LogLevelDebug).Info("Node is not-ready", "node", nodeUpgradeState.Node.Name)
				unavailableNodes++
			}
		}
	}
	return unavailableNodes
}

func (m *ClusterUpgradeStateManagerImpl) IsNodeUnschedulable(node *corev1.Node) bool {
	return node.Spec.Unschedulable
}

func (m *ClusterUpgradeStateManagerImpl) isNodeConditionReady(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status != corev1.ConditionTrue {
			return false
		}
	}
	return true
}

func (m *ClusterUpgradeStateManagerImpl) SkipNodeUpgrade(node *corev1.Node) bool {
	return node.Labels[UpgradeSkipNodeLabelKey] == trueString
}

func (m *ClusterUpgradeStateManagerImpl) ProcessCordonRequiredNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState,
) error {
	m.Log.V(consts.LogLevelInfo).Info("ProcessCordonRequiredNodes")

	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateCordonRequired] {
		err := m.CordonManager.Cordon(ctx, nodeState.Node)
		if err != nil {
			m.Log.V(consts.LogLevelWarning).Error(
				err, "Node cordon failed", "node", nodeState.Node)
			return err
		}
		err = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, UpgradeStateWaitForJobsRequired)
		if err != nil {
			m.Log.V(consts.LogLevelError).Error(
				err, "Failed to change node upgrade state", "state", UpgradeStateWaitForJobsRequired)
			return err
		}
	}
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) ProcessWaitForJobsRequiredNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState,
	waitForCompletionSpec *v1beta1.WaitForCompletionSpec,
) error {
	m.Log.Info("ProcessWaitForJobsRequiredNodes")

	waitForJobsRequiredNodes := currentClusterState.NodeStates[UpgradeStateWaitForJobsRequired]
	if len(waitForJobsRequiredNodes) == 0 {
		return nil
	}

	nodes := make([]*corev1.Node, 0, len(waitForJobsRequiredNodes))
	for _, nodeState := range waitForJobsRequiredNodes {
		nodes = append(nodes, nodeState.Node)
		if waitForCompletionSpec == nil || waitForCompletionSpec.PodSelector == "" {
			m.Log.V(consts.LogLevelInfo).Info("No jobs to wait for as no pod selector was provided. Moving to next state.")
			nextState := UpgradeStatePodDeletionRequired
			if !m.IsPodDeletionEnabled() {
				nextState = UpgradeStateDrainRequired
			}
			_ = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, nextState)
			m.Log.Info("Updated the node state", "node", nodeState.Node.Name, "state", nextState)
		}
	}
	if waitForCompletionSpec == nil || waitForCompletionSpec.PodSelector == "" {
		return nil
	}

	podManagerConfig := PodManagerConfig{WaitForCompletionSpec: waitForCompletionSpec, Nodes: nodes}
	err := m.PodManager.ScheduleCheckOnPodCompletion(ctx, &podManagerConfig)
	if err != nil {
		return err
	}
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) IsPodDeletionEnabled() bool {
	return m.podDeletionStateEnabled
}

func (m *ClusterUpgradeStateManagerImpl) ProcessPodDeletionRequiredNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState, podDeletionSpec *v1beta1.PodDeletionSpec,
	drainEnabled bool, rebootRequired bool,
) error {
	m.Log.V(consts.LogLevelInfo).Info("ProcessPodDeletionRequiredNodes")

	if !m.IsPodDeletionEnabled() {
		m.Log.Info("PodDeletion is not enabled, proceeding straight to the next state")
		for _, nodeState := range currentClusterState.NodeStates[UpgradeStatePodDeletionRequired] {
			_ = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, UpgradeStateDrainRequired)
		}
		return nil
	}

	podManagerConfig := PodManagerConfig{
		DeletionSpec:   podDeletionSpec,
		DrainEnabled:   drainEnabled,
		RebootRequired: rebootRequired,
		Nodes:          make([]*corev1.Node, 0, len(currentClusterState.NodeStates[UpgradeStatePodDeletionRequired])),
	}

	for _, nodeState := range currentClusterState.NodeStates[UpgradeStatePodDeletionRequired] {
		podManagerConfig.Nodes = append(podManagerConfig.Nodes, nodeState.Node)
	}

	if len(podManagerConfig.Nodes) == 0 {
		return nil
	}

	return m.PodManager.SchedulePodEviction(ctx, &podManagerConfig)
}

func (m *ClusterUpgradeStateManagerImpl) ProcessDrainNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState, drainSpec *v1beta1.DrainSpec,
) error {
	m.Log.V(consts.LogLevelInfo).Info("ProcessDrainNodes")
	if drainSpec == nil || !drainSpec.Enable {
		m.Log.V(consts.LogLevelInfo).Info("Node drain is disabled by policy, skipping this step")
		for _, nodeState := range currentClusterState.NodeStates[UpgradeStateDrainRequired] {
			err := m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, UpgradeStatePodRestartRequired)
			if err != nil {
				m.Log.V(consts.LogLevelError).Error(
					err, "Failed to change node upgrade state", "state", UpgradeStatePodRestartRequired)
				return err
			}
		}
		return nil
	}

	drainConfig := DrainConfiguration{
		Spec:  drainSpec,
		Nodes: make([]*corev1.Node, 0, len(currentClusterState.NodeStates[UpgradeStateDrainRequired])),
	}
	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateDrainRequired] {
		drainConfig.Nodes = append(drainConfig.Nodes, nodeState.Node)
	}

	m.Log.V(consts.LogLevelInfo).Info("Scheduling nodes drain", "drainConfig", drainConfig)

	return m.DrainManager.ScheduleNodesDrain(ctx, &drainConfig)
}

func (m *ClusterUpgradeStateManagerImpl) isDriverPodInSync(ctx context.Context,
	nodeState *NodeUpgradeState,
) (bool, error) {
	isPodSynced, isOrphaned, err := m.podInSyncWithDS(ctx, nodeState)
	if err != nil {
		m.Log.V(consts.LogLevelError).Error(err, "Failed to get daemonset template/pod revision hash")
		return false, err
	}
	if isOrphaned {
		return false, nil
	}
	if isPodSynced &&
		nodeState.DriverPod.Status.Phase == corev1.PodRunning &&
		len(nodeState.DriverPod.Status.ContainerStatuses) != 0 {
		for i := range nodeState.DriverPod.Status.ContainerStatuses {
			if !nodeState.DriverPod.Status.ContainerStatuses[i].Ready {
				return false, nil
			}
		}

		return true, nil
	}

	return false, nil
}

func (m *ClusterUpgradeStateManagerImpl) updateNodeToUncordonOrDoneState(ctx context.Context,
	nodeState *NodeUpgradeState,
) error {
	node := nodeState.Node
	newUpgradeState := UpgradeStateUncordonRequired
	annotationKey := UpgradeInitialStateAnnotationKey
	isNodeUnderRequestorMode := IsNodeInRequestorMode(node)

	if _, ok := node.Annotations[annotationKey]; ok {
		if !isNodeUnderRequestorMode {
			m.Log.V(consts.LogLevelInfo).Info("Node was Unschedulable at beginning of upgrade, skipping uncordon",
				"node", node.Name)
			newUpgradeState = UpgradeStateDone
		}
	}

	err := m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, newUpgradeState)
	if err != nil {
		m.Log.V(consts.LogLevelError).Error(
			err, "Failed to change node upgrade state", "node", node.Name, "state", newUpgradeState)
		return err
	}

	if newUpgradeState == UpgradeStateDone || isNodeUnderRequestorMode {
		m.Log.V(consts.LogLevelDebug).Info("Removing node upgrade annotation",
			"node", node.Name, "annotation", annotationKey)
		err = m.NodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, annotationKey)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) ProcessPodRestartNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState, rebootRequired bool,
) error {
	m.Log.V(consts.LogLevelInfo).Info("ProcessPodRestartNodes")

	pods := make([]*corev1.Pod, 0, len(currentClusterState.NodeStates[UpgradeStatePodRestartRequired]))
	for _, nodeState := range currentClusterState.NodeStates[UpgradeStatePodRestartRequired] {
		isPodSynced, isOrphaned, err := m.podInSyncWithDS(ctx, nodeState)
		if err != nil {
			m.Log.V(consts.LogLevelError).Error(err, "Failed to get daemonset template/pod revision hash")
			return err
		}
		if !isPodSynced || isOrphaned {
			if nodeState.DriverPod.DeletionTimestamp.IsZero() {
				pods = append(pods, nodeState.DriverPod)
			}
		} else {
			err := m.SafeDriverLoadManager.UnblockLoading(ctx, nodeState.Node)
			if err != nil {
				m.Log.V(consts.LogLevelError).Error(
					err, "Failed to unblock loading of the driver", "nodeState", nodeState)
				return err
			}
			driverPodInSync, err := m.isDriverPodInSync(ctx, nodeState)
			if err != nil {
				m.Log.V(consts.LogLevelError).Error(
					err, "Failed to check if driver pod on the node is in sync", "nodeState", nodeState)
				return err
			}
			if driverPodInSync {
				if rebootRequired {
					err = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node,
						UpgradeStateRebootRequired)
					if err != nil {
						m.Log.V(consts.LogLevelError).Error(
							err, "Failed to change node upgrade state", "state", UpgradeStateRebootRequired)
						return err
					}
					continue
				}

				if !m.IsValidationEnabled() {
					err = m.updateNodeToUncordonOrDoneState(ctx, nodeState)
					if err != nil {
						return err
					}
					continue
				}

				err = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node,
					UpgradeStateValidationRequired)
				if err != nil {
					m.Log.V(consts.LogLevelError).Error(
						err, "Failed to change node upgrade state", "state", UpgradeStateValidationRequired)
					return err
				}
			} else {
				if !m.isDriverPodFailing(nodeState.DriverPod) {
					continue
				}
				m.Log.V(consts.LogLevelInfo).Info("Driver pod is failing on node with repeated restarts",
					"node", nodeState.Node.Name, "pod", nodeState.DriverPod.Name)
				err = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, UpgradeStateFailed)
				if err != nil {
					m.Log.V(consts.LogLevelError).Error(
						err, "Failed to change node upgrade state for node", "node", nodeState.Node.Name,
						"state", UpgradeStateFailed)
					return err
				}
			}
		}
	}

	return m.PodManager.SchedulePodsRestart(ctx, pods)
}

func (m *ClusterUpgradeStateManagerImpl) ProcessRebootRequiredNodes(
	ctx context.Context,
	namespace string,
	currentClusterState *ClusterUpgradeState,
	rebootSpec *v1beta1.RebootSpec,
) error {
	m.Log.V(consts.LogLevelInfo).Info("ProcessRebootRequiredNodes")
	if namespace == "" {
		return fmt.Errorf("namespace must be provided for reboot processing")
	}
	rebootImage := resolveRebootPodImage(rebootSpec)

	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateRebootRequired] {
		node := nodeState.Node
		preRebootBootID := node.Status.NodeInfo.BootID
		if preRebootBootID == "" {
			err := fmt.Errorf("node %q bootID is empty", node.Name)
			m.Log.V(consts.LogLevelError).Error(err, "Failed to read node bootID before reboot")
			return err
		}

		err := m.NodeUpgradeStateProvider.SetNodeUpgradeAnnotation(
			ctx, node, UpgradePreRebootBootIDAnnotationKey, preRebootBootID)
		if err != nil {
			m.Log.V(consts.LogLevelError).Error(
				err, "Failed to set node pre-reboot bootID annotation",
				"annotation", UpgradePreRebootBootIDAnnotationKey,
				"node", node.Name)
			return err
		}

		rebootRequestedAt := time.Now().Unix()
		rebootRequestedAtValue := strconv.FormatInt(rebootRequestedAt, 10)
		rebootPodName := BuildRebootPodName(node.Name, rebootRequestedAt)
		err = m.NodeUpgradeStateProvider.SetNodeUpgradeAnnotation(
			ctx, node, UpgradeRebootRequestedAtAnnotationKey, rebootRequestedAtValue)
		if err != nil {
			m.Log.V(consts.LogLevelError).Error(
				err, "Failed to set node reboot request annotation",
				"annotation", UpgradeRebootRequestedAtAnnotationKey,
				"node", node.Name)
			return err
		}

		err = m.NodeUpgradeStateProvider.SetNodeUpgradeAnnotation(
			ctx, node, UpgradeRebootPodNameAnnotationKey, rebootPodName)
		if err != nil {
			m.Log.V(consts.LogLevelError).Error(
				err, "Failed to set node reboot pod annotation",
				"annotation", UpgradeRebootPodNameAnnotationKey,
				"node", node.Name)
			return err
		}

		if m.RebootManager == nil {
			return fmt.Errorf("reboot manager is not configured")
		}
		err = m.RebootManager.Trigger(ctx, node, RebootTriggerRequest{
			RequestedAtUnix: rebootRequestedAt,
			PreRebootBootID: preRebootBootID,
			Namespace:       namespace,
			PodName:         rebootPodName,
			Image:           rebootImage,
		})
		if err != nil {
			_ = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, UpgradeStateFailed)
			m.Log.V(consts.LogLevelError).Error(
				err, "Failed to trigger node reboot", "node", node.Name)
			return err
		}

		err = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(
			ctx, node, UpgradeStateRebootValidationRequired)
		if err != nil {
			m.Log.V(consts.LogLevelError).Error(
				err, "Failed to change node upgrade state", "state", UpgradeStateRebootValidationRequired,
				"node", node.Name)
			return err
		}
	}
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) ProcessRebootValidationRequiredNodes(
	ctx context.Context,
	namespace string,
	currentClusterState *ClusterUpgradeState,
	rebootSpec *v1beta1.RebootSpec,
) error {
	m.Log.V(consts.LogLevelInfo).Info("ProcessRebootValidationRequiredNodes")
	if namespace == "" {
		return fmt.Errorf("namespace must be provided for reboot validation")
	}
	rebootTimeoutSeconds := int64(0)
	if rebootSpec != nil && rebootSpec.RebootTimeoutSeconds > 0 {
		rebootTimeoutSeconds = int64(rebootSpec.RebootTimeoutSeconds)
	}

	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateRebootValidationRequired] {
		node := nodeState.Node
		if rebootTimeoutSeconds > 0 {
			requestedAtRaw, ok := node.Annotations[UpgradeRebootRequestedAtAnnotationKey]
			if !ok || requestedAtRaw == "" {
				m.Log.Info(
					"Reboot request timestamp annotation is not set yet; waiting",
					"node", node.Name,
					"annotation", UpgradeRebootRequestedAtAnnotationKey,
				)
				continue
			}
			requestedAt, err := strconv.ParseInt(requestedAtRaw, 10, 64)
			if err != nil {
				m.Log.V(consts.LogLevelWarning).Error(
					err, "Failed to parse reboot request timestamp; marking upgrade failed",
					"node", node.Name,
					"value", requestedAtRaw,
					"annotation", UpgradeRebootRequestedAtAnnotationKey,
				)
				_ = m.cleanupRebootArtifacts(ctx, namespace, node)
				_ = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, UpgradeStateFailed)
				continue
			}
			if time.Now().Unix() > requestedAt+rebootTimeoutSeconds {
				timeoutErr := fmt.Errorf("reboot validation timed out after %d seconds", rebootTimeoutSeconds)
				m.Log.V(consts.LogLevelWarning).Error(
					timeoutErr, "Reboot validation timed out; marking upgrade failed", "node", node.Name)
				_ = m.cleanupRebootArtifacts(ctx, namespace, node)
				_ = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, UpgradeStateFailed)
				continue
			}
		}

		preRebootBootID, ok := node.Annotations[UpgradePreRebootBootIDAnnotationKey]
		if !ok || preRebootBootID == "" {
			m.Log.Info(
				"Pre-reboot bootID annotation is not set yet; waiting",
				"node", node.Name,
				"annotation", UpgradePreRebootBootIDAnnotationKey,
			)
			continue
		}

		if !m.isNodeConditionReady(node) {
			m.Log.Info("Node is not ready yet after reboot trigger; waiting", "node", node.Name)
			continue
		}

		currentBootID := node.Status.NodeInfo.BootID
		if currentBootID == "" {
			m.Log.Info("Current node bootID is empty; waiting", "node", node.Name)
			continue
		}
		if currentBootID == preRebootBootID {
			m.Log.Info(
				"BootID did not change yet; waiting for actual reboot completion",
				"node", node.Name,
				"preBootID", preRebootBootID,
				"currentBootID", currentBootID,
			)
			continue
		}

		err := m.cleanupRebootArtifacts(ctx, namespace, node)
		if err != nil {
			return err
		}

		err = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(
			ctx, node, UpgradeStateRebootPostRequired)
		if err != nil {
			m.Log.V(consts.LogLevelError).Error(
				err, "Failed to change node upgrade state", "state", UpgradeStateRebootPostRequired)
			return err
		}
	}
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) ProcessRebootPostRequiredNodes(
	ctx context.Context,
	namespace string,
	currentClusterState *ClusterUpgradeState,
	rebootSpec *v1beta1.RebootSpec,
) error {
	m.Log.V(consts.LogLevelInfo).Info("ProcessRebootPostRequiredNodes")
	if namespace == "" {
		return fmt.Errorf("namespace must be provided for reboot post-processing")
	}

	rebootPostTimeoutSeconds := resolveRebootPostTimeoutSeconds(rebootSpec)
	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateRebootPostRequired] {
		node := nodeState.Node
		if !m.isNodeConditionReady(node) {
			m.Log.Info("Node is not ready yet during post-reboot processing; waiting", "node", node.Name)
			continue
		}

		timedOut := m.handleRebootPostTimeout(ctx, node, rebootPostTimeoutSeconds)
		if timedOut {
			continue
		}

		ready, err := m.cleanupUnknownDaemonSetPodsAndCheckReadiness(ctx, namespace, node.Name)
		if err != nil {
			return err
		}
		if !ready {
			continue
		}

		err = m.NodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(
			ctx, node, UpgradeRebootPostStartTimeAnnotationKey)
		if err != nil {
			return err
		}

		if !m.IsValidationEnabled() {
			err = m.updateNodeToUncordonOrDoneState(ctx, nodeState)
			if err != nil {
				return err
			}
			continue
		}

		err = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(
			ctx, node, UpgradeStateValidationRequired)
		if err != nil {
			m.Log.V(consts.LogLevelError).Error(
				err, "Failed to change node upgrade state", "state", UpgradeStateValidationRequired)
			return err
		}
	}
	return nil
}

func resolveRebootPostTimeoutSeconds(rebootSpec *v1beta1.RebootSpec) int64 {
	if rebootSpec != nil && rebootSpec.RebootTimeoutSeconds > 0 {
		return int64(rebootSpec.RebootTimeoutSeconds)
	}
	return defaultRebootPostTimeoutSeconds
}

func (m *ClusterUpgradeStateManagerImpl) handleRebootPostTimeout(
	ctx context.Context,
	node *corev1.Node,
	timeoutSeconds int64,
) bool {
	if timeoutSeconds <= 0 {
		return false
	}

	annotationKey := UpgradeRebootPostStartTimeAnnotationKey
	currentTime := time.Now().Unix()
	if _, present := node.Annotations[annotationKey]; !present {
		err := m.NodeUpgradeStateProvider.SetNodeUpgradeAnnotation(ctx, node, annotationKey, strconv.FormatInt(currentTime, 10))
		if err != nil {
			m.Log.V(consts.LogLevelWarning).Error(
				err, "Failed to set reboot post start-time annotation; waiting",
				"node", node.Name,
				"annotation", annotationKey,
			)
			return false
		}
		return false
	}

	startTime, err := strconv.ParseInt(node.Annotations[annotationKey], 10, 64)
	if err != nil {
		m.Log.V(consts.LogLevelWarning).Error(
			err, "Failed to parse reboot post start-time annotation; marking upgrade failed",
			"node", node.Name,
			"annotation", annotationKey,
			"value", node.Annotations[annotationKey],
		)
		_ = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, UpgradeStateFailed)
		cleanupErr := m.NodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, annotationKey)
		if cleanupErr != nil {
			m.Log.V(consts.LogLevelWarning).Error(
				cleanupErr, "Failed to remove reboot post start-time annotation after parse failure",
				"node", node.Name,
				"annotation", annotationKey,
			)
		}
		return true
	}

	if currentTime > startTime+timeoutSeconds {
		timeoutErr := fmt.Errorf("post-reboot stabilization timed out after %d seconds", timeoutSeconds)
		m.Log.V(consts.LogLevelWarning).Error(
			timeoutErr, "Post-reboot stabilization timed out; marking upgrade failed", "node", node.Name)
		_ = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, UpgradeStateFailed)
		cleanupErr := m.NodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, annotationKey)
		if cleanupErr != nil {
			m.Log.V(consts.LogLevelWarning).Error(
				cleanupErr, "Failed to remove reboot post start-time annotation after timeout",
				"node", node.Name,
				"annotation", annotationKey,
			)
		}
		return true
	}
	return false
}

func (m *ClusterUpgradeStateManagerImpl) cleanupUnknownDaemonSetPodsAndCheckReadiness(
	ctx context.Context,
	namespace string,
	nodeName string,
) (bool, error) {
	daemonSetPods, err := m.listNodeDaemonSetPods(ctx, namespace, nodeName)
	if err != nil {
		return false, err
	}

	if len(daemonSetPods) == 0 {
		m.Log.Info("No DaemonSet-managed pods found on node during post-reboot processing; waiting", "node", nodeName)
		return false, nil
	}
	m.Log.V(consts.LogLevelDebug).Info(
		"Collected DaemonSet-managed pods for post-reboot processing",
		"node", nodeName,
		"namespace", namespace,
		"pods", len(daemonSetPods),
	)

	unknownDeleted := 0
	for _, pod := range daemonSetPods {
		m.Log.V(consts.LogLevelDebug).Info(
			"Evaluating DaemonSet pod for Unknown cleanup",
			"node", nodeName,
			"namespace", pod.Namespace,
			"pod", pod.Name,
			"phase", pod.Status.Phase,
			"deleting", pod.DeletionTimestamp != nil,
		)
		if pod.Status.Phase != corev1.PodUnknown {
			m.Log.V(consts.LogLevelDebug).Info(
				"Skipping pod for Unknown cleanup because phase is not Unknown",
				"node", nodeName,
				"namespace", pod.Namespace,
				"pod", pod.Name,
				"phase", pod.Status.Phase,
			)
			continue
		}

		m.Log.Info(
			"Deleting Unknown DaemonSet pod after reboot",
			"node", nodeName,
			"namespace", pod.Namespace,
			"pod", pod.Name,
		)
		err = m.K8sInterface.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		unknownDeleted++
	}
	if unknownDeleted > 0 {
		m.Log.Info("Deleted Unknown DaemonSet pods; waiting for replacement pods to become ready",
			"node", nodeName,
			"deletedPods", unknownDeleted)
		return false, nil
	}

	for _, pod := range daemonSetPods {
		if isRebootPostPodReady(pod) {
			m.Log.V(consts.LogLevelDebug).Info(
				"DaemonSet pod is ready during post-reboot stabilization",
				"node", nodeName,
				"namespace", pod.Namespace,
				"pod", pod.Name,
			)
			continue
		}

		m.Log.Info(
			"DaemonSet pod is not ready yet after reboot; waiting",
			"node", nodeName,
			"namespace", pod.Namespace,
			"pod", pod.Name,
			"phase", pod.Status.Phase,
		)
		return false, nil
	}

	return true, nil
}

func (m *ClusterUpgradeStateManagerImpl) listNodeDaemonSetPods(
	ctx context.Context,
	namespace string,
	nodeName string,
) ([]corev1.Pod, error) {
	listOptions := metav1.ListOptions{
		FieldSelector: fmt.Sprintf(nodeNameFieldSelectorFmt, nodeName),
	}
	podList, err := m.K8sInterface.CoreV1().Pods(namespace).List(ctx, listOptions)
	if err != nil {
		return nil, err
	}

	daemonSetPods := make([]corev1.Pod, 0, len(podList.Items))
	for _, pod := range podList.Items {
		ownerRef := metav1.GetControllerOf(&pod)
		if ownerRef == nil || ownerRef.Kind != "DaemonSet" {
			continue
		}
		m.Log.V(consts.LogLevelDebug).Info(
			"Selected DaemonSet-managed pod on node for post-reboot stabilization",
			"node", nodeName,
			"namespace", pod.Namespace,
			"pod", pod.Name,
			"owner", ownerRef.Name,
			"phase", pod.Status.Phase,
		)
		daemonSetPods = append(daemonSetPods, pod)
	}
	return daemonSetPods, nil
}

func isRebootPostPodReady(pod corev1.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return false
	}
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	if len(pod.Status.ContainerStatuses) == 0 {
		return false
	}
	for _, status := range pod.Status.ContainerStatuses {
		if !status.Ready {
			return false
		}
	}
	return true
}

func (m *ClusterUpgradeStateManagerImpl) cleanupRebootArtifacts(
	ctx context.Context,
	namespace string,
	node *corev1.Node,
) error {
	rebootPodName := node.Annotations[UpgradeRebootPodNameAnnotationKey]
	if rebootPodName != "" {
		if podRebootManager, ok := m.RebootManager.(*PodRebootManager); ok {
			err := podRebootManager.DeleteRebootPod(ctx, namespace, rebootPodName)
			if err != nil {
				m.Log.V(consts.LogLevelWarning).Error(
					err, "Failed to delete reboot pod",
					"namespace", namespace,
					"pod", rebootPodName,
					"node", node.Name)
			}
		}
	}

	err := m.NodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, UpgradePreRebootBootIDAnnotationKey)
	if err != nil {
		return err
	}
	err = m.NodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, UpgradeRebootRequestedAtAnnotationKey)
	if err != nil {
		return err
	}
	err = m.NodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, UpgradeRebootPodNameAnnotationKey)
	if err != nil {
		return err
	}
	err = m.NodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, UpgradeRebootPostStartTimeAnnotationKey)
	if err != nil {
		return err
	}
	return nil
}

func resolveRebootPodImage(rebootSpec *v1beta1.RebootSpec) string {
	return rebootSpec.Image.Registry + "/" +
		rebootSpec.Image.Image + ":" +
		rebootSpec.Image.Version
}

func (m *ClusterUpgradeStateManagerImpl) isDriverPodFailing(pod *corev1.Pod) bool {
	for _, status := range pod.Status.InitContainerStatuses {
		if !status.Ready && status.RestartCount > 10 {
			return true
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if !status.Ready && status.RestartCount > 10 {
			return true
		}
	}
	return false
}

func (m *ClusterUpgradeStateManagerImpl) ProcessUpgradeFailedNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState,
) error {
	m.Log.V(consts.LogLevelInfo).Info("ProcessUpgradeFailedNodes")

	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateFailed] {
		driverPodInSync, err := m.isDriverPodInSync(ctx, nodeState)
		if err != nil {
			m.Log.V(consts.LogLevelError).Error(
				err, "Failed to check if driver pod on the node is in sync", "nodeState", nodeState)
			return err
		}
		if driverPodInSync {
			newUpgradeState := UpgradeStateUncordonRequired
			annotationKey := UpgradeInitialStateAnnotationKey
			if _, ok := nodeState.Node.Annotations[annotationKey]; ok {
				m.Log.V(consts.LogLevelInfo).Info("Node was Unschedulable at beginning of upgrade, skipping uncordon",
					"node", nodeState.Node.Name)
				newUpgradeState = UpgradeStateDone
			}

			err = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, newUpgradeState)
			if err != nil {
				m.Log.V(consts.LogLevelError).Error(
					err, "Failed to change node upgrade state", "state", newUpgradeState)
				return err
			}

			if newUpgradeState == UpgradeStateDone {
				m.Log.V(consts.LogLevelDebug).Info("Removing node upgrade annotation",
					"node", nodeState.Node.Name, "annotation", annotationKey)
				err = m.NodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, nodeState.Node, annotationKey)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (m *ClusterUpgradeStateManagerImpl) ProcessValidationRequiredNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState,
) error {
	m.Log.V(consts.LogLevelInfo).Info("ProcessValidationRequiredNodes")

	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateValidationRequired] {
		node := nodeState.Node
		err := m.SafeDriverLoadManager.UnblockLoading(ctx, nodeState.Node)
		if err != nil {
			m.Log.V(consts.LogLevelError).Error(
				err, "Failed to unblock loading of the driver", "nodeState", nodeState)
			return err
		}
		validationDone, err := m.ValidationManager.Validate(ctx, node)
		if err != nil {
			m.Log.V(consts.LogLevelError).Error(err, "Failed to validate driver upgrade", "node", node.Name)
			return err
		}

		if !validationDone {
			m.Log.V(consts.LogLevelInfo).Info("Validations not complete on the node", "node", node.Name)
			continue
		}

		err = m.updateNodeToUncordonOrDoneState(ctx, nodeState)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) IsValidationEnabled() bool {
	return m.validationStateEnabled
}

func (m *ClusterUpgradeStateManagerImpl) ProcessUncordonRequiredNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState,
) error {
	m.Log.V(consts.LogLevelInfo).Info("ProcessUncordonRequiredNodes")

	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateUncordonRequired] {
		if IsNodeInRequestorMode(nodeState.Node) {
			continue
		}
		err := m.CordonManager.Uncordon(ctx, nodeState.Node)
		if err != nil {
			m.Log.V(consts.LogLevelWarning).Error(
				err, "Node uncordon failed", "node", nodeState.Node)
			return err
		}
		err = m.NodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, UpgradeStateDone)
		if err != nil {
			m.Log.V(consts.LogLevelError).Error(
				err, "Failed to change node upgrade state", "state", UpgradeStateDone)
			return err
		}
	}
	return nil
}
