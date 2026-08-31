package upgrade

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func (m *ClusterUpgradeStateManagerImpl) podInSyncWithDS(ctx context.Context,
	nodeState *NodeUpgradeState,
) (isPodSynced, isOrphened bool, err error) {
	if isOrphened = nodeState.IsOrphanedPod(); isOrphened {
		return isPodSynced, isOrphened, nil
	}
	podRevisionHash, err := m.podManager.GetPodControllerRevisionHash(nodeState.DriverPod)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to get pod template revision hash", "pod", nodeState.DriverPod)
		return isPodSynced, isOrphened, err
	}
	log.FromContext(ctx).V(consts.VDebug).Info("Pod template revision hash", "hash", podRevisionHash)
	daemonsetRevisionHash, err := m.podManager.GetDaemonsetControllerRevisionHash(ctx, nodeState.DriverDaemonSet)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to get daemonset template revision hash", "daemonset", nodeState.DriverDaemonSet)
		return isPodSynced, isOrphened, err
	}
	log.FromContext(ctx).V(consts.VDebug).Info("Daemonset template revision hash", "hash", daemonsetRevisionHash)
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
	log.FromContext(ctx).V(consts.VDebug).Info("ProcessDoneOrUnknownNodes")

	var errs []error
	for _, nodeState := range currentClusterState.NodeStates[nodeStateName] {
		requireUpgrade, err := m.shouldRequireUpgradeForDoneOrUnknownNode(ctx, nodeState)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if requireUpgrade {
			if err := m.transitionDoneOrUnknownNodeToUpgradeRequired(ctx, nodeState); err != nil {
				errs = append(errs, err)
			}
			continue
		}

		if nodeStateName == UpgradeStateUnknown {
			if err := m.transitionUnknownNodeToDone(ctx, nodeState); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		log.FromContext(ctx).V(consts.VDebug).Info("Node in UpgradeDone state, upgrade not required",
			"node", nodeState.Node.Name)
	}
	return errors.Join(errs...)
}

func (m *ClusterUpgradeStateManagerImpl) shouldRequireUpgradeForDoneOrUnknownNode(
	ctx context.Context, nodeState *NodeUpgradeState,
) (bool, error) {
	isPodSynced, isOrphaned, err := m.podInSyncWithDS(ctx, nodeState)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to get daemonset template/pod revision hash")
		return false, err
	}

	isUpgradeRequested := m.IsUpgradeRequested(nodeState.Node)
	isWaitingForSafeDriverLoad := m.safeDriverLoadManager.IsWaitingForSafeDriverLoad(ctx, nodeState.Node)
	if isWaitingForSafeDriverLoad {
		log.FromContext(ctx).Info("Node is waiting for safe driver load, initialize upgrade",
			"node", nodeState.Node.Name)
	}

	return (!isPodSynced && !isOrphaned) || isWaitingForSafeDriverLoad || isUpgradeRequested, nil
}

func (m *ClusterUpgradeStateManagerImpl) transitionDoneOrUnknownNodeToUpgradeRequired(
	ctx context.Context, nodeState *NodeUpgradeState,
) error {
	if IsNodeUnschedulable(nodeState.Node) {
		log.FromContext(ctx).Info("Node is unschedulable, adding annotation to track initial state of the node",
			"node", nodeState.Node.Name, "annotation", UpgradeInitialStateAnnotationKey)
		err := m.nodeUpgradeStateProvider.SetNodeUpgradeAnnotation(ctx, nodeState.Node, UpgradeInitialStateAnnotationKey,
			trueString)
		if err != nil {
			return err
		}
	}

	err := m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, UpgradeStateUpgradeRequired)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to change node upgrade state", "state", UpgradeStateUpgradeRequired, "node", nodeState.Node)
		return err
	}

	log.FromContext(ctx).Info("Node requires upgrade, changed its state to UpgradeRequired",
		"node", nodeState.Node.Name)
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) transitionUnknownNodeToDone(
	ctx context.Context, nodeState *NodeUpgradeState,
) error {
	err := m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, UpgradeStateDone)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to change node upgrade state", "state", UpgradeStateDone)
		return err
	}

	log.FromContext(ctx).Info("Changed node state to UpgradeDone",
		"node", nodeState.Node.Name)
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) countActiveUpgradeNodes(ctx context.Context) (int, error) {
	nodeList := &corev1.NodeList{}
	if err := m.k8sClient.List(ctx, nodeList, client.HasLabels{UpgradeStateLabelKey}); err != nil {
		return 0, fmt.Errorf("failed to list nodes for upgrade-slot accounting: %w", err)
	}

	count := 0
	for i := range nodeList.Items {
		if IsInProgressUpgradeState(nodeList.Items[i].Labels[UpgradeStateLabelKey]) {
			count++
		}
	}
	return count, nil
}

func (m *ClusterUpgradeStateManagerImpl) ProcessUpgradeRequiredNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState,
	upgradePolicy *v1beta1.DriverUpgradePolicySpec,
) error {
	upgradesInProgress, err := m.countActiveUpgradeNodes(ctx)
	if err != nil {
		return err
	}
	upgradesAvailable := len(currentClusterState.NodeStates[UpgradeStateUpgradeRequired])
	if upgradePolicy.MaxParallelUpgrades != 0 {
		upgradesAvailable = upgradePolicy.MaxParallelUpgrades - upgradesInProgress
	}
	log.FromContext(ctx).Info("Upgrades in progress",
		"currently in progress", upgradesInProgress,
		"max parallel upgrades", upgradePolicy.MaxParallelUpgrades,
		"upgrade slots available", upgradesAvailable)

	var errs []error
	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateUpgradeRequired] {
		if m.IsUpgradeRequested(nodeState.Node) {
			err := m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, nodeState.Node, UpgradeRequestedAnnotationKey)
			if err != nil {
				log.FromContext(ctx).Error(err, "Failed to delete node upgrade-requested annotation")
				errs = append(errs, err)
				continue
			}
		}
		if m.SkipNodeUpgrade(nodeState.Node) {
			log.FromContext(ctx).Info("Node is marked for skipping upgrades", "node", nodeState.Node.Name)
			continue
		}

		if upgradesAvailable <= 0 {
			// Already cordoned nodes are allowed to continue to avoid stalling upgrades that have already progressed.
			if IsNodeUnschedulable(nodeState.Node) {
				log.FromContext(ctx).Info("Node is already cordoned, progressing for driver upgrade",
					"node", nodeState.Node.Name)
			} else {
				log.FromContext(ctx).Info("Node upgrade limit reached, pausing further upgrades",
					"node", nodeState.Node.Name)
				continue
			}
		}

		err := m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, UpgradeStateCordonRequired)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to change node upgrade state", "state", UpgradeStateCordonRequired)
			errs = append(errs, err)
			continue
		}
		upgradesAvailable--
		log.FromContext(ctx).Info("Node waiting for cordon",
			"node", nodeState.Node.Name)
	}

	return errors.Join(errs...)
}

func (m *ClusterUpgradeStateManagerImpl) IsUpgradeRequested(node *corev1.Node) bool {
	return node.Annotations[UpgradeRequestedAnnotationKey] == trueString
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
	log.FromContext(ctx).V(consts.VDebug).Info("ProcessCordonRequiredNodes")

	var errs []error
	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateCordonRequired] {
		err := m.cordonManager.Cordon(ctx, nodeState.Node)
		if err != nil {
			log.FromContext(ctx).Error(err, "Node cordon failed", "node", nodeState.Node)
			errs = append(errs, err)
			continue
		}
		err = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, UpgradeStateWaitForJobsRequired)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to change node upgrade state", "state", UpgradeStateWaitForJobsRequired)
			errs = append(errs, err)
			continue
		}
	}
	return errors.Join(errs...)
}

func (m *ClusterUpgradeStateManagerImpl) ProcessWaitForJobsRequiredNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState,
	waitForCompletionSpec *v1beta1.WaitForCompletionSpec,
) error {
	log.FromContext(ctx).V(consts.VDebug).Info("ProcessWaitForJobsRequiredNodes")

	waitForJobsRequiredNodes := currentClusterState.NodeStates[UpgradeStateWaitForJobsRequired]
	if len(waitForJobsRequiredNodes) == 0 {
		return nil
	}

	nodes := make([]*corev1.Node, 0, len(waitForJobsRequiredNodes))
	for _, nodeState := range waitForJobsRequiredNodes {
		nodes = append(nodes, nodeState.Node)
		if waitForCompletionSpec == nil || waitForCompletionSpec.PodSelector == "" {
			log.FromContext(ctx).Info("No jobs to wait for as no pod selector was provided. Moving to next state.")
			nextState := UpgradeStatePodDeletionRequired
			if !m.IsPodDeletionEnabled() {
				nextState = UpgradeStateDrainRequired
			}
			if err := m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, nextState); err != nil {
				log.FromContext(ctx).Info("Failed to change node upgrade state, will retry next cycle", "error", err,
					"node", nodeState.Node.Name, "state", nextState)
				continue
			}
			log.FromContext(ctx).Info("Updated the node state", "node", nodeState.Node.Name, "state", nextState)
		}
	}
	if waitForCompletionSpec == nil || waitForCompletionSpec.PodSelector == "" {
		return nil
	}

	podManagerConfig := PodManagerConfig{WaitForCompletionSpec: waitForCompletionSpec, Nodes: nodes}
	err := m.podManager.ScheduleCheckOnPodCompletion(ctx, &podManagerConfig)
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
	log.FromContext(ctx).V(consts.VDebug).Info("ProcessPodDeletionRequiredNodes")

	if !m.IsPodDeletionEnabled() {
		log.FromContext(ctx).Info("PodDeletion is not enabled, proceeding straight to the next state")
		for _, nodeState := range currentClusterState.NodeStates[UpgradeStatePodDeletionRequired] {
			if err := m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, UpgradeStateDrainRequired); err != nil {
				log.FromContext(ctx).Info("Failed to change node upgrade state, will retry next cycle", "error", err,
					"node", nodeState.Node.Name, "state", UpgradeStateDrainRequired)
				continue
			}
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

	return m.podManager.SchedulePodEviction(ctx, &podManagerConfig)
}

func (m *ClusterUpgradeStateManagerImpl) ProcessDrainNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState, drainSpec *v1beta1.DrainSpec,
) error {
	log.FromContext(ctx).V(consts.VDebug).Info("ProcessDrainNodes")
	if drainSpec == nil || !drainSpec.Enable {
		log.FromContext(ctx).Info("Node drain is disabled by policy, skipping this step")
		var errs []error
		for _, nodeState := range currentClusterState.NodeStates[UpgradeStateDrainRequired] {
			err := m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, UpgradeStatePodRestartRequired)
			if err != nil {
				log.FromContext(ctx).Error(err, "Failed to change node upgrade state", "state", UpgradeStatePodRestartRequired)
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}

	drainConfig := DrainConfiguration{
		Spec:  drainSpec,
		Nodes: make([]*corev1.Node, 0, len(currentClusterState.NodeStates[UpgradeStateDrainRequired])),
	}
	nodeNames := make([]string, 0, cap(drainConfig.Nodes))
	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateDrainRequired] {
		drainConfig.Nodes = append(drainConfig.Nodes, nodeState.Node)
		nodeNames = append(nodeNames, nodeState.Node.Name)
	}

	log.FromContext(ctx).Info("Scheduling nodes drain", "nodes", nodeNames,
		"force", drainSpec.Force, "deleteEmptyDirData", drainSpec.DeleteEmptyDirData)

	return m.drainManager.ScheduleNodesDrain(ctx, &drainConfig)
}

func (m *ClusterUpgradeStateManagerImpl) isDriverPodInSync(ctx context.Context,
	nodeState *NodeUpgradeState,
) (bool, error) {
	isPodSynced, isOrphaned, err := m.podInSyncWithDS(ctx, nodeState)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to get daemonset template/pod revision hash")
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
			log.FromContext(ctx).Info("Node was Unschedulable at beginning of upgrade, skipping uncordon",
				"node", node.Name)
			newUpgradeState = UpgradeStateDone
		}
	}

	err := m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, newUpgradeState)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to change node upgrade state", "node", node.Name, "state", newUpgradeState)
		return err
	}

	if newUpgradeState == UpgradeStateDone || isNodeUnderRequestorMode {
		log.FromContext(ctx).V(consts.VDebug).Info("Removing node upgrade annotation",
			"node", node.Name, "annotation", annotationKey)
		err = m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, annotationKey)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) ProcessPodRestartNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState, rebootRequired bool,
) error {
	log.FromContext(ctx).V(consts.VDebug).Info("ProcessPodRestartNodes")

	var errs []error
	pods := make([]*corev1.Pod, 0, len(currentClusterState.NodeStates[UpgradeStatePodRestartRequired]))
	for _, nodeState := range currentClusterState.NodeStates[UpgradeStatePodRestartRequired] {
		if err := m.processPodRestartNode(ctx, nodeState, rebootRequired, &pods); err != nil {
			errs = append(errs, err)
		}
	}

	if err := m.podManager.SchedulePodsRestart(ctx, pods); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (m *ClusterUpgradeStateManagerImpl) processPodRestartNode(
	ctx context.Context, nodeState *NodeUpgradeState, rebootRequired bool, pods *[]*corev1.Pod,
) error {
	isPodSynced, isOrphaned, err := m.podInSyncWithDS(ctx, nodeState)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to get daemonset template/pod revision hash")
		return err
	}
	if !isPodSynced || isOrphaned {
		if nodeState.DriverPod.DeletionTimestamp.IsZero() {
			*pods = append(*pods, nodeState.DriverPod)
		}
		return nil
	}

	err = m.safeDriverLoadManager.UnblockLoading(ctx, nodeState.Node)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to unblock loading of the driver", "node", nodeState.Node.Name)
		return err
	}
	driverPodInSync, err := m.isDriverPodInSync(ctx, nodeState)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to check if driver pod on the node is in sync", "node", nodeState.Node.Name)
		return err
	}
	if driverPodInSync {
		if rebootRequired {
			err = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node,
				UpgradeStateRebootRequired)
			if err != nil {
				log.FromContext(ctx).Error(err, "Failed to change node upgrade state", "state", UpgradeStateRebootRequired)
				return err
			}
			return nil
		}

		if !m.IsValidationEnabled() {
			return m.updateNodeToUncordonOrDoneState(ctx, nodeState)
		}

		err = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node,
			UpgradeStateValidationRequired)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to change node upgrade state", "state", UpgradeStateValidationRequired)
			return err
		}
		return nil
	}

	if !m.isDriverPodFailing(nodeState.DriverPod) {
		return nil
	}
	log.FromContext(ctx).Info("Driver pod is failing on node with repeated restarts",
		"node", nodeState.Node.Name, "pod", nodeState.DriverPod.Name)
	err = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, UpgradeStateFailed)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to change node upgrade state for node", "node", nodeState.Node.Name,
			"state", UpgradeStateFailed)
		return err
	}
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) ProcessRebootRequiredNodes(
	ctx context.Context,
	namespace string,
	currentClusterState *ClusterUpgradeState,
	rebootSpec *v1beta1.RebootSpec,
) error {
	log.FromContext(ctx).V(consts.VDebug).Info("ProcessRebootRequiredNodes")
	if namespace == "" {
		return fmt.Errorf("namespace must be provided for reboot processing")
	}

	nodes := currentClusterState.NodeStates[UpgradeStateRebootRequired]
	if len(nodes) == 0 {
		return nil
	}
	if rebootSpec == nil {
		log.FromContext(ctx).Info("RebootSpec is nil but nodes are in reboot-required state; marking as failed")
		var errs []error
		for _, nodeState := range nodes {
			if err := m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(
				ctx, nodeState.Node, UpgradeStateFailed); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}
	if m.rebootManager == nil {
		return fmt.Errorf("reboot manager is not configured")
	}
	rebootImage := resolveRebootPodImage(rebootSpec)

	var errs []error
	for _, nodeState := range nodes {
		if err := m.processRebootRequiredNode(ctx, namespace, nodeState.Node, rebootImage); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (m *ClusterUpgradeStateManagerImpl) processRebootRequiredNode(
	ctx context.Context, namespace string, node *corev1.Node, rebootImage string,
) error {
	var preRebootBootID string
	var rebootRequestedAt int64
	var rebootPodName string

	// Check if a reboot was already partially initiated (crash recovery).
	// If annotations exist from a previous attempt, reuse them to avoid
	// creating orphaned reboot pods.
	if existingPodName, ok := node.Annotations[UpgradeRebootPodNameAnnotationKey]; ok && existingPodName != "" {
		log.FromContext(ctx).Info("Reboot annotations already set from previous attempt; resuming",
			"node", node.Name, "existingPod", existingPodName)
		preRebootBootID = node.Annotations[UpgradePreRebootBootIDAnnotationKey]
		requestedAtRaw := node.Annotations[UpgradeRebootRequestedAtAnnotationKey]
		var parseErr error
		rebootRequestedAt, parseErr = strconv.ParseInt(requestedAtRaw, 10, 64)
		if parseErr != nil || preRebootBootID == "" {
			logArgs := []any{
				"node", node.Name,
				"rebootRequestedAt", requestedAtRaw, "preRebootBootID", preRebootBootID,
			}
			if parseErr != nil {
				logArgs = append(logArgs, "error", parseErr)
			}
			log.FromContext(ctx).Info("Corrupted reboot annotations; cleaning up and retrying next cycle", logArgs...)
			if cleanupErr := m.cleanupRebootArtifacts(ctx, namespace, node); cleanupErr != nil {
				log.FromContext(ctx).Info("Failed to cleanup corrupted reboot artifacts", "error", cleanupErr,
					"node", node.Name)
			}
			return nil
		}
		rebootPodName = existingPodName
	} else {
		preRebootBootID = node.Status.NodeInfo.BootID
		if preRebootBootID == "" {
			err := fmt.Errorf("node %q bootID is empty", node.Name)
			log.FromContext(ctx).Error(err, "Failed to read node bootID before reboot")
			return err
		}

		err := m.nodeUpgradeStateProvider.SetNodeUpgradeAnnotation(
			ctx, node, UpgradePreRebootBootIDAnnotationKey, preRebootBootID)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to set node pre-reboot bootID annotation",
				"annotation", UpgradePreRebootBootIDAnnotationKey,
				"node", node.Name)
			return err
		}

		rebootRequestedAt = time.Now().Unix()
		rebootRequestedAtValue := strconv.FormatInt(rebootRequestedAt, 10)
		rebootPodName = BuildRebootPodName(node.Name, rebootRequestedAt)
		err = m.nodeUpgradeStateProvider.SetNodeUpgradeAnnotation(
			ctx, node, UpgradeRebootRequestedAtAnnotationKey, rebootRequestedAtValue)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to set node reboot request annotation",
				"annotation", UpgradeRebootRequestedAtAnnotationKey,
				"node", node.Name)
			return err
		}

		err = m.nodeUpgradeStateProvider.SetNodeUpgradeAnnotation(
			ctx, node, UpgradeRebootPodNameAnnotationKey, rebootPodName)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to set node reboot pod annotation",
				"annotation", UpgradeRebootPodNameAnnotationKey,
				"node", node.Name)
			return err
		}
	}

	err := m.rebootManager.Trigger(ctx, node, RebootTriggerRequest{
		RequestedAtUnix: rebootRequestedAt,
		PreRebootBootID: preRebootBootID,
		Namespace:       namespace,
		PodName:         rebootPodName,
		Image:           rebootImage,
	})
	if err != nil {
		if stateErr := m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, UpgradeStateFailed); stateErr != nil {
			log.FromContext(ctx).Info("Failed to mark node as failed after reboot trigger error", "error", stateErr,
				"node", node.Name)
		}
		log.FromContext(ctx).Error(err, "Failed to trigger node reboot", "node", node.Name)
		return err
	}

	err = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(
		ctx, node, UpgradeStateRebootValidationRequired)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to change node upgrade state", "state", UpgradeStateRebootValidationRequired,
			"node", node.Name)
		return err
	}
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) ProcessRebootValidationRequiredNodes(
	ctx context.Context,
	namespace string,
	currentClusterState *ClusterUpgradeState,
	rebootSpec *v1beta1.RebootSpec,
) error {
	log.FromContext(ctx).V(consts.VDebug).Info("ProcessRebootValidationRequiredNodes")
	if namespace == "" {
		return fmt.Errorf("namespace must be provided for reboot validation")
	}
	rebootTimeoutSeconds := int64(0)
	if rebootSpec != nil && rebootSpec.RebootTimeoutSeconds > 0 {
		rebootTimeoutSeconds = int64(rebootSpec.RebootTimeoutSeconds)
	}

	var errs []error
	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateRebootValidationRequired] {
		node := nodeState.Node
		if rebootTimeoutSeconds > 0 {
			requestedAtRaw, ok := node.Annotations[UpgradeRebootRequestedAtAnnotationKey]
			if !ok || requestedAtRaw == "" {
				log.FromContext(ctx).Info("Reboot request timestamp annotation is not set yet; waiting",
					"node", node.Name,
					"annotation", UpgradeRebootRequestedAtAnnotationKey,
				)
				continue
			}
			requestedAt, err := strconv.ParseInt(requestedAtRaw, 10, 64)
			if err != nil {
				log.FromContext(ctx).Error(err, "Failed to parse reboot request timestamp; marking upgrade failed",
					"node", node.Name,
					"value", requestedAtRaw,
					"annotation", UpgradeRebootRequestedAtAnnotationKey,
				)
				if cleanupErr := m.cleanupRebootArtifacts(ctx, namespace, node); cleanupErr != nil {
					log.FromContext(ctx).Info("Failed to cleanup reboot artifacts", "error", cleanupErr,
						"node", node.Name)
				}
				if stateErr := m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, UpgradeStateFailed); stateErr != nil {
					log.FromContext(ctx).Info("Failed to mark node as failed; will retry next cycle", "error", stateErr,
						"node", node.Name)
				}
				continue
			}
			if time.Now().Unix() > requestedAt+rebootTimeoutSeconds {
				timeoutErr := fmt.Errorf("reboot validation timed out after %d seconds", rebootTimeoutSeconds)
				log.FromContext(ctx).Error(timeoutErr, "Reboot validation timed out; marking upgrade failed", "node", node.Name)
				if cleanupErr := m.cleanupRebootArtifacts(ctx, namespace, node); cleanupErr != nil {
					log.FromContext(ctx).Info("Failed to cleanup reboot artifacts", "error", cleanupErr,
						"node", node.Name)
				}
				if stateErr := m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, UpgradeStateFailed); stateErr != nil {
					log.FromContext(ctx).Info("Failed to mark node as failed; will retry next cycle", "error", stateErr,
						"node", node.Name)
				}
				continue
			}
		}

		preRebootBootID, ok := node.Annotations[UpgradePreRebootBootIDAnnotationKey]
		if !ok || preRebootBootID == "" {
			log.FromContext(ctx).Info("Pre-reboot bootID annotation is not set yet; waiting",
				"node", node.Name,
				"annotation", UpgradePreRebootBootIDAnnotationKey,
			)
			continue
		}

		if !m.isNodeConditionReady(node) {
			log.FromContext(ctx).Info("Node is not ready yet after reboot trigger; waiting", "node", node.Name)
			continue
		}

		currentBootID := node.Status.NodeInfo.BootID
		if currentBootID == "" {
			log.FromContext(ctx).Info("Current node bootID is empty; waiting", "node", node.Name)
			continue
		}
		if currentBootID == preRebootBootID {
			log.FromContext(ctx).Info("BootID did not change yet; waiting for actual reboot completion",
				"node", node.Name,
				"preBootID", preRebootBootID,
				"currentBootID", currentBootID,
			)
			continue
		}

		err := m.cleanupRebootArtifacts(ctx, namespace, node)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		err = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(
			ctx, node, UpgradeStateRebootPostRequired)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to change node upgrade state", "state", UpgradeStateRebootPostRequired)
			errs = append(errs, err)
			continue
		}
	}
	return errors.Join(errs...)
}

func (m *ClusterUpgradeStateManagerImpl) ProcessRebootPostRequiredNodes(
	ctx context.Context,
	namespace string,
	currentClusterState *ClusterUpgradeState,
	rebootSpec *v1beta1.RebootSpec,
) error {
	log.FromContext(ctx).V(consts.VDebug).Info("ProcessRebootPostRequiredNodes")
	if namespace == "" {
		return fmt.Errorf("namespace must be provided for reboot post-processing")
	}

	rebootPostTimeoutSeconds := resolveRebootPostTimeoutSeconds(rebootSpec)
	var errs []error
	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateRebootPostRequired] {
		node := nodeState.Node
		if !m.isNodeConditionReady(node) {
			log.FromContext(ctx).Info("Node is not ready yet during post-reboot processing; waiting", "node", node.Name)
			continue
		}

		timedOut := m.handleRebootPostTimeout(ctx, node, rebootPostTimeoutSeconds)
		if timedOut {
			continue
		}

		ready, err := m.cleanupUnknownDaemonSetPodsAndCheckReadiness(ctx, namespace, node.Name)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !ready {
			continue
		}

		err = m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(
			ctx, node, UpgradeRebootPostStartTimeAnnotationKey)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		if !m.IsValidationEnabled() {
			if err := m.updateNodeToUncordonOrDoneState(ctx, nodeState); err != nil {
				errs = append(errs, err)
			}
			continue
		}

		err = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(
			ctx, node, UpgradeStateValidationRequired)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to change node upgrade state", "state", UpgradeStateValidationRequired)
			errs = append(errs, err)
			continue
		}
	}
	return errors.Join(errs...)
}

func resolveRebootPostTimeoutSeconds(rebootSpec *v1beta1.RebootSpec) int64 {
	if rebootSpec != nil && rebootSpec.RebootTimeoutSeconds > 0 {
		return int64(rebootSpec.RebootTimeoutSeconds)
	}
	return DefaultRebootPostTimeoutSeconds
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

	timedOut, err := checkAnnotationTimeout(ctx, m.nodeUpgradeStateProvider, node, annotationKey, timeoutSeconds)
	if err != nil {
		log.FromContext(ctx).Error(
			err, "Failed to check reboot post timeout; waiting",
			"node", node.Name,
			"annotation", annotationKey,
		)
		return false
	}

	if timedOut {
		timeoutErr := fmt.Errorf("post-reboot stabilization timed out after %d seconds", timeoutSeconds)
		log.FromContext(ctx).Error(timeoutErr, "Post-reboot stabilization timed out; marking upgrade failed", "node", node.Name)
		if stateErr := m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, UpgradeStateFailed); stateErr != nil {
			log.FromContext(ctx).Info("Failed to mark node as failed; will retry next cycle", "error", stateErr,
				"node", node.Name)
			return false
		}
		cleanupErr := m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, annotationKey)
		if cleanupErr != nil {
			log.FromContext(ctx).Info("Failed to remove reboot post start-time annotation after timeout",
				"error", cleanupErr,
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
		log.FromContext(ctx).Info("No DaemonSet-managed pods found on node during post-reboot processing; waiting", "node", nodeName)
		return false, nil
	}
	log.FromContext(ctx).V(consts.VDebug).Info(
		"Collected DaemonSet-managed pods for post-reboot processing",
		"node", nodeName,
		"namespace", namespace,
		"pods", len(daemonSetPods),
	)

	unknownDeleted := 0
	for _, pod := range daemonSetPods {
		log.FromContext(ctx).V(consts.VDebug).Info(
			"Evaluating DaemonSet pod for Unknown cleanup",
			"node", nodeName,
			"namespace", pod.Namespace,
			"pod", pod.Name,
			"phase", pod.Status.Phase,
			"deleting", pod.DeletionTimestamp != nil,
		)
		if pod.Status.Phase != corev1.PodUnknown {
			log.FromContext(ctx).V(consts.VDebug).Info(
				"Skipping pod for Unknown cleanup because phase is not Unknown",
				"node", nodeName,
				"namespace", pod.Namespace,
				"pod", pod.Name,
				"phase", pod.Status.Phase,
			)
			continue
		}

		log.FromContext(ctx).Info("Deleting Unknown DaemonSet pod after reboot",
			"node", nodeName,
			"namespace", pod.Namespace,
			"pod", pod.Name,
		)
		err = m.k8sInterface.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		unknownDeleted++
	}
	if unknownDeleted > 0 {
		log.FromContext(ctx).Info("Deleted Unknown DaemonSet pods; waiting for replacement pods to become ready",
			"node", nodeName,
			"deletedPods", unknownDeleted)
		return false, nil
	}

	for _, pod := range daemonSetPods {
		if isRebootPostPodReady(pod) {
			log.FromContext(ctx).V(consts.VDebug).Info(
				"DaemonSet pod is ready during post-reboot stabilization",
				"node", nodeName,
				"namespace", pod.Namespace,
				"pod", pod.Name,
			)
			continue
		}

		log.FromContext(ctx).Info("DaemonSet pod is not ready yet after reboot; waiting",
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
	podList, err := m.k8sInterface.CoreV1().Pods(namespace).List(ctx, listOptions)
	if err != nil {
		return nil, err
	}

	daemonSetPods := make([]corev1.Pod, 0, len(podList.Items))
	for _, pod := range podList.Items {
		ownerRef := metav1.GetControllerOf(&pod)
		if ownerRef == nil || ownerRef.Kind != "DaemonSet" {
			continue
		}
		log.FromContext(ctx).V(consts.VDebug).Info(
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
	// Pod deletion is best-effort: the pod will be garbage-collected eventually.
	// Annotation cleanup below is required for correct state machine behavior.
	rebootPodName := node.Annotations[UpgradeRebootPodNameAnnotationKey]
	if rebootPodName != "" {
		if podRebootManager, ok := m.rebootManager.(*PodRebootManager); ok {
			if err := podRebootManager.DeleteRebootPod(ctx, namespace, rebootPodName); err != nil {
				log.FromContext(ctx).Info("Failed to delete reboot pod (best-effort); continuing with annotation cleanup",
					"error", err,
					"namespace", namespace,
					"pod", rebootPodName,
					"node", node.Name)
			}
		}
	}

	err := m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, UpgradePreRebootBootIDAnnotationKey)
	if err != nil {
		return err
	}
	err = m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, UpgradeRebootRequestedAtAnnotationKey)
	if err != nil {
		return err
	}
	err = m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, UpgradeRebootPodNameAnnotationKey)
	if err != nil {
		return err
	}
	err = m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, UpgradeRebootPostStartTimeAnnotationKey)
	if err != nil {
		return err
	}
	return nil
}

func resolveRebootPodImage(rebootSpec *v1beta1.RebootSpec) string {
	// Image is optional; the reboot manager falls back to its default image on "".
	if rebootSpec == nil || rebootSpec.Image == nil {
		return ""
	}
	return rebootSpec.Image.Registry + "/" +
		rebootSpec.Image.Image + ":" +
		rebootSpec.Image.Version
}

func (m *ClusterUpgradeStateManagerImpl) isDriverPodFailing(pod *corev1.Pod) bool {
	for _, status := range pod.Status.InitContainerStatuses {
		if !status.Ready && status.RestartCount > MaxPodRestartCount {
			return true
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if !status.Ready && status.RestartCount > MaxPodRestartCount {
			return true
		}
	}
	return false
}

func (m *ClusterUpgradeStateManagerImpl) ProcessUpgradeFailedNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState,
) error {
	log.FromContext(ctx).V(consts.VDebug).Info("ProcessUpgradeFailedNodes")

	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateFailed] {
		driverPodInSync, err := m.isDriverPodInSync(ctx, nodeState)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to check if driver pod on the node is in sync", "node", nodeState.Node.Name)
			return err
		}
		if driverPodInSync {
			newUpgradeState := UpgradeStateUncordonRequired
			annotationKey := UpgradeInitialStateAnnotationKey
			if _, ok := nodeState.Node.Annotations[annotationKey]; ok {
				log.FromContext(ctx).Info("Node was Unschedulable at beginning of upgrade, skipping uncordon",
					"node", nodeState.Node.Name)
				newUpgradeState = UpgradeStateDone
			}

			err = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, newUpgradeState)
			if err != nil {
				log.FromContext(ctx).Error(err, "Failed to change node upgrade state", "state", newUpgradeState)
				return err
			}

			if newUpgradeState == UpgradeStateDone {
				log.FromContext(ctx).V(consts.VDebug).Info("Removing node upgrade annotation",
					"node", nodeState.Node.Name, "annotation", annotationKey)
				err = m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, nodeState.Node, annotationKey)
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
	log.FromContext(ctx).V(consts.VDebug).Info("ProcessValidationRequiredNodes")

	var errs []error
	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateValidationRequired] {
		node := nodeState.Node
		err := m.safeDriverLoadManager.UnblockLoading(ctx, nodeState.Node)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to unblock loading of the driver", "node", nodeState.Node.Name)
			errs = append(errs, err)
			continue
		}
		validationDone, err := m.validationManager.Validate(ctx, node)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to validate driver upgrade", "node", node.Name)
			errs = append(errs, err)
			continue
		}

		if !validationDone {
			log.FromContext(ctx).Info("Validations not complete on the node", "node", node.Name)
			continue
		}

		err = m.updateNodeToUncordonOrDoneState(ctx, nodeState)
		if err != nil {
			errs = append(errs, err)
			continue
		}
	}
	return errors.Join(errs...)
}

func (m *ClusterUpgradeStateManagerImpl) IsValidationEnabled() bool {
	return m.validationStateEnabled
}

func (m *ClusterUpgradeStateManagerImpl) ProcessUncordonRequiredNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState,
) error {
	log.FromContext(ctx).V(consts.VDebug).Info("ProcessUncordonRequiredNodes")

	var errs []error
	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateUncordonRequired] {
		if IsNodeInRequestorMode(nodeState.Node) {
			continue
		}
		err := m.cordonManager.Uncordon(ctx, nodeState.Node)
		if err != nil {
			log.FromContext(ctx).Error(err, "Node uncordon failed", "node", nodeState.Node)
			errs = append(errs, err)
			continue
		}
		err = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, UpgradeStateDone)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to change node upgrade state", "state", UpgradeStateDone)
			errs = append(errs, err)
			continue
		}
	}
	return errors.Join(errs...)
}
