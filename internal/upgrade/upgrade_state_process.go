package upgrade

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
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
	podDigest := m.podManager.GetPodDriverConfigDigest(nodeState.DriverPod)
	log.FromContext(ctx).V(consts.VDebug).Info("Pod driver config digest", "digest", podDigest)
	daemonsetDigest, err := m.podManager.GetDaemonSetDriverConfigDigest(nodeState.DriverDaemonSet)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to get daemonset driver config digest", "daemonset", nodeState.DriverDaemonSet.Name)
		return isPodSynced, isOrphened, err
	}
	log.FromContext(ctx).V(consts.VDebug).Info("Daemonset driver config digest", "digest", daemonsetDigest)
	isPodSynced = podDigest == daemonsetDigest
	return isPodSynced, isOrphened, nil
}

// driverPodTemplateOutdated reports whether the pod was rendered from an older
// pod template than its DaemonSet now carries. Admission never looks at this:
// DRIVER_CONFIG_DIGEST alone starts a rollout. But a node already in the
// rollout replaces its pod whenever any part of the template changed, so an
// operator-requested attempt lands init-container, volume and scheduling
// changes too instead of completing on the old pod, and a node parked in
// upgrade-failed at the pod-restart step is retried when the template moves.
// Callers must rule out an orphaned pod first.
func driverPodTemplateOutdated(nodeState *NodeUpgradeState) bool {
	return nodeState.DriverPod.Annotations[consts.DriverTemplateHashAnnotation] !=
		nodeState.DriverDaemonSet.Spec.Template.Annotations[consts.DriverTemplateHashAnnotation]
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
		log.FromContext(ctx).Error(err, "Failed to compare pod and daemonset driver config digest")
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
		if err := m.recordPreexistingCordon(ctx, nodeState.Node); err != nil {
			return err
		}
	}

	// A fresh upgrade must not inherit the previous attempt's judgement artifacts.
	if err := m.clearParkedBookkeeping(ctx, nodeState.Node); err != nil {
		return err
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

// recordPreexistingCordon decides whose cordon a node arrives with. An
// administrator's is recorded in the initial-state annotation so the rollout
// leaves it in place. One carrying k8s-driver-manager's claim is not the
// administrator's: the binary took it on its own eviction path, with
// autoUpgrade off, and was killed before releasing it. The rollout adopts that
// cordon and lifts it at the end like its own, because neither side would
// otherwise: the binary skips its uncordon under auto-upgrade, and the operator
// skips a cordon it believes predates the rollout. The claim goes with the
// adoption; left behind, a later manual-mode run would read the administrator's
// next cordon as its own and lift it.
func (m *ClusterUpgradeStateManagerImpl) recordPreexistingCordon(ctx context.Context, node *corev1.Node) error {
	if _, claimed := node.Annotations[consts.DriverManagerCordonClaimAnnotation]; claimed {
		log.FromContext(ctx).Info("Adopting the cordon k8s-driver-manager left on the node; the rollout will lift it",
			"node", node.Name, "annotation", consts.DriverManagerCordonClaimAnnotation)
		return m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, consts.DriverManagerCordonClaimAnnotation)
	}
	log.FromContext(ctx).Info("Node is unschedulable, adding annotation to track initial state of the node",
		"node", node.Name, "annotation", UpgradeInitialStateAnnotationKey)
	return m.nodeUpgradeStateProvider.SetNodeUpgradeAnnotation(ctx, node, UpgradeInitialStateAnnotationKey, trueString)
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
			// maxParallelUpgrades bounds every admission, cordoned nodes included.
			// A cordon says nothing about whether the node is idle (operators
			// cordon nodes to pin NPU workloads), and a node parked in
			// upgrade-failed keeps its cordon, so exempting cordoned nodes let a
			// retried failure push the rollout past the cap.
			log.FromContext(ctx).Info("Node upgrade limit reached, pausing further upgrades",
				"node", nodeState.Node.Name)
			continue
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
) error {
	log.FromContext(ctx).V(consts.VDebug).Info("ProcessPodDeletionRequiredNodes")

	if !m.IsPodDeletionEnabled() {
		log.FromContext(ctx).Info("PodDeletion is not enabled, proceeding straight to the next state")
		for _, nodeState := range currentClusterState.NodeStates[UpgradeStatePodDeletionRequired] {
			if err := m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, nodeState.Node, UpgradeStatePodRestartRequired); err != nil {
				log.FromContext(ctx).Info("Failed to change node upgrade state, will retry next cycle", "error", err,
					"node", nodeState.Node.Name, "state", UpgradeStatePodRestartRequired)
				continue
			}
		}
		return nil
	}

	podManagerConfig := PodManagerConfig{
		DeletionSpec: podDeletionSpec,
		Nodes:        make([]*corev1.Node, 0, len(currentClusterState.NodeStates[UpgradeStatePodDeletionRequired])),
	}

	for _, nodeState := range currentClusterState.NodeStates[UpgradeStatePodDeletionRequired] {
		podManagerConfig.Nodes = append(podManagerConfig.Nodes, nodeState.Node)
	}

	if len(podManagerConfig.Nodes) == 0 {
		return nil
	}

	return m.podManager.SchedulePodEviction(ctx, &podManagerConfig)
}

func (m *ClusterUpgradeStateManagerImpl) isDriverPodInSync(ctx context.Context,
	nodeState *NodeUpgradeState,
) (bool, error) {
	isPodSynced, isOrphaned, err := m.podInSyncWithDS(ctx, nodeState)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to compare pod and daemonset driver config digest")
		return false, err
	}
	if isOrphaned {
		return false, nil
	}
	if isPodSynced && !driverPodTemplateOutdated(nodeState) &&
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
	ctx context.Context, currentClusterState *ClusterUpgradeState, podRestartTimeoutSeconds int64,
) error {
	log.FromContext(ctx).V(consts.VDebug).Info("ProcessPodRestartNodes")

	var errs []error
	pods := make([]*corev1.Pod, 0, len(currentClusterState.NodeStates[UpgradeStatePodRestartRequired]))
	for _, nodeState := range currentClusterState.NodeStates[UpgradeStatePodRestartRequired] {
		if err := m.processPodRestartNode(ctx, nodeState, podRestartTimeoutSeconds, &pods); err != nil {
			errs = append(errs, err)
		}
	}

	if err := m.podManager.SchedulePodsRestart(ctx, pods); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (m *ClusterUpgradeStateManagerImpl) processPodRestartNode(
	ctx context.Context, nodeState *NodeUpgradeState, podRestartTimeoutSeconds int64, pods *[]*corev1.Pod,
) error {
	// Signal fast, judge slow: warn on a bad waiting reason immediately, but
	// only the elapsed timeout judges the node failed.
	if reason, stuck := driverPodBadWaitingReason(nodeState.DriverPod); stuck {
		recordNodeEvent(m.eventRecorder, nodeState.Node, corev1.EventTypeWarning,
			consts.RBLNEventReasonDriverUpgradePodStuck,
			fmt.Sprintf("Driver pod replacement is not progressing: %s", reason))
	}

	timedOut, err := m.handlePodRestartTimeout(ctx, nodeState, podRestartTimeoutSeconds)
	if timedOut || err != nil {
		return err
	}

	isPodSynced, isOrphaned, err := m.podInSyncWithDS(ctx, nodeState)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to compare pod and daemonset driver config digest")
		return err
	}
	if isOrphaned || !isPodSynced || driverPodTemplateOutdated(nodeState) {
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
		m.clearPodRestartClock(ctx, nodeState.Node)
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
	err = markNodeUpgradeFailed(ctx, m.nodeUpgradeStateProvider, nodeState.Node, UpgradeStatePodRestartRequired,
		fmt.Sprintf("driver pod crash-looping: %s", summarizeDriverPodBlockage(nodeState.DriverPod)))
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to change node upgrade state for node", "node", nodeState.Node.Name,
			"state", UpgradeStateFailed)
		return err
	}
	m.clearPodRestartClock(ctx, nodeState.Node)
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) handlePodRestartTimeout(
	ctx context.Context, nodeState *NodeUpgradeState, timeoutSeconds int64,
) (bool, error) {
	if timeoutSeconds <= 0 {
		return false, nil
	}
	node := nodeState.Node

	timedOut, err := checkAnnotationTimeout(ctx, m.nodeUpgradeStateProvider, node,
		UpgradePodRestartStartTimeAnnotationKey, timeoutSeconds)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to check pod-restart timeout; waiting",
			"node", node.Name, "annotation", UpgradePodRestartStartTimeAnnotationKey)
		return false, nil
	}
	if !timedOut {
		return false, nil
	}

	reason := fmt.Sprintf("pod-restart timeout after %ds: %s",
		timeoutSeconds, summarizeDriverPodBlockage(nodeState.DriverPod))
	log.FromContext(ctx).Error(fmt.Errorf("%s", reason),
		"Driver pod replacement timed out; marking upgrade failed", "node", node.Name)
	if err := markNodeUpgradeFailed(ctx, m.nodeUpgradeStateProvider, node,
		UpgradeStatePodRestartRequired, reason); err != nil {
		return false, err
	}
	m.clearPodRestartClock(ctx, node)
	return true, nil
}

// clearPodRestartClock removes the entry-time annotation when the node leaves
// the state, so a later upgrade cannot inherit a stale deadline.
func (m *ClusterUpgradeStateManagerImpl) clearPodRestartClock(ctx context.Context, node *corev1.Node) {
	if _, ok := node.Annotations[UpgradePodRestartStartTimeAnnotationKey]; !ok {
		return
	}
	if err := m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node,
		UpgradePodRestartStartTimeAnnotationKey); err != nil {
		log.FromContext(ctx).Info("Failed to remove pod-restart start-time annotation",
			"error", err, "node", node.Name)
	}
}

func driverPodBadWaitingReason(pod *corev1.Pod) (string, bool) {
	if pod == nil {
		return "", false
	}
	statuses := make([]corev1.ContainerStatus, 0,
		len(pod.Status.InitContainerStatuses)+len(pod.Status.ContainerStatuses))
	statuses = append(statuses, pod.Status.InitContainerStatuses...)
	statuses = append(statuses, pod.Status.ContainerStatuses...)
	for _, status := range statuses {
		if status.Ready || status.State.Waiting == nil {
			continue
		}
		if _, bad := badContainerWaitingReasons[status.State.Waiting.Reason]; bad {
			return fmt.Sprintf("container %q: %s", status.Name, status.State.Waiting.Reason), true
		}
	}
	return "", false
}

func summarizeDriverPodBlockage(pod *corev1.Pod) string {
	if pod == nil {
		return "driver pod not found"
	}
	if reason, ok := driverPodBadWaitingReason(pod); ok {
		return fmt.Sprintf("pod %q: %s", pod.Name, reason)
	}
	return fmt.Sprintf("pod %q in phase %s", pod.Name, pod.Status.Phase)
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

func (m *ClusterUpgradeStateManagerImpl) currentDriverConfigDigest(nodeState *NodeUpgradeState) (string, error) {
	if nodeState.IsOrphanedPod() {
		return "", nil
	}
	return m.podManager.GetDaemonSetDriverConfigDigest(nodeState.DriverDaemonSet)
}

// clearParkedBookkeeping drops the attempt's judgement artifacts.
func (m *ClusterUpgradeStateManagerImpl) clearParkedBookkeeping(ctx context.Context, node *corev1.Node) error {
	remove := map[string]any{}
	for _, key := range []string{
		UpgradeFailureReasonAnnotationKey,
		UpgradeFailureStepAnnotationKey,
		UpgradeSkipReasonAnnotationKey,
		UpgradeAttemptedRevisionAnnotationKey,
		UpgradePodRestartStartTimeAnnotationKey,
	} {
		if _, ok := node.Annotations[key]; ok {
			remove[key] = nil
		}
	}
	if len(remove) == 0 {
		return nil
	}
	return m.nodeUpgradeStateProvider.SetNodeUpgradeAnnotations(ctx, node, remove)
}

// wakeParkedNode leaves the upgrade-requested annotation in place for
// ProcessUpgradeRequiredNodes to consume — one instruction, one attempt.
func (m *ClusterUpgradeStateManagerImpl) wakeParkedNode(
	ctx context.Context, node *corev1.Node, cause string,
) error {
	if err := m.clearParkedBookkeeping(ctx, node); err != nil {
		return err
	}
	if err := m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, UpgradeStateUpgradeRequired); err != nil {
		return err
	}
	log.FromContext(ctx).Info("Parked node returned to the upgrade queue",
		"node", node.Name, "cause", cause)
	return nil
}

// newRevisionPushed stamps the current revision on first sight, so the node
// never wakes on the very revision it was parked under.
func (m *ClusterUpgradeStateManagerImpl) newRevisionPushed(
	ctx context.Context, nodeState *NodeUpgradeState,
) (bool, error) {
	currentRevision, err := m.currentDriverConfigDigest(nodeState)
	if err != nil {
		return false, err
	}
	if currentRevision == "" {
		// Orphaned pod: only the upgrade-requested annotation can wake this node.
		return false, nil
	}

	node := nodeState.Node
	recorded := node.Annotations[UpgradeAttemptedRevisionAnnotationKey]
	if recorded == "" {
		if err := m.nodeUpgradeStateProvider.SetNodeUpgradeAnnotation(ctx, node,
			UpgradeAttemptedRevisionAnnotationKey, currentRevision); err != nil {
			return false, err
		}
		return false, nil
	}
	return recorded != currentRevision, nil
}

func (m *ClusterUpgradeStateManagerImpl) ProcessUpgradeFailedNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState,
) error {
	log.FromContext(ctx).V(consts.VDebug).Info("ProcessUpgradeFailedNodes")

	var errs []error
	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateFailed] {
		if err := m.processUpgradeFailedNode(ctx, nodeState); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Self-heal is allowed only for pod-restart failures: the replacement pod
// becoming in-sync and Ready is recovery evidence there, while for validation
// failures a Ready pod is only the entry condition.
//
// A pod-restart failure is also retried when the pod template changes. The
// fix for a failure the init container caused (a bad k8s-driver-manager tag)
// leaves DRIVER_CONFIG_DIGEST alone, so newRevisionPushed never sees it, and
// kubelet cannot make the stuck pod Ready from its stale spec. The wake is
// self-limiting: the replacement pod carries the current template hash, so a
// node that fails again on the new template parks until the next change.
// That relies on a replacement appearing, so a pod already being deleted
// does not wake the node: pod-restart-required skips a Terminating pod, and
// waking on it would cycle failed -> wake -> timeout -> failed for as long
// as the pod lingers (kubelet down). Once it is gone, the replacement is
// judged on the current template like any other.
func (m *ClusterUpgradeStateManagerImpl) processUpgradeFailedNode(
	ctx context.Context, nodeState *NodeUpgradeState,
) error {
	node := nodeState.Node

	if node.Annotations[UpgradeFailureStepAnnotationKey] == UpgradeStatePodRestartRequired {
		driverPodInSync, err := m.isDriverPodInSync(ctx, nodeState)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to check if driver pod on the node is in sync", "node", node.Name)
			return err
		}
		if driverPodInSync {
			log.FromContext(ctx).Info("Driver pod recovered after pod-restart failure; resuming upgrade",
				"node", node.Name)
			if err := m.clearParkedBookkeeping(ctx, node); err != nil {
				return err
			}
			return m.updateNodeToUncordonOrDoneState(ctx, nodeState)
		}
		if !nodeState.IsOrphanedPod() && driverPodTemplateOutdated(nodeState) &&
			nodeState.DriverPod.DeletionTimestamp.IsZero() {
			return m.wakeParkedNode(ctx, node, "driver pod template updated")
		}
	}

	if m.IsUpgradeRequested(node) {
		return m.wakeParkedNode(ctx, node, "upgrade requested by operator")
	}

	pushed, err := m.newRevisionPushed(ctx, nodeState)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to compare driver revision for failed node", "node", node.Name)
		return err
	}
	if pushed {
		return m.wakeParkedNode(ctx, node, "new driver revision pushed")
	}
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) ProcessUpgradeSkippedNodes(
	ctx context.Context, currentClusterState *ClusterUpgradeState,
) error {
	log.FromContext(ctx).V(consts.VDebug).Info("ProcessUpgradeSkippedNodes")

	var errs []error
	for _, nodeState := range currentClusterState.NodeStates[UpgradeStateSkipped] {
		if err := m.processUpgradeSkippedNode(ctx, nodeState); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (m *ClusterUpgradeStateManagerImpl) processUpgradeSkippedNode(
	ctx context.Context, nodeState *NodeUpgradeState,
) error {
	node := nodeState.Node

	// The old driver is intact, so the node returns to service on it.
	_, wasInitiallyUnschedulable := node.Annotations[UpgradeInitialStateAnnotationKey]
	if IsNodeUnschedulable(node) && !wasInitiallyUnschedulable && !IsNodeInRequestorMode(node) {
		if err := m.cordonManager.Uncordon(ctx, node); err != nil {
			log.FromContext(ctx).Error(err, "Failed to uncordon skipped node", "node", node.Name)
			return err
		}
		node.Spec.Unschedulable = false
		log.FromContext(ctx).Info("Uncordoned skipped node; back in service on the old driver",
			"node", node.Name)
	}

	// Stamp the attempt revision on first sight so the node never wakes on
	// the very revision it was parked under.
	if node.Annotations[UpgradeAttemptedRevisionAnnotationKey] == "" {
		currentRevision, err := m.currentDriverConfigDigest(nodeState)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to resolve driver revision for skipped node", "node", node.Name)
			return err
		}
		if currentRevision != "" {
			if err := m.nodeUpgradeStateProvider.SetNodeUpgradeAnnotation(ctx, node,
				UpgradeAttemptedRevisionAnnotationKey, currentRevision); err != nil {
				return err
			}
		}
	}

	if m.IsUpgradeRequested(node) {
		return m.wakeParkedNode(ctx, node, "upgrade requested by operator")
	}

	pushed, err := m.newRevisionPushed(ctx, nodeState)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to compare driver revision for skipped node", "node", node.Name)
		return err
	}
	if pushed {
		return m.wakeParkedNode(ctx, node, "new driver revision pushed")
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
