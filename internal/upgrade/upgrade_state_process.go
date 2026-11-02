package upgrade

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/RBLN-SW/rbln-npu-operator/api/v1beta1"
	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
)

// Phase selection uses the immutable observation. A successful write updates
// the published status but cannot select the node for another phase this pass.
func visitNodes(ctx context.Context, state *ClusterUpgradeState, key string, visit func(context.Context, *NodeUpgradeState) error) error {
	var result error
	for _, n := range state.nodesAt(key) {
		if err := visit(ctx, n); err != nil {
			result = errors.Join(result, fmt.Errorf("node %s: %w", n.Node.Name, err))
		}
	}
	return result
}

func (m *ClusterUpgradeStateManagerImpl) podInSyncWithDS(_ context.Context, n *NodeUpgradeState) (bool, bool, error) {
	if n.IsOrphanedPod() {
		return false, true, nil
	}
	desired, err := m.podManager.GetDaemonSetDriverConfigDigest(n.DriverDaemonSet)
	if err != nil {
		return false, false, err
	}
	actual := m.podManager.GetPodDriverConfigDigest(n.DriverPod)
	return actual == desired, false, nil
}

// Admission compares the driver configuration digest. Once an attempt has
// started, a changed pod template also requires replacement so a retry can
// pick up fixes to init containers, volumes and scheduling configuration.
func driverPodTemplateOutdated(n *NodeUpgradeState) bool {
	if n.DriverPod == nil || n.DriverDaemonSet == nil {
		return false
	}
	expected := n.DriverDaemonSet.Spec.Template.Annotations[consts.DriverTemplateHashAnnotation]
	return n.DriverPod.Annotations[consts.DriverTemplateHashAnnotation] != expected
}

func (m *ClusterUpgradeStateManagerImpl) IsUpgradeRequested(n *corev1.Node) bool {
	return n.Annotations[UpgradeRequestedAnnotationKey] == trueString
}

func (m *ClusterUpgradeStateManagerImpl) SkipNodeUpgrade(n *corev1.Node) bool {
	return n.Labels[UpgradeSkipNodeLabelKey] == trueString
}

func (m *ClusterUpgradeStateManagerImpl) ProcessDoneOrUnknownNodes(ctx context.Context, s *ClusterUpgradeState, key string) error {
	return m.processDoneOrUnknownNodes(ctx, s, key)
}

func (m *ClusterUpgradeStateManagerImpl) ProcessUnknownNodes(ctx context.Context, s *ClusterUpgradeState) error {
	return m.processDoneOrUnknownNodes(ctx, s, UpgradeStateUnknown)
}

func (m *ClusterUpgradeStateManagerImpl) ProcessDoneNodes(ctx context.Context, s *ClusterUpgradeState) error {
	return m.processDoneOrUnknownNodes(ctx, s, UpgradeStateDone)
}

func (m *ClusterUpgradeStateManagerImpl) processDoneOrUnknownNodes(ctx context.Context, s *ClusterUpgradeState, key string) error {
	return visitNodes(ctx, s, key, func(ctx context.Context, n *NodeUpgradeState) error {
		synced, orphan, err := m.podInSyncWithDS(ctx, n)
		if err != nil {
			return err
		}
		observed := m.observeDriver(n, synced, orphan)
		observed.requested = m.IsUpgradeRequested(n.Node)
		observed.safeLoad = waitingForDriverLoad(n.Node)
		return m.executeNodePlan(ctx, n, decideNode(key, observed), nil)
	})
}

// A fresh attempt must not inherit the previous failure diagnosis or restart
// deadline. The administrator's request is consumed later, at admission.
var judgementAnnotations = []string{UpgradeFailureReasonAnnotationKey, UpgradeFailureStepAnnotationKey, UpgradeSkipReasonAnnotationKey, UpgradeAttemptedRevisionAnnotationKey, UpgradePodRestartStartTimeAnnotationKey}

func (m *ClusterUpgradeStateManagerImpl) clearParkedBookkeeping(ctx context.Context, n *corev1.Node) error {
	values := map[string]any{}
	for _, key := range judgementAnnotations {
		if _, exists := n.Annotations[key]; exists {
			values[key] = nil
		}
	}
	if len(values) == 0 {
		return nil
	}
	return m.nodeUpgradeStateProvider.SetNodeUpgradeAnnotations(ctx, n, values)
}

func (m *ClusterUpgradeStateManagerImpl) wakeParkedNode(ctx context.Context, n *corev1.Node, cause string) error {
	from := n.Labels[UpgradeStateLabelKey]
	err := m.nodeUpgradeStateProvider.mutate(ctx, n, &from, func(live *corev1.Node) bool {
		if live.Annotations == nil {
			live.Annotations = map[string]string{}
		}
		// Legacy ownership depends on the parked state. Preserve it with an
		// explicit claim before moving back to the admission queue.
		if live.Spec.Unschedulable && cordonOwnerOf(live) == cordonOwnedByLegacyOperator {
			live.Annotations[consts.DriverManagerCordonClaimAnnotation] = consts.OperatorCordonClaimValue
		}
		for _, key := range judgementAnnotations {
			delete(live.Annotations, key)
		}
		if live.Labels == nil {
			live.Labels = map[string]string{}
		}
		live.Labels[UpgradeStateLabelKey] = UpgradeStateUpgradeRequired
		return true
	})
	if err == nil && from != UpgradeStateUpgradeRequired && n.Labels[UpgradeStateLabelKey] == UpgradeStateUpgradeRequired {
		log.FromContext(ctx).Info("Node queued for a driver upgrade", "node", n.Name, "cause", cause)
	}
	return err
}

// Read persisted labels after departed/parked-node processing. A snapshot
// taken by BuildState can still include slots those phases have just freed.
// Failed nodes keep their slot until recovery; skipped nodes release theirs.
func (m *ClusterUpgradeStateManagerImpl) countActiveUpgradeNodes(ctx context.Context) (int, error) {
	nodes := &corev1.NodeList{}
	if err := m.k8sClient.List(ctx, nodes, client.HasLabels{UpgradeStateLabelKey}); err != nil {
		return 0, err
	}
	count := 0
	for _, n := range nodes.Items {
		state := n.Labels[UpgradeStateLabelKey]
		if IsInProgressUpgradeState(state) {
			count++
		}
	}
	return count, nil
}

func (m *ClusterUpgradeStateManagerImpl) ProcessUpgradeRequiredNodes(ctx context.Context, s *ClusterUpgradeState, policy *v1beta1.DriverUpgradePolicySpec) error {
	active, err := m.countActiveUpgradeNodes(ctx)
	if err != nil {
		return err
	}
	limit := 0
	if policy != nil {
		limit = policy.MaxParallelUpgrades
	}
	log.FromContext(ctx).V(consts.VDebug).Info("Driver upgrade admission capacity",
		"activeNodes", active, "maxParallelUpgrades", limit, "queuedNodes", len(s.nodesAt(UpgradeStateUpgradeRequired)))
	return visitNodes(ctx, s, UpgradeStateUpgradeRequired, func(ctx context.Context, n *NodeUpgradeState) error {
		from := UpgradeStateUpgradeRequired
		before := n.Node.Labels[UpgradeStateLabelKey]
		if err := m.nodeUpgradeStateProvider.mutate(ctx, n.Node, &from, func(live *corev1.Node) bool {
			requested := m.IsUpgradeRequested(live)
			if requested {
				delete(live.Annotations, UpgradeRequestedAnnotationKey)
			}
			// An existing cordon does not exempt a node from the budget: its
			// workloads can still be running, or the cordon can be a failure's.
			if m.SkipNodeUpgrade(live) || (limit > 0 && active >= limit) {
				return requested
			}
			live.Labels[UpgradeStateLabelKey] = UpgradeStateCordonRequired
			return true
		}); err != nil {
			return err
		}
		if before == UpgradeStateUpgradeRequired && n.Node.Labels[UpgradeStateLabelKey] == UpgradeStateCordonRequired {
			active++
		}
		return nil
	})
}

// Judge ownership at the cordon step, not while waiting for an admission
// slot. An administrator may change scheduling in between those two steps.
func (m *ClusterUpgradeStateManagerImpl) recordCordonProvenance(ctx context.Context, n *corev1.Node, foreign bool) error {
	var value any
	if foreign {
		value = trueString
	}
	return m.nodeUpgradeStateProvider.SetNodeUpgradeAnnotations(ctx, n, map[string]any{UpgradeInitialStateAnnotationKey: value})
}

func (m *ClusterUpgradeStateManagerImpl) ProcessCordonRequiredNodes(ctx context.Context, s *ClusterUpgradeState) error {
	return visitNodes(ctx, s, UpgradeStateCordonRequired, func(ctx context.Context, n *NodeUpgradeState) error {
		foreign, err := m.cordonManager.Cordon(ctx, n.Node)
		if err != nil {
			return err
		}
		if err := m.recordCordonProvenance(ctx, n.Node, foreign); err != nil {
			return err
		}
		return m.nodeUpgradeStateProvider.ChangeNodeUpgradeStateFrom(ctx, n.Node, UpgradeStateCordonRequired, UpgradeStateWaitForJobsRequired)
	})
}

func nodesForStage(s *ClusterUpgradeState, key string) []*corev1.Node {
	nodes := s.nodesAt(key)
	result := make([]*corev1.Node, 0, len(nodes))
	for _, n := range nodes {
		result = append(result, n.Node)
	}
	return result
}

func (m *ClusterUpgradeStateManagerImpl) ProcessWaitForJobsRequiredNodes(ctx context.Context, s *ClusterUpgradeState, spec *v1beta1.WaitForCompletionSpec) error {
	nodes := nodesForStage(s, UpgradeStateWaitForJobsRequired)
	if len(nodes) == 0 {
		return nil
	}
	if spec == nil || spec.PodSelector == "" {
		return visitNodes(ctx, s, UpgradeStateWaitForJobsRequired, func(ctx context.Context, n *NodeUpgradeState) error {
			return m.nodeUpgradeStateProvider.ChangeNodeUpgradeStateFrom(ctx, n.Node, UpgradeStateWaitForJobsRequired, UpgradeStatePodDeletionRequired)
		})
	}
	return m.podManager.ScheduleCheckOnPodCompletion(ctx, &PodManagerConfig{Nodes: nodes, WaitForCompletionSpec: spec})
}

func (m *ClusterUpgradeStateManagerImpl) ProcessPodDeletionRequiredNodes(ctx context.Context, s *ClusterUpgradeState, spec *v1beta1.PodDeletionSpec, deviceClass string) error {
	nodes := nodesForStage(s, UpgradeStatePodDeletionRequired)
	if len(nodes) == 0 {
		return nil
	}
	if !m.IsPodDeletionEnabled() {
		return visitNodes(ctx, s, UpgradeStatePodDeletionRequired, func(ctx context.Context, n *NodeUpgradeState) error {
			return m.nodeUpgradeStateProvider.ChangeNodeUpgradeStateFrom(ctx, n.Node, UpgradeStatePodDeletionRequired, UpgradeStatePodRestartRequired)
		})
	}
	return m.podManager.SchedulePodEviction(ctx, &PodManagerConfig{Nodes: nodes, DeletionSpec: spec, NPUDeviceClass: deviceClass})
}

func driverPodReady(p *corev1.Pod) bool {
	if p == nil || p.Status.Phase != corev1.PodRunning {
		return false
	}
	if len(p.Status.ContainerStatuses) == 0 {
		return false
	}
	for _, status := range p.Status.ContainerStatuses {
		if !status.Ready {
			return false
		}
	}
	return true
}

func (m *ClusterUpgradeStateManagerImpl) isDriverPodInSync(ctx context.Context, n *NodeUpgradeState) (bool, error) {
	same, _, err := m.podInSyncWithDS(ctx, n)
	return same && !driverPodTemplateOutdated(n) && driverPodReady(n.DriverPod), err
}

func (m *ClusterUpgradeStateManagerImpl) updateNodeToUncordonOrDoneState(ctx context.Context, n *NodeUpgradeState) error {
	return m.executeNodePlan(ctx, n, completionPlan(schedulingObservation(n.Node)), nil)
}

func driverPodBadWaitingReason(p *corev1.Pod) (string, bool) {
	if p == nil {
		return "", false
	}
	for _, statuses := range [][]corev1.ContainerStatus{p.Status.InitContainerStatuses, p.Status.ContainerStatuses} {
		for _, s := range statuses {
			if !s.Ready && s.State.Waiting != nil && badContainerWaitingReasons[s.State.Waiting.Reason] {
				return fmt.Sprintf("container %q: %s", s.Name, s.State.Waiting.Reason), true
			}
		}
	}
	return "", false
}

func summarizeDriverPodBlockage(p *corev1.Pod) string {
	if p == nil {
		return "driver pod not found"
	}
	if reason, bad := driverPodBadWaitingReason(p); bad {
		return fmt.Sprintf("pod %q: %s", p.Name, reason)
	}
	return fmt.Sprintf("pod %q in phase %s", p.Name, p.Status.Phase)
}

func (m *ClusterUpgradeStateManagerImpl) isDriverPodFailing(p *corev1.Pod) bool {
	if p == nil {
		return false
	}
	for _, statuses := range [][]corev1.ContainerStatus{p.Status.InitContainerStatuses, p.Status.ContainerStatuses} {
		for _, s := range statuses {
			if !s.Ready && s.RestartCount > MaxPodRestartCount {
				return true
			}
		}
	}
	return false
}

func (m *ClusterUpgradeStateManagerImpl) clearPodRestartClock(ctx context.Context, n *corev1.Node) {
	if _, exists := n.Annotations[UpgradePodRestartStartTimeAnnotationKey]; !exists {
		return
	}
	if err := m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, n, UpgradePodRestartStartTimeAnnotationKey); err != nil {
		log.FromContext(ctx).Error(err, "Cannot clear restart deadline", "node", n.Name)
	}
}

func (m *ClusterUpgradeStateManagerImpl) handlePodRestartTimeout(ctx context.Context, n *NodeUpgradeState, seconds int64) (bool, error) {
	if seconds <= 0 {
		return false, nil
	}
	expired, err := checkAnnotationTimeout(ctx, m.nodeUpgradeStateProvider, n.Node, UpgradePodRestartStartTimeAnnotationKey, seconds)
	if err != nil {
		log.FromContext(ctx).Error(err, "Cannot read restart deadline", "node", n.Node.Name)
		return false, nil
	}
	if !expired {
		return false, nil
	}
	reason := fmt.Sprintf("pod-restart timeout after %ds: %s", seconds, summarizeDriverPodBlockage(n.DriverPod))
	if err := markNodeUpgradeFailed(ctx, m.nodeUpgradeStateProvider, n.Node, UpgradeStatePodRestartRequired, reason); err != nil {
		return false, err
	}
	m.clearPodRestartClock(ctx, n.Node)
	return true, nil
}

func (m *ClusterUpgradeStateManagerImpl) ProcessPodRestartNodes(ctx context.Context, s *ClusterUpgradeState, seconds int64) error {
	var restart []*corev1.Pod
	err := visitNodes(ctx, s, UpgradeStatePodRestartRequired, func(ctx context.Context, n *NodeUpgradeState) error {
		return m.processPodRestartNode(ctx, n, seconds, &restart)
	})
	if len(restart) > 0 {
		err = errors.Join(err, m.podManager.SchedulePodsRestart(ctx, restart))
	}
	return err
}

func (m *ClusterUpgradeStateManagerImpl) processPodRestartNode(ctx context.Context, n *NodeUpgradeState, seconds int64, restart *[]*corev1.Pod) error {
	if reason, bad := driverPodBadWaitingReason(n.DriverPod); bad {
		recordNodeEvent(m.eventRecorder, n.Node, corev1.EventTypeWarning,
			consts.RBLNEventReasonDriverUpgradePodStuck,
			"Driver pod replacement is not progressing: "+reason)
	}
	// The deadline applies to the whole replacement phase, including an old
	// pod that has not been deleted yet. No replacement is started on expiry.
	if expired, err := m.handlePodRestartTimeout(ctx, n, seconds); expired || err != nil {
		return err
	}
	same, orphan, err := m.podInSyncWithDS(ctx, n)
	if err != nil {
		return err
	}
	observed := m.observeDriver(n, same, orphan)
	return m.executeNodePlan(ctx, n, decideNode(UpgradeStatePodRestartRequired, observed), restart)
}

func (m *ClusterUpgradeStateManagerImpl) currentDriverConfigDigest(n *NodeUpgradeState) (string, error) {
	if n.DriverDaemonSet == nil {
		return "", nil
	}
	return m.podManager.GetDaemonSetDriverConfigDigest(n.DriverDaemonSet)
}

// The first observation records the attempted revision without waking the
// node. Only a later revision change or an explicit request retries it.
func (m *ClusterUpgradeStateManagerImpl) newRevisionPushed(ctx context.Context, n *NodeUpgradeState) (bool, error) {
	desired, err := m.currentDriverConfigDigest(n)
	if err != nil || desired == "" {
		return false, err
	}
	previous := n.Node.Annotations[UpgradeAttemptedRevisionAnnotationKey]
	if previous == "" {
		return false, m.nodeUpgradeStateProvider.SetNodeUpgradeAnnotation(ctx, n.Node, UpgradeAttemptedRevisionAnnotationKey, desired)
	}
	return desired != previous, nil
}

func (m *ClusterUpgradeStateManagerImpl) ProcessUpgradeFailedNodes(ctx context.Context, s *ClusterUpgradeState) error {
	return visitNodes(ctx, s, UpgradeStateFailed, m.processUpgradeFailedNode)
}

func (m *ClusterUpgradeStateManagerImpl) processUpgradeFailedNode(ctx context.Context, n *NodeUpgradeState) error {
	// Recovery evidence takes precedence over another admission. A pod that
	// has already recovered must not re-enter eviction just because a retry
	// request or an older attempted-revision annotation is still present.
	if n.Node.Annotations[UpgradeFailureStepAnnotationKey] == UpgradeStatePodRestartRequired {
		if handled, err := m.recoverRestartFailure(ctx, n); handled || err != nil {
			return err
		}
	}
	if m.IsUpgradeRequested(n.Node) {
		return m.wakeParkedNode(ctx, n.Node, "manual retry")
	}
	changed, err := m.newRevisionPushed(ctx, n)
	if err != nil {
		return err
	}
	if changed {
		return m.wakeParkedNode(ctx, n.Node, "new driver revision")
	}
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) recoverRestartFailure(ctx context.Context, n *NodeUpgradeState) (bool, error) {
	// Readiness is recovery evidence only for a replacement failure. A
	// validation failure already had a Ready driver and needs another retry.
	ready, err := m.isDriverPodInSync(ctx, n)
	if err != nil {
		return false, err
	}
	if ready {
		if err := m.clearParkedBookkeeping(ctx, n.Node); err != nil {
			return false, err
		}
		return true, m.updateNodeToUncordonOrDoneState(ctx, n)
	}
	if n.DriverPod != nil && n.DriverPod.DeletionTimestamp == nil && driverPodTemplateOutdated(n) {
		// A terminating old pod cannot apply the new template yet. Waking
		// on it would repeat the timeout/retry cycle while kubelet is down.
		return true, m.wakeParkedNode(ctx, n.Node, "new pod template")
	}
	return false, nil
}

func (m *ClusterUpgradeStateManagerImpl) ProcessUpgradeSkippedNodes(ctx context.Context, s *ClusterUpgradeState) error {
	return visitNodes(ctx, s, UpgradeStateSkipped, m.processUpgradeSkippedNode)
}

func (m *ClusterUpgradeStateManagerImpl) processUpgradeSkippedNode(ctx context.Context, n *NodeUpgradeState) error {
	// An unsuccessful eviction leaves the old driver usable. Release our
	// cordon before a retry waits for a new admission slot.
	if n.Node.Spec.Unschedulable && claimedCordon(n.Node) && !IsNodeInRequestorMode(n.Node) {
		if err := m.cordonManager.Uncordon(ctx, n.Node); err != nil {
			return err
		}
		n.Node.Spec.Unschedulable = false
	}
	if m.IsUpgradeRequested(n.Node) {
		return m.wakeParkedNode(ctx, n.Node, "manual retry")
	}
	changed, err := m.newRevisionPushed(ctx, n)
	if err != nil {
		return err
	}
	if changed {
		return m.wakeParkedNode(ctx, n.Node, "new driver revision")
	}
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) ProcessValidationRequiredNodes(ctx context.Context, s *ClusterUpgradeState) error {
	return visitNodes(ctx, s, UpgradeStateValidationRequired, func(ctx context.Context, n *NodeUpgradeState) error {
		if err := m.unblockDriverLoad(ctx, n.Node); err != nil {
			return err
		}
		done, err := m.validationManager.Validate(ctx, n.Node)
		if err != nil || !done {
			return err
		}
		return m.updateNodeToUncordonOrDoneState(ctx, n)
	})
}

func (m *ClusterUpgradeStateManagerImpl) ProcessUncordonRequiredNodes(ctx context.Context, s *ClusterUpgradeState) error {
	return visitNodes(ctx, s, UpgradeStateUncordonRequired, func(ctx context.Context, n *NodeUpgradeState) error {
		return m.executeNodePlan(ctx, n, decideNode(UpgradeStateUncordonRequired, schedulingObservation(n.Node)), nil)
	})
}

// Leaving the driver scope releases even a failed node: its next workload no
// longer depends on this driver. Disabling autoUpgrade instead preserves a
// failed node's isolation. Both paths retain foreign and requestor cordons.
func (m *ClusterUpgradeStateManagerImpl) ProcessDepartedNodes(ctx context.Context, s *ClusterUpgradeState) error {
	var result error
	for _, node := range s.DepartedNodes {
		err := m.nodeUpgradeStateProvider.mutate(ctx, node, nil, func(live *corev1.Node) bool {
			if live.Labels[consts.RBLNDeployDriverLabelKey] == trueString {
				return false
			}
			release := !IsNodeInRequestorMode(live) && OwnsCordon(live)
			_, changed := live.Labels[UpgradeStateLabelKey]
			delete(live.Labels, UpgradeStateLabelKey)
			for _, key := range teardownAnnotationKeys {
				if _, present := live.Annotations[key]; present {
					delete(live.Annotations, key)
					changed = true
				}
			}
			if release {
				live.Spec.Unschedulable = false
				delete(live.Annotations, consts.DriverManagerCordonClaimAnnotation)
				delete(live.Annotations, consts.DriverManagerEvictionBlockedAnnotation)
				changed = true
			}
			// A requestor keeps its claim until it releases scheduling. It can
			// be discovered again without leaving any bookkeeping to remove.
			return changed
		})
		if err != nil {
			result = errors.Join(result, fmt.Errorf("node %s: %w", node.Name, err))
		}
	}
	return result
}
