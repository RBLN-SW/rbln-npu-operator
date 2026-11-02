package upgrade

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

func schedulingObservation(node *corev1.Node) nodeObservation {
	return nodeObservation{
		cordoned:  node.Spec.Unschedulable,
		claimed:   claimedCordon(node),
		requestor: IsNodeInRequestorMode(node),
	}
}

func (m *ClusterUpgradeStateManagerImpl) observeDriver(n *NodeUpgradeState, synced, orphan bool) nodeObservation {
	observed := schedulingObservation(n.Node)
	observed.validationEnabled = m.IsValidationEnabled()
	observed.driver = driverObservation{
		managed:         !orphan,
		configMatches:   synced,
		templateMatches: !driverPodTemplateOutdated(n),
		present:         n.DriverPod != nil,
		terminating:     n.DriverPod != nil && n.DriverPod.DeletionTimestamp != nil,
		ready:           driverPodReady(n.DriverPod),
		failing:         m.isDriverPodFailing(n.DriverPod),
	}
	return observed
}

func (m *ClusterUpgradeStateManagerImpl) executeNodePlan(ctx context.Context, n *NodeUpgradeState, plan nodePlan, replacements *[]*corev1.Pod) error {
	for _, intent := range plan {
		if err := m.executeNodeIntent(ctx, n, intent, replacements); err != nil {
			return err
		}
	}
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) executeNodeIntent(ctx context.Context, n *NodeUpgradeState, intent nodeIntent, replacements *[]*corev1.Pod) error {
	switch intent.operation {
	case enqueueAttempt:
		return m.wakeParkedNode(ctx, n.Node, intent.cause)
	case releaseScheduling:
		return m.cordonManager.Uncordon(ctx, n.Node)
	case publishNodeState:
		return m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, n.Node, intent.state)
	case acknowledgeUncordon:
		return m.nodeUpgradeStateProvider.ChangeNodeUpgradeStateFrom(ctx, n.Node, UpgradeStateUncordonRequired, UpgradeStateDone)
	case clearInitialCordon:
		return m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, n.Node, UpgradeInitialStateAnnotationKey)
	case allowDriverLoad:
		return m.unblockDriverLoad(ctx, n.Node)
	case replaceDriverPod:
		if replacements == nil {
			return fmt.Errorf("replacement queue is missing for node %s", n.Node.Name)
		}
		*replacements = append(*replacements, n.DriverPod)
	case clearRestartDeadline:
		// Deadline cleanup has always been best-effort after readiness or a
		// recorded failure; it must not hold up the following state write.
		m.clearPodRestartClock(ctx, n.Node)
	case recordRestartFailure:
		return markNodeUpgradeFailed(ctx, m.nodeUpgradeStateProvider, n.Node,
			UpgradeStatePodRestartRequired, "driver pod crash-looping: "+summarizeDriverPodBlockage(n.DriverPod))
	case finishAttempt:
		// Earlier writes refresh node metadata. In particular, releasing the
		// safe-load gate may reveal a new cordon claim; finish against that
		// observation rather than the one preceding the gate write.
		return m.updateNodeToUncordonOrDoneState(ctx, n)
	default:
		return fmt.Errorf("unsupported node operation %d", intent.operation)
	}
	return nil
}
