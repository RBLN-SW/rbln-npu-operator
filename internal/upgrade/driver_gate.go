package upgrade

import (
	"context"

	corev1 "k8s.io/api/core/v1"
)

// Any nonempty value means the driver is waiting, including values used by
// older integrations. Removing this annotation is the protocol acknowledgement.
func waitingForDriverLoad(node *corev1.Node) bool {
	return node != nil && node.Annotations[UpgradeWaitForSafeDriverLoadAnnotationKey] != ""
}

func (m *ClusterUpgradeStateManagerImpl) unblockDriverLoad(ctx context.Context, node *corev1.Node) error {
	if !waitingForDriverLoad(node) {
		return nil
	}
	return m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, UpgradeWaitForSafeDriverLoadAnnotationKey)
}
