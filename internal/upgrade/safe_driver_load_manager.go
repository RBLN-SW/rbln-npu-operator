package upgrade

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// SafeDriverLoadManagerInterface abstracts safe driver load operations for testability.
type SafeDriverLoadManagerInterface interface {
	IsWaitingForSafeDriverLoad(ctx context.Context, node *corev1.Node) bool
	UnblockLoading(ctx context.Context, node *corev1.Node) error
}

type SafeDriverLoadManager struct {
	nodeUpgradeStateProvider *NodeUpgradeStateProvider
}

func (s *SafeDriverLoadManager) IsWaitingForSafeDriverLoad(_ context.Context, node *corev1.Node) bool {
	if node == nil {
		return false
	}
	return node.Annotations[UpgradeWaitForSafeDriverLoadAnnotationKey] != ""
}

func NewSafeDriverLoadManager(
	nodeUpgradeStateProvider *NodeUpgradeStateProvider,
) *SafeDriverLoadManager {
	mgr := &SafeDriverLoadManager{
		nodeUpgradeStateProvider: nodeUpgradeStateProvider,
	}
	return mgr
}

func (s *SafeDriverLoadManager) UnblockLoading(ctx context.Context, node *corev1.Node) error {
	annotationKey := UpgradeWaitForSafeDriverLoadAnnotationKey
	if node.Annotations[annotationKey] == "" {
		return nil
	}
	err := s.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, annotationKey)
	if err != nil {
		log.FromContext(ctx).Error(
			err, "Failed to change node upgrade annotation for node", "node",
			node, "annotation", annotationKey)
		return err
	}
	return nil
}
