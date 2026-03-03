package upgrade

import (
	"context"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

type SafeDriverLoadManager struct {
	log                      logr.Logger
	nodeUpgradeStateProvider *NodeUpgradeStateProvider
}

func (s *SafeDriverLoadManager) IsWaitingForSafeDriverLoad(_ context.Context, node *corev1.Node) bool {
	if node == nil {
		return false
	}
	return node.Annotations[UpgradeWaitForSafeDriverLoadAnnotationKey] != ""
}

func NewSafeDriverLoadManager(
	nodeUpgradeStateProvider *NodeUpgradeStateProvider, log logr.Logger,
) *SafeDriverLoadManager {
	mgr := &SafeDriverLoadManager{
		log:                      log,
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
		s.log.V(consts.LogLevelError).Error(
			err, "Failed to change node upgrade annotation for node", "node",
			node, "annotation", annotationKey)
		return err
	}
	return nil
}
