package upgrade

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

type CordonManager struct {
	k8sInterface kubernetes.Interface
	Log          logr.Logger
}

func (m *CordonManager) Cordon(ctx context.Context, node *corev1.Node) error {
	return m.setUnschedulable(ctx, node, true)
}

func (m *CordonManager) Uncordon(ctx context.Context, node *corev1.Node) error {
	return m.setUnschedulable(ctx, node, false)
}

func (m *CordonManager) setUnschedulable(ctx context.Context, node *corev1.Node, unschedulable bool) error {
	if node.Spec.Unschedulable == unschedulable {
		m.Log.Info("Node unschedulable state already set, skipping patch",
			"node", node.Name, "unschedulable", unschedulable)
		return nil
	}

	patch := fmt.Appendf(nil, `{"spec":{"unschedulable":%t}}`, unschedulable)
	_, err := m.k8sInterface.CoreV1().Nodes().Patch(
		ctx,
		node.Name,
		types.StrategicMergePatchType,
		patch,
		metav1.PatchOptions{},
	)
	if err != nil {
		if errors.IsNotFound(err) {
			m.Log.Info("Node not found", "node", node.Name)
		}
		m.Log.Error(err, "Failed to patch node unschedulable state", "node", node.Name, "unschedulable", unschedulable)
		return fmt.Errorf("failed to patch node unschedulable state for node %q: %w", node.Name, err)
	}

	return nil
}

func NewCordonManager(k8sInterface kubernetes.Interface, log logr.Logger) *CordonManager {
	return &CordonManager{
		k8sInterface: k8sInterface,
		Log:          log,
	}
}
