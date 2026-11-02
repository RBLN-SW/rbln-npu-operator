package upgrade

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type ValidationManagerInterface interface {
	Validate(context.Context, *corev1.Node) (bool, error)
}
type ValidationManager struct {
	k8sInterface             kubernetes.Interface
	nodeUpgradeStateProvider *NodeUpgradeStateProvider
	podSelector              string
}

func NewValidationManager(k kubernetes.Interface, p *NodeUpgradeStateProvider, selector string) *ValidationManager {
	return &ValidationManager{k8sInterface: k, nodeUpgradeStateProvider: p, podSelector: selector}
}

func (m *ValidationManager) isPodReady(_ context.Context, p corev1.Pod) bool {
	return driverPodReady(&p)
}

func (m *ValidationManager) Validate(ctx context.Context, n *corev1.Node) (bool, error) {
	if m.podSelector == "" {
		return true, nil
	}
	pods, err := m.k8sInterface.CoreV1().Pods("").List(ctx, metav1.ListOptions{LabelSelector: m.podSelector, FieldSelector: fmt.Sprintf(nodeNameFieldSelectorFmt, n.Name)})
	if err != nil {
		return false, err
	}
	count, allReady := 0, true
	for _, p := range pods.Items {
		if p.Spec.NodeName != n.Name {
			continue
		}
		count++
		allReady = allReady && m.isPodReady(ctx, p)
	}
	if count > 0 && allReady {
		return true, m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, n, UpgradeValidationStartTimeAnnotationKey)
	}
	return false, m.handleTimeout(ctx, n, DefaultValidationTimeoutSeconds)
}

func (m *ValidationManager) handleTimeout(ctx context.Context, n *corev1.Node, seconds int64) error {
	expired, err := checkAnnotationTimeout(ctx, m.nodeUpgradeStateProvider, n, UpgradeValidationStartTimeAnnotationKey, seconds)
	if err != nil || !expired {
		return err
	}
	if err := markNodeUpgradeFailed(ctx, m.nodeUpgradeStateProvider, n, UpgradeStateValidationRequired, fmt.Sprintf("driver validation timed out after %d seconds", seconds)); err != nil {
		return err
	}
	return m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, n, UpgradeValidationStartTimeAnnotationKey)
}
