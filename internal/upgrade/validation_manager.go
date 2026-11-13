package upgrade

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
)

// ValidationOptions is the current policy's workload context for this pass.
// A deploy label alone cannot tell which of the two VM plugins is enabled.
type ValidationOptions struct {
	Namespace                  string
	DefaultWorkload            string
	DRAEnabled                 bool
	SandboxEnabled             bool
	DeviceConfigManagerEnabled bool
}

type ValidationManagerInterface interface {
	Validate(context.Context, *corev1.Node, ValidationOptions) (bool, error)
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
	return p.DeletionTimestamp == nil && driverPodReady(&p)
}

func (m *ValidationManager) Validate(ctx context.Context, n *corev1.Node, options ValidationOptions) (bool, error) {
	if options.DeviceConfigManagerEnabled && !partitionTransitionComplete(n) {
		return false, m.handleTimeout(ctx, n, DefaultValidationTimeoutSeconds)
	}
	if m.podSelector == "" {
		return true, nil
	}
	selector, err := options.podSelector(n, m.podSelector)
	if err != nil {
		return false, err
	}
	pods, err := m.k8sInterface.CoreV1().Pods(options.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, FieldSelector: fmt.Sprintf(nodeNameFieldSelectorFmt, n.Name)})
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

func partitionTransitionComplete(node *corev1.Node) bool {
	if node.Annotations["rebellions.ai/npu.partition.transition-pfs"] != "" || node.Annotations["rebellions.ai/npu.partition.cordoned-by"] != "" {
		return false
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == "NPUPartitionReady" {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (o ValidationOptions) podSelector(node *corev1.Node, containerSelector string) (string, error) {
	workload := node.Labels[consts.RBLNWorkloadConfigLabelKey]
	if workload != consts.RBLNWorkloadConfigContainer && workload != consts.RBLNWorkloadConfigVMPassthrough {
		workload = o.DefaultWorkload
	}
	if workload != consts.RBLNWorkloadConfigVMPassthrough {
		return containerSelector, nil
	}
	if o.DRAEnabled == o.SandboxEnabled {
		return "", fmt.Errorf("VM upgrade validation requires exactly one enabled DRA or sandbox device plugin")
	}
	component := consts.RBLNSandboxDevicePluginName
	if o.DRAEnabled {
		component = consts.RBLNDRAKubeletPluginName
	}
	return labels.SelectorFromSet(labels.Set{
		"app.kubernetes.io/name":      consts.OperatorName,
		"app.kubernetes.io/component": component,
	}).String(), nil
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
