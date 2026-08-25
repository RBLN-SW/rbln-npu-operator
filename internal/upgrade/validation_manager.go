package upgrade

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ValidationManagerInterface abstracts driver upgrade validation for testability.
type ValidationManagerInterface interface {
	Validate(ctx context.Context, node *corev1.Node) (bool, error)
}

type ValidationManager struct {
	k8sInterface             kubernetes.Interface
	nodeUpgradeStateProvider *NodeUpgradeStateProvider

	podSelector string
}

func NewValidationManager(
	k8sInterface kubernetes.Interface,
	nodeUpgradeStateProvider *NodeUpgradeStateProvider,
	podSelector string,
) *ValidationManager {
	mgr := &ValidationManager{
		k8sInterface:             k8sInterface,
		nodeUpgradeStateProvider: nodeUpgradeStateProvider,
		podSelector:              podSelector,
	}

	return mgr
}

func (m *ValidationManager) Validate(ctx context.Context, node *corev1.Node) (bool, error) {
	if m.podSelector == "" {
		return true, nil
	}

	listOptions := metav1.ListOptions{
		LabelSelector: m.podSelector,
		FieldSelector: fmt.Sprintf(nodeNameFieldSelectorFmt, node.Name),
	}
	podList, err := m.k8sInterface.CoreV1().Pods("").List(ctx, listOptions)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to list pods", "selector", m.podSelector, "node", node.Name)
		return false, err
	}

	if len(podList.Items) == 0 {
		log.FromContext(ctx).Info("No validation pods found on the node", "node", node.Name,
			"podSelector", m.podSelector)
		return false, nil
	}

	log.FromContext(ctx).Info("Found validation pods", "selector", m.podSelector, "node", node.Name,
		"pods", len(podList.Items))

	done := true
	for _, pod := range podList.Items {
		if !m.isPodReady(ctx, pod) {
			err = m.handleTimeout(ctx, node, DefaultValidationTimeoutSeconds)
			if err != nil {
				return false, fmt.Errorf("unable to handle timeout for validation state: %v", err)
			}
			done = false
			break
		}
	}

	if done {
		annotationKey := UpgradeValidationStartTimeAnnotationKey
		err = m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, annotationKey)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to remove annotation used to track validation completion",
				"node", node.Name, "annotation", annotationKey)
			return false, err
		}
	}
	return done, nil
}

func (m *ValidationManager) isPodReady(ctx context.Context, pod corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		log.FromContext(ctx).Info("Pod not Running", "pod", pod.Name, "podPhase", pod.Status.Phase)
		return false
	}
	if len(pod.Status.ContainerStatuses) == 0 {
		log.FromContext(ctx).Info("No containers running in pod", "pod", pod.Name)
		return false
	}

	for i := range pod.Status.ContainerStatuses {
		if !pod.Status.ContainerStatuses[i].Ready {
			log.FromContext(ctx).Info("Not all containers ready in pod", "pod", pod.Name)
			return false
		}
	}

	return true
}

func (m *ValidationManager) handleTimeout(ctx context.Context, node *corev1.Node, timeoutSeconds int64) error {
	annotationKey := UpgradeValidationStartTimeAnnotationKey

	timedOut, err := checkAnnotationTimeout(ctx, m.nodeUpgradeStateProvider, node, annotationKey, timeoutSeconds)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to check validation timeout",
			"node", node.Name, "annotation", annotationKey)
		return err
	}

	if timedOut {
		if stateErr := m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, UpgradeStateFailed); stateErr != nil {
			log.FromContext(ctx).Error(stateErr, "Failed to mark node as failed after validation timeout; will retry next cycle",
				"node", node.Name)
			return stateErr
		}
		log.FromContext(ctx).Info("Timeout exceeded for validation, updated the node state", "node", node.Name,
			"state", UpgradeStateFailed)
		err = m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, annotationKey)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to remove annotation used to track validation completion",
				"node", node.Name, "annotation", annotationKey)
			return err
		}
	}
	return nil
}
