package upgrade

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	validationTimeoutSeconds = 600
)

type ValidationManager struct {
	k8sInterface             kubernetes.Interface
	Log                      logr.Logger
	nodeUpgradeStateProvider *NodeUpgradeStateProvider

	podSelector string
}

func NewValidationManager(
	k8sInterface kubernetes.Interface,
	log logr.Logger,
	nodeUpgradeStateProvider *NodeUpgradeStateProvider,
	podSelector string,
) *ValidationManager {
	mgr := &ValidationManager{
		k8sInterface:             k8sInterface,
		Log:                      log,
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
		m.Log.Error(err, "Failed to list pods", "selector", m.podSelector, "node", node.Name)
		return false, err
	}

	if len(podList.Items) == 0 {
		m.Log.Info("No validation pods found on the node", "node", node.Name,
			"podSelector", m.podSelector)
		return false, nil
	}

	m.Log.Info("Found validation pods", "selector", m.podSelector, "node", node.Name,
		"pods", len(podList.Items))

	done := true
	for _, pod := range podList.Items {
		if !m.isPodReady(pod) {
			err = m.handleTimeout(ctx, node, int64(validationTimeoutSeconds))
			if err != nil {
				return false, fmt.Errorf("unable to handle timeout for validation state: %v", err)
			}
			done = false
			break
		}
		annotationKey := UpgradeValidationStartTimeAnnotationKey
		err = m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, annotationKey)
		if err != nil {
			m.Log.Error(err, "Failed to remove annotation used to track validation completion",
				"node", node.Name, "annotation", annotationKey)
			return done, err
		}
	}
	return done, nil
}

func (m *ValidationManager) isPodReady(pod corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		m.Log.Info("Pod not Running", "pod", pod.Name, "podPhase", pod.Status.Phase)
		return false
	}
	if len(pod.Status.ContainerStatuses) == 0 {
		m.Log.Info("No containers running in pod", "pod", pod.Name)
		return false
	}

	for i := range pod.Status.ContainerStatuses {
		if !pod.Status.ContainerStatuses[i].Ready {
			m.Log.Info("Not all containers ready in pod", "pod", pod.Name)
			return false
		}
	}

	return true
}

func (m *ValidationManager) handleTimeout(ctx context.Context, node *corev1.Node, timeoutSeconds int64) error {
	annotationKey := UpgradeValidationStartTimeAnnotationKey
	currentTime := time.Now().Unix()
	if _, present := node.Annotations[annotationKey]; !present {
		err := m.nodeUpgradeStateProvider.SetNodeUpgradeAnnotation(ctx, node, annotationKey,
			strconv.FormatInt(currentTime, 10))
		if err != nil {
			m.Log.Error(err, "Failed to add annotation to track validation completion",
				"node", node.Name, "annotation", annotationKey)
			return err
		}
		return nil
	}
	startTime, err := strconv.ParseInt(node.Annotations[annotationKey], 10, 64)
	if err != nil {
		m.Log.Error(err, "Failed to convert start time to track validation completion",
			"node", node.Name)
		return err
	}
	if currentTime > startTime+timeoutSeconds {
		_ = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, UpgradeStateFailed)
		m.Log.Info("Timeout exceeded for validation, updated the node state", "node", node.Name,
			"state", UpgradeStateFailed)
		err = m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, annotationKey)
		if err != nil {
			m.Log.Error(err, "Failed to remove annotation used to track validation completion",
				"node", node.Name, "annotation", annotationKey)
			return err
		}
	}
	return nil
}
