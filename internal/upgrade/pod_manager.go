package upgrade

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/drain"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// PodManagerInterface abstracts pod lifecycle operations for testability.
type PodManagerInterface interface {
	GetPodDriverConfigDigest(pod *corev1.Pod) string
	GetDaemonSetDriverConfigDigest(ds *appsv1.DaemonSet) (string, error)
	ScheduleCheckOnPodCompletion(ctx context.Context, config *PodManagerConfig) error
	SchedulePodEviction(ctx context.Context, config *PodManagerConfig) error
	SchedulePodsRestart(ctx context.Context, pods []*corev1.Pod) error
}

type PodManager struct {
	k8sInterface             kubernetes.Interface
	nodeUpgradeStateProvider *NodeUpgradeStateProvider
	podDeletionFilter        PodDeletionFilter
	nodesInProgress          *StringSet
}

type PodManagerConfig struct {
	Nodes                 []*corev1.Node
	DeletionSpec          *v1beta1.PodDeletionSpec
	WaitForCompletionSpec *v1beta1.WaitForCompletionSpec
	// NPUDeviceClass names the container-mode DRA DeviceClass whose claims
	// mark a pod as an NPU pod the eviction has to move (drivermanager.NPUDeviceClass).
	NPUDeviceClass string
}

type PodDeletionFilter func(corev1.Pod) bool

// changeNodeUpgradeStateAsync transitions the node upgrade state using a
// short-lived context derived from the parent so that the operation completes
// even when the parent reconcile context is close to expiring. Errors are
// logged and left for the next reconcile cycle to retry.
func (m *PodManager) changeNodeUpgradeStateAsync(ctx context.Context, node *corev1.Node, state string) {
	stateCtx, cancel := context.WithTimeout(ctx, 30*time.Second) //nolint:contextcheck // intentional short-lived timeout for goroutine state transition
	defer cancel()
	if err := m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(stateCtx, node, state); err != nil {
		log.FromContext(ctx).Error(err, "Failed to transition node state in goroutine; will retry next reconcile",
			"node", node.Name, "targetState", state)
	}
}

func NewPodManager(
	k8sInterface kubernetes.Interface,
	nodeUpgradeStateProvider *NodeUpgradeStateProvider,
	podDeletionFilter PodDeletionFilter,
) *PodManager {
	mgr := &PodManager{
		k8sInterface:             k8sInterface,
		nodeUpgradeStateProvider: nodeUpgradeStateProvider,
		podDeletionFilter:        podDeletionFilter,
		nodesInProgress:          NewStringSet(),
	}

	return mgr
}

// GetPodDriverConfigDigest returns the driver config digest the pod was
// rendered with. A pod predating the digest reads as "" — out of sync with any
// stamped DaemonSet — rather than as an error that would strand its node.
func (m *PodManager) GetPodDriverConfigDigest(pod *corev1.Pod) string {
	return driverConfigDigestOf(pod.Spec.InitContainers)
}

// GetDaemonSetDriverConfigDigest returns the digest the driver reconciler
// stamped into the DaemonSet's pod template. Every rendered driver DaemonSet
// carries one, so its absence is an operator bug rather than a legacy object.
func (m *PodManager) GetDaemonSetDriverConfigDigest(ds *appsv1.DaemonSet) (string, error) {
	digest := driverConfigDigestOf(ds.Spec.Template.Spec.InitContainers)
	if digest == "" {
		return "", fmt.Errorf("daemonset %s carries no %s in its pod template", ds.Name, consts.DriverConfigDigestEnv)
	}
	return digest, nil
}

func driverConfigDigestOf(initContainers []corev1.Container) string {
	for i := range initContainers {
		for _, env := range initContainers[i].Env {
			if env.Name == consts.DriverConfigDigestEnv {
				return env.Value
			}
		}
	}
	return ""
}

func (m *PodManager) ListPods(ctx context.Context, selector string, nodeName string) (*corev1.PodList, error) {
	listOptions := metav1.ListOptions{
		LabelSelector: selector,
		FieldSelector: fmt.Sprintf(nodeNameFieldSelectorFmt, nodeName),
	}
	podList, err := m.k8sInterface.CoreV1().Pods("").List(ctx, listOptions)
	if err != nil {
		return nil, err
	}
	return podList, nil
}

func (m *PodManager) IsPodRunningOrPending(ctx context.Context, pod corev1.Pod) bool {
	log.FromContext(ctx).Info("Pod status", "pod", pod.Name, "node", pod.Spec.NodeName,
		"state", pod.Status.Phase)
	return podRunningOrPending(&pod)
}

func (m *PodManager) HandleTimeoutOnPodCompletions(ctx context.Context, node *corev1.Node,
	timeoutSeconds int64,
) error {
	annotationKey := UpgradeWaitForPodCompletionStartTimeAnnotationKey

	timedOut, err := checkAnnotationTimeout(ctx, m.nodeUpgradeStateProvider, node, annotationKey, timeoutSeconds)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to check pod completion timeout",
			"node", node.Name, "annotation", annotationKey)
		return err
	}

	if timedOut {
		if stateErr := m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, UpgradeStatePodDeletionRequired); stateErr != nil {
			log.FromContext(ctx).Error(stateErr, "Failed to change node state after pod completion timeout; will retry next cycle",
				"node", node.Name, "state", UpgradeStatePodDeletionRequired)
			return stateErr
		}
		log.FromContext(ctx).Info("Timeout exceeded for job completions, updated the node state",
			"node", node.Name, "state", UpgradeStatePodDeletionRequired)
		err = m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, annotationKey)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to remove annotation used to track job completions",
				"node", node.Name, "annotation", annotationKey)
			return err
		}
	}
	return nil
}

func (m *PodManager) ScheduleCheckOnPodCompletion(ctx context.Context, config *PodManagerConfig) error {
	log.FromContext(ctx).Info("Pod Manager, starting checks on pod statuses")
	var wg sync.WaitGroup

	var errs []error
	for _, node := range config.Nodes {
		log.FromContext(ctx).Info("Schedule checks for pod completion", "node", node.Name)
		podList, err := m.ListPods(ctx, config.WaitForCompletionSpec.PodSelector, node.Name)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to list pods",
				"selector", config.WaitForCompletionSpec.PodSelector, "node", node.Name)
			errs = append(errs, err)
			continue
		}
		if len(podList.Items) > 0 {
			log.FromContext(ctx).Error(err, "Found workload pods",
				"selector", config.WaitForCompletionSpec.PodSelector, "node", node.Name, "pods", len(podList.Items))
		}
		wg.Add(1)
		go func(node corev1.Node) {
			defer wg.Done()
			running := false
			for _, pod := range podList.Items {
				running = m.IsPodRunningOrPending(ctx, pod)
				if running {
					break
				}
			}
			if running {
				log.FromContext(ctx).Info("Workload pods are still running on the node", "node", node.Name)
				if config.WaitForCompletionSpec.TimeoutSeconds != 0 {
					err = m.HandleTimeoutOnPodCompletions(ctx, &node, int64(config.WaitForCompletionSpec.TimeoutSeconds))
					if err != nil {
						return
					}
				}
				return
			}
			err = m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, &node, UpgradeWaitForPodCompletionStartTimeAnnotationKey)
			if err != nil {
				return
			}
			m.changeNodeUpgradeStateAsync(ctx, &node, UpgradeStatePodDeletionRequired)
			log.FromContext(ctx).Info("Updated the node state", "node", node.Name,
				"state", UpgradeStatePodDeletionRequired)
		}(*node)
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (m *PodManager) SchedulePodEviction(ctx context.Context, config *PodManagerConfig) error {
	log.FromContext(ctx).Info("Starting Pod Deletion")

	if len(config.Nodes) == 0 {
		log.FromContext(ctx).Info("No nodes scheduled for pod deletion")
		return nil
	}

	podDeletionSpec := config.DeletionSpec

	if podDeletionSpec == nil {
		return fmt.Errorf("pod deletion spec should not be empty")
	}

	deviceClasses := resolveNPUDeviceClasses(ctx, m.k8sInterface, config.NPUDeviceClass)

	for _, node := range config.Nodes {
		if !m.nodesInProgress.Has(node.Name) {
			log.FromContext(ctx).Info("Deleting pods on node", "node", node.Name)
			m.nodesInProgress.Add(node.Name)

			go func(node corev1.Node) {
				defer m.nodesInProgress.Remove(node.Name)

				log.FromContext(ctx).Info("Identifying pods to delete", "node", node.Name)

				podList, err := m.ListPods(ctx, "", node.Name)
				if err != nil {
					log.FromContext(ctx).Error(err, "Failed to list pods", "node", node.Name)
					return
				}

				// The pod-spec filter alone cannot see a pod whose only NPU
				// reference is a ResourceClaim. One matcher per node, so both
				// passes over the node's pods share its claim verdicts.
				claims := newNPUClaimMatcher(m.k8sInterface, deviceClasses)
				isNPUPod := func(pod corev1.Pod) bool {
					return m.podDeletionFilter(pod) || claims.holdsNPUClaim(ctx, &pod)
				}

				npuPods := make([]corev1.Pod, 0, len(podList.Items))
				for _, pod := range podList.Items {
					if isNPUPod(pod) {
						npuPods = append(npuPods, pod)
					}
				}

				if len(npuPods) == 0 {
					log.FromContext(ctx).Info("No pods require deletion", "node", node.Name)
					m.changeNodeUpgradeStateAsync(ctx, &node, UpgradeStatePodRestartRequired)
					return
				}

				drainHelper := drain.Helper{
					Ctx:                 ctx,
					Client:              m.k8sInterface,
					Out:                 os.Stdout,
					ErrOut:              os.Stderr,
					GracePeriodSeconds:  -1,
					IgnoreAllDaemonSets: true,
					Force:               podDeletionSpec.Force,
					DeleteEmptyDirData:  podDeletionSpec.DeleteEmptyDirData,
					Timeout:             time.Duration(podDeletionSpec.TimeoutSeconds) * time.Second,
					AdditionalFilters: []drain.PodFilter{func(pod corev1.Pod) drain.PodDeleteStatus {
						if !isNPUPod(pod) {
							return drain.MakePodDeleteStatusSkip()
						}
						return drain.MakePodDeleteStatusOkay()
					}},
				}

				log.FromContext(ctx).Info("Identifying which pods can be deleted", "node", node.Name)
				podDeleteList, errs := drainHelper.GetPodsForDeletion(node.Name)
				if podDeleteList == nil {
					log.FromContext(ctx).Error(errors.Join(errs...), "Failed to list pods for eviction; will retry next cycle",
						"node", node.Name)
					return
				}

				if blocked := blockedNPUPods(npuPods, podDeleteList.Pods()); len(blocked) > 0 {
					log.FromContext(ctx).Error(nil, "Cannot delete all required pods",
						"node", node.Name, "blockedPods", podKeys(blocked))
					for _, err := range errs {
						log.FromContext(ctx).Error(err, "Error reported by drain helper", "node", node.Name)
					}
					m.markNodeUpgradeSkippedAsync(ctx, node,
						"pod eviction blocked: "+evictionBlockReason(blocked, podDeletionSpec, errs))
					return
				}

				for _, p := range podDeleteList.Pods() {
					log.FromContext(ctx).Info("Identified pod to delete", "node", node.Name,
						"namespace", p.Namespace, "name", p.Name)
				}
				log.FromContext(ctx).Info("Warnings when identifying pods to delete",
					"warnings", podDeleteList.Warnings(), "node", node.Name)

				err = drainHelper.DeleteOrEvictPods(podDeleteList.Pods())
				if err != nil {
					log.FromContext(ctx).Error(err, "Failed to delete pods on the node", "node", node.Name)
					m.markNodeUpgradeSkippedAsync(ctx, node,
						fmt.Sprintf("pod eviction failed: %v", err))
					return
				}

				log.FromContext(ctx).Info("Deleted pods on the node", "node", node.Name)
				m.changeNodeUpgradeStateAsync(ctx, &node, UpgradeStatePodRestartRequired)
			}(*node)
		} else {
			log.FromContext(ctx).Info("Node is already getting pods deleted, skipping", "node", node.Name)
		}
	}
	return nil
}

func (m *PodManager) markNodeUpgradeSkippedAsync(ctx context.Context, node corev1.Node, reason string) {
	stateCtx, cancel := context.WithTimeout(ctx, 30*time.Second) //nolint:contextcheck // intentional short-lived timeout for goroutine state transition
	defer cancel()
	if err := markNodeUpgradeSkipped(stateCtx, m.nodeUpgradeStateProvider, &node, reason); err != nil {
		log.FromContext(ctx).Error(err, "Failed to mark node upgrade skipped; will retry next reconcile",
			"node", node.Name, "reason", reason)
	}
}

func (m *PodManager) SchedulePodsRestart(ctx context.Context, pods []*corev1.Pod) error {
	log.FromContext(ctx).Info("Starting Pod Delete")
	if len(pods) == 0 {
		log.FromContext(ctx).Info("No pods scheduled to restart")
		return nil
	}
	var errs []error
	for _, pod := range pods {
		log.FromContext(ctx).Info("Deleting pod", "pod", pod.Name)
		deleteOptions := metav1.DeleteOptions{}
		err := m.k8sInterface.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, deleteOptions)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to delete pod", "pod", pod.Name)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
