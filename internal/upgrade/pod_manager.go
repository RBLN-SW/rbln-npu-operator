package upgrade

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/drain"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

const (
	PodControllerRevisionHashLabelKey = "controller-revision-hash"
)

// PodManagerInterface abstracts pod lifecycle operations for testability.
type PodManagerInterface interface {
	GetPodControllerRevisionHash(pod *corev1.Pod) (string, error)
	GetDaemonsetControllerRevisionHash(ctx context.Context, ds *appsv1.DaemonSet) (string, error)
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
	DrainEnabled          bool
	RebootRequired        bool
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

func (m *PodManager) GetPodControllerRevisionHash(pod *corev1.Pod) (string, error) {
	if hash, ok := pod.Labels[PodControllerRevisionHashLabelKey]; ok {
		return hash, nil
	}
	return "", fmt.Errorf("controller-revision-hash label not present for pod %s", pod.Name)
}

func (m *PodManager) GetDaemonsetControllerRevisionHash(ctx context.Context,
	daemonset *appsv1.DaemonSet,
) (string, error) {
	listOptions := metav1.ListOptions{LabelSelector: labels.SelectorFromSet(daemonset.Spec.Selector.MatchLabels).String()}
	controllerRevisionList, err := m.k8sInterface.AppsV1().ControllerRevisions(daemonset.Namespace).List(ctx, listOptions)
	if err != nil {
		return "", fmt.Errorf("error getting controller revision list for daemonset %s: %v", daemonset.Name, err)
	}

	var revisions []appsv1.ControllerRevision
	for _, controllerRevision := range controllerRevisionList.Items {
		if strings.HasPrefix(controllerRevision.Name, daemonset.Name) {
			revisions = append(revisions, controllerRevision)
		}
	}

	if len(revisions) == 0 {
		return "", fmt.Errorf("no revision found for daemonset %s", daemonset.Name)
	}

	sort.Slice(revisions, func(i, j int) bool { return revisions[i].Revision < revisions[j].Revision })

	currentRevision := revisions[len(revisions)-1]
	hash := strings.TrimPrefix(currentRevision.Name, fmt.Sprintf("%s-", daemonset.Name))
	return hash, nil
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
	switch pod.Status.Phase {
	case corev1.PodRunning:
		log.FromContext(ctx).Info("Pod status", "pod", pod.Name, "node", pod.Spec.NodeName,
			"state", corev1.PodRunning)
		return true
	case corev1.PodPending:
		log.FromContext(ctx).Info("Pod status", "pod", pod.Name, "node", pod.Spec.NodeName,
			"state", corev1.PodPending)
		return true
	case corev1.PodFailed:
		log.FromContext(ctx).Info("Pod status", "pod", pod.Name, "node", pod.Spec.NodeName,
			"state", corev1.PodFailed)
		return false
	case corev1.PodSucceeded:
		log.FromContext(ctx).Info("Pod status", "pod", pod.Name, "node", pod.Spec.NodeName,
			"state", corev1.PodSucceeded)
		return false
	}
	return false
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

	customDrainFilter := func(pod corev1.Pod) drain.PodDeleteStatus {
		deleteFunc := m.podDeletionFilter(pod)
		if !deleteFunc {
			return drain.MakePodDeleteStatusSkip()
		}
		return drain.MakePodDeleteStatusOkay()
	}

	drainHelper := drain.Helper{
		Ctx:                 ctx,
		Client:              m.k8sInterface,
		Out:                 os.Stdout,
		ErrOut:              os.Stderr,
		GracePeriodSeconds:  -1,
		IgnoreAllDaemonSets: true,
		Force:               podDeletionSpec.Force,
		Timeout:             time.Duration(podDeletionSpec.TimeoutSeconds) * time.Second,
		AdditionalFilters:   []drain.PodFilter{customDrainFilter},
	}

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

				numPodsToDelete := 0
				for _, pod := range podList.Items {
					if m.podDeletionFilter(pod) {
						numPodsToDelete++
					}
				}

				if numPodsToDelete == 0 {
					log.FromContext(ctx).Info("No pods require deletion", "node", node.Name)
					m.changeNodeUpgradeStateAsync(ctx, &node, m.nextStateAfterPodDeletion(config.RebootRequired))
					return
				}

				log.FromContext(ctx).Info("Identifying which pods can be deleted", "node", node.Name)
				podDeleteList, errs := drainHelper.GetPodsForDeletion(node.Name)

				numPodsCanDelete := len(podDeleteList.Pods())
				if numPodsCanDelete != numPodsToDelete {
					log.FromContext(ctx).Error(nil, "Cannot delete all required pods", "node", node.Name)
					for _, err := range errs {
						log.FromContext(ctx).Error(err, "Error reported by drain helper", "node", node.Name)
					}
					m.updateNodeToDrainOrFailed(ctx, node, config.DrainEnabled)
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
					m.updateNodeToDrainOrFailed(ctx, node, config.DrainEnabled)
					return
				}

				log.FromContext(ctx).Info("Deleted pods on the node", "node", node.Name)
				m.changeNodeUpgradeStateAsync(ctx, &node, m.nextStateAfterPodDeletion(config.RebootRequired))
			}(*node)
		} else {
			log.FromContext(ctx).Info("Node is already getting pods deleted, skipping", "node", node.Name)
		}
	}
	return nil
}

func (m *PodManager) nextStateAfterPodDeletion(rebootRequired bool) string {
	if rebootRequired {
		return UpgradeStateDrainRequired
	}
	return UpgradeStatePodRestartRequired
}

func (m *PodManager) updateNodeToDrainOrFailed(ctx context.Context, node corev1.Node, drainEnabled bool) {
	nextState := UpgradeStateFailed
	if drainEnabled {
		log.FromContext(ctx).Info("Pod deletion failed but drain is enabled in spec. Will attempt a node drain",
			"node", node.Name)
		nextState = UpgradeStateDrainRequired
	}
	m.changeNodeUpgradeStateAsync(ctx, &node, nextState)
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
