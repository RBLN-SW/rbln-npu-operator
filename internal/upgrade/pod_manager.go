package upgrade

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/drain"

	"github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

const (
	PodControllerRevisionHashLabelKey = "controller-revision-hash"
)

type PodManager struct {
	k8sInterface             kubernetes.Interface
	nodeUpgradeStateProvider *NodeUpgradeStateProvider
	podDeletionFilter        PodDeletionFilter
	nodesInProgress          *StringSet
	Log                      logr.Logger
}

type PodManagerConfig struct {
	Nodes                 []*corev1.Node
	DeletionSpec          *v1beta1.PodDeletionSpec
	WaitForCompletionSpec *v1beta1.WaitForCompletionSpec
	DrainEnabled          bool
	RebootRequired        bool
}

type PodDeletionFilter func(corev1.Pod) bool

func NewPodManager(
	k8sInterface kubernetes.Interface,
	nodeUpgradeStateProvider *NodeUpgradeStateProvider,
	log logr.Logger,
	podDeletionFilter PodDeletionFilter,
) *PodManager {
	mgr := &PodManager{
		k8sInterface:             k8sInterface,
		Log:                      log,
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

func (m *PodManager) IsPodRunningOrPending(pod corev1.Pod) bool {
	switch pod.Status.Phase {
	case corev1.PodRunning:
		m.Log.Info("Pod status", "pod", pod.Name, "node", pod.Spec.NodeName,
			"state", corev1.PodRunning)
		return true
	case corev1.PodPending:
		m.Log.Info("Pod status", "pod", pod.Name, "node", pod.Spec.NodeName,
			"state", corev1.PodPending)
		return true
	case corev1.PodFailed:
		m.Log.Info("Pod status", "pod", pod.Name, "node", pod.Spec.NodeName,
			"state", corev1.PodFailed)
		return false
	case corev1.PodSucceeded:
		m.Log.Info("Pod status", "pod", pod.Name, "node", pod.Spec.NodeName,
			"state", corev1.PodSucceeded)
		return false
	}
	return false
}

func (m *PodManager) HandleTimeoutOnPodCompletions(ctx context.Context, node *corev1.Node,
	timeoutSeconds int64,
) error {
	annotationKey := UpgradeWaitForPodCompletionStartTimeAnnotationKey
	currentTime := time.Now().Unix()
	if _, present := node.Annotations[annotationKey]; !present {
		err := m.nodeUpgradeStateProvider.SetNodeUpgradeAnnotation(ctx, node, annotationKey,
			strconv.FormatInt(currentTime, 10))
		if err != nil {
			m.Log.Error(err, "Failed to add annotation to track job completions",
				"node", node.Name, "annotation", annotationKey)
			return err
		}
		return nil
	}
	startTime, err := strconv.ParseInt(node.Annotations[annotationKey], 10, 64)
	if err != nil {
		m.Log.Error(err, "Failed to convert start time to track job completions",
			"node", node.Name)
		return err
	}
	if currentTime > startTime+timeoutSeconds {
		_ = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, UpgradeStatePodDeletionRequired)
		m.Log.Info("Timeout exceeded for job completions, updated the node state",
			"node", node.Name, "state", UpgradeStatePodDeletionRequired)
		err = m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, annotationKey)
		if err != nil {
			m.Log.Error(err, "Failed to remove annotation used to track job completions",
				"node", node.Name, "annotation", annotationKey)
			return err
		}
	}
	return nil
}

func (m *PodManager) ScheduleCheckOnPodCompletion(ctx context.Context, config *PodManagerConfig) error {
	m.Log.Info("Pod Manager, starting checks on pod statuses")
	var wg sync.WaitGroup

	for _, node := range config.Nodes {
		m.Log.Info("Schedule checks for pod completion", "node", node.Name)
		podList, err := m.ListPods(ctx, config.WaitForCompletionSpec.PodSelector, node.Name)
		if err != nil {
			m.Log.Error(err, "Failed to list pods",
				"selector", config.WaitForCompletionSpec.PodSelector, "node", node.Name)
			return err
		}
		if len(podList.Items) > 0 {
			m.Log.Error(err, "Found workload pods",
				"selector", config.WaitForCompletionSpec.PodSelector, "node", node.Name, "pods", len(podList.Items))
		}
		wg.Add(1)
		go func(node corev1.Node) {
			defer wg.Done()
			running := false
			for _, pod := range podList.Items {
				running = m.IsPodRunningOrPending(pod)
				if running {
					break
				}
			}
			if running {
				m.Log.Info("Workload pods are still running on the node", "node", node.Name)
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
			_ = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, &node, UpgradeStatePodDeletionRequired)
			m.Log.Info("Updated the node state", "node", node.Name,
				"state", UpgradeStatePodDeletionRequired)
		}(*node)
	}
	wg.Wait()
	return nil
}

func (m *PodManager) SchedulePodEviction(ctx context.Context, config *PodManagerConfig) error {
	m.Log.Info("Starting Pod Deletion")

	if len(config.Nodes) == 0 {
		m.Log.Info("No nodes scheduled for pod deletion")
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
			m.Log.Info("Deleting pods on node", "node", node.Name)
			m.nodesInProgress.Add(node.Name)

			go func(node corev1.Node) {
				defer m.nodesInProgress.Remove(node.Name)

				m.Log.Info("Identifying pods to delete", "node", node.Name)

				podList, err := m.ListPods(ctx, "", node.Name)
				if err != nil {
					m.Log.Error(err, "Failed to list pods", "node", node.Name)
					return
				}

				numPodsToDelete := 0
				for _, pod := range podList.Items {
					if m.podDeletionFilter(pod) {
						numPodsToDelete++
					}
				}

				if numPodsToDelete == 0 {
					m.Log.Info("No pods require deletion", "node", node.Name)
					_ = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(
						ctx,
						&node,
						m.nextStateAfterPodDeletion(config.RebootRequired),
					)
					return
				}

				m.Log.Info("Identifying which pods can be deleted", "node", node.Name)
				podDeleteList, errs := drainHelper.GetPodsForDeletion(node.Name)

				numPodsCanDelete := len(podDeleteList.Pods())
				if numPodsCanDelete != numPodsToDelete {
					m.Log.Error(nil, "Cannot delete all required pods", "node", node.Name)
					for _, err := range errs {
						m.Log.Error(err, "Error reported by drain helper", "node", node.Name)
					}
					m.updateNodeToDrainOrFailed(ctx, node, config.DrainEnabled)
					return
				}

				for _, p := range podDeleteList.Pods() {
					m.Log.Info("Identified pod to delete", "node", node.Name,
						"namespace", p.Namespace, "name", p.Name)
				}
				m.Log.Info("Warnings when identifying pods to delete",
					"warnings", podDeleteList.Warnings(), "node", node.Name)

				err = drainHelper.DeleteOrEvictPods(podDeleteList.Pods())
				if err != nil {
					m.Log.Error(err, "Failed to delete pods on the node", "node", node.Name)
					m.updateNodeToDrainOrFailed(ctx, node, config.DrainEnabled)
					return
				}

				m.Log.Info("Deleted pods on the node", "node", node.Name)
				_ = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(
					ctx,
					&node,
					m.nextStateAfterPodDeletion(config.RebootRequired),
				)
			}(*node)
		} else {
			m.Log.Info("Node is already getting pods deleted, skipping", "node", node.Name)
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
		m.Log.Info("Pod deletion failed but drain is enabled in spec. Will attempt a node drain",
			"node", node.Name)
		nextState = UpgradeStateDrainRequired
	}
	_ = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, &node, nextState)
}

func (m *PodManager) SchedulePodsRestart(ctx context.Context, pods []*corev1.Pod) error {
	m.Log.Info("Starting Pod Delete")
	if len(pods) == 0 {
		m.Log.Info("No pods scheduled to restart")
		return nil
	}
	for _, pod := range pods {
		m.Log.Info("Deleting pod", "pod", pod.Name)
		deleteOptions := metav1.DeleteOptions{}
		err := m.k8sInterface.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, deleteOptions)
		if err != nil {
			m.Log.Error(err, "Failed to delete pod", "pod", pod.Name)
			return err
		}
	}
	return nil
}
