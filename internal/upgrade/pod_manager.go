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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/drain"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/RBLN-SW/rbln-npu-operator/api/v1beta1"
	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
)

type PodManagerInterface interface {
	GetPodDriverConfigDigest(*corev1.Pod) string
	GetDaemonSetDriverConfigDigest(*appsv1.DaemonSet) (string, error)
	ScheduleCheckOnPodCompletion(context.Context, *PodManagerConfig) error
	SchedulePodEviction(context.Context, *PodManagerConfig) error
	SchedulePodsRestart(context.Context, []*corev1.Pod) error
}
type PodManager struct {
	k8sInterface             kubernetes.Interface
	nodeUpgradeStateProvider *NodeUpgradeStateProvider
	podDeletionFilter        PodDeletionFilter
	nodesInProgress          sync.Map // node name -> reservation; zero value is ready to use
}
type PodManagerConfig struct {
	Nodes                 []*corev1.Node
	DeletionSpec          *v1beta1.PodDeletionSpec
	WaitForCompletionSpec *v1beta1.WaitForCompletionSpec
	NPUDeviceClass        string
}
type PodDeletionFilter func(corev1.Pod) bool

func NewPodManager(k kubernetes.Interface, p *NodeUpgradeStateProvider, f PodDeletionFilter) *PodManager {
	return &PodManager{k8sInterface: k, nodeUpgradeStateProvider: p, podDeletionFilter: f}
}

func driverConfigDigestOf(containers []corev1.Container) string {
	for _, c := range containers {
		for _, e := range c.Env {
			if e.Name == consts.DriverConfigDigestEnv {
				return e.Value
			}
		}
	}
	return ""
}

func (m *PodManager) GetPodDriverConfigDigest(p *corev1.Pod) string {
	if p == nil {
		return ""
	}
	return driverConfigDigestOf(p.Spec.InitContainers)
}

func (m *PodManager) GetDaemonSetDriverConfigDigest(ds *appsv1.DaemonSet) (string, error) {
	if ds == nil {
		return "", errors.New("driver DaemonSet is missing")
	}
	value := driverConfigDigestOf(ds.Spec.Template.Spec.InitContainers)
	if value == "" {
		return "", fmt.Errorf("DaemonSet %s/%s has no %s", ds.Namespace, ds.Name, consts.DriverConfigDigestEnv)
	}
	return value, nil
}

func (m *PodManager) ListPods(ctx context.Context, selector, node string) (*corev1.PodList, error) {
	return m.k8sInterface.CoreV1().Pods("").List(ctx, metav1.ListOptions{LabelSelector: selector, FieldSelector: fmt.Sprintf(nodeNameFieldSelectorFmt, node)})
}

func (m *PodManager) IsPodRunningOrPending(_ context.Context, p corev1.Pod) bool {
	return podRunningOrPending(&p)
}

func (m *PodManager) changeNodeUpgradeStateAsync(ctx context.Context, n *corev1.Node, from, to string) {
	stateCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := m.nodeUpgradeStateProvider.ChangeNodeUpgradeStateFrom(stateCtx, n, from, to); err != nil {
		log.FromContext(ctx).Error(err, "Cannot publish completed node operation", "node", n.Name)
	}
}

func (m *PodManager) markNodeUpgradeSkippedAsync(ctx context.Context, n corev1.Node, reason string) {
	stateCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := markNodeUpgradeSkipped(stateCtx, m.nodeUpgradeStateProvider, &n, UpgradeStatePodDeletionRequired, reason); err != nil {
		log.FromContext(ctx).Error(err, "Cannot publish skipped node", "node", n.Name)
	}
}

// Reserve the node before launching work; repeated reconciles do not create
// another worker, and workers never mutate the reconcile's input objects.
func (m *PodManager) schedule(ctx context.Context, nodes []*corev1.Node, task func(context.Context, *corev1.Node) error) {
	for _, node := range nodes {
		if _, running := m.nodesInProgress.LoadOrStore(node.Name, struct{}{}); running {
			continue
		}
		workNode := node.DeepCopy()
		go func() {
			defer m.nodesInProgress.Delete(workNode.Name)
			if err := task(ctx, workNode); err != nil {
				log.FromContext(ctx).Error(err, "Node operation will be retried", "node", workNode.Name)
			}
		}()
	}
}

func (m *PodManager) HandleTimeoutOnPodCompletions(ctx context.Context, n *corev1.Node, seconds int64) error {
	expired, err := checkAnnotationTimeout(ctx, m.nodeUpgradeStateProvider, n, UpgradeWaitForPodCompletionStartTimeAnnotationKey, seconds)
	if err != nil || !expired {
		return err
	}
	// Retain the expired clock if the state write fails, so the next pass
	// retries the transition instead of starting another full wait window.
	if err := m.nodeUpgradeStateProvider.ChangeNodeUpgradeStateFrom(ctx, n, UpgradeStateWaitForJobsRequired, UpgradeStatePodDeletionRequired); err != nil {
		return err
	}
	return m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, n, UpgradeWaitForPodCompletionStartTimeAnnotationKey)
}

func (m *PodManager) completeWait(ctx context.Context, n *corev1.Node) error {
	if err := m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, n, UpgradeWaitForPodCompletionStartTimeAnnotationKey); err != nil {
		return err
	}
	return m.nodeUpgradeStateProvider.ChangeNodeUpgradeStateFrom(ctx, n, UpgradeStateWaitForJobsRequired, UpgradeStatePodDeletionRequired)
}

func (m *PodManager) ScheduleCheckOnPodCompletion(ctx context.Context, cfg *PodManagerConfig) error {
	if cfg == nil || cfg.WaitForCompletionSpec == nil {
		return errors.New("pod completion policy is required")
	}
	spec := *cfg.WaitForCompletionSpec
	var result error
	for _, n := range cfg.Nodes {
		// Completion checks are synchronous, so successful writes belong in
		// the reconcile's snapshot just like other synchronous transitions.
		result = errors.Join(result, m.checkPodCompletion(ctx, n, &spec))
	}
	return result
}

func (m *PodManager) checkPodCompletion(ctx context.Context, n *corev1.Node, spec *v1beta1.WaitForCompletionSpec) error {
	list, err := m.ListPods(ctx, spec.PodSelector, n.Name)
	if err != nil {
		return err
	}
	for _, p := range list.Items {
		if p.Spec.NodeName == n.Name && m.IsPodRunningOrPending(ctx, p) {
			if spec.TimeoutSeconds <= 0 {
				return nil
			}
			return m.HandleTimeoutOnPodCompletions(ctx, n, int64(spec.TimeoutSeconds))
		}
	}
	return m.completeWait(ctx, n)
}

// Only an eviction verdict parks a node. Errors while observing pods leave
// the operation pending so the next reconcile can retry that observation.
type evictionVerdict struct{ reason string }

func (e *evictionVerdict) Error() string { return e.reason }

func (m *PodManager) SchedulePodEviction(ctx context.Context, cfg *PodManagerConfig) error {
	if cfg == nil {
		return errors.New("eviction configuration is required")
	}
	if len(cfg.Nodes) == 0 {
		return nil
	}
	if cfg.DeletionSpec == nil {
		return errors.New("pod deletion spec should not be empty")
	}
	spec := *cfg.DeletionSpec
	// Class names are immutable for this batch. Claim verdicts remain local
	// to each worker because their cache is not safe for concurrent use.
	deviceClasses := resolveNPUDeviceClasses(ctx, m.k8sInterface, cfg.NPUDeviceClass)
	m.schedule(ctx, cfg.Nodes, func(ctx context.Context, n *corev1.Node) error {
		if err := m.evictNodePods(ctx, n, &spec, deviceClasses); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var verdict *evictionVerdict
			if !errors.As(err, &verdict) {
				return err
			}
			m.markNodeUpgradeSkippedAsync(ctx, *n, verdict.Error())
			return nil
		}
		m.changeNodeUpgradeStateAsync(ctx, n, UpgradeStatePodDeletionRequired, UpgradeStatePodRestartRequired)
		return nil
	})
	return nil
}

func (m *PodManager) evictNodePods(ctx context.Context, n *corev1.Node, spec *v1beta1.PodDeletionSpec, deviceClasses []string) error {
	logger := log.FromContext(ctx).WithValues("node", n.Name)
	matcher := newNPUClaimMatcher(m.k8sInterface, deviceClasses)
	matches := func(p corev1.Pod) bool {
		return (m.podDeletionFilter != nil && m.podDeletionFilter(p)) || matcher.holdsNPUClaim(ctx, &p)
	}
	selectHeld := func() ([]corev1.Pod, error) {
		list, err := m.ListPods(ctx, "", n.Name)
		if err != nil {
			return nil, err
		}
		var held []corev1.Pod
		for _, p := range list.Items {
			if p.Spec.NodeName == n.Name && matches(p) {
				held = append(held, p)
			}
		}
		return held, nil
	}
	initial, err := selectHeld()
	if err != nil {
		return err
	}
	if len(initial) == 0 {
		logger.V(consts.VDebug).Info("No NPU workloads require eviction")
		return nil
	}
	helper := &drain.Helper{
		Ctx: ctx, Client: m.k8sInterface, Force: spec.Force, DeleteEmptyDirData: spec.DeleteEmptyDirData, IgnoreAllDaemonSets: true, GracePeriodSeconds: -1, ChunkSize: 500, Timeout: time.Duration(spec.TimeoutSeconds) * time.Second, Out: os.Stdout, ErrOut: os.Stderr,
		AdditionalFilters: []drain.PodFilter{func(p corev1.Pod) drain.PodDeleteStatus {
			if matches(p) {
				return drain.MakePodDeleteStatusOkay()
			}
			return drain.MakePodDeleteStatusSkip()
		}},
	}
	list, issues := helper.GetPodsForDeletion(n.Name)
	if list == nil {
		return fmt.Errorf("pod list failed: %w", errors.Join(issues...))
	}
	// Retain the policy check for NPU workloads missing from the deletable
	// set. Comparing against another LIST here would misclassify pods that
	// arrived after the helper's observation as policy-blocked workloads.
	victims := list.Pods()
	if blocked := blockedNPUPods(initial, victims); len(blocked) > 0 {
		reason := "pod eviction blocked: " + evictionBlockReason(blocked, spec, issues)
		logger.Info("NPU workload eviction blocked", "pods", podKeys(blocked), "reason", reason)
		return &evictionVerdict{reason: reason}
	}
	logger.Info("Evicting NPU workloads", "pods", len(victims))
	if warnings := list.Warnings(); warnings != "" {
		logger.V(consts.VDebug).Info("Eviction candidate warnings", "warnings", warnings)
	}
	if err := helper.DeleteOrEvictPods(victims); err != nil {
		reason := fmt.Sprintf("pod eviction failed: %v", err)
		logger.Info("NPU workload eviction did not complete", "reason", reason)
		return &evictionVerdict{reason: reason}
	}
	logger.Info("NPU workload eviction completed", "pods", len(victims))
	return nil
}

func (m *PodManager) SchedulePodsRestart(ctx context.Context, pods []*corev1.Pod) error {
	var result error
	for _, p := range pods {
		if p == nil || p.DeletionTimestamp != nil {
			continue
		}
		options := metav1.DeleteOptions{}
		if p.UID != "" {
			uid := p.UID
			options.Preconditions = &metav1.Preconditions{UID: &uid}
		}
		err := m.k8sInterface.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, options)
		if !apierrors.IsNotFound(err) {
			result = errors.Join(result, err)
		}
	}
	return result
}
