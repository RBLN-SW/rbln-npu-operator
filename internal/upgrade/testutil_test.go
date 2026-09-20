package upgrade

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// ---------------------------------------------------------------------------
// Mock: PodManagerInterface
// ---------------------------------------------------------------------------

type mockPodManager struct {
	podDigest            string
	dsDigest             string
	dsDigestErr          error
	schedulePodEvictErr  error
	schedulePodsRestart  error
	scheduleCheckOnPodCp error

	evictionConfigs []*PodManagerConfig
	restartedPods   []*corev1.Pod
}

func (m *mockPodManager) GetPodDriverConfigDigest(_ *corev1.Pod) string {
	return m.podDigest
}

func (m *mockPodManager) GetDaemonSetDriverConfigDigest(_ *appsv1.DaemonSet) (string, error) {
	return m.dsDigest, m.dsDigestErr
}

func (m *mockPodManager) ScheduleCheckOnPodCompletion(_ context.Context, _ *PodManagerConfig) error {
	return m.scheduleCheckOnPodCp
}

func (m *mockPodManager) SchedulePodEviction(_ context.Context, config *PodManagerConfig) error {
	m.evictionConfigs = append(m.evictionConfigs, config)
	return m.schedulePodEvictErr
}

func (m *mockPodManager) SchedulePodsRestart(_ context.Context, pods []*corev1.Pod) error {
	m.restartedPods = append(m.restartedPods, pods...)
	return m.schedulePodsRestart
}

// markPodTemplateOutdated gives the pod and its DaemonSet different template
// hashes, so the pod reads as rendered from an older template while its
// DRIVER_CONFIG_DIGEST stays whatever the mock reports.
func markPodTemplateOutdated(ns *NodeUpgradeState) {
	ns.DriverPod.Annotations = map[string]string{consts.DriverTemplateHashAnnotation: "tmpl-old"}
	ns.DriverDaemonSet.Spec.Template.Annotations = map[string]string{consts.DriverTemplateHashAnnotation: "tmpl-new"}
}

// ---------------------------------------------------------------------------
// Mock: CordonManagerInterface
// ---------------------------------------------------------------------------

type mockCordonManager struct {
	cordonErr   error
	uncordonErr error

	cordonedNodes   []string
	uncordonedNodes []string
}

func (m *mockCordonManager) Cordon(_ context.Context, node *corev1.Node) error {
	if m.cordonErr == nil {
		m.cordonedNodes = append(m.cordonedNodes, node.Name)
	}
	return m.cordonErr
}

func (m *mockCordonManager) Uncordon(_ context.Context, node *corev1.Node) error {
	if m.uncordonErr == nil {
		m.uncordonedNodes = append(m.uncordonedNodes, node.Name)
	}
	return m.uncordonErr
}

// ---------------------------------------------------------------------------
// Mock: ValidationManagerInterface
// ---------------------------------------------------------------------------

type mockValidationManager struct {
	done bool
	err  error
	// errOn makes Validate fail for that node only.
	errOn string
}

func (m *mockValidationManager) Validate(_ context.Context, node *corev1.Node) (bool, error) {
	if m.errOn != "" && node.Name == m.errOn {
		return false, fmt.Errorf("validation rejected for %s by test", node.Name)
	}
	return m.done, m.err
}

// ---------------------------------------------------------------------------
// Mock: SafeDriverLoadManagerInterface
// ---------------------------------------------------------------------------

type mockSafeDriverLoadManager struct {
	waiting bool
	err     error
}

func (m *mockSafeDriverLoadManager) IsWaitingForSafeDriverLoad(_ context.Context, _ *corev1.Node) bool {
	return m.waiting
}

func (m *mockSafeDriverLoadManager) UnblockLoading(_ context.Context, _ *corev1.Node) error {
	return m.err
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newTestManager(t *testing.T, opts ...func(*ClusterUpgradeStateManagerImpl)) *ClusterUpgradeStateManagerImpl {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	provider := NewNodeUpgradeStateProvider(k8sClient, nil)

	mgr := &ClusterUpgradeStateManagerImpl{
		log:                      logr.Discard(),
		k8sClient:                k8sClient,
		nodeUpgradeStateProvider: provider,
		podManager:               &mockPodManager{podDigest: "rev1", dsDigest: "rev1"},
		cordonManager:            &mockCordonManager{},
		validationManager:        &mockValidationManager{done: true},
		safeDriverLoadManager:    &mockSafeDriverLoadManager{},
	}

	for _, opt := range opts {
		opt(mgr)
	}
	return mgr
}

func withPodManager(pm PodManagerInterface) func(*ClusterUpgradeStateManagerImpl) {
	return func(m *ClusterUpgradeStateManagerImpl) { m.podManager = pm }
}

func withCordonManager(cm CordonManagerInterface) func(*ClusterUpgradeStateManagerImpl) {
	return func(m *ClusterUpgradeStateManagerImpl) { m.cordonManager = cm }
}

func withSafeDriverLoadManager(s SafeDriverLoadManagerInterface) func(*ClusterUpgradeStateManagerImpl) {
	return func(m *ClusterUpgradeStateManagerImpl) { m.safeDriverLoadManager = s }
}

func withValidationManager(v ValidationManagerInterface) func(*ClusterUpgradeStateManagerImpl) {
	return func(m *ClusterUpgradeStateManagerImpl) { m.validationManager = v }
}

func withValidationEnabled() func(*ClusterUpgradeStateManagerImpl) {
	return func(m *ClusterUpgradeStateManagerImpl) { m.validationStateEnabled = true }
}

func withPodDeletionEnabled() func(*ClusterUpgradeStateManagerImpl) {
	return func(m *ClusterUpgradeStateManagerImpl) { m.podDeletionStateEnabled = true }
}

func newNodeUpgradeState(nodeName, stateLabel, podDigest string) *NodeUpgradeState {
	return newNodeUpgradeStateWithDS(nodeName, stateLabel, podDigest, "ds-1")
}

func newNodeUpgradeStateWithDS(nodeName, stateLabel, podDigest, dsName string) *NodeUpgradeState {
	labels := map[string]string{}
	if stateLabel != "" {
		labels[UpgradeStateLabelKey] = stateLabel
	}
	var initContainers []corev1.Container
	if podDigest != "" {
		initContainers = []corev1.Container{{
			Name: "k8s-driver-manager",
			Env:  []corev1.EnvVar{{Name: consts.DriverConfigDigestEnv, Value: podDigest}},
		}}
	}

	var ds *appsv1.DaemonSet
	if dsName != "" {
		ds = &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: dsName}}
	}

	return &NodeUpgradeState{
		Node: &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName, Labels: labels},
		},
		DriverPod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName + "-pod"},
			Spec:       corev1.PodSpec{InitContainers: initContainers},
		},
		DriverDaemonSet: ds,
	}
}

func newClusterState(states map[string][]*NodeUpgradeState) *ClusterUpgradeState {
	cs := NewClusterUpgradeState()
	for k, v := range states {
		cs.NodeStates[k] = v
	}
	return &cs
}

func registerNodes(t *testing.T, mgr *ClusterUpgradeStateManagerImpl, nodes ...*corev1.Node) {
	t.Helper()
	for _, n := range nodes {
		if err := mgr.k8sClient.Create(context.Background(), n); err != nil {
			t.Fatalf("create node %s: %v", n.Name, err)
		}
	}
}
