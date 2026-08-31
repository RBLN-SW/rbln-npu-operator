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
)

// ---------------------------------------------------------------------------
// Mock: PodManagerInterface
// ---------------------------------------------------------------------------

type mockPodManager struct {
	podRevisionHash      string
	podRevisionHashErr   error
	dsRevisionHash       string
	dsRevisionHashErr    error
	schedulePodEvictErr  error
	schedulePodsRestart  error
	scheduleCheckOnPodCp error
}

func (m *mockPodManager) GetPodControllerRevisionHash(_ *corev1.Pod) (string, error) {
	return m.podRevisionHash, m.podRevisionHashErr
}

func (m *mockPodManager) GetDaemonsetControllerRevisionHash(_ context.Context, _ *appsv1.DaemonSet) (string, error) {
	return m.dsRevisionHash, m.dsRevisionHashErr
}

func (m *mockPodManager) ScheduleCheckOnPodCompletion(_ context.Context, _ *PodManagerConfig) error {
	return m.scheduleCheckOnPodCp
}

func (m *mockPodManager) SchedulePodEviction(_ context.Context, _ *PodManagerConfig) error {
	return m.schedulePodEvictErr
}

func (m *mockPodManager) SchedulePodsRestart(_ context.Context, _ []*corev1.Pod) error {
	return m.schedulePodsRestart
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
// Mock: DrainManagerInterface
// ---------------------------------------------------------------------------

type mockDrainManager struct {
	err error
}

func (m *mockDrainManager) ScheduleNodesDrain(_ context.Context, _ *DrainConfiguration) error {
	return m.err
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
// Mock: RebootManager
// ---------------------------------------------------------------------------

type mockRebootManager struct {
	err error
}

func (m *mockRebootManager) Trigger(_ context.Context, _ *corev1.Node, _ RebootTriggerRequest) error {
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
		podManager:               &mockPodManager{podRevisionHash: "rev1", dsRevisionHash: "rev1"},
		cordonManager:            &mockCordonManager{},
		drainManager:             &mockDrainManager{},
		validationManager:        &mockValidationManager{done: true},
		safeDriverLoadManager:    &mockSafeDriverLoadManager{},
		rebootManager:            &mockRebootManager{},
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

func withDrainManager(d DrainManagerInterface) func(*ClusterUpgradeStateManagerImpl) {
	return func(m *ClusterUpgradeStateManagerImpl) { m.drainManager = d }
}

func newNodeUpgradeState(nodeName, stateLabel, podRevHash string) *NodeUpgradeState {
	return newNodeUpgradeStateWithDS(nodeName, stateLabel, podRevHash, "ds-1")
}

func newNodeUpgradeStateWithDS(nodeName, stateLabel, podRevHash, dsName string) *NodeUpgradeState {
	labels := map[string]string{}
	if stateLabel != "" {
		labels[UpgradeStateLabelKey] = stateLabel
	}
	podLabels := map[string]string{}
	if podRevHash != "" {
		podLabels[PodControllerRevisionHashLabelKey] = podRevHash
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
			ObjectMeta: metav1.ObjectMeta{Name: nodeName + "-pod", Labels: podLabels},
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
