package upgrade

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/RBLN-SW/rbln-npu-operator/api/v1beta1"
	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
	"github.com/RBLN-SW/rbln-npu-operator/internal/metrics"
)

func TestRecordNodeStateMetrics(t *testing.T) {
	defer metrics.DriverUpgradeNodes.Reset()

	state := newClusterState(map[string][]*NodeUpgradeState{
		UpgradeStateUnknown:             {newNodeUpgradeState("unknown-one", UpgradeStateUnknown, "rev1"), newNodeUpgradeState("unknown-two", UpgradeStateUnknown, "rev1")},
		UpgradeStateDone:                {newNodeUpgradeState("done-one", UpgradeStateDone, "rev1"), newNodeUpgradeState("done-two", UpgradeStateDone, "rev1"), newNodeUpgradeState("done-three", UpgradeStateDone, "rev1")},
		UpgradeStatePodDeletionRequired: {newNodeUpgradeState("evict", UpgradeStatePodDeletionRequired, "rev1")},
	})

	recordNodeStateMetrics(state)

	tests := map[string]float64{
		"unknown":               2, // UpgradeStateUnknown ("") maps to a non-empty label
		"upgrade-done":          3,
		"pod-deletion-required": 1,
		"upgrade-failed":        0, // absent states must be published as 0
		"cordon-required":       0,
	}
	for label, want := range tests {
		got := testutil.ToFloat64(metrics.DriverUpgradeNodes.WithLabelValues(label))
		if got != want {
			t.Errorf("state %q = %v, want %v", label, got, want)
		}
	}
}

func TestRecordNodeStateMetricsCoversAllManagedStates(t *testing.T) {
	defer metrics.DriverUpgradeNodes.Reset()

	recordNodeStateMetrics(&ClusterUpgradeState{})

	if got, want := testutil.CollectAndCount(metrics.DriverUpgradeNodes), len(managedUpgradeStates); got != want {
		t.Errorf("published %d series, want one per managed state (%d)", got, want)
	}
}

func TestApplyStateRunsAllStepsAndJoinsErrors(t *testing.T) {
	defer metrics.DriverUpgradeNodes.Reset()

	mgr := newTestManager(t, withCordonManager(&mockCordonManager{
		cordonErr: fmt.Errorf("cordon rejected by test"),
	}))

	blocked := newNodeUpgradeState("node-blocked", UpgradeStateCordonRequired, "rev1")
	finishing := newNodeUpgradeState("node-finishing", UpgradeStateUncordonRequired, "rev1")
	registerNodes(t, mgr, blocked.Node, finishing.Node)

	departed := departedTestNode("node-departed", UpgradeStatePodRestartRequired, "", nil)
	registerNodes(t, mgr, departed)

	state := newClusterState(map[string][]*NodeUpgradeState{
		UpgradeStateCordonRequired:   {blocked},
		UpgradeStateUncordonRequired: {finishing},
	})
	state.DepartedNodes = []*corev1.Node{departed}

	err := mgr.ApplyState(context.Background(), state,
		&v1beta1.DriverUpgradePolicySpec{AutoUpgrade: true}, "npu.rebellions.ai", ValidationOptions{})
	if err == nil {
		t.Fatal("expected the failing cordon step's error to be reported")
	}

	var updated corev1.Node
	if err := mgr.k8sClient.Get(context.Background(), types.NamespacedName{Name: "node-finishing"}, &updated); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got := updated.Labels[UpgradeStateLabelKey]; got != UpgradeStateDone {
		t.Fatalf("node state = %q, want %q (steps after a failing step must still run)", got, UpgradeStateDone)
	}
	if err := mgr.k8sClient.Get(context.Background(), types.NamespacedName{Name: "node-blocked"}, &updated); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got := updated.Labels[UpgradeStateLabelKey]; got != UpgradeStateCordonRequired {
		t.Fatalf("node state = %q, want unchanged %q (failed operation retries next cycle)", got, UpgradeStateCordonRequired)
	}
	if err := mgr.k8sClient.Get(context.Background(), types.NamespacedName{Name: "node-departed"}, &updated); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if _, labeled := updated.Labels[UpgradeStateLabelKey]; labeled {
		t.Fatal("departed node must be swept in the same pass")
	}
}

func TestNilState(t *testing.T) {
	m := newTestManager(t)
	for _, policy := range []*v1beta1.DriverUpgradePolicySpec{nil, {}, {AutoUpgrade: true}} {
		if err := m.ApplyState(context.Background(), nil, policy, "", ValidationOptions{}); err == nil {
			t.Fatal("nil state must return an error without panicking")
		}
	}
}

func TestApplyStateDoesNotRestartDepartedNodes(t *testing.T) {
	defer metrics.DriverUpgradeNodes.Reset()
	pods := &mockPodManager{podDigest: "old", dsDigest: "new"}
	manager := newTestManager(t, withPodManager(pods))
	observed := newNodeUpgradeState("departed", UpgradeStatePodRestartRequired, "old")
	observed.Node = departedTestNode("departed", UpgradeStatePodRestartRequired, "false", map[string]string{consts.DriverManagerCordonClaimAnnotation: consts.OperatorCordonClaimValue})
	registerNodes(t, manager, observed.Node)
	state := newClusterState(map[string][]*NodeUpgradeState{UpgradeStatePodRestartRequired: {observed}})
	state.DepartedNodes = []*corev1.Node{observed.Node}
	if err := manager.ApplyState(context.Background(), state, &v1beta1.DriverUpgradePolicySpec{AutoUpgrade: true}, "npu.rebellions.ai", ValidationOptions{}); err != nil {
		t.Fatal(err)
	}
	if len(pods.restartedPods) != 0 {
		t.Fatal("a departed node's old driver Pod must not enter the restart phase")
	}
	node := &corev1.Node{}
	if err := manager.k8sClient.Get(context.Background(), types.NamespacedName{Name: observed.Node.Name}, node); err != nil {
		t.Fatal(err)
	}
	if _, present := node.Labels[UpgradeStateLabelKey]; present || node.Spec.Unschedulable {
		t.Fatalf("departed node was not released: %+v", node)
	}
}

func TestAdmissionSeesSlotFreedEarlierInThePass(t *testing.T) {
	defer metrics.DriverUpgradeNodes.Reset()
	m := newTestManager(t)
	failed := newNodeUpgradeState("failed", UpgradeStateFailed, "rev1")
	failed.Node.Annotations = map[string]string{
		UpgradeRequestedAnnotationKey:   trueString,
		UpgradeFailureStepAnnotationKey: UpgradeStateValidationRequired,
	}
	queued := newNodeUpgradeState("queued", UpgradeStateUpgradeRequired, "rev1")
	registerNodes(t, m, failed.Node, queued.Node)
	state := newClusterState(map[string][]*NodeUpgradeState{
		UpgradeStateFailed: {failed}, UpgradeStateUpgradeRequired: {queued},
	})
	if err := m.ApplyState(context.Background(), state, &v1beta1.DriverUpgradePolicySpec{AutoUpgrade: true, MaxParallelUpgrades: 1}, "npu.example.com", ValidationOptions{}); err != nil {
		t.Fatal(err)
	}
	if failed.Node.Labels[UpgradeStateLabelKey] != UpgradeStateUpgradeRequired {
		t.Fatal("failed node was not returned to the queue")
	}
	if queued.Node.Labels[UpgradeStateLabelKey] != UpgradeStateCordonRequired {
		t.Fatal("admission counted a slot that the failed-node phase had already released")
	}
}
