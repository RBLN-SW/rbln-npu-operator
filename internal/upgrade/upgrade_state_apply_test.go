package upgrade

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/metrics"
)

func TestRecordNodeStateMetrics(t *testing.T) {
	defer metrics.DriverUpgradeNodes.Reset()

	state := &ClusterUpgradeState{
		NodeStates: map[string][]*NodeUpgradeState{
			UpgradeStateUnknown:       {{}, {}},
			UpgradeStateDone:          {{}, {}, {}},
			UpgradeStateDrainRequired: {{}},
		},
	}

	recordNodeStateMetrics(state)

	tests := map[string]float64{
		"unknown":         2, // UpgradeStateUnknown ("") maps to a non-empty label
		"upgrade-done":    3,
		"drain-required":  1,
		"upgrade-failed":  0, // absent states must be published as 0
		"cordon-required": 0,
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

	recordNodeStateMetrics(&ClusterUpgradeState{NodeStates: map[string][]*NodeUpgradeState{}})

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

	state := newClusterState(map[string][]*NodeUpgradeState{
		UpgradeStateCordonRequired:   {blocked},
		UpgradeStateUncordonRequired: {finishing},
	})

	err := mgr.ApplyState(context.Background(), "test-ns", state,
		&v1beta1.DriverUpgradePolicySpec{AutoUpgrade: true})
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
}
