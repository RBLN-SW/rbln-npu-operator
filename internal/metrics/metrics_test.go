package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

func TestDriverUpgradeNodesRegistered(t *testing.T) {
	DriverUpgradeNodes.WithLabelValues("upgrade-done").Set(1)
	defer DriverUpgradeNodes.Reset()

	families, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() == "rbln_operator_driver_upgrade_nodes" {
			return
		}
	}
	t.Fatal("rbln_operator_driver_upgrade_nodes not found in controller-runtime registry")
}

func TestCleanupDriverSeries(t *testing.T) {
	defer func() {
		DriverPoolReady.Reset()
		DriverReconcileStatus.Reset()
		DriverOwnedNodes.Reset()
		DriverSelectorConflicts.Reset()
	}()
	DriverPoolReady.WithLabelValues("gone", "ubuntu22.04-5.15").Set(1)
	DriverPoolReady.WithLabelValues("gone", "ubuntu24.04-6.8").Set(1)
	DriverPoolReady.WithLabelValues("kept", "ubuntu22.04-5.15").Set(1)
	DriverReconcileStatus.WithLabelValues("gone").Set(ReconcileStatusSuccess)
	DriverOwnedNodes.WithLabelValues("gone").Set(2)
	DriverSelectorConflicts.WithLabelValues("gone").Set(1)

	CleanupDriverSeries("gone")

	if got := testutil.CollectAndCount(DriverPoolReady); got != 1 {
		t.Errorf("DriverPoolReady series = %d, want 1 (only the surviving driver)", got)
	}
	if got := testutil.CollectAndCount(DriverReconcileStatus); got != 0 {
		t.Errorf("DriverReconcileStatus series = %d, want 0", got)
	}
	if got := testutil.CollectAndCount(DriverOwnedNodes); got != 0 {
		t.Errorf("DriverOwnedNodes series = %d, want 0", got)
	}
	if got := testutil.CollectAndCount(DriverSelectorConflicts); got != 0 {
		t.Errorf("DriverSelectorConflicts series = %d, want 0", got)
	}
}
