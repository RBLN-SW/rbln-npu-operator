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

func TestDriverUpgradeNodesSetAndReset(t *testing.T) {
	DriverUpgradeNodes.WithLabelValues("upgrade-done").Set(3)
	DriverUpgradeNodes.WithLabelValues("drain-required").Set(1)

	if got := testutil.ToFloat64(DriverUpgradeNodes.WithLabelValues("upgrade-done")); got != 3 {
		t.Errorf("upgrade-done = %v, want 3", got)
	}
	if got := testutil.ToFloat64(DriverUpgradeNodes.WithLabelValues("drain-required")); got != 1 {
		t.Errorf("drain-required = %v, want 1", got)
	}

	DriverUpgradeNodes.Reset()
	if got := testutil.CollectAndCount(DriverUpgradeNodes); got != 0 {
		t.Errorf("expected no series after reset, got %d", got)
	}
}
