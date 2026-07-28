package upgrade

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

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
