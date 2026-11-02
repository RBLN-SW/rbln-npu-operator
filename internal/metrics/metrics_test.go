/*
Copyright 2026 Rebellions Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
