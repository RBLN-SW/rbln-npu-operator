// Package metrics defines Prometheus gauges/counters that complement the
// CRD .status fields. They expose time-series signals suitable for alerting
// (e.g. workload uncovered for N minutes) which a snapshot-style CR status
// cannot. All metrics are registered with controller-runtime's metrics
// registry so they appear at /metrics on the operator's manager-served port.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	ReconcileStatusSuccess     float64 = 0
	ReconcileStatusNotReady    float64 = 1
	ReconcileStatusUnavailable float64 = 2
)

// Ordered so a higher value indicates a more severe condition, allowing
// `max()` in alert expressions to surface the worst-off workload.
const (
	WorkloadCoverageEmpty       float64 = 0
	WorkloadCoverageReady       float64 = 1
	WorkloadCoverageProgressing float64 = 2
	WorkloadCoverageUncovered   float64 = 3
)

const namespace = "rbln_operator"

var (
	ClusterPolicyReconcileStatus = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "clusterpolicy_reconcile_status",
		Help:      "Last RBLNClusterPolicy reconcile outcome (0=success, 1=notReady, 2=unavailable).",
	})

	DriverReconcileStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "driver_reconcile_status",
		Help:      "Last RBLNDriver reconcile outcome per CR (0=success, 1=notReady, 2=unavailable).",
	}, []string{"name"})

	ReconcileTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "reconcile_total",
		Help:      "Total number of reconciliations executed per controller.",
	}, []string{"controller"})

	ReconcileFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "reconcile_failed_total",
		Help:      "Total number of reconciliations that ended in a non-ready state.",
	}, []string{"controller"})

	NPUNodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "npu_nodes",
		Help:      "Number of NPU-eligible nodes labeled for each workload type.",
	}, []string{"workload"})

	WorkloadCoverage = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "workload_coverage_state",
		Help:      "Aggregate state per workload type (0=empty, 1=ready, 2=progressing, 3=uncovered).",
	}, []string{"workload"})

	DriverPoolReady = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "driver_pool_ready_ratio",
		Help:      "Per-pool ratio of ready pods to desired pods for the driver-manager DaemonSet.",
	}, []string{"driver", "pool"})

	// Owner-routing gauges are recomputed and Reset() on every resolve pass.
	// DriverOwnedNodes carries a 0 series for every routable RBLNDriver so a
	// selector that matches nothing stays visible; conflict series exist only
	// while a tie persists, so alert rules must not rely on absent().
	DriverOwnedNodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "driver_owned_nodes",
		Help:      "Number of nodes owned by each RBLNDriver instance.",
	}, []string{"driver"})

	DriverUncoveredNodes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "driver_uncovered_nodes",
		Help:      "Number of driver-deploy NPU nodes with no owning RBLNDriver (no selector matches, or an unresolved selector tie).",
	})

	DriverSelectorConflicts = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "driver_selector_conflict_nodes",
		Help:      "Number of nodes where this RBLNDriver's nodeSelector ties with another RBLNDriver.",
	}, []string{"driver"})

	DriverUpgradeNodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "driver_upgrade_nodes",
		Help:      "Number of nodes per driver upgrade state.",
	}, []string{"state"})
)

// CleanupDriverSeries drops per-driver series for a deleted RBLNDriver so
// stale label values do not linger until the next operator restart.
func CleanupDriverSeries(driverName string) {
	DriverPoolReady.DeletePartialMatch(prometheus.Labels{"driver": driverName})
	DriverReconcileStatus.DeleteLabelValues(driverName)
	DriverOwnedNodes.DeleteLabelValues(driverName)
	DriverSelectorConflicts.DeleteLabelValues(driverName)
}

func init() {
	metrics.Registry.MustRegister(
		ClusterPolicyReconcileStatus,
		DriverReconcileStatus,
		ReconcileTotal,
		ReconcileFailed,
		NPUNodes,
		WorkloadCoverage,
		DriverPoolReady,
		DriverOwnedNodes,
		DriverUncoveredNodes,
		DriverSelectorConflicts,
		DriverUpgradeNodes,
	)
}
