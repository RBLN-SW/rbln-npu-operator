package components

import (
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

// applyUpdateStrategy runs inside CreateOrPatch, after the existing DaemonSet
// has been fetched. An unset policy must not take ownership of its strategy:
// older operator versions preserved this field, including manual overrides.
func applyUpdateStrategy(ds *appsv1.DaemonSet, desired *rblnv1beta1.DaemonSetUpdateStrategySpec) {
	if desired == nil {
		return
	}

	strategy := appsv1.DaemonSetUpdateStrategy{Type: desired.Type}
	if strategy.Type == "" {
		strategy.Type = appsv1.RollingUpdateDaemonSetStrategyType
	}
	if strategy.Type == appsv1.RollingUpdateDaemonSetStrategyType {
		maxUnavailable := intstr.FromInt32(1)
		if desired.RollingUpdate != nil && desired.RollingUpdate.MaxUnavailable != nil {
			maxUnavailable = *desired.RollingUpdate.MaxUnavailable
		}
		maxSurge := intstr.FromInt32(0)
		// Include API defaults so reconciliation is stable after defaulting.
		strategy.RollingUpdate = &appsv1.RollingUpdateDaemonSet{
			MaxUnavailable: &maxUnavailable,
			MaxSurge:       &maxSurge,
		}
	}
	// Replace the entire strategy so RollingUpdate -> OnDelete also clears the
	// rollingUpdate block, which the API rejects for OnDelete.
	ds.Spec.UpdateStrategy = strategy
}
