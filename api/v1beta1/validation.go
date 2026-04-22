package v1beta1

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Validate performs defense-in-depth spec validation beyond what the CRD
// schema and CEL rules already enforce. It is called by the reconciler at
// entry so that CRs created via paths that bypass admission (e.g. legacy
// objects persisted before newer CRD markers were deployed) still surface a
// clear InvalidSpec status rather than failing opaquely during reconcile.
//
// CEL-expressible invariants live on the type directly via
// +kubebuilder:validation:XValidation; this function is reserved for checks
// that cross multiple fields in ways CEL cannot cleanly express.
func (s *RBLNClusterPolicySpec) Validate() error {
	var errs field.ErrorList
	specPath := field.NewPath("spec")

	// WorkloadType is declared as an enum via +kubebuilder:validation:Enum
	// and defaulted to "container" at the schema level; this check is a
	// runtime safety net for objects persisted before the CRD enforced the
	// enum.
	switch s.WorkloadType {
	case "", "container", "vm-passthrough":
	default:
		errs = append(errs, field.NotSupported(
			specPath.Child("workloadType"),
			s.WorkloadType,
			[]string{"container", "vm-passthrough"},
		))
	}

	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("RBLNClusterPolicy spec is invalid: %w", errs.ToAggregate())
}
