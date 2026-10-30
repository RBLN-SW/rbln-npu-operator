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
