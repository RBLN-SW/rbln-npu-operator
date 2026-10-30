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
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// DaemonSetUpdateStrategySpec configures an individual ClusterPolicy component.
// An omitted strategy preserves the existing DaemonSet strategy, including
// manual overrides. Newly created DaemonSets use Kubernetes defaults.
// Driver and SMD do not expose this configuration.
// +kubebuilder:validation:XValidation:rule="!has(self.type) || self.type != 'OnDelete' || !has(self.rollingUpdate)",message="rollingUpdate must be omitted when type is OnDelete"
type DaemonSetUpdateStrategySpec struct {
	// Type selects automatic rolling updates or replacement only after pod deletion.
	// +optional
	// +kubebuilder:default=RollingUpdate
	// +kubebuilder:validation:Enum=RollingUpdate;OnDelete
	Type appsv1.DaemonSetUpdateStrategyType `json:"type,omitempty"`

	// RollingUpdate configures rolling updates. Omit when Type is OnDelete.
	// +optional
	RollingUpdate *RollingUpdateDaemonSetSpec `json:"rollingUpdate,omitempty"`
}

// RollingUpdateDaemonSetSpec controls rollout concurrency without allowing
// overlapping old and new pods on a node. MaxSurge is always zero.
type RollingUpdateDaemonSetSpec struct {
	// MaxUnavailable is a positive pod count or a percentage from 1% to 100%.
	// Defaults to 1. The limit applies independently to each DaemonSet.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:XValidation:rule="type(self) == int ? self > 0 && self <= 2147483647 : self.matches('^([1-9][0-9]?|100)%$')",message="maxUnavailable must be a positive integer or a percentage from 1% to 100%"
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
}
