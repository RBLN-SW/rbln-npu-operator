package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ComponentState string

const (
	ComponentStateReady    ComponentState = "ready"
	ComponentStateNotReady ComponentState = "notReady"
)

// WorkloadState describes the aggregate state of a workload group.
type WorkloadState string

const (
	// WorkloadStateReady: NodeCount > 0 and all enabled components covering this workload are ready.
	WorkloadStateReady WorkloadState = "ready"
	// WorkloadStateProgressing: NodeCount > 0 and at least one covering component is not yet ready.
	WorkloadStateProgressing WorkloadState = "progressing"
	// WorkloadStateEmpty: NodeCount == 0 (no NPU nodes claim this workload type — normal idle).
	WorkloadStateEmpty WorkloadState = "empty"
	// WorkloadStateUncovered: NodeCount > 0 but no enabled component is configured for this workload.
	WorkloadStateUncovered WorkloadState = "uncovered"
)

// RBLNWorkloadStatus is the aggregate readiness summary for a single workload type.
type RBLNWorkloadStatus struct {
	// Type is the workload type ("container" or "vm-passthrough").
	Type string `json:"type"`
	// NodeCount is the number of NPU nodes labeled for this workload.
	NodeCount int32 `json:"nodeCount"`
	// ComponentCount is the number of enabled components that target this workload.
	ComponentCount int32 `json:"componentCount"`
	// ReadyCount is the number of those components currently ready.
	ReadyCount int32 `json:"readyCount"`
	// +kubebuilder:validation:Enum=ready;progressing;empty;uncovered
	State WorkloadState `json:"state"`
	// Message contains a short diagnostic when the workload is not ready.
	// +optional
	Message string `json:"message,omitempty"`
}

type RBLNComponentStatus struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	// WorkloadType is the workload group this component serves ("container" or "vm-passthrough").
	// +optional
	WorkloadType string         `json:"workloadType,omitempty"`
	State        ComponentState `json:"state"`
	// Desired is the DaemonSet's spec.DesiredNumberScheduled at the time of observation.
	// +optional
	Desired int32 `json:"desired"`
	// Ready is the DaemonSet's spec.NumberReady at the time of observation.
	// +optional
	Ready int32 `json:"ready"`
	// Message contains details about why the component is not ready.
	// +optional
	Message string `json:"message,omitempty"`
}

// DriverUpgradeState values persisted in .status.driverUpgrade.state.
const (
	// DriverUpgradeStateInProgress: nodes are pending or moving through the pipeline.
	DriverUpgradeStateInProgress = "InProgress"
	// DriverUpgradeStateDegraded: upgrade-failed nodes exist; each holds an
	// upgrade slot, so rollout parallelism shrinks until they are resolved.
	DriverUpgradeStateDegraded = "Degraded"
	// DriverUpgradeStatePartiallyComplete: the rollout finished but skipped
	// nodes still run the old driver.
	DriverUpgradeStatePartiallyComplete = "PartiallyComplete"
	// DriverUpgradeStateComplete: every managed node runs the target driver.
	DriverUpgradeStateComplete = "Complete"
)

// DriverUpgradeStatus summarizes the cluster-wide driver upgrade rollout.
type DriverUpgradeStatus struct {
	// Total is the number of nodes managed by the driver upgrade flow.
	Total int32 `json:"total"`
	// Done is the number of nodes whose upgrade is complete.
	Done int32 `json:"done"`
	// InProgress is the number of nodes currently moving through the upgrade
	// pipeline (excluding skipped and failed nodes).
	InProgress int32 `json:"inProgress"`
	// Pending is the number of nodes waiting for an upgrade slot.
	Pending int32 `json:"pending"`
	// Skipped is the number of nodes this rollout could not empty before the
	// driver swap; they were returned to service on the old driver and never
	// count into Done — see the UpgradeIncomplete condition.
	Skipped int32 `json:"skipped"`
	// Failed is the number of nodes parked in upgrade-failed. Each failed node
	// holds an upgrade slot — see the UpgradeDegraded condition.
	Failed int32 `json:"failed"`
	// State is the rollout-level disposition.
	// +kubebuilder:validation:Enum=InProgress;Degraded;PartiallyComplete;Complete
	// +optional
	State string `json:"state,omitempty"`
	// Progress is a human-readable summary used by printcolumns, e.g.
	// "38/40 (2 skipped)". Skipped nodes are never folded into the done count.
	// +optional
	Progress string `json:"progress,omitempty"`
	// LastTransitionTime is when the per-state node counts last changed; a
	// long-unchanged value while nodes are in progress indicates a stalled
	// rollout (surfaced via the UpgradeStalled condition).
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
}

// RBLNClusterPolicyStatus defines the observed state of RBLNClusterPolicy
type RBLNClusterPolicyStatus struct {
	// +kubebuilder:validation:Enum=ready;notReady;ignored
	// +optional
	// State indicates the overall status of the RBLNClusterPolicy.
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="State",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	State string `json:"state,omitempty"`
	// Namespace is the namespace where the operator manages its operands.
	// +optional
	Namespace string `json:"namespace,omitempty"`
	// ObservedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Workloads is the aggregate readiness summary for each workload type.
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Workloads",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	Workloads []RBLNWorkloadStatus `json:"workloads,omitempty"`
	// Components is a list of components and their status
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Components",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	Components []RBLNComponentStatus `json:"components,omitempty"`
	// DriverUpgrade summarizes the driver upgrade rollout across managed nodes.
	// +optional
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Driver Upgrade",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	DriverUpgrade *DriverUpgradeStatus `json:"driverUpgrade,omitempty"`
	// Conditions is a list of conditions representing the RBLNClusterPolicy's current state
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Conditions",xDescriptors="urn:alm:descriptor:io.kubernetes.conditions"
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}
