package v1beta1

// DriverSpec holds the driver rollout settings of an RBLNClusterPolicy.
type DriverSpec struct {
	// UpgradePolicy defines automatic upgrade behavior for the driver rollout.
	// +optional
	UpgradePolicy *DriverUpgradePolicySpec `json:"upgradePolicy,omitempty"`
}

// DriverUpgradePolicySpec controls how the operator rolls a new driver version
// across the nodes of the cluster.
type DriverUpgradePolicySpec struct {
	// AutoUpgrade enables/disables the automatic upgrade workflow.
	// If false, other upgrade policy fields are ignored, except podDeletion:
	// with the workflow off, k8s-driver-manager empties the node itself on a
	// driver pod restart and obeys that block.
	// +optional
	// +kubebuilder:default:=false
	AutoUpgrade bool `json:"autoUpgrade,omitempty"`
	// MaxParallelUpgrades caps the number of nodes that may be mid-upgrade at the
	// same time. Zero removes the cap so every eligible node proceeds at once.
	// +optional
	// +kubebuilder:default:=1
	// +kubebuilder:validation:Minimum:=0
	MaxParallelUpgrades int `json:"maxParallelUpgrades,omitempty"`
	// PodRestartTimeoutSeconds bounds how long a node may stay in the
	// pod-restart-required state (driver pod replacement). Failures that never
	// raise the pod restart count — ImagePullBackOff, a hung init container —
	// otherwise wait silently forever. On expiry the node is marked
	// upgrade-failed with the pod status recorded as the failure reason.
	// 0 disables the timeout.
	// +optional
	// +kubebuilder:default:=1800
	// +kubebuilder:validation:Minimum:=0
	PodRestartTimeoutSeconds int `json:"podRestartTimeoutSeconds,omitempty"`
	// +optional
	PodDeletion *PodDeletionSpec `json:"podDeletion,omitempty"`
	// +optional
	WaitForCompletion *WaitForCompletionSpec `json:"waitForCompletion,omitempty"`
}

// PodDeletionSpec describes how NPU pods are evicted from a node before its
// driver is replaced or its NPUs are handed to vfio-pci. It governs every such
// eviction: the operator's own during an automatic upgrade, k8s-driver-manager's
// when autoUpgrade is off, and k8s-driver-manager's before a node is switched to
// the vm-passthrough workload. Only TimeoutSeconds is operator-only —
// k8s-driver-manager has no park state to fall back to, so it waits instead of
// giving up.
type PodDeletionSpec struct {
	// Force also evicts pods that no controller would recreate (bare pods),
	// instead of refusing the eviction because of them.
	// +optional
	// +kubebuilder:default:=false
	Force bool `json:"force,omitempty"`
	// TimeoutSeconds is how long, in seconds, to wait for evicted pods to
	// terminate before the node is parked. Zero waits indefinitely.
	// +optional
	// +kubebuilder:default:=300
	// +kubebuilder:validation:Minimum:=0
	TimeoutSeconds int `json:"timeoutSeconds,omitempty"`
	// DeleteEmptyDirData allows evicting NPU pods that mount emptyDir volumes,
	// whose contents are lost with the pod. Off by default: such a pod blocks
	// the eviction and parks the node in upgrade-skipped with the pod named in
	// the skip reason. Pods that do not request an NPU are never evicted
	// regardless of this setting.
	// +optional
	// +kubebuilder:default:=false
	DeleteEmptyDirData bool `json:"deleteEmptyDirData,omitempty"`
}

// WaitForCompletionSpec holds a node's upgrade back until the pods matching
// PodSelector have finished, so that running jobs are not cut short.
type WaitForCompletionSpec struct {
	// PodSelector is a Kubernetes label selector. While a pod on the node that
	// matches it is still Running or Pending, the node's upgrade does not start;
	// pods that have already Succeeded or Failed do not hold it back.
	// +optional
	PodSelector string `json:"podSelector,omitempty"`
	// TimeoutSeconds bounds, in seconds, how long to wait for the selected pods
	// to finish. Zero waits indefinitely.
	// +optional
	// +kubebuilder:default:=0
	// +kubebuilder:validation:Minimum:=0
	TimeoutSeconds int `json:"timeoutSeconds,omitempty"`
}
