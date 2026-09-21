package v1beta1

// DriverSpec defines the properties for RBLN Driver deployment
type DriverSpec struct {
	// UpgradePolicy defines automatic upgrade behavior for the driver rollout.
	// +optional
	UpgradePolicy *DriverUpgradePolicySpec `json:"upgradePolicy,omitempty"`
}

// DriverUpgradePolicySpec describes policy configuration for automatic upgrades
type DriverUpgradePolicySpec struct {
	// AutoUpgrade enables/disables the automatic upgrade workflow.
	// If false, other upgrade policy fields are ignored, except podDeletion:
	// with the workflow off, k8s-driver-manager empties the node itself on a
	// driver pod restart and obeys that block.
	// +optional
	// +kubebuilder:default:=false
	AutoUpgrade bool `json:"autoUpgrade,omitempty"`
	// MaxParallelUpgrades indicates how many nodes can be upgraded in parallel
	// 0 means no limit, all nodes will be upgraded in parallel
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
	// Force indicates if force deletion is allowed
	// +optional
	// +kubebuilder:default:=false
	Force bool `json:"force,omitempty"`
	// TimeoutSeconds specifies the length of time in seconds to wait before giving up on pod termination, zero means
	// infinite
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

// WaitForCompletionSpec describes the configuration for waiting on job completions
type WaitForCompletionSpec struct {
	// PodSelector specifies a label selector for the pods to wait for completion
	// For more details on label selectors, see:
	// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +optional
	PodSelector string `json:"podSelector,omitempty"`
	// TimeoutSeconds specifies the length of time in seconds to wait before giving up on pod termination, zero means
	// infinite
	// +optional
	// +kubebuilder:default:=0
	// +kubebuilder:validation:Minimum:=0
	TimeoutSeconds int `json:"timeoutSeconds,omitempty"`
}
