package v1beta1

// DriverSpec defines the properties for RBLN Driver deployment
type DriverSpec struct {
	// UseRBLNDriverCRD controls whether driver lifecycle is managed by RBLNDriver resources.
	// When true, the operator expects RBLNDriver CRs to exist and does not rely on ClusterPolicy-only management.
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors=true
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors.displayName="Enable RBLN Driver deployment through RBLNDriver CRD type"
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors.x-descriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	UseRBLNDriverCRD *bool `json:"useRBLNDriverCRD,omitempty"`

	// UpgradePolicy defines automatic upgrade behavior for the driver rollout.
	// +optional
	UpgradePolicy *DriverUpgradePolicySpec `json:"upgradePolicy,omitempty"`
}

// DriverUpgradePolicySpec describes policy configuration for automatic upgrades
type DriverUpgradePolicySpec struct {
	// AutoUpgrade enables/disables the automatic upgrade workflow.
	// If false, other upgrade policy fields are ignored.
	// +optional
	// +kubebuilder:default:=false
	AutoUpgrade bool `json:"autoUpgrade,omitempty"`
	// MaxParallelUpgrades indicates how many nodes can be upgraded in parallel
	// 0 means no limit, all nodes will be upgraded in parallel
	// +optional
	// +kubebuilder:default:=1
	// +kubebuilder:validation:Minimum:=0
	MaxParallelUpgrades int `json:"maxParallelUpgrades,omitempty"`
	// +optional
	PodDeletion *PodDeletionSpec `json:"podDeletion,omitempty"`
	// +optional
	WaitForCompletion *WaitForCompletionSpec `json:"waitForCompletion,omitempty"`
	// +optional
	DrainSpec *DrainSpec `json:"drain,omitempty"`
	// Reboot describes reboot workflow configuration.
	// +optional
	Reboot *RebootSpec `json:"reboot,omitempty"`
}

// RebootSpec describes reboot behavior during automatic upgrade.
type RebootSpec struct {
	// Enable indicates if node reboot workflow is enabled.
	// +optional
	// +kubebuilder:default:=false
	Enable bool `json:"enable,omitempty"`
	// RebootTimeoutSeconds specifies the length of time in seconds to wait for reboot validation.
	// 0 means infinite wait.
	// +optional
	// +kubebuilder:default:=0
	// +kubebuilder:validation:Minimum:=0
	RebootTimeoutSeconds int `json:"rebootTimeoutSeconds,omitempty"`
	// Image specifies reboot trigger pod image configuration.
	// +optional
	Image *RebootImageSpec `json:"image,omitempty"`
}

// RebootImageSpec describes container image coordinates for reboot trigger pod.
type RebootImageSpec struct {
	// +optional
	Registry string `json:"registry,omitempty"`
	// +optional
	Image string `json:"image,omitempty"`
	// +optional
	Version string `json:"version,omitempty"`
}

// PodDeletionSpec describes pod deletion behavior during automatic upgrade.
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

// DrainSpec describes configuration for node drain during automatic upgrade
type DrainSpec struct {
	// Enable indicates if node draining is allowed during upgrade
	// +optional
	// +kubebuilder:default:=false
	Enable bool `json:"enable,omitempty"`
	// Force indicates if force draining is allowed
	// +optional
	// +kubebuilder:default:=false
	Force bool `json:"force,omitempty"`
	// DeleteEmptyDirData indicates whether to allow deleting pods that use emptyDir/local ephemeral storage
	// during drain.
	// +optional
	// +kubebuilder:default:=false
	DeleteEmptyDirData bool `json:"deleteEmptyDirData,omitempty"`
	// PodSelector specifies a label selector to filter pods on the node that need to be drained
	// For more details on label selectors, see:
	// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +optional
	PodSelector string `json:"podSelector,omitempty"`
	// TimeoutSeconds specifies the length of time in seconds to wait before giving up drain, zero means infinite
	// +optional
	// +kubebuilder:default:=300
	// +kubebuilder:validation:Minimum:=0
	TimeoutSeconds int `json:"timeoutSeconds,omitempty"`
}
