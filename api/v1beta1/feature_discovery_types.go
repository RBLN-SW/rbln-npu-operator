package v1beta1

// RBLNNPUFeatureDiscoverySpec defines the desired state of RBLNNPUFeatureDiscovery
type RBLNNPUFeatureDiscoverySpec struct {
	// Enabled indicates if deployment of RBLN NPU feature discovery is enabled
	// +kubebuilder:default:=true
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Enable RBLN NPU Feature Discovery deployment",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	Enabled bool `json:"enabled"`

	// RBLN NPU Feature Discovery image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions/rbln-npu-feature-discovery
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Image string `json:"image,omitempty"`

	// Registry override for the RBLN NPU Feature Discovery image
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=docker.io
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Registry",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Registry string `json:"registry,omitempty"`

	// RBLN NPU Feature Discovery image tag
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=latest
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Version",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Version string `json:"version,omitempty"`

	// Logging gates NPU feature discovery's own log stream, forwarded to the
	// container as RBLN_NPU_FEATURE_DISCOVERY_LOG_LEVEL / _LOG_FORMAT.
	// +kubebuilder:validation:Optional
	Logging *LoggingSpec `json:"logging,omitempty"`

	// PodSpec defines common DaemonSet configurations
	// +kubebuilder:validation:Optional
	PodSpec `json:",inline"`
}

func (s RBLNNPUFeatureDiscoverySpec) IsEnabled() bool { return s.Enabled }
