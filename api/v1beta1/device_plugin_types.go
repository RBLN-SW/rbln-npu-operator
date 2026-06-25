package v1beta1

// RBLNDevicePluginSpec defines the desired state of RBLNDevicePlugin
type RBLNDevicePluginSpec struct {
	// Enabled indicates if deployment of RBLN device plugin is enabled
	// +kubebuilder:default:=true
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Enable RBLN Device Plugin deployment",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	Enabled bool `json:"enabled"`

	// RBLN Device Plugin image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions/k8s-device-plugin
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Image string `json:"image,omitempty"`

	// Registry override for the RBLN Device Plugin image
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=docker.io
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Registry",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Registry string `json:"registry,omitempty"`

	// RBLN Device Plugin image tag
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=latest
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Version",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Version string `json:"version,omitempty"`

	// UseGenericResourceName advertises the generic resource name (rebellions.ai/npu) in
	// addition to product-specific ones. Forwarded to the device plugin container as the
	// USE_GENERIC_RESOURCE_NAME environment variable.
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=true
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Use Generic Resource Name",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	UseGenericResourceName bool `json:"useGenericResourceName"`

	// PodSpec defines common DaemonSet configurations
	// +kubebuilder:validation:Optional
	PodSpec `json:",inline"`
}

// RBLNSandboxDevicePluginSpec defines the desired state of RBLNSandboxDevicePlugin
type RBLNSandboxDevicePluginSpec struct {
	// Enabled indicates if deployment of RBLN sandbox device plugin is enabled
	// +kubebuilder:default:=true
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Enable RBLN Sandbox Device Plugin deployment",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	Enabled bool `json:"enabled"`

	// RBLN Sandbox Device Plugin image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions/k8s-device-plugin
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Image string `json:"image,omitempty"`

	// Registry override for the RBLN Sandbox Device Plugin image
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=docker.io
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Registry",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Registry string `json:"registry,omitempty"`

	// RBLN Sandbox Device Plugin image tag
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="latest"
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Version",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Version string `json:"version,omitempty"`

	// PodSpec defines common DaemonSet configurations
	// +kubebuilder:validation:Optional
	PodSpec `json:",inline"`
}

func (s RBLNDevicePluginSpec) IsEnabled() bool        { return s.Enabled }
func (s RBLNSandboxDevicePluginSpec) IsEnabled() bool { return s.Enabled }
