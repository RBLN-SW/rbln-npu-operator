package v1beta1

// RBLNVFIOManagerSpec defines the desired state of RBLNVFIOManager
type RBLNVFIOManagerSpec struct {
	// Enabled indicates if deployment of RBLN VFIO manager is enabled
	// +kubebuilder:default:=true
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Enable RBLN VFIO Manager deployment",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	Enabled bool `json:"enabled,omitempty"`

	// RBLN VFIO Manager image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions/rbln-vfio-manager
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Image string `json:"image,omitempty"`

	// Registry override for the RBLN VFIO Manager image
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=docker.io
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Registry",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Registry string `json:"registry,omitempty"`

	// RBLN VFIO Manager image tag
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="latest"
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Version",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Version string `json:"version,omitempty"`

	// PodSpec defines common DaemonSet configurations
	// +kubebuilder:validation:Optional
	PodSpec `json:",inline"`

	// PriorityClassName specifies the priority class for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="system-node-critical"
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="PriorityClassName",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	PriorityClassName string `json:"priorityClassName,omitempty"`
}

type VFIOCheckerSpec struct {
	// VFIO Checker image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions/rbln-vfio-manager
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Image string `json:"image,omitempty"`

	// Registry override for the VFIO Checker image
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=docker.io
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Registry",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Registry string `json:"registry,omitempty"`

	// VFIO Checker image tag
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="latest"
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Version",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Version string `json:"version,omitempty"`
}

func (s RBLNVFIOManagerSpec) IsEnabled() bool { return s.Enabled }
