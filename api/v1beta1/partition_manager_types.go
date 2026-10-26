package v1beta1

import corev1 "k8s.io/api/core/v1"

// RBLNPartitionManagerSpec defines the desired state of RBLN Partition Manager
type RBLNPartitionManagerSpec struct {
	// Enabled indicates if deployment of RBLN partition manager is enabled
	// +kubebuilder:default:=false
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Enable RBLN Partition Manager deployment",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	Enabled bool `json:"enabled"`

	// RBLN Partition Manager image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions/k8s-partition-manager
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Image string `json:"image,omitempty"`

	// Registry override for the RBLN Partition Manager image
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=docker.io
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Registry",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Registry string `json:"registry,omitempty"`

	// RBLN Partition Manager image tag
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=latest
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Version",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Version string `json:"version,omitempty"`

	// Env specifies additional environment variables for the partition manager container
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Environment Variables",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	Env []corev1.EnvVar `json:"env,omitempty"`

	// PodSpec defines common DaemonSet configurations
	// +kubebuilder:validation:Optional
	PodSpec `json:",inline"`
}

func (s RBLNPartitionManagerSpec) IsEnabled() bool { return s.Enabled }
