package v1beta1

import corev1 "k8s.io/api/core/v1"

// RBLNVFIOManagerSpec defines the desired state of RBLNVFIOManager
type RBLNVFIOManagerSpec struct {
	// Enabled indicates if deployment of RBLN VFIO manager is enabled
	// +kubebuilder:default:=true
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Enable RBLN VFIO Manager deployment",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	Enabled bool `json:"enabled"`

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

	// DriverManager configures the rbln-k8s-driver-manager init container that
	// evicts operator-owned pods on the node before the main container binds
	// devices to vfio-pci. Mirrors NVIDIA gpu-operator's driver-manager init.
	// +kubebuilder:validation:Optional
	DriverManager VFIODriverManagerSpec `json:"driverManager,omitempty"`

	// PodSpec defines common DaemonSet configurations
	// +kubebuilder:validation:Optional
	PodSpec `json:",inline"`
}

// VFIODriverManagerSpec configures the init container used by vfio-manager to
// drain operator-owned pods (rbln-daemon, device-plugin, container-toolkit,
// etc.) on the node before vfio-pci binding takes over.
type VFIODriverManagerSpec struct {
	// Image name of the rbln-k8s-driver-manager container.
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions/rbln-k8s-driver-manager
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	Image string `json:"image,omitempty"`

	// Registry hosting the rbln-k8s-driver-manager image.
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=docker.io
	Registry string `json:"registry,omitempty"`

	// Version (tag) of the rbln-k8s-driver-manager image.
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="latest"
	Version string `json:"version,omitempty"`

	// ImagePullPolicy applied to the init container.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Enum=Always;IfNotPresent;Never
	// +kubebuilder:default:=IfNotPresent
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// Env vars passed to the init container (e.g., DRIVER_MANAGER_TIMEOUT).
	// +kubebuilder:validation:Optional
	Env []corev1.EnvVar `json:"env,omitempty"`
}

func (s RBLNVFIOManagerSpec) IsEnabled() bool { return s.Enabled }
