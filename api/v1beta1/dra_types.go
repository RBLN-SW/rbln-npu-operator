package v1beta1

// RBLNDRAKubeletPluginSpec defines the desired state of RBLN DRA kubelet plugin
type RBLNDRAKubeletPluginSpec struct {
	// Enabled indicates if deployment of RBLN DRA kubelet plugin is enabled
	// +kubebuilder:default:=false
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Enable RBLN DRA Kubelet Plugin deployment",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	Enabled bool `json:"enabled,omitempty"`

	// RBLN DRA kubelet plugin image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions/k8s-dra-driver-npu
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Image string `json:"image,omitempty"`

	// Registry override for the RBLN DRA kubelet plugin image
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=docker.io
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Registry",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Registry string `json:"registry,omitempty"`

	// RBLN DRA kubelet plugin image tag
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=latest
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Version",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Version string `json:"version"`

	// PodSpec defines common DaemonSet configurations
	// +kubebuilder:validation:Optional
	PodSpec `json:",inline"`

	// PriorityClassName specifies the priority class for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="system-node-critical"
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="PriorityClassName",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// DriverName is the DRA driver name used by kubelet plugin registration
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=npu.rebellions.ai
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Driver Name",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	DriverName string `json:"driverName,omitempty"`

	// KubeletRegistrarDirectoryPath is the host path for kubelet plugin registration
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=/var/lib/kubelet/plugins_registry
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Kubelet Registrar Directory Path",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	KubeletRegistrarDirectoryPath string `json:"kubeletRegistrarDirectoryPath,omitempty"`

	// KubeletPluginsDirectoryPath is the host path for kubelet plugin sockets
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=/var/lib/kubelet/plugins
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Kubelet Plugins Directory Path",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	KubeletPluginsDirectoryPath string `json:"kubeletPluginsDirectoryPath,omitempty"`

	// HealthcheckPort is the gRPC liveness port. Set to <=0 to disable liveness probe.
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=51515
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Healthcheck Port",xDescriptors="urn:alm:descriptor:com.tectonic.ui:number"
	HealthcheckPort int32 `json:"healthcheckPort,omitempty"`
}

func (s RBLNDRAKubeletPluginSpec) IsEnabled() bool { return s.Enabled }
