package v1beta1

import corev1 "k8s.io/api/core/v1"

// RBLNMetricsExporterSpec defines the desired state of RBLNMetricsExporter
type RBLNMetricsExporterSpec struct {
	// Enabled indicates if deployment of RBLN metrics exporter is enabled
	// +kubebuilder:default:=true
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Enable RBLN Metrics Exporter deployment",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	Enabled bool `json:"enabled"`

	// RBLN Metrics Exporter image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions/rbln-metrics-exporter
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Image string `json:"image,omitempty"`

	// Registry override for the RBLN Metrics Exporter image
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=docker.io
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Registry",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Registry string `json:"registry,omitempty"`

	// RBLN Metrics Exporter image tag
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=latest
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Version",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Version string `json:"version,omitempty"`

	// PodSpec defines common DaemonSet configurations
	// +kubebuilder:validation:Optional
	PodSpec `json:",inline"`
}

// RBLNDaemonSpec defines the desired state of RBLN Daemon
type RBLNDaemonSpec struct {
	// Enabled indicates if deployment of RBLN daemon is enabled
	// +kubebuilder:default:=true
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Enable RBLN Daemon deployment",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	Enabled bool `json:"enabled"`

	// RBLN Daemon image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions/rbln-daemon
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Image string `json:"image,omitempty"`

	// Registry override for the RBLN Daemon image
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=docker.io
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Registry",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Registry string `json:"registry,omitempty"`

	// RBLN Daemon image tag
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=latest
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Version",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Version string `json:"version,omitempty"`

	// PodSpec defines common DaemonSet configurations
	// +kubebuilder:validation:Optional
	PodSpec `json:",inline"`

	// Optional: List of environment variables
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Environment Variables",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced,urn:alm:descriptor:com.tectonic.ui:text"
	Env []corev1.EnvVar `json:"env,omitempty"`

	// HostPort represents host port that needs to be bound for rbln-daemon (Default: 50051)
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Host port to bind for rbln-daemon",xDescriptors="urn:alm:descriptor:com.tectonic.ui:number"
	// +kubebuilder:default:=50051
	HostPort int32 `json:"hostPort,omitempty"`
}

func (s RBLNDaemonSpec) IsEnabled() bool          { return s.Enabled }
func (s RBLNMetricsExporterSpec) IsEnabled() bool { return s.Enabled }
