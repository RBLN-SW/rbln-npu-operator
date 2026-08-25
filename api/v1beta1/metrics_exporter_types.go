package v1beta1

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

	// Logging gates the metrics exporter's own log stream. Unlike the other
	// operands it reads the unprefixed LOG_LEVEL / LOG_FORMAT pair.
	// +kubebuilder:validation:Optional
	Logging *LoggingSpec `json:"logging,omitempty"`

	// PodSpec defines common DaemonSet configurations
	// +kubebuilder:validation:Optional
	PodSpec `json:",inline"`
}

func (s RBLNMetricsExporterSpec) IsEnabled() bool { return s.Enabled }
