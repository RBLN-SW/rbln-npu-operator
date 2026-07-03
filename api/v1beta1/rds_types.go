package v1beta1

// RBLNRDSSpec configures RDS (Rebellions Device Storage): NPU direct-DMA access
// to a node's spare NVMe SSD, exposed to containers as /dev/rblnfs*.
type RBLNRDSSpec struct {
	// Enabled provisions a dedicated driver pool that binds RDS on nodes
	// labeled rebellions.ai/rds.present=true. Binding formats the node's spare
	// NVMe, so only explicitly-labeled nodes are targeted.
	// +kubebuilder:default:=false
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Enable RDS binding",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	Enabled bool `json:"enabled"`
}

func (s RBLNRDSSpec) IsEnabled() bool { return s.Enabled }
