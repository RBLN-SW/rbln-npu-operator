/*
Copyright 2026 Rebellions Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1beta1

// RBLNRDSSpec configures RDS (Rebellions Direct Storage): NPU direct-DMA access
// to a node's spare NVMe SSD, exposed to containers as /dev/rblnfs*.
type RBLNRDSSpec struct {
	// Enabled provisions a dedicated driver pool that binds RDS on nodes
	// labeled rebellions.ai/rds.present=true. Binding formats the node's spare
	// NVMe, so only explicitly-labeled nodes are targeted.
	// +kubebuilder:default:=false
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Enable RDS binding",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	Enabled bool `json:"enabled"`

	// DeviceSelection restricts, per node, which NVMe PCI devices RDS binds
	// (an allowlist written to /etc/rebellions/rblnfs-bind.conf as TARGET_BDFS).
	// Omit → every rds.present node uses the bind script's default auto opt-out.
	// +optional
	// +listType=map
	// +listMapKey=nodeName
	DeviceSelection []RBLNRDSDeviceSelection `json:"deviceSelection,omitempty"`
}

// RBLNRDSDeviceSelection pins the RDS bind allowlist for one node.
type RBLNRDSDeviceSelection struct {
	// NodeName targets a node by its .metadata.name.
	// +kubebuilder:validation:MinLength=1
	NodeName string `json:"nodeName"`

	// TargetBDFs is the allowlist of PCI BDFs RDS may bind on that node.
	// +kubebuilder:validation:MinItems=1
	TargetBDFs []PCIDeviceAddress `json:"targetBDFs"`
}

// PCIDeviceAddress is a PCI address in BDF notation, e.g. "0000:d8:00.0".
// +kubebuilder:validation:Pattern=`^[0-9a-fA-F]{4}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}\.[0-7]$`
type PCIDeviceAddress string

func (s RBLNRDSSpec) IsEnabled() bool { return s.Enabled }
