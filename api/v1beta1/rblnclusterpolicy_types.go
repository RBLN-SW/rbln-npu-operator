/*
Copyright 2025.

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

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RBLNClusterPolicySpec defines the desired state of RBLNClusterPolicy
// +kubebuilder:object:generate=true
type RBLNClusterPolicySpec struct {
	// BaseName of rbln components
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rbln
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Base Name",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	BaseName string `json:"name,omitempty"`

	// Namespace of the controller
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rbln-system
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Namespace",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Namespace string `json:"namespace,omitempty"`

	// WorkloadType specifies the type of default workload.
	// +kubebuilder:validation:Enum=container;vm-passthrough
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=container
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Workload Type",xDescriptors="urn:alm:descriptor:com.tectonic.ui:select:container,urn:alm:descriptor:com.tectonic.ui:select:vm-passthrough"
	WorkloadType string `json:"workloadType"`

	// DaemonSets is common spec of rbln daemonset components
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Daemonsets",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	Daemonsets *DaemonsetsSpec `json:"daemonsets,omitempty"`

	// VFIOManager component spec
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="VFIO Manager",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	VFIOManager RBLNVFIOManagerSpec `json:"vfioManager"`

	// SandboxDevicePlugin component spec
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Sandbox Device Plugin",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	SandboxDevicePlugin RBLNSandboxDevicePluginSpec `json:"sandboxDevicePlugin"`

	// DevicePlugin component spec
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Device Plugin",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	DevicePlugin RBLNDevicePluginSpec `json:"devicePlugin"`

	// DRAKubeletPlugin component spec
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="DRA Kubelet Plugin",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	DRAKubeletPlugin RBLNDRAKubeletPluginSpec `json:"draKubeletPlugin,omitempty"`

	// MetricsExporter component spec
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Metrics Exporter",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	MetricsExporter RBLNMetricsExporterSpec `json:"metricsExporter"`

	// RBLN Daemon component spec
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="RBLN Daemon",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	RBLNDaemon RBLNDaemonSpec `json:"rblnDaemon"`

	// NPUFeatureDiscovery component spec
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="NPU Feature Discovery",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	NPUFeatureDiscovery RBLNNPUFeatureDiscoverySpec `json:"npuFeatureDiscovery"`

	// ContainerToolkit component spec
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Container Toolkit",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	ContainerToolkit RBLNContainerToolkitSpec `json:"containerToolkit"`

	// Validator defines the spec for operator-validator daemonset
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Validator",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	Validator ValidatorSpec `json:"validator,omitempty"`

	// Driver component spec
	Driver DriverSpec `json:"driver"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +operator-sdk:csv:customresourcedefinitions:resources={{DaemonSet,v1,apps},{ConfigMap,v1,""},{Service,v1,""},{ServiceAccount,v1,""},{ClusterRole,v1,rbac.authorization.k8s.io},{ClusterRoleBinding,v1,rbac.authorization.k8s.io}}

// RBLNClusterPolicy is the Schema for the RBLNClusterPolicys API
type RBLNClusterPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RBLNClusterPolicySpec   `json:"spec,omitempty"`
	Status RBLNClusterPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName={"rcp","rblncp"}

// RBLNClusterPolicyList contains a list of RBLNClusterPolicy
type RBLNClusterPolicyList struct {
	metav1.TypeMeta `json:",,inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RBLNClusterPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RBLNClusterPolicy{}, &RBLNClusterPolicyList{})
}
