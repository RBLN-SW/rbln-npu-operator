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
	corev1 "k8s.io/api/core/v1"
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

	// MetricsExporter component spec
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Metrics Exporter",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	MetricsExporter RBLNMetricsExporterSpec `json:"metricsExporter"`

	// NPUFeatureDiscovery component spec
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="NPU Feature Discovery",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	NPUFeatureDiscovery RBLNNPUFeatureDiscoverySpec `json:"npuFeatureDiscovery"`
}

// DaemonsetsSpec indicates common configuration for all Daemonsets managed by RBLN NPU Operator
type DaemonsetsSpec struct {
	// Labels specifies the labels for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Labels",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations specifies the annotations for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Annotations",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Annotations map[string]string `json:"annotations,omitempty"`

	// Affinity specifies the affinity for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Affinity",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced,urn:alm:descriptor:io.kubernetes:Affinity"
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// Tolerations specifies the tolerations for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Tolerations",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced,urn:alm:descriptor:io.kubernetes:Tolerations"
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// PriorityClassName specifies the priority class for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="PriorityClassName",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	PriorityClassName string `json:"priorityClassName,omitempty"`
}

// PodSpec defines common configuration for individual DaemonSet components
type PodSpec struct {
	// ImagePullPolicy specifies the image pull policy for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=IfNotPresent
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image Pull Policy",xDescriptors="urn:alm:descriptor:com.tectonic.ui:imagePullPolicy"
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// ImagePullSecrets specifies the image pull secrets for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image pull secrets",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	ImagePullSecrets []string `json:"imagePullSecrets,omitempty"`

	// Resources specifies the resource requirements for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Resource Requirements",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced,urn:alm:descriptor:com.tectonic.ui:resourceRequirements"
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Labels specifies the labels for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Labels",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations specifies the annotations for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Annotations",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Annotations map[string]string `json:"annotations,omitempty"`

	// Affinity specifies the affinity for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Affinity",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced,urn:alm:descriptor:io.kubernetes:Affinity"
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// Tolerations specifies the tolerations for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Tolerations",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced,urn:alm:descriptor:io.kubernetes:Tolerations"
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`
}

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

// RBLNSandboxDevicePluginSpec defines the desired state of RBLNSandboxDevicePlugin
type RBLNSandboxDevicePluginSpec struct {
	// Enabled indicates if deployment of RBLN sandbox device plugin is enabled
	// +kubebuilder:default:=true
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Enable RBLN Sandbox Device Plugin deployment",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	Enabled bool `json:"enabled,omitempty"`

	// RBLN Sandbox Device Plugin image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions/k8s-device-plugin
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Image string `json:"image,omitempty"`

	// RBLN Sandbox Device Plugin image tag
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="latest"
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Version",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Version string `json:"version,omitempty"`

	// PodSpec defines common DaemonSet configurations
	// +kubebuilder:validation:Optional
	PodSpec `json:",inline"`

	// VFIOChecker specifies the configuration for the VFIO bind status checker
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="VFIO Checker",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	VFIOChecker VFIOCheckerSpec `json:"vfioChecker,omitempty"`

	// PriorityClassName specifies the priority class for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="system-node-critical"
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="PriorityClassName",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// ResourceList is the list of resources to be managed by the device plugin
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:={{resourceName:ATOM,resourcePrefix:rebellions.ai,productCardNames:{RBLN-CA12,RBLN-CA22,RBLN-CA25}}}
	ResourceList []RBLNDevicePluginResourceSpec `json:"resourceList"`
}

type VFIOCheckerSpec struct {
	// VFIO Checker image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions/rbln-vfio-manager
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Image string `json:"image,omitempty"`

	// VFIO Checker image tag
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="latest"
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Version",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Version string `json:"version,omitempty"`
}

type RBLNDevicePluginResourceSpec struct {
	// ResourceName is the name of the resource
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=ATOM
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Resource Name",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	ResourceName string `json:"resourceName,omitempty"`

	// ResourcePrefix is the prefix of the resource
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions.ai
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Resource Prefix",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	ResourcePrefix string `json:"resourcePrefix,omitempty"`

	// ProductCardNames is the name of the product card
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:={RBLN-CA12,RBLN-CA22,RBLN-CA25}
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Product Card Names",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	ProductCardNames []string `json:"productCardNames"`
}

// RBLNDevicePluginSpec defines the desired state of RBLNDevicePlugin
type RBLNDevicePluginSpec struct {
	// Enabled indicates if deployment of RBLN device plugin is enabled
	// +kubebuilder:default:=true
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Enable RBLN Device Plugin deployment",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	Enabled bool `json:"enabled,omitempty"`

	// RBLN Device Plugin image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions/k8s-device-plugin
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Image string `json:"image,omitempty"`

	// RBLN Device Plugin image tag
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=latest
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Version",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Version string `json:"version"`

	// PodSpec defines common DaemonSet configurations
	// +kubebuilder:validation:Optional
	PodSpec `json:",inline"`

	// HostBinPath specifies the host directory that contains binaries required by the device plugin
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=/usr/bin
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Host binary path",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	HostBinPath string `json:"hostBinPath,omitempty"`

	// PriorityClassName specifies the priority class for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="PriorityClassName",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// ResourceList is the list of resources to be managed by the device plugin
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:={{resourceName:ATOM,resourcePrefix:rebellions.ai,productCardNames:{RBLN-CA12,RBLN-CA22,RBLN-CA25}}}
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Resource List",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	ResourceList []RBLNDevicePluginResourceSpec `json:"resourceList"`
}

// RBLNMetricsExporterSpec defines the desired state of RBLNMetricsExporter
type RBLNMetricsExporterSpec struct {
	// Enabled indicates if deployment of RBLN metrics exporter is enabled
	// +kubebuilder:default:=true
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Enable RBLN Metrics Exporter deployment",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	Enabled bool `json:"enabled,omitempty"`

	// RBLN Metrics Exporter image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions/rbln-metrics-exporter
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Image string `json:"image,omitempty"`

	// RBLN Metrics Exporter image tag
	// +kubebuilder:default:=latest
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Version",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Version string `json:"version"`

	// PodSpec defines common DaemonSet configurations
	// +kubebuilder:validation:Optional
	PodSpec `json:",inline"`

	// PriorityClassName specifies the priority class for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="PriorityClassName",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	PriorityClassName string `json:"priorityClassName,omitempty"`
}

// RBLNNPUFeatureDiscoverySpec defines the desired state of RBLNNPUFeatureDiscovery
type RBLNNPUFeatureDiscoverySpec struct {
	// Enabled indicates if deployment of RBLN NPU feature discovery is enabled
	// +kubebuilder:default:=true
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Enable RBLN NPU Feature Discovery deployment",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	Enabled bool `json:"enabled,omitempty"`

	// RBLN NPU Feature Discovery image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions/rbln-npu-feature-discovery
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Image string `json:"image,omitempty"`

	// RBLN NPU Feature Discovery image tag
	// +kubebuilder:default:=latest
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Version",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Version string `json:"version"`

	// PodSpec defines common DaemonSet configurations
	// +kubebuilder:validation:Optional
	PodSpec `json:",inline"`

	// PriorityClassName specifies the priority class for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="PriorityClassName",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	PriorityClassName string `json:"priorityClassName,omitempty"`
}

// IsEnabled implementations for component specs
func (s RBLNVFIOManagerSpec) IsEnabled() bool         { return s.Enabled }
func (s RBLNDevicePluginSpec) IsEnabled() bool        { return s.Enabled }
func (s RBLNMetricsExporterSpec) IsEnabled() bool     { return s.Enabled }
func (s RBLNNPUFeatureDiscoverySpec) IsEnabled() bool { return s.Enabled }
func (s RBLNSandboxDevicePluginSpec) IsEnabled() bool { return s.Enabled }

type ClusterState string

type NodeState string

type ComponentState string

const (
	// Ready indicates RBLNClusterPolicy are ready
	ClusterReady ClusterState = "ready"
	// NotReady indicates RBLNClusterPolicy are not ready
	ClusterNotReady ClusterState = "notReady"
	// ClusterIgnored indicates any additional ClusterPolicies are ignored once the singleton already exists
	ClusterIgnored ClusterState = "ignored"
)

const (
	ComponentStateReady    ComponentState = "ready"
	ComponentStateNotReady ComponentState = "notReady"
)

type RBLNComponentStatus struct {
	Name      string             `json:"name"`
	Namespace string             `json:"namespace"`
	State     ComponentState     `json:"state"`
	Condition []metav1.Condition `json:"condition,omitempty"`
}

// RBLNClusterPolicyStatus defines the observed state of RBLNClusterPolicy
type RBLNClusterPolicyStatus struct {
	// +kubebuilder:validation:Enum=ready;notReady
	// +optional
	// State indicates status of ClusterPolicy
	State ClusterState `json:"state,omitempty"`
	// Components is a list of components and their status
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Components",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	Components []RBLNComponentStatus `json:"components,omitempty"`
	// Conditions is a list of conditions representing the RBLNClusterPolicy's current state
	// +operator-sdk:csv:customresourcedefinitions:type=status,displayName="Conditions",xDescriptors="urn:alm:descriptor:io.kubernetes.conditions"
	Conditions []metav1.Condition `json:"conditions,omitempty"`
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

// SetStatus sets state of ClusterPolicy instance
func (p *RBLNClusterPolicy) SetStatus(s ClusterState) {
	p.Status.State = s
}
