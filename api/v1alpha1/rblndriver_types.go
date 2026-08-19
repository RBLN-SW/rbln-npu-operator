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

package v1alpha1

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type DriverState string

const (
	DriverStateReady    DriverState = "ready"
	DriverStateNotReady DriverState = "notReady"
	DriverStateIgnored  DriverState = "ignored"
)

// DriverPoolState is the per-pool DaemonSet aggregate state.
type DriverPoolState string

const (
	DriverPoolStateReady       DriverPoolState = "ready"
	DriverPoolStateProgressing DriverPoolState = "progressing"
)

// RBLNDriverSpec defines the desired state of RBLNDriver
// +kubebuilder:object:generate=true
type RBLNDriverSpec struct {
	// Registry override for the Rebellions driver container image
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=repo.rebellions.ai
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Registry",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Registry string `json:"registry,omitempty"`

	// Rebellions Driver container image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions/rbln-driver
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Image string `json:"image,omitempty"`

	// Rebellions Driver version
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Version",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Version string `json:"version,omitempty"`

	// ImagePullPolicy specifies the image pull policy for the driver pod
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Enum=Always;IfNotPresent;Never
	// +kubebuilder:default:=IfNotPresent
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors=true
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors.displayName="Image Pull Policy"
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors.x-descriptors="urn:alm:descriptor:com.tectonic.ui:imagePullPolicy"
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// ImagePullSecrets specifies the image pull secrets for the driver pod
	// +kubebuilder:validation:Optional
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors=true
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors.displayName="Image pull secrets"
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors.x-descriptors="urn:alm:descriptor:io.kubernetes:Secret"
	ImagePullSecrets []string `json:"imagePullSecrets,omitempty"`

	// Manager represents configuration for Rebellions Driver Manager initContainer
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:={}
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Driver Manager",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	Manager DriverManagerSpec `json:"manager"`

	// Smd represents configuration for the rbln-smd node daemon deployed
	// alongside the driver. Its image tag always equals Version.
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:={}
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Smd",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	Smd SmdSpec `json:"smd"`

	// NodeSelector specifies a selector for installation of the driver
	// +kubebuilder:validation:Optional
	// +mapType=atomic
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Node Selector",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations specifies the tolerations for the driver pod
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Tolerations",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced,urn:alm:descriptor:io.kubernetes:Tolerations"
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// Annotations specifies the annotations for the driver pod
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Annotations",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Annotations map[string]string `json:"annotations,omitempty"`

	// PriorityClassName specifies the priority class for the driver pod
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="system-node-critical"
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="PriorityClassName",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// Resources specifies the resource requirements for the driver pod
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Resource Requirements",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced,urn:alm:descriptor:com.tectonic.ui:resourceRequirements"
	Resources corev1.ResourceRequirements `json:"resources"`

	// Env specifies environment variables for the driver container
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Environment Variables",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced,urn:alm:descriptor:com.tectonic.ui:text"
	Env []corev1.EnvVar `json:"env,omitempty"`
}

// RBLNDriverPoolStatus reports the per-pool DaemonSet readiness.
type RBLNDriverPoolStatus struct {
	// Name is the pool's DaemonSet name, "<RBLNDriver name>-<pool>", where pool
	// is "<family>-<osID><osVersion>-<sanitizedKernel>".
	Name string `json:"name"`
	// Desired is the DaemonSet's spec.DesiredNumberScheduled.
	Desired int32 `json:"desired"`
	// Ready is the DaemonSet's spec.NumberReady.
	Ready int32 `json:"ready"`
	// +kubebuilder:validation:Enum=ready;progressing
	State DriverPoolState `json:"state"`
}

// RBLNDriverSmdStatus reports the per-CR rbln-smd DaemonSet readiness.
type RBLNDriverSmdStatus struct {
	// Desired is the DaemonSet's status.DesiredNumberScheduled.
	Desired int32 `json:"desired"`
	// Ready is the DaemonSet's status.NumberReady.
	Ready int32 `json:"ready"`
	// +kubebuilder:validation:Enum=ready;progressing
	State DriverPoolState `json:"state"`
}

// RBLNDriverStatus defines the observed state of RBLNDriver
type RBLNDriverStatus struct {
	// +kubebuilder:validation:Enum=ready;notReady;ignored
	// +optional
	// State indicates status of RBLNDriver instance
	State DriverState `json:"state,omitempty"`
	// Namespace is the namespace where the operator manages its operands.
	// +optional
	Namespace string `json:"namespace,omitempty"`
	// DesiredNodes is the sum of DesiredNumberScheduled across all per-pool DaemonSets.
	// +optional
	DesiredNodes int32 `json:"desiredNodes"`
	// ReadyNodes is the sum of NumberReady across all per-pool DaemonSets.
	// +optional
	ReadyNodes int32 `json:"readyNodes"`
	// NodePools reports per-pool DaemonSet readiness; one entry per discovered OS/kernel pool.
	// +optional
	NodePools []RBLNDriverPoolStatus `json:"nodePools"`
	// Smd reports the rbln-smd DaemonSet readiness. Deliberately excluded from
	// DesiredNodes/ReadyNodes, which stay driver-pool sums (external tooling
	// gates on those).
	// +optional
	Smd *RBLNDriverSmdStatus `json:"smd,omitempty"`
	// Conditions is a list of conditions representing the RBLNDriver's current state
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyNodes`
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.status.desiredNodes`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// RBLNDriver is the Schema for the rblndrivers API
type RBLNDriver struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RBLNDriverSpec   `json:"spec,omitempty"`
	Status RBLNDriverStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RBLNDriverList contains a list of RBLNDriver
type RBLNDriverList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RBLNDriver `json:"items"`
}

// DriverManagerSpec describes configuration for Rebellions Driver Manager (initContainer)
type DriverManagerSpec struct {
	// Registry represents Driver Manager registry path
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=docker.io
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Manager Registry",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Registry string `json:"registry,omitempty"`

	// Image represents Rebellions Driver Manager image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions/rbln-k8s-driver-manager
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Manager Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Image string `json:"image,omitempty"`

	// Version represents Rebellions Driver Manager image tag (version)
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="latest"
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Manager Version",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Version string `json:"version,omitempty"`

	// Image pull policy
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Enum=Always;IfNotPresent;Never
	// +kubebuilder:default:=IfNotPresent
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image Pull Policy",xDescriptors="urn:alm:descriptor:com.tectonic.ui:imagePullPolicy"
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// Image pull secrets
	// +kubebuilder:validation:Optional
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors=true
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors.displayName="Image pull secrets"
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors.x-descriptors="urn:alm:descriptor:io.kubernetes:Secret"
	ImagePullSecrets []string `json:"imagePullSecrets,omitempty"`
}

// SmdSpec describes configuration for the rbln-smd node daemon. It carries no
// version: the smd image tag always equals RBLNDriverSpec.Version so the
// daemon on a node matches the driver installed there. Pull policy, pull
// secrets, tolerations, and priority class are inherited from the top-level
// driver spec.
type SmdSpec struct {
	// Registry represents the rbln-smd image registry path
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=docker.io
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Smd Registry",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Registry string `json:"registry,omitempty"`

	// Image represents the rbln-smd image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions/rbln-daemon
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Smd Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Image string `json:"image,omitempty"`
}

func init() {
	SchemeBuilder.Register(&RBLNDriver{}, &RBLNDriverList{})
}

// GetPrecompiledImagePath composes the pull path for a node's precompiled
// driver image. Images are published per NPU family with no family-agnostic
// path, so family is required and is spliced in before the image's final
// component: "rebellions/rbln-driver" -> "rebellions/atom/rbln-driver".
func (d *RBLNDriverSpec) GetPrecompiledImagePath(osVersion, kernelVersion, family string) (string, error) {
	if osVersion == "" || kernelVersion == "" {
		return "", fmt.Errorf("osVersion and kernelVersion are required")
	}
	if family == "" {
		return "", fmt.Errorf("NPU family is required to compose the driver image path")
	}

	registry := strings.TrimSuffix(strings.TrimSpace(d.Registry), "/")
	image := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(d.Image), "/"), "/")
	if image == "" {
		return "", fmt.Errorf("driver image is required")
	}
	version := strings.TrimSpace(d.Version)
	if version == "" {
		return "", fmt.Errorf("driver version is required")
	}

	if strings.Contains(image, "@sha256:") || strings.Contains(version, "sha256:") {
		return "", fmt.Errorf("specifying image digest is not supported when precompiled is enabled")
	}

	segments := strings.Split(image, "/")
	leaf := segments[len(segments)-1]
	segments[len(segments)-1] = family
	image = strings.Join(append(segments, leaf), "/")

	imagePath := fmt.Sprintf("%s/%s:%s-%s-%s", registry, image, version, kernelVersion, osVersion)
	return imagePath, nil
}
