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

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// DriverState represents the overall readiness of the driver deployment.
type DriverState string

const (
	// DriverStateReady indicates that the driver deployment is healthy.
	DriverStateReady DriverState = "ready"
	// DriverStateNotReady indicates that the driver deployment is not yet healthy.
	DriverStateNotReady DriverState = "notReady"
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
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Driver Manager",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced"
	Manager DriverManagerSpec `json:"manager,omitempty"`

	// NodeSelector specifies a selector for installation of the driver
	// +kubebuilder:validation:Optional
	// +mapType=atomic
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Node Selector",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// +kubebuilder:validation:Optional
	// Affinity specifies node affinity rules for driver pods
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Node Affinity",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced,urn:alm:descriptor:io.kubernetes:Affinity"
	NodeAffinity *corev1.NodeAffinity `json:"nodeAffinity,omitempty"`

	// Tolerations specifies the tolerations for the driver pod
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Tolerations",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced,urn:alm:descriptor:io.kubernetes:Tolerations"
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// Labels specifies the labels for the driver pod
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Labels",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Labels map[string]string `json:"labels,omitempty"`

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
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Args specifies additional command line arguments for the driver container
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Arguments",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced,urn:alm:descriptor:com.tectonic.ui:text"
	Args []string `json:"args,omitempty"`

	// Env specifies environment variables for the driver container
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Environment Variables",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced,urn:alm:descriptor:com.tectonic.ui:text"
	Env []corev1.EnvVar `json:"env,omitempty"`
}

// RBLNDriverStatus defines the observed state of RBLNDriver
type RBLNDriverStatus struct {
	// +kubebuilder:validation:Enum=ready;notReady
	// +optional
	// State indicates status of RBLNDriver instance
	State DriverState `json:"state,omitempty"`
	// Conditions is a list of conditions representing the RBLNDriver's current state
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

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
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Manager Registry",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Registry string `json:"registry,omitempty"`

	// Image represents Rebellions Driver Manager image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Manager Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Image string `json:"image,omitempty"`

	// Version represents Rebellions Driver Manager image tag (version)
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Manager Version",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Version string `json:"version,omitempty"`

	// Image pull policy
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image Pull Policy",xDescriptors="urn:alm:descriptor:com.tectonic.ui:imagePullPolicy"
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// Image pull secrets
	// +kubebuilder:validation:Optional
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors=true
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors.displayName="Image pull secrets"
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors.x-descriptors="urn:alm:descriptor:io.kubernetes:Secret"
	ImagePullSecrets []string `json:"imagePullSecrets,omitempty"`

	// Optional: List of environment variables
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors=true
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors.displayName="Environment Variables"
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors.x-descriptors="urn:alm:descriptor:com.tectonic.ui:advanced,urn:alm:descriptor:com.tectonic.ui:text"
	Env []EnvVar `json:"env,omitempty"`
}

// EnvVar represents an environment variable present in a Container.
type EnvVar struct {
	// Name of the environment variable.
	Name string `json:"name"`

	// Value of the environment variable.
	Value string `json:"value,omitempty"`
}

func init() {
	SchemeBuilder.Register(&RBLNDriver{}, &RBLNDriverList{})
}

func (d *RBLNDriverSpec) GetPrecompiledImagePath(osVersion string, kernelVersion string) (string, error) {
	if osVersion == "" || kernelVersion == "" {
		return "", fmt.Errorf("osVersion and kernelVersion are required")
	}

	registry := strings.TrimSuffix(strings.TrimSpace(d.Registry), "/")
	image := strings.TrimPrefix(strings.TrimSpace(d.Image), "/")
	version := strings.TrimSpace(d.Version)
	if version == "" {
		return "", fmt.Errorf("driver version is required")
	}

	if strings.Contains(image, "@sha256:") || strings.Contains(version, "sha256:") {
		return "", fmt.Errorf("specifying image digest is not supported when precompiled is enabled")
	}

	imagePath := fmt.Sprintf("%s/%s:%s-%s-%s", registry, image, version, kernelVersion, osVersion)
	return imagePath, nil
}

// GetNodeSelector returns node selector labels for Rebellions driver installation.
func (d *RBLNDriver) GetNodeSelector() map[string]string {
	if d == nil || len(d.Spec.NodeSelector) == 0 {
		return map[string]string{
			"rebellions.ai/npu.deploy.driver": "true",
		}
	}
	return d.Spec.NodeSelector
}
