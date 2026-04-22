package v1beta1

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// ValidatorSpec describes configuration options for validation daemonset
type ValidatorSpec struct {
	// Toolkit validator spec
	Toolkit ToolkitValidatorSpec `json:"toolkit,omitempty"`

	// Driver validator spec
	Driver DriverValidatorSpec `json:"driver,omitempty"`

	// Validator image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	Image string `json:"image,omitempty"`

	// Validator image registry
	// +kubebuilder:validation:Optional
	Registry string `json:"registry,omitempty"`

	// Validator image tag
	// +kubebuilder:validation:Optional
	Version string `json:"version,omitempty"`

	// Image pull policy
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Enum=Always;IfNotPresent;Never
	// +kubebuilder:default:=IfNotPresent
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image Pull Policy",xDescriptors="urn:alm:descriptor:com.tectonic.ui:imagePullPolicy"
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// Image pull secrets
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image pull secrets",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	ImagePullSecrets []string `json:"imagePullSecrets,omitempty"`

	// Optional: Define resources requests and limits for each pod
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Resource Requirements",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced,urn:alm:descriptor:com.tectonic.ui:resourceRequirements"
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// Optional: List of environment variables
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Environment Variables",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced,urn:alm:descriptor:com.tectonic.ui:text"
	Env []corev1.EnvVar `json:"env,omitempty"`
}

// ToolkitValidatorSpec describes configuration for RBLN Container Toolkit validation
type ToolkitValidatorSpec struct {
	// Optional: List of environment variables
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors=true
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors.displayName="Environment Variables"
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors.x-descriptors="urn:alm:descriptor:com.tectonic.ui:advanced,urn:alm:descriptor:com.tectonic.ui:text"
	Env []corev1.EnvVar `json:"env,omitempty"`
}

// DriverValidatorSpec describes configuration for RBLN Driver validation
type DriverValidatorSpec struct {
	// Optional: List of environment variables
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors=true
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors.displayName="Environment Variables"
	// +operator-sdk:gen-csv:customresourcedefinitions.specDescriptors.x-descriptors="urn:alm:descriptor:com.tectonic.ui:advanced,urn:alm:descriptor:com.tectonic.ui:text"
	Env []corev1.EnvVar `json:"env,omitempty"`
}

func (v *ValidatorSpec) GetValidatorImage() (string, error) {
	image := strings.TrimPrefix(strings.TrimSpace(v.Image), "/")
	if image == "" {
		return "", fmt.Errorf("validator image is required")
	}
	registry := strings.TrimSuffix(strings.TrimSpace(v.Registry), "/")
	if registry != "" {
		image = fmt.Sprintf("%s/%s", registry, image)
	}
	version := strings.TrimSpace(v.Version)
	if version != "" {
		image = fmt.Sprintf("%s:%s", image, version)
	}
	return image, nil
}
