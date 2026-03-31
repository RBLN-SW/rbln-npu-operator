package v1beta1

import corev1 "k8s.io/api/core/v1"

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

// PodDefaultsSpec defines common configuration applied to all DaemonSet pods managed by the operator.
type PodDefaultsSpec struct {
	// Labels specifies the labels for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Labels",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations specifies the annotations for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Annotations",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Annotations map[string]string `json:"annotations,omitempty"`

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
