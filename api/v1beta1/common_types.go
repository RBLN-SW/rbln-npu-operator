package v1beta1

import corev1 "k8s.io/api/core/v1"

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

// LoggingSpec gates an operand's own log stream. It is declared per component
// rather than on PodSpec because not every operand image implements the
// contract -- a field on PodSpec would be silently ignored by those.
// Each operand defaults to info/json on its own, so an unset field renders no
// environment variable at all and leaves that default in place.
type LoggingSpec struct {
	// Level gates which records the operand emits.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Enum=error;warning;warn;info;debug
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Log Level",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Level string `json:"level,omitempty"`

	// Format selects the operand's log encoding.
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Enum=json;text
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Log Format",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Format string `json:"format,omitempty"`
}

// PodSpec defines common configuration for individual DaemonSet components
type PodSpec struct {
	// ImagePullPolicy specifies the image pull policy for the DaemonSet pods
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Enum=Always;IfNotPresent;Never
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
