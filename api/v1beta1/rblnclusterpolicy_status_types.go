package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type (
	ClusterState   string
	NodeState      string
	ComponentState string
)

const (
	ClusterReady    ClusterState = "ready"
	ClusterNotReady ClusterState = "notReady"
	ClusterIgnored  ClusterState = "ignored"
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
