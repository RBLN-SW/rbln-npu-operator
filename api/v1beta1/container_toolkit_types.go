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

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RBLNContainerToolkitSpec defines the desired state of RBLN container toolkit
type RBLNContainerToolkitSpec struct {
	// Enabled indicates if deployment of RBLN container toolkit is enabled
	// +kubebuilder:default:=true
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Enable RBLN Container Toolkit deployment",xDescriptors="urn:alm:descriptor:com.tectonic.ui:booleanSwitch"
	Enabled bool `json:"enabled"`

	// RBLN Container Toolkit image name
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=rebellions/rbln-container-toolkit
	// +kubebuilder:validation:Pattern=[a-zA-Z0-9\-]+
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Image",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Image string `json:"image,omitempty"`

	// Registry override for the RBLN Container Toolkit image
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=docker.io
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Registry",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Registry string `json:"registry,omitempty"`

	// RBLN Container Toolkit image tag
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:=latest
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Version",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	Version string `json:"version,omitempty"`

	// PodSpec defines common DaemonSet configurations
	// +kubebuilder:validation:Optional
	PodSpec `json:",inline"`

	// RefreshInterval controls how often rbln-ctk-daemon refreshes its state.
	// Accepts Go duration format ("5s", "1m", "1h30m"). "0s" disables auto-refresh.
	// Injected as RBLN_CTK_DAEMON_REFRESH_INTERVAL on the container.
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="5s"
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Refresh Interval",xDescriptors="urn:alm:descriptor:com.tectonic.ui:text"
	RefreshInterval *metav1.Duration `json:"refreshInterval,omitempty"`

	// Optional: List of environment variables
	// +operator-sdk:csv:customresourcedefinitions:type=spec,displayName="Environment Variables",xDescriptors="urn:alm:descriptor:com.tectonic.ui:advanced,urn:alm:descriptor:com.tectonic.ui:text"
	Env []corev1.EnvVar `json:"env,omitempty"`
}

func (s RBLNContainerToolkitSpec) IsEnabled() bool { return s.Enabled }
