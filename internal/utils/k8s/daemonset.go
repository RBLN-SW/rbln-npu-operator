package k8sutil

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type DaemonSetBuilder struct {
	*Builder[appsv1.DaemonSet]
}

func NewDaemonSetBuilder(name, namespace string) *DaemonSetBuilder {
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: make(map[string]string),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      make(map[string]string),
					Annotations: make(map[string]string),
				},
			},
		},
	}
	return &DaemonSetBuilder{
		Builder: NewBuilder[appsv1.DaemonSet](ds),
	}
}

func (b *DaemonSetBuilder) WithLabelSelectors(labels map[string]string) *DaemonSetBuilder {
	if len(labels) == 0 {
		return b
	}
	b.obj.Spec.Selector.MatchLabels = labels
	b.obj.Spec.Template.Labels = labels
	return b
}

func (b *DaemonSetBuilder) WithLabels(labels map[string]string) *DaemonSetBuilder {
	if len(labels) == 0 {
		return b
	}
	b.obj.Labels = labels
	return b
}

func (b *DaemonSetBuilder) WithAnnotations(annotations map[string]string) *DaemonSetBuilder {
	if len(annotations) == 0 {
		return b
	}
	b.obj.Annotations = MergeMaps(b.obj.Annotations, annotations)
	return b
}

func (b *DaemonSetBuilder) WithPodAnnotations(annotations map[string]string) *DaemonSetBuilder {
	if len(annotations) == 0 {
		return b
	}
	b.obj.Spec.Template.Annotations = MergeMaps(b.obj.Spec.Template.Annotations, annotations)
	return b
}

// WithPodSpec merges the desired PodSpec fields into the existing spec.
// Only operator-managed fields are overwritten; API-server-defaulted fields
// (restartPolicy, dnsPolicy, schedulerName, etc.) are preserved to avoid
// spurious updates on every reconciliation.
func (b *DaemonSetBuilder) WithPodSpec(desired *corev1.PodSpec) *DaemonSetBuilder {
	spec := &b.obj.Spec.Template.Spec

	spec.InitContainers = desired.InitContainers
	spec.Containers = desired.Containers
	spec.Volumes = desired.Volumes
	spec.ServiceAccountName = desired.ServiceAccountName
	spec.NodeSelector = desired.NodeSelector
	spec.Tolerations = desired.Tolerations
	spec.Affinity = desired.Affinity
	spec.ImagePullSecrets = desired.ImagePullSecrets
	spec.PriorityClassName = desired.PriorityClassName
	spec.HostNetwork = desired.HostNetwork
	spec.HostPID = desired.HostPID

	if desired.TerminationGracePeriodSeconds != nil {
		spec.TerminationGracePeriodSeconds = desired.TerminationGracePeriodSeconds
	}

	return b
}

func (b *DaemonSetBuilder) WithUpdateStrategy(strategy appsv1.DaemonSetUpdateStrategy) *DaemonSetBuilder {
	b.obj.Spec.UpdateStrategy = strategy
	return b
}
