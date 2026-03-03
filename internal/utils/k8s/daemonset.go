package k8sutil

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type DaemonSetBuilder struct {
	*OwnableBuilder[appsv1.DaemonSet, *appsv1.DaemonSet]
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
		OwnableBuilder: &OwnableBuilder[appsv1.DaemonSet, *appsv1.DaemonSet]{
			Builder: NewBuilder[appsv1.DaemonSet](ds),
		},
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

func (b *DaemonSetBuilder) WithPodSpec(podSpec *corev1.PodSpec) *DaemonSetBuilder {
	b.obj.Spec.Template.Spec = *podSpec.DeepCopy()
	return b
}

func (b *DaemonSetBuilder) WithUpdateStrategy(strategy appsv1.DaemonSetUpdateStrategy) *DaemonSetBuilder {
	b.obj.Spec.UpdateStrategy = strategy
	return b
}
