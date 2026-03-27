package k8sutil

import (
	corev1 "k8s.io/api/core/v1"
)

type PodSpecBuilder struct {
	*Builder[corev1.PodSpec]
}

func NewPodSpecBuilder(v ...*corev1.PodSpec) *PodSpecBuilder {
	podSpec := &corev1.PodSpec{}
	if len(v) > 0 {
		podSpec = v[0]
	}
	return &PodSpecBuilder{Builder: NewBuilder(podSpec)}
}

func (b *PodSpecBuilder) WithAffinity(affinity *corev1.Affinity) *PodSpecBuilder {
	b.obj.Affinity = affinity
	return b
}

func (b *PodSpecBuilder) WithTolerations(tolerations []corev1.Toleration) *PodSpecBuilder {
	b.obj.Tolerations = tolerations
	return b
}

func (b *PodSpecBuilder) WithImagePullSecrets(secrets []string) *PodSpecBuilder {
	imgPullSecrets := []corev1.LocalObjectReference{}
	for _, secret := range secrets {
		imgPullSecrets = append(imgPullSecrets, corev1.LocalObjectReference{Name: secret})
	}
	b.obj.ImagePullSecrets = imgPullSecrets
	return b
}

func (b *PodSpecBuilder) WithPriorityClassName(priorityClass string) *PodSpecBuilder {
	b.obj.PriorityClassName = priorityClass
	return b
}

func (b *PodSpecBuilder) WithVolumes(volumes []corev1.Volume) *PodSpecBuilder {
	b.obj.Volumes = volumes
	return b
}

func (b *PodSpecBuilder) WithNodeSelector(selector map[string]string) *PodSpecBuilder {
	b.obj.NodeSelector = selector
	return b
}

func (b *PodSpecBuilder) WithContainers(containers []*corev1.Container) *PodSpecBuilder {
	b.obj.Containers = make([]corev1.Container, len(containers))
	for idx, cont := range containers {
		b.obj.Containers[idx] = *cont
	}
	return b
}

func (b *PodSpecBuilder) WithInitContainers(containers []*corev1.Container) *PodSpecBuilder {
	b.obj.InitContainers = make([]corev1.Container, len(containers))
	for idx, cont := range containers {
		b.obj.InitContainers[idx] = *cont
	}
	return b
}

func (b *PodSpecBuilder) WithHostNetwork(enabled bool) *PodSpecBuilder {
	b.obj.HostNetwork = enabled
	return b
}

func (b *PodSpecBuilder) WithHostPID(enabled bool) *PodSpecBuilder {
	b.obj.HostPID = enabled
	return b
}

func (b *PodSpecBuilder) WithTerminationGracePeriodSeconds(seconds int64) *PodSpecBuilder {
	b.obj.TerminationGracePeriodSeconds = &[]int64{seconds}[0]
	return b
}

func (b *PodSpecBuilder) WithServiceAccountName(serviceAccountName string) *PodSpecBuilder {
	b.obj.ServiceAccountName = serviceAccountName
	return b
}
