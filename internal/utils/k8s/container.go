package k8sutil

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

type ContainerBuilder struct {
	*Builder[corev1.Container]
}

func NewContainerBuilder(v ...*corev1.Container) *ContainerBuilder {
	container := &corev1.Container{}
	if len(v) > 0 {
		container = v[0]
	}
	return &ContainerBuilder{Builder: NewBuilder(container)}
}

func (b *ContainerBuilder) WithImage(image, tag string, imgPullPolicy corev1.PullPolicy) *ContainerBuilder {
	// use "latest" if tag is empty
	if tag == "" {
		tag = "latest"
	}

	if digest, ok := imageDigest(tag); ok {
		b.obj.Image = image + "@" + digest
		b.obj.ImagePullPolicy = imgPullPolicy
		return b
	}

	// pull Always if tag is "latest"
	pullPolicy := imgPullPolicy
	if tag == "latest" {
		pullPolicy = corev1.PullAlways
	}

	b.obj.Image = fmt.Sprintf("%s:%s", image, tag)
	b.obj.ImagePullPolicy = pullPolicy

	return b
}

func imageDigest(ver string) (string, bool) {
	ver = strings.TrimPrefix(ver, "@")
	if strings.Contains(ver, "sha256:") {
		return ver, true
	}
	return ver, false
}

func (b *ContainerBuilder) WithResources(resources corev1.ResourceRequirements, defaultCPUReq string, defaultMemoryReq string) *ContainerBuilder {
	// Ensure Requests is initialized if partially or fully omitted
	if resources.Requests == nil {
		resources.Requests = make(corev1.ResourceList)
	}
	// Apply default CPU request if not specified
	if _, exists := resources.Requests[corev1.ResourceCPU]; !exists {
		resources.Requests[corev1.ResourceCPU] = resource.MustParse(defaultCPUReq)
	}
	// Apply default Memory request if not specified
	if _, exists := resources.Requests[corev1.ResourceMemory]; !exists {
		resources.Requests[corev1.ResourceMemory] = resource.MustParse(defaultMemoryReq)
	}
	// Limits are only set if explicitly provided in the spec; otherwise, they remain unset

	b.obj.Resources = resources

	return b
}

func (b *ContainerBuilder) WithName(name string) *ContainerBuilder {
	b.obj.Name = name
	return b
}

func (b *ContainerBuilder) WithVolumeMounts(mounts []corev1.VolumeMount) *ContainerBuilder {
	b.obj.VolumeMounts = mounts
	return b
}

func (b *ContainerBuilder) WithCommands(cmd []string) *ContainerBuilder {
	b.obj.Command = cmd
	return b
}

func (b *ContainerBuilder) WithArgs(args []string) *ContainerBuilder {
	b.obj.Args = args
	return b
}

func (b *ContainerBuilder) WithSecurityContext(securityCtx *corev1.SecurityContext) *ContainerBuilder {
	b.obj.SecurityContext = securityCtx
	return b
}

func (b *ContainerBuilder) WithLifeCycle(lifeCycle *corev1.Lifecycle) *ContainerBuilder {
	b.obj.Lifecycle = lifeCycle
	return b
}

func (b *ContainerBuilder) WithEnvs(envs []corev1.EnvVar) *ContainerBuilder {
	b.obj.Env = envs
	return b
}

func (b *ContainerBuilder) WithPorts(ports []corev1.ContainerPort) *ContainerBuilder {
	b.obj.Ports = ports
	return b
}

func (b *ContainerBuilder) WithLivenessProbe(probe *corev1.Probe) *ContainerBuilder {
	b.obj.LivenessProbe = probe
	return b
}

// Build returns the container with Kubernetes API server defaults populated.
// This prevents spurious diffs when used with CreateOrPatch, since the API
// server automatically sets these fields on persisted containers.
func (b *ContainerBuilder) Build() *corev1.Container {
	if b.obj.TerminationMessagePath == "" {
		b.obj.TerminationMessagePath = "/dev/termination-log"
	}
	if b.obj.TerminationMessagePolicy == "" {
		b.obj.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	}
	return b.obj
}
