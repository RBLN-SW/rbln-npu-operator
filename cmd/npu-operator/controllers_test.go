package main

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestUpgradeResourceFilter(t *testing.T) {
	for _, tc := range []struct {
		name, resource, quantity string
		phase                    corev1.PodPhase
		init, sidecar, want      bool
	}{
		{name: "container", resource: "rebellions.ai/npu", quantity: "1", phase: corev1.PodRunning, want: true},
		{name: "passthrough", resource: "rebellions.ai/RBLN-CA22_PF", quantity: "1", phase: corev1.PodRunning, want: true},
		{
			name: "native sidecar", resource: "rebellions.ai/npu", quantity: "1",
			phase: corev1.PodRunning, init: true, sidecar: true, want: true,
		},
		{
			name: "pending init", resource: "rebellions.ai/npu", quantity: "1",
			phase: corev1.PodPending, init: true, want: true,
		},
		{name: "zero", resource: "rebellions.ai/npu", quantity: "0", phase: corev1.PodRunning},
		{name: "other vendor", resource: "notrebellions.ai/npu", quantity: "1", phase: corev1.PodRunning},
		{name: "completed", resource: "rebellions.ai/npu", quantity: "1", phase: corev1.PodSucceeded, init: true},
		{
			name: "unknown phase retains upgrade policy", resource: "rebellions.ai/npu", quantity: "1",
			phase: corev1.PodUnknown,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := corev1.Container{Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
				corev1.ResourceName(tc.resource): resource.MustParse(tc.quantity),
			}}}
			p := corev1.Pod{Status: corev1.PodStatus{Phase: tc.phase}}
			if tc.sidecar {
				always := corev1.ContainerRestartPolicyAlways
				c.RestartPolicy = &always
			}
			if tc.init {
				p.Spec.InitContainers = []corev1.Container{c}
			} else {
				p.Spec.Containers = []corev1.Container{c}
			}
			if got := npuPodSpecFilter(p); got != tc.want {
				t.Fatalf("npuPodSpecFilter = %v, want %v", got, tc.want)
			}
		})
	}
}
