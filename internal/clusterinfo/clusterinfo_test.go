package clusterinfo

import (
	"testing"

	configv1 "github.com/openshift/api/config/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func TestChooseContainerRuntime(t *testing.T) {
	cases := map[string]struct {
		nodes []corev1.Node
		want  string
	}{
		"DefaultToContainerd": {
			nodes: nil,
			want:  consts.Containerd,
		},
		"DockerOnly": {
			nodes: []corev1.Node{
				{Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "docker://24.0.7"}}},
			},
			want: consts.Docker,
		},
		"PreferCRIOOverDocker": {
			nodes: []corev1.Node{
				{Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "docker://24.0.7"}}},
				{Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "cri-o://1.31.0"}}},
			},
			want: consts.CRIO,
		},
		"PreferContainerdOverOthers": {
			nodes: []corev1.Node{
				{Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "docker://24.0.7"}}},
				{Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://1.7.22"}}},
				{Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "cri-o://1.31.0"}}},
			},
			want: consts.Containerd,
		},
		"IgnoreUnknownRuntime": {
			nodes: []corev1.Node{
				{Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "unknown://1.0.0"}}},
			},
			want: consts.Containerd,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := chooseContainerRuntime(tc.nodes)
			if got != tc.want {
				t.Fatalf("chooseContainerRuntime() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCompletedOpenShiftVersion(t *testing.T) {
	cases := map[string]struct {
		history []configv1.UpdateHistory
		want    string
		ok      bool
	}{
		"ReturnCompletedMinorVersion": {
			history: []configv1.UpdateHistory{
				{State: "Partial", Version: "4.16.1"},
				{State: "Completed", Version: "4.17.3"},
			},
			want: "4.17",
			ok:   true,
		},
		"ReturnSingleSegmentVersion": {
			history: []configv1.UpdateHistory{
				{State: "Completed", Version: "4"},
			},
			want: "4",
			ok:   true,
		},
		"NoCompletedVersion": {
			history: []configv1.UpdateHistory{
				{State: "Partial", Version: "4.16.1"},
			},
			want: "",
			ok:   false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, ok := completedOpenShiftVersion(tc.history)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("completedOpenShiftVersion() = (%q, %v), want (%q, %v)", got, ok, tc.want, tc.ok)
			}
		})
	}
}
