package clusterpolicy

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

const pausedTestNamespace = "rbln-system"

// driverManagerPod is a pod running the k8s-driver-manager init container on
// nodeName, in the state given; a nil state is a pod the kubelet has not
// reported on yet.
func driverManagerPod(name, nodeName string, state *corev1.ContainerState) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: pausedTestNamespace},
		Spec: corev1.PodSpec{
			NodeName:       nodeName,
			InitContainers: []corev1.Container{{Name: consts.DriverManagerInitContainerName}},
			Containers:     []corev1.Container{{Name: "rbln-driver-container"}},
		},
	}
	if state != nil {
		pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{
			Name: consts.DriverManagerInitContainerName, State: *state,
		}}
	}
	return pod
}

// k8s-driver-manager pauses a node's deploy labels while it replaces the
// driver and restores them at the end of the run. A run killed in between, or
// whose final label write failed, leaves the pause behind, and the fill-only
// reconciliation never repairs it: the node's components stay gated off with
// nothing reporting why. The operator restores such a pause only once every
// k8s-driver-manager init container on the node has terminated. While one is
// running, or has not reported a state yet, the pause is live, and undoing it
// would reschedule the very components the run is evicting.
func TestReconcileNodesRestoresStalePausedLabels(t *testing.T) {
	const (
		nodeName         = "node-a"
		devicePluginKey  = "rebellions.ai/npu.deploy.device-plugin"
		containerToolkit = "rebellions.ai/npu.deploy.container-toolkit"
	)
	terminated := &corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}
	failed := &corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}
	running := &corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	waiting := &corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}

	pausedNode := func(overrides map[string]string) *corev1.Node {
		labels := mergeLabelMaps(map[string]string{
			consts.NFDDevicePCILabelKey: labelValueTrue,
			consts.RBLNPresentLabelKey:  labelValueTrue,
		}, rblnComponentLabels[consts.RBLNWorkloadConfigContainer])
		labels[devicePluginKey] = consts.RBLNDeployPausedForDriverUpgrade
		labels[consts.RBLNDeploySmdLabelKey] = consts.RBLNDeployPausedForDriverUpgrade
		for key, value := range overrides {
			labels[key] = value
		}
		return &corev1.Node{ObjectMeta: newObjectMeta(nodeName, labels)}
	}
	stillPaused := map[string]string{
		devicePluginKey:              consts.RBLNDeployPausedForDriverUpgrade,
		consts.RBLNDeploySmdLabelKey: consts.RBLNDeployPausedForDriverUpgrade,
	}
	restored := map[string]string{
		devicePluginKey:              labelValueTrue,
		consts.RBLNDeploySmdLabelKey: labelValueTrue,
	}

	tests := map[string]struct {
		node *corev1.Node
		pods []client.Object
		want map[string]string
	}{
		"restored once the init container has terminated": {
			node: pausedNode(nil),
			pods: []client.Object{driverManagerPod("driver", nodeName, terminated)},
			want: restored,
		},
		// A failed run already tried to restore the labels itself; the next
		// attempt pauses again anyway, so restoring in between costs nothing.
		"restored after a failed init container attempt": {
			node: pausedNode(nil),
			pods: []client.Object{driverManagerPod("driver", nodeName, failed)},
			want: restored,
		},
		"left paused while the init container is running": {
			node: pausedNode(nil),
			pods: []client.Object{driverManagerPod("driver", nodeName, running)},
			want: stillPaused,
		},
		"left paused while the init container is waiting to start": {
			node: pausedNode(nil),
			pods: []client.Object{driverManagerPod("driver", nodeName, waiting)},
			want: stillPaused,
		},
		"left paused while the kubelet has not reported the init container": {
			node: pausedNode(nil),
			pods: []client.Object{driverManagerPod("driver", nodeName, nil)},
			want: stillPaused,
		},
		// With nothing to judge by, the pause is presumed live: the next
		// driver pod's run restores it in the ordinary way.
		"left paused with no k8s-driver-manager pod on the node": {
			node: pausedNode(nil),
			want: stillPaused,
		},
		"a terminated init container on another node does not count": {
			node: pausedNode(nil),
			pods: []client.Object{driverManagerPod("driver", "node-b", terminated)},
			want: stillPaused,
		},
		// The vfio-manager pod runs the same init container; either one
		// running keeps the pause live.
		"left paused while a second driver-manager init on the node is still running": {
			node: pausedNode(nil),
			pods: []client.Object{
				driverManagerPod("driver", nodeName, terminated),
				driverManagerPod("vfio-manager", nodeName, running),
			},
			want: stillPaused,
		},
		// Only the pause value is restored; a user's opt-out is a value the
		// operator never rewrites.
		"a false opt-out is not rewritten": {
			node: pausedNode(map[string]string{containerToolkit: labelValueFalse}),
			pods: []client.Object{driverManagerPod("driver", nodeName, terminated)},
			want: mergeLabelMaps(restored, map[string]string{containerToolkit: labelValueFalse}),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			objs := append([]client.Object{tc.node}, tc.pods...)
			k8sClient := newNodeLabelsFakeClient(t, objs...)
			service := newTestClusterPolicyService(k8sClient, consts.RBLNWorkloadConfigContainer)
			service.namespace = pausedTestNamespace

			if _, err := service.ReconcileNodes(context.Background(), []corev1.Node{*tc.node}); err != nil {
				t.Fatalf("ReconcileNodes() unexpected error: %v", err)
			}

			var updated corev1.Node
			if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: nodeName}, &updated); err != nil {
				t.Fatalf("get node: %v", err)
			}
			for key, want := range tc.want {
				if got := updated.Labels[key]; got != want {
					t.Fatalf("label %s = %q, want %q", key, got, want)
				}
			}
		})
	}
}
