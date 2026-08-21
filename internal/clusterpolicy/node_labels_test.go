package clusterpolicy

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func newNodeLabelsFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 to scheme: %v", err)
	}
	if err := rblnv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add rblnv1beta1 to scheme: %v", err)
	}

	runtimeObjs := make([]client.Object, len(objs))
	copy(runtimeObjs, objs)

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(runtimeObjs...).
		Build()
}

func newTestClusterPolicyService(k8sClient client.Client, workloadType string) *ClusterPolicyService {
	return &ClusterPolicyService{
		client: k8sClient,
		log:    logr.Discard(),
		policy: &rblnv1beta1.RBLNClusterPolicy{
			Spec: rblnv1beta1.RBLNClusterPolicySpec{
				WorkloadType: workloadType,
			},
		},
	}
}

func newObjectMeta(name string, labelsMap map[string]string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:   name,
		Labels: labelsMap,
	}
}

func TestListAndClassifyNodes(t *testing.T) {
	tests := map[string]struct {
		nodes          []*corev1.Node
		wantNFD        bool
		wantCandidates int
	}{
		"detects NFD and returns NPU candidates": {
			nodes: []*corev1.Node{
				{ObjectMeta: newObjectMeta("npu-node", map[string]string{
					consts.NFDDevicePCILabelKey:                       labelValueTrue,
					"feature.node.kubernetes.io/system-os_release.ID": "ubuntu",
				})},
				{ObjectMeta: newObjectMeta("plain-node", map[string]string{
					"feature.node.kubernetes.io/system-os_release.ID": "ubuntu",
				})},
			},
			wantNFD:        true,
			wantCandidates: 1,
		},
		"returns false for NFD when no NFD labels exist": {
			nodes: []*corev1.Node{
				{ObjectMeta: newObjectMeta("plain-node", map[string]string{
					"kubernetes.io/arch": "amd64",
				})},
			},
			wantNFD:        false,
			wantCandidates: 0,
		},
		"includes already-managed RBLN nodes without device labels": {
			nodes: []*corev1.Node{
				{ObjectMeta: newObjectMeta("managed-node", map[string]string{
					consts.RBLNPresentLabelKey:                        labelValueTrue,
					"feature.node.kubernetes.io/system-os_release.ID": "ubuntu",
				})},
			},
			wantNFD:        false,
			wantCandidates: 1,
		},
		"returns false for NFD and zero candidates on empty cluster": {
			nodes:          nil,
			wantNFD:        false,
			wantCandidates: 0,
		},
		"includes nodes with alternate device label": {
			nodes: []*corev1.Node{
				{ObjectMeta: newObjectMeta("alt-npu-node", map[string]string{
					consts.NFDDevicePCIAltLabelKey: labelValueTrue,
				})},
			},
			wantNFD:        true,
			wantCandidates: 1,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			objs := make([]client.Object, len(tc.nodes))
			for i, n := range tc.nodes {
				objs[i] = n
			}
			k8sClient := newNodeLabelsFakeClient(t, objs...)

			candidates, nfdInstalled, err := ListAndClassifyNodes(context.Background(), k8sClient)
			if err != nil {
				t.Fatalf("ListAndClassifyNodes() unexpected error: %v", err)
			}
			if nfdInstalled != tc.wantNFD {
				t.Fatalf("nfdInstalled = %t, want %t", nfdInstalled, tc.wantNFD)
			}
			if len(candidates) != tc.wantCandidates {
				t.Fatalf("len(candidates) = %d, want %d", len(candidates), tc.wantCandidates)
			}
		})
	}
}

func TestReconcileNodes(t *testing.T) {
	type want struct {
		count        int
		labels       map[string]string
		absentLabels []string
	}

	tests := map[string]struct {
		workloadType string
		node         *corev1.Node
		want         want
	}{
		"marks detected nodes as present and adds container workload labels": {
			workloadType: consts.RBLNWorkloadConfigContainer,
			node: &corev1.Node{
				ObjectMeta: newObjectMeta("node-a", map[string]string{
					consts.NFDDevicePCILabelKey: labelValueTrue,
				}),
			},
			want: want{
				count: 1,
				labels: mergeLabelMaps(
					map[string]string{consts.RBLNPresentLabelKey: labelValueTrue},
					rblnComponentLabels[consts.RBLNWorkloadConfigContainer],
				),
				absentLabels: labelKeysNotIn(rblnComponentLabels[consts.RBLNWorkloadConfigVMPassthrough], rblnComponentLabels[consts.RBLNWorkloadConfigContainer]),
			},
		},
		"removes deploy labels and excludes skipped nodes from the deployable count": {
			workloadType: consts.RBLNWorkloadConfigContainer,
			node: &corev1.Node{
				ObjectMeta: newObjectMeta("node-skip", mergeLabelMaps(
					map[string]string{
						consts.NFDDevicePCILabelKey:   labelValueTrue,
						consts.RBLNPresentLabelKey:    labelValueTrue,
						consts.RBLNDeploySkipLabelKey: labelValueTrue,
					},
					rblnComponentLabels[consts.RBLNWorkloadConfigContainer],
				)),
			},
			want: want{
				count: 0,
				labels: map[string]string{
					consts.RBLNPresentLabelKey:    labelValueTrue,
					consts.RBLNDeploySkipLabelKey: labelValueTrue,
				},
				absentLabels: allComponentLabelKeys(),
			},
		},
		"marks nodes as not present and removes deploy labels when the device is gone": {
			workloadType: consts.RBLNWorkloadConfigContainer,
			node: &corev1.Node{
				ObjectMeta: newObjectMeta("node-removed", mergeLabelMaps(
					map[string]string{
						consts.RBLNPresentLabelKey: labelValueTrue,
					},
					rblnComponentLabels[consts.RBLNWorkloadConfigContainer],
				)),
			},
			want: want{
				count: 0,
				labels: map[string]string{
					consts.RBLNPresentLabelKey: labelValueFalse,
				},
				absentLabels: allComponentLabelKeys(),
			},
		},
		"falls back to the default workload when the node workload label is invalid": {
			workloadType: consts.RBLNWorkloadConfigVMPassthrough,
			node: &corev1.Node{
				ObjectMeta: newObjectMeta("node-vm", map[string]string{
					consts.NFDDevicePCILabelKey:       labelValueTrue,
					consts.RBLNWorkloadConfigLabelKey: consts.RBLNWorkloadConfigUnknown,
				}),
			},
			want: want{
				count: 1,
				labels: mergeLabelMaps(
					map[string]string{consts.RBLNPresentLabelKey: labelValueTrue},
					rblnComponentLabels[consts.RBLNWorkloadConfigVMPassthrough],
				),
				absentLabels: labelKeysNotIn(rblnComponentLabels[consts.RBLNWorkloadConfigContainer], rblnComponentLabels[consts.RBLNWorkloadConfigVMPassthrough]),
			},
		},
		"applies VMPassthrough workload labels when the node workload label is valid": {
			workloadType: consts.RBLNWorkloadConfigVMPassthrough,
			node: &corev1.Node{
				ObjectMeta: newObjectMeta("node-vm-valid", map[string]string{
					consts.NFDDevicePCILabelKey:       labelValueTrue,
					consts.RBLNWorkloadConfigLabelKey: consts.RBLNWorkloadConfigVMPassthrough,
				}),
			},
			want: want{
				count: 1,
				labels: mergeLabelMaps(
					map[string]string{consts.RBLNPresentLabelKey: labelValueTrue},
					rblnComponentLabels[consts.RBLNWorkloadConfigVMPassthrough],
				),
				absentLabels: labelKeysNotIn(rblnComponentLabels[consts.RBLNWorkloadConfigContainer], rblnComponentLabels[consts.RBLNWorkloadConfigVMPassthrough]),
			},
		},
		"excludes rbln-daemon on pre-installed driver nodes": {
			workloadType: consts.RBLNWorkloadConfigContainer,
			node: &corev1.Node{
				ObjectMeta: newObjectMeta("node-preinstalled", map[string]string{
					consts.NFDDevicePCILabelKey:     labelValueTrue,
					consts.RBLNDeployDriverLabelKey: consts.RBLNDeployDriverPreInstalled,
				}),
			},
			want: want{
				count: 1,
				labels: map[string]string{
					consts.RBLNPresentLabelKey:                   labelValueTrue,
					consts.RBLNDeployDriverLabelKey:              consts.RBLNDeployDriverPreInstalled,
					"rebellions.ai/npu.deploy.device-plugin":     labelValueTrue,
					"rebellions.ai/npu.deploy.container-toolkit": labelValueTrue,
				},
				absentLabels: append(
					[]string{consts.RBLNDeployRBLNDaemonLabelKey},
					labelKeysNotIn(rblnComponentLabels[consts.RBLNWorkloadConfigVMPassthrough], rblnComponentLabels[consts.RBLNWorkloadConfigContainer])...,
				),
			},
		},
		"removes the rbln-daemon label once the driver becomes pre-installed": {
			workloadType: consts.RBLNWorkloadConfigContainer,
			node: &corev1.Node{
				ObjectMeta: newObjectMeta("node-preinstalled-existing", mergeLabelMaps(
					rblnComponentLabels[consts.RBLNWorkloadConfigContainer],
					map[string]string{
						consts.NFDDevicePCILabelKey:     labelValueTrue,
						consts.RBLNPresentLabelKey:      labelValueTrue,
						consts.RBLNDeployDriverLabelKey: consts.RBLNDeployDriverPreInstalled,
					},
				)),
			},
			want: want{
				count: 1,
				labels: map[string]string{
					consts.RBLNPresentLabelKey:               labelValueTrue,
					consts.RBLNDeployDriverLabelKey:          consts.RBLNDeployDriverPreInstalled,
					"rebellions.ai/npu.deploy.device-plugin": labelValueTrue,
				},
				absentLabels: []string{consts.RBLNDeployRBLNDaemonLabelKey},
			},
		},
		"does not modify labels when the node already has the correct workload labels": {
			workloadType: consts.RBLNWorkloadConfigContainer,
			node: &corev1.Node{
				ObjectMeta: newObjectMeta("node-idempotent", mergeLabelMaps(
					map[string]string{
						consts.NFDDevicePCILabelKey: labelValueTrue,
						consts.RBLNPresentLabelKey:  labelValueTrue,
					},
					rblnComponentLabels[consts.RBLNWorkloadConfigContainer],
				)),
			},
			want: want{
				count: 1,
				labels: mergeLabelMaps(
					map[string]string{consts.RBLNPresentLabelKey: labelValueTrue},
					rblnComponentLabels[consts.RBLNWorkloadConfigContainer],
				),
				absentLabels: labelKeysNotIn(rblnComponentLabels[consts.RBLNWorkloadConfigVMPassthrough], rblnComponentLabels[consts.RBLNWorkloadConfigContainer]),
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			k8sClient := newNodeLabelsFakeClient(t, tc.node)
			service := newTestClusterPolicyService(k8sClient, tc.workloadType)

			census, err := service.ReconcileNodes(context.Background(), []corev1.Node{*tc.node})
			if err != nil {
				t.Fatalf("ReconcileNodes() unexpected error: %v", err)
			}
			if int(census.TotalNPU) != tc.want.count {
				t.Fatalf("ReconcileNodes() count = %d, want %d", census.TotalNPU, tc.want.count)
			}

			var updated corev1.Node
			if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: tc.node.Name}, &updated); err != nil {
				t.Fatalf("get node: %v", err)
			}

			for key, want := range tc.want.labels {
				if got := updated.Labels[key]; got != want {
					t.Fatalf("label %s = %q, want %q", key, got, want)
				}
			}

			for _, key := range tc.want.absentLabels {
				if _, exists := updated.Labels[key]; exists {
					t.Fatalf("label %s should be absent", key)
				}
			}
		})
	}
}

func mergeLabelMaps(mapsToMerge ...map[string]string) map[string]string {
	merged := map[string]string{}
	for _, labels := range mapsToMerge {
		for key, value := range labels {
			merged[key] = value
		}
	}
	return merged
}

func labelKeys(labels map[string]string) []string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	return keys
}

// The container and vm-passthrough component maps are not disjoint
// (dra-kubelet-plugin serves both), so "the other workload's labels must be
// absent" assertions have to skip the shared keys.
func labelKeysNotIn(labels, exclude map[string]string) []string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		if _, shared := exclude[key]; !shared {
			keys = append(keys, key)
		}
	}
	return keys
}

func allComponentLabelKeys() []string {
	keys := make([]string, 0)
	for _, labels := range rblnComponentLabels {
		keys = append(keys, labelKeys(labels)...)
	}
	return keys
}
