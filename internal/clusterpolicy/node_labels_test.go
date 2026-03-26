package clusterpolicy

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func TestHasNFDLabeledNodes(t *testing.T) {
	tests := map[string]struct {
		nodes *corev1.NodeList
		want  bool
	}{
		"returns true when at least one node has an NFD label": {
			nodes: &corev1.NodeList{Items: []corev1.Node{{
				ObjectMeta: newObjectMeta("node-a", map[string]string{
					"feature.node.kubernetes.io/pci-1eff.present": labelValueTrue,
				}),
			}}},
			want: true,
		},
		"returns false when no nodes have NFD labels": {
			nodes: &corev1.NodeList{Items: []corev1.Node{{
				ObjectMeta: newObjectMeta("node-a", map[string]string{
					"kubernetes.io/arch": "amd64",
				}),
			}}},
			want: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := HasNFDLabeledNodes(tc.nodes); got != tc.want {
				t.Fatalf("HasNFDLabeledNodes() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestReconcileNodeLabels(t *testing.T) {
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
					"feature.node.kubernetes.io/pci-1eff.present": labelValueTrue,
				}),
			},
			want: want{
				count: 1,
				labels: mergeLabelMaps(
					map[string]string{consts.RBLNPresentLabelKey: labelValueTrue},
					rblnComponentLabels[consts.RBLNWorkloadConfigContainer],
				),
				absentLabels: labelKeys(rblnComponentLabels[consts.RBLNWorkloadConfigVMPassthrough]),
			},
		},
		"removes deploy labels and excludes skipped nodes from the deployable count": {
			workloadType: consts.RBLNWorkloadConfigContainer,
			node: &corev1.Node{
				ObjectMeta: newObjectMeta("node-skip", mergeLabelMaps(
					map[string]string{
						"feature.node.kubernetes.io/pci-1eff.present": labelValueTrue,
						consts.RBLNPresentLabelKey:                    labelValueTrue,
						consts.RBLNDeploySkipLabelKey:                 labelValueTrue,
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
					"feature.node.kubernetes.io/pci-1eff.present": labelValueTrue,
					consts.RBLNWorkloadConfigLabelKey:             consts.RBLNWorkloadConfigUnknown,
				}),
			},
			want: want{
				count: 1,
				labels: mergeLabelMaps(
					map[string]string{consts.RBLNPresentLabelKey: labelValueTrue},
					rblnComponentLabels[consts.RBLNWorkloadConfigVMPassthrough],
				),
				absentLabels: labelKeys(rblnComponentLabels[consts.RBLNWorkloadConfigContainer]),
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			k8sClient := newFakeClient(t, tc.node)
			service := newTestClusterPolicyService(k8sClient, tc.workloadType)

			nodeList, err := ListNodes(context.Background(), k8sClient)
			if err != nil {
				t.Fatalf("ListNodes() unexpected error: %v", err)
			}

			count, err := service.ReconcileNodeLabels(context.Background(), nodeList)
			if err != nil {
				t.Fatalf("ReconcileNodeLabels() unexpected error: %v", err)
			}
			if count != tc.want.count {
				t.Fatalf("ReconcileNodeLabels() count = %d, want %d", count, tc.want.count)
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

func allComponentLabelKeys() []string {
	keys := make([]string, 0)
	for _, labels := range rblnComponentLabels {
		keys = append(keys, labelKeys(labels)...)
	}
	return keys
}
