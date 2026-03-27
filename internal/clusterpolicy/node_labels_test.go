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

func newNodeList(nodes ...*corev1.Node) *corev1.NodeList {
	items := make([]corev1.Node, len(nodes))
	for i, n := range nodes {
		items[i] = *n
	}
	return &corev1.NodeList{Items: items}
}

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
		"returns false when the node list is empty": {
			nodes: &corev1.NodeList{},
			want:  false,
		},
		"returns true when only one of multiple nodes has an NFD label": {
			nodes: &corev1.NodeList{Items: []corev1.Node{
				{
					ObjectMeta: newObjectMeta("node-no-nfd", map[string]string{
						"kubernetes.io/arch": "amd64",
					}),
				},
				{
					ObjectMeta: newObjectMeta("node-with-nfd", map[string]string{
						"feature.node.kubernetes.io/pci-1eff.present": labelValueTrue,
					}),
				},
			}},
			want: true,
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
		"applies VMPassthrough workload labels when the node workload label is valid": {
			workloadType: consts.RBLNWorkloadConfigVMPassthrough,
			node: &corev1.Node{
				ObjectMeta: newObjectMeta("node-vm-valid", map[string]string{
					"feature.node.kubernetes.io/pci-1eff.present": labelValueTrue,
					consts.RBLNWorkloadConfigLabelKey:             consts.RBLNWorkloadConfigVMPassthrough,
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
		"does not modify labels when the node already has the correct workload labels": {
			workloadType: consts.RBLNWorkloadConfigContainer,
			node: &corev1.Node{
				ObjectMeta: newObjectMeta("node-idempotent", mergeLabelMaps(
					map[string]string{
						"feature.node.kubernetes.io/pci-1eff.present": labelValueTrue,
						consts.RBLNPresentLabelKey:                    labelValueTrue,
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
				absentLabels: labelKeys(rblnComponentLabels[consts.RBLNWorkloadConfigVMPassthrough]),
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			k8sClient := newNodeLabelsFakeClient(t, tc.node)
			service := newTestClusterPolicyService(k8sClient, tc.workloadType)

			count, err := service.ReconcileNodeLabels(context.Background(), newNodeList(tc.node))
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
