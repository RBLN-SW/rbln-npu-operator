package clusterpolicy

import (
	"context"
	"maps"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
)

func TestReconcileNodesPreservesPartitionLabels(t *testing.T) {
	for _, workload := range []string{consts.RBLNWorkloadConfigContainer, consts.RBLNWorkloadConfigVMPassthrough} {
		for _, nodeDefault := range []string{"", "pf", "vf1", "vf4"} {
			t.Run(workload+"/default="+nodeDefault, func(t *testing.T) {
				labels := map[string]string{
					consts.NFDDevicePCILabelKey:                labelValueTrue,
					consts.RBLNWorkloadConfigLabelKey:          workload,
					consts.RBLNPartitionIndexLabelPrefix + "0": "vf1",
					consts.RBLNPartitionIndexLabelPrefix + "1": "vf4",
					consts.RBLNPartitionIndexLabelPrefix + "2": "pf",
					consts.RBLNPartitionIndexLabelPrefix + "3": "none",
					consts.RBLNPartitionIndexLabelPrefix + "4": "vf2",
					consts.RBLNPartitionIndexLabelPrefix + "5": "",
				}
				if nodeDefault != "" {
					labels[consts.RBLNPartitionLabelKey] = nodeDefault
				}
				original := maps.Clone(labels)
				node := &corev1.Node{ObjectMeta: newObjectMeta("node-1", labels)}
				k8sClient := newNodeLabelsFakeClient(t, node)
				svc := newTestClusterPolicyService(k8sClient, workload)
				ctx := context.Background()

				// Check the API object after initial label generation and a repeat
				// reconcile: fallback must neither rewrite nor prune user intent.
				for range 2 {
					if _, err := svc.ReconcileNodes(ctx, []corev1.Node{*node}); err != nil {
						t.Fatal(err)
					}
					if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(node), node); err != nil {
						t.Fatal(err)
					}
					if node.Labels[consts.RBLNPresentLabelKey] != labelValueTrue {
						t.Fatal("node labels were not reconciled")
					}
					for key, value := range original {
						if got, exists := node.Labels[key]; !exists || got != value {
							t.Fatalf("label %s changed: got %q (present=%v), want %q", key, got, exists, value)
						}
					}
					if _, exists := node.Labels[consts.RBLNPartitionLabelKey]; exists != (nodeDefault != "") {
						t.Fatal("reconcile changed whether the node default is explicit")
					}
				}
			})
		}
	}
}
