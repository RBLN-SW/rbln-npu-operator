package clusterpolicy

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	kapierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// The node is co-written by kubelet, k8s-driver-manager and the upgrade
// controller, so the operator's Update loses the resourceVersion race
// routinely. A conflict must be retried from a fresh read with the decision
// recomputed, and never patched over: the writer that won may have been
// k8s-driver-manager pausing a deploy label the stale copy still saw as
// absent, and the fill-only contract says an existing value is never
// overwritten.
func TestReconcileNodesRetriesConflictFromFreshRead(t *testing.T) {
	const (
		nodeName        = "node-a"
		devicePluginKey = "rebellions.ai/npu.deploy.device-plugin"
	)
	ctx := context.Background()

	conflicts := 0
	k8sClient := newNodeLabelsFakeClientWithInterceptor(t, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if conflicts > 0 {
				return c.Update(ctx, obj, opts...)
			}
			conflicts++
			// Another writer lands first: k8s-driver-manager pauses a label the
			// operator's copy does not carry yet.
			current := &corev1.Node{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), current); err != nil {
				return err
			}
			current.Labels[devicePluginKey] = consts.RBLNDeployPausedForDriverUpgrade
			if err := c.Update(ctx, current); err != nil {
				return err
			}
			return kapierrors.NewConflict(schema.GroupResource{Resource: "nodes"}, obj.GetName(),
				errors.New("the object has been modified"))
		},
	}, &corev1.Node{
		ObjectMeta: newObjectMeta(nodeName, map[string]string{consts.NFDDevicePCILabelKey: labelValueTrue}),
	})
	service := newTestClusterPolicyService(k8sClient, consts.RBLNWorkloadConfigContainer)

	candidate := &corev1.Node{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, candidate); err != nil {
		t.Fatalf("get candidate: %v", err)
	}

	census, err := service.ReconcileNodes(ctx, []corev1.Node{*candidate})
	if err != nil {
		t.Fatalf("ReconcileNodes: %v", err)
	}
	if conflicts != 1 {
		t.Fatalf("conflicts = %d, want 1", conflicts)
	}
	if census.TotalNPU != 1 || census.ContainerNodes != 1 {
		t.Fatalf("census = %+v, want one container node", census)
	}

	updated := &corev1.Node{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, updated); err != nil {
		t.Fatalf("get updated node: %v", err)
	}
	if got := updated.Labels[consts.RBLNPresentLabelKey]; got != labelValueTrue {
		t.Errorf("present label = %q, want %q", got, labelValueTrue)
	}
	// The concurrent pause survives; with no k8s-driver-manager pod on the
	// node it is presumed live.
	if got := updated.Labels[devicePluginKey]; got != consts.RBLNDeployPausedForDriverUpgrade {
		t.Errorf("%s = %q, want the concurrent pause preserved", devicePluginKey, got)
	}
	for key, want := range rblnComponentLabels[consts.RBLNWorkloadConfigContainer] {
		if key == devicePluginKey {
			continue
		}
		if got := updated.Labels[key]; got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
}
