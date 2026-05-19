package components

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func TestIsReady(t *testing.T) {
	const (
		name      = "test-component"
		namespace = "test-ns"
	)
	ctx := context.Background()

	tests := map[string]struct {
		ds        *appsv1.DaemonSet
		nodeCount int32
		wantState rblnv1beta1.ComponentState
	}{
		"DaemonSet not found, nodes present → notReady": {
			ds:        nil,
			nodeCount: 1,
			wantState: rblnv1beta1.ComponentStateNotReady,
		},
		"DaemonSet not found, no nodes for workload → notReady (Patch must have run first)": {
			ds:        nil,
			nodeCount: 0,
			wantState: rblnv1beta1.ComponentStateNotReady,
		},
		"DesiredNumberScheduled is zero, nodes present → notReady (label mismatch)": {
			ds: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Status: appsv1.DaemonSetStatus{
					DesiredNumberScheduled: 0,
					NumberReady:            0,
				},
			},
			nodeCount: 1,
			wantState: rblnv1beta1.ComponentStateNotReady,
		},
		"DesiredNumberScheduled is zero, no nodes for workload → ready (idle)": {
			ds: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Status: appsv1.DaemonSetStatus{
					DesiredNumberScheduled: 0,
					NumberReady:            0,
				},
			},
			nodeCount: 0,
			wantState: rblnv1beta1.ComponentStateReady,
		},
		"NumberReady < DesiredNumberScheduled → notReady (progressing)": {
			ds: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Status: appsv1.DaemonSetStatus{
					DesiredNumberScheduled: 3,
					NumberReady:            1,
					NumberUnavailable:      2,
				},
			},
			nodeCount: 3,
			wantState: rblnv1beta1.ComponentStateNotReady,
		},
		"all pods ready → ready": {
			ds: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Status: appsv1.DaemonSetStatus{
					DesiredNumberScheduled: 3,
					NumberReady:            3,
					NumberUnavailable:      0,
				},
			},
			nodeCount: 3,
			wantState: rblnv1beta1.ComponentStateReady,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			scheme := newTestScheme(t)
			var c client.Client
			if tc.ds != nil {
				c = newFakeClient(t, scheme, tc.ds)
			} else {
				c = newFakeClient(t, scheme)
			}

			bp := &basePatcher{
				client:       c,
				name:         name,
				namespace:    namespace,
				workloadType: consts.RBLNWorkloadConfigContainer,
			}

			got := bp.IsReady(ctx, tc.nodeCount)
			if got.State != tc.wantState {
				t.Fatalf("state: got %q, want %q (msg=%q)", got.State, tc.wantState, got.Message)
			}
		})
	}
}
