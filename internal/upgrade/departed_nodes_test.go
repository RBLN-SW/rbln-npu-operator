package upgrade

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func departedTestNode(name, state, deploy string, annotations map[string]string) *corev1.Node {
	labels := map[string]string{}
	if state != "" {
		labels[UpgradeStateLabelKey] = state
	}
	if deploy != "" {
		labels[consts.RBLNDeployDriverLabelKey] = deploy
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels, Annotations: annotations},
		Spec:       corev1.NodeSpec{Unschedulable: true},
	}
}

func TestFindDepartedNodes(t *testing.T) {
	mgr := newTestManager(t)
	registerNodes(t, mgr,
		departedTestNode("in-scope", UpgradeStatePodRestartRequired, "true", nil),
		departedTestNode("vm-passthrough", UpgradeStatePodRestartRequired, "", nil),
		departedTestNode("host-driver", UpgradeStateFailed, consts.RBLNDeployDriverPreInstalled, nil),
		departedTestNode("finished-and-left", UpgradeStateDone, "false", nil),
		departedTestNode("never-in-rollout", "", "", nil),
		departedTestNode("not-an-npu-node", "", "", map[string]string{consts.DriverManagerCordonClaimAnnotation: "driver"}),
	)

	departed, err := mgr.findDepartedNodes(context.Background())
	if err != nil {
		t.Fatalf("findDepartedNodes: %v", err)
	}
	got := map[string]bool{}
	for _, n := range departed {
		got[n.Name] = true
	}
	want := map[string]bool{"vm-passthrough": true, "host-driver": true, "finished-and-left": true}
	if len(got) != len(want) {
		t.Fatalf("departed = %v, want %v", got, want)
	}
	for name := range want {
		if !got[name] {
			t.Fatalf("departed = %v, want %v", got, want)
		}
	}
}

func TestProcessDepartedNodes(t *testing.T) {
	claim := consts.DriverManagerCordonClaimAnnotation
	tests := map[string]struct {
		node              *corev1.Node
		wantUnschedulable bool
	}{
		"in-flight node with the rollout's cordon goes back into service": {
			node: departedTestNode("n", UpgradeStatePodRestartRequired, "", map[string]string{
				claim: consts.OperatorCordonClaimValue, UpgradePodRestartStartTimeAnnotationKey: "1",
			}),
		},
		// The driver DaemonSet no longer targets the node, so the cordon that
		// kept a broken driver isolated guards nothing; the node's next role is
		// somebody else's to manage.
		"upgrade-failed node is released too": {
			node: departedTestNode("n", UpgradeStateFailed, consts.RBLNDeployDriverPreInstalled, map[string]string{
				claim: consts.OperatorCordonClaimValue, UpgradeFailureReasonAnnotationKey: "crash",
			}),
		},
		"administrator's cordon stays": {
			node: departedTestNode("n", UpgradeStatePodRestartRequired, "", map[string]string{
				UpgradeInitialStateAnnotationKey: trueString,
			}),
			wantUnschedulable: true,
		},
		"k8s-driver-manager's cordon stays": {
			node:              departedTestNode("n", UpgradeStatePodRestartRequired, "", map[string]string{claim: "driver"}),
			wantUnschedulable: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			mgr := newTestManager(t)
			registerNodes(t, mgr, tc.node)

			state := NewClusterUpgradeState()
			state.DepartedNodes = []*corev1.Node{tc.node}
			if err := mgr.ProcessDepartedNodes(context.Background(), &state); err != nil {
				t.Fatalf("ProcessDepartedNodes: %v", err)
			}

			var updated corev1.Node
			if err := mgr.k8sClient.Get(context.Background(), types.NamespacedName{Name: "n"}, &updated); err != nil {
				t.Fatalf("get node: %v", err)
			}
			if _, labeled := updated.Labels[UpgradeStateLabelKey]; labeled {
				t.Fatal("state label must go")
			}
			for _, key := range teardownAnnotationKeys {
				if _, left := updated.Annotations[key]; left {
					t.Fatalf("bookkeeping annotation %s must go", key)
				}
			}
			if updated.Spec.Unschedulable != tc.wantUnschedulable {
				t.Fatalf("unschedulable = %v, want %v", updated.Spec.Unschedulable, tc.wantUnschedulable)
			}
			if got := updated.Annotations[claim]; got == consts.OperatorCordonClaimValue {
				t.Fatal("the rollout's claim must go with its cordon")
			}
		})
	}
}
