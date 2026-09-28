package upgrade

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func TestCordonOwnerOf(t *testing.T) {
	node := func(state string, annotations map[string]string) *corev1.Node {
		labels := map[string]string{}
		if state != "" {
			labels[UpgradeStateLabelKey] = state
		}
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations}}
	}
	claim := consts.DriverManagerCordonClaimAnnotation

	tests := map[string]struct {
		node *corev1.Node
		want cordonOwner
	}{
		"operator claim": {
			node: node(UpgradeStatePodRestartRequired, map[string]string{claim: consts.OperatorCordonClaimValue}),
			want: cordonOwnedByOperator,
		},
		"operator claim survives the state label": {
			node: node("", map[string]string{claim: consts.OperatorCordonClaimValue}),
			want: cordonOwnedByOperator,
		},
		"driver claim":        {node: node(UpgradeStateDone, map[string]string{claim: "driver"}), want: cordonOwnedByDriverManager},
		"vfio claim":          {node: node(UpgradeStateDone, map[string]string{claim: "vfio"}), want: cordonOwnedByDriverManager},
		"pre-rename claim":    {node: node(UpgradeStateDone, map[string]string{claim: "true"}), want: cordonOwnedByDriverManager},
		"unknown claim value": {node: node(UpgradeStateDone, map[string]string{claim: "someone"}), want: cordonOwnedByDriverManager},
		// Legacy: builds before the claim owned every cordon on a node past the
		// cordon step unless they had recorded the administrator's.
		"no claim, in flight past the cordon step": {node: node(UpgradeStatePodRestartRequired, nil), want: cordonOwnedByLegacyOperator},
		"no claim, skipped":                        {node: node(UpgradeStateSkipped, nil), want: cordonOwnedByLegacyOperator},
		"no claim, failed":                         {node: node(UpgradeStateFailed, nil), want: cordonOwnedByLegacyOperator},
		"no claim, administrator's recorded": {
			node: node(UpgradeStatePodRestartRequired, map[string]string{UpgradeInitialStateAnnotationKey: trueString}),
			want: cordonUnclaimed,
		},
		"no claim, not yet cordoned by the rollout": {node: node(UpgradeStateCordonRequired, nil), want: cordonUnclaimed},
		"no claim, queued":                          {node: node(UpgradeStateUpgradeRequired, nil), want: cordonUnclaimed},
		"no claim, done":                            {node: node(UpgradeStateDone, nil), want: cordonUnclaimed},
		"no claim, no label":                        {node: node("", nil), want: cordonUnclaimed},
		"claim explicitly emptied is no claim":      {node: node(UpgradeStateDone, map[string]string{claim: ""}), want: cordonUnclaimed},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := cordonOwnerOf(tc.node); got != tc.want {
				t.Fatalf("cordonOwnerOf() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCarriesRolloutBookkeeping(t *testing.T) {
	tests := map[string]struct {
		node *corev1.Node
		want bool
	}{
		"state label": {
			node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{UpgradeStateLabelKey: UpgradeStateDone}}},
			want: true,
		},
		"operator claim without label": {
			node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
				consts.DriverManagerCordonClaimAnnotation: consts.OperatorCordonClaimValue,
			}}},
			want: true,
		},
		"driver-manager claim only": {
			node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
				consts.DriverManagerCordonClaimAnnotation: "driver",
			}}},
			want: false,
		},
		"nothing": {node: &corev1.Node{}, want: false},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := CarriesRolloutBookkeeping(tc.node); got != tc.want {
				t.Fatalf("CarriesRolloutBookkeeping() = %v, want %v", got, tc.want)
			}
		})
	}
}
