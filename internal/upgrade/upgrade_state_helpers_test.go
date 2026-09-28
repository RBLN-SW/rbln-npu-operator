package upgrade

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func TestIsNodeUnschedulable(t *testing.T) {
	tests := map[string]struct {
		unschedulable bool
		want          bool
	}{
		"returns true for unschedulable node": {
			unschedulable: true,
			want:          true,
		},
		"returns false for schedulable node": {
			unschedulable: false,
			want:          false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			node := &corev1.Node{
				Spec: corev1.NodeSpec{Unschedulable: tc.unschedulable},
			}
			if got := IsNodeUnschedulable(node); got != tc.want {
				t.Fatalf("IsNodeUnschedulable() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestIsNodeInRequestorMode(t *testing.T) {
	tests := map[string]struct {
		annotations map[string]string
		want        bool
	}{
		"returns true when requestor mode annotation is set": {
			annotations: map[string]string{
				UpgradeRequestorModeAnnotationKey: "true",
			},
			want: true,
		},
		"returns false when requestor mode annotation is absent": {
			annotations: nil,
			want:        false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Annotations: tc.annotations},
			}
			if got := IsNodeInRequestorMode(node); got != tc.want {
				t.Fatalf("IsNodeInRequestorMode() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestIsManagedUpgradeState(t *testing.T) {
	tests := map[string]struct {
		state string
		want  bool
	}{
		"empty string (Unknown) is managed": {
			state: UpgradeStateUnknown,
			want:  true,
		},
		"Done is managed": {
			state: UpgradeStateDone,
			want:  true,
		},
		"arbitrary string is not managed": {
			state: "some-random-state",
			want:  false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := IsManagedUpgradeState(tc.state); got != tc.want {
				t.Fatalf("IsManagedUpgradeState(%q) = %t, want %t", tc.state, got, tc.want)
			}
		})
	}
}

func TestNewClusterUpgradeState(t *testing.T) {
	state := NewClusterUpgradeState()
	if state.NodeStates == nil {
		t.Fatal("NodeStates should not be nil")
	}
	if len(state.NodeStates) != 0 {
		t.Fatalf("NodeStates should be empty, got %d entries", len(state.NodeStates))
	}
}

func TestShouldReleaseCordonOnTeardown(t *testing.T) {
	node := func(state string, annotations map[string]string) *corev1.Node {
		labels := map[string]string{}
		if state != "" {
			labels[UpgradeStateLabelKey] = state
		}
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations},
			Spec:       corev1.NodeSpec{Unschedulable: true},
		}
	}
	claim := func(value string) map[string]string {
		return map[string]string{consts.DriverManagerCordonClaimAnnotation: value}
	}

	tests := map[string]struct {
		node *corev1.Node
		want bool
	}{
		"the rollout's claimed cordon goes back into service": {
			node: node(UpgradeStatePodRestartRequired, claim(consts.OperatorCordonClaimValue)),
			want: true,
		},
		// The label was lost, the claim was not: still the rollout's.
		"claimed cordon with no state label": {
			node: node("", claim(consts.OperatorCordonClaimValue)),
			want: true,
		},
		"cordon step wrote the claim but not yet the label": {
			node: node(UpgradeStateCordonRequired, claim(consts.OperatorCordonClaimValue)),
			want: true,
		},
		// Not judged by the cordon step yet: whatever cordon is there is not
		// the rollout's to lift.
		"unclaimed cordon in cordon-required": {
			node: node(UpgradeStateCordonRequired, nil),
			want: false,
		},
		// The driver did not come up; the node stays isolated.
		"upgrade-failed keeps its cordon": {
			node: node(UpgradeStateFailed, claim(consts.OperatorCordonClaimValue)),
			want: false,
		},
		"administrator's cordon is left alone": {
			node: node(UpgradeStatePodRestartRequired, map[string]string{UpgradeInitialStateAnnotationKey: trueString}),
			want: false,
		},
		// Manual-mode ownership: autoUpgrade is off, so the binary may be
		// holding this one legitimately.
		"k8s-driver-manager's cordon is left alone": {
			node: node(UpgradeStatePodRestartRequired, claim("driver")),
			want: false,
		},
		"requestor mode owns its own cordon": {
			node: node(UpgradeStatePodRestartRequired, map[string]string{
				consts.DriverManagerCordonClaimAnnotation: consts.OperatorCordonClaimValue,
				UpgradeRequestorModeAnnotationKey:         trueString,
			}),
			want: false,
		},
		// TODO(remove after two releases): cordons taken by builds that wrote no claim.
		"legacy build's cordon mid-rollout": {
			node: node(UpgradeStatePodRestartRequired, nil),
			want: true,
		},
		"legacy build's cordon on a skipped node": {
			node: node(UpgradeStateSkipped, nil),
			want: true,
		},
		"upgrade-required was never cordoned by the rollout": {
			node: node(UpgradeStateUpgradeRequired, nil),
			want: false,
		},
		"upgrade-done is finished": {
			node: node(UpgradeStateDone, nil),
			want: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := ShouldReleaseCordonOnTeardown(tc.node); got != tc.want {
				t.Fatalf("ShouldReleaseCordonOnTeardown() = %v, want %v", got, tc.want)
			}
		})
	}
}
