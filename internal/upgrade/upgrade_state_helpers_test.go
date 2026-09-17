package upgrade

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	node := func(state string, unschedulable bool, annotations map[string]string) *corev1.Node {
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      map[string]string{UpgradeStateLabelKey: state},
				Annotations: annotations,
			},
			Spec: corev1.NodeSpec{Unschedulable: unschedulable},
		}
	}

	tests := map[string]struct {
		node *corev1.Node
		want bool
	}{
		"mid-rollout node goes back into service": {
			node: node(UpgradeStatePodRestartRequired, true, nil),
			want: true,
		},
		"node waiting to be cordoned may already carry the cordon": {
			node: node(UpgradeStateCordonRequired, true, nil),
			want: true,
		},
		"node about to be uncordoned anyway": {
			node: node(UpgradeStateUncordonRequired, true, nil),
			want: true,
		},
		// The driver did not come up; the node stays isolated and the
		// failure-reason annotation outlives the label to explain why.
		"upgrade-failed keeps its cordon": {
			node: node(UpgradeStateFailed, true, nil),
			want: false,
		},
		// Someone cordoned this node before the rollout ever saw it.
		"administrator's cordon is left alone": {
			node: node(UpgradeStatePodRestartRequired, true,
				map[string]string{UpgradeInitialStateAnnotationKey: trueString}),
			want: false,
		},
		"requestor mode owns its own cordon": {
			node: node(UpgradeStatePodRestartRequired, true,
				map[string]string{UpgradeRequestorModeAnnotationKey: trueString}),
			want: false,
		},
		// The node is read from the informer cache, which may not have seen
		// the cordon yet; the release is idempotent, so it is sent anyway.
		"cache still shows the node schedulable": {
			node: node(UpgradeStatePodRestartRequired, false, nil),
			want: true,
		},
		// markNodeUpgradeSkipped only labels; the uncordon happens on the
		// next pass, so the rollout's cordon may still be on the node.
		"upgrade-skipped not yet uncordoned by its next pass": {
			node: node(UpgradeStateSkipped, true, nil),
			want: true,
		},
		// Not admitted yet, so any cordon on it is not this rollout's.
		"upgrade-required was never cordoned by the rollout": {
			node: node(UpgradeStateUpgradeRequired, true, nil),
			want: false,
		},
		"upgrade-done is finished": {
			node: node(UpgradeStateDone, true, nil),
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
