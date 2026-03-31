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

func TestIsOrphanedPod(t *testing.T) {
	tests := map[string]struct {
		ownerRefs []metav1.OwnerReference
		want      bool
	}{
		"returns true when no owner references": {
			ownerRefs: nil,
			want:      true,
		},
		"returns false when owner references exist": {
			ownerRefs: []metav1.OwnerReference{{Name: "ds"}},
			want:      false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{OwnerReferences: tc.ownerRefs},
			}
			if got := IsOrphanedPod(pod); got != tc.want {
				t.Fatalf("IsOrphanedPod() = %t, want %t", got, tc.want)
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
