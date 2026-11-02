package upgrade

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestWaitingForDriverLoad(t *testing.T) {
	tests := map[string]struct {
		node *corev1.Node
		want bool
	}{
		"nil node returns false": {
			node: nil,
			want: false,
		},
		"node without annotation returns false": {
			node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}},
			want: false,
		},
		"node with annotation returns true": {
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "n1",
					Annotations: map[string]string{
						UpgradeWaitForSafeDriverLoadAnnotationKey: "true",
					},
				},
			},
			want: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := waitingForDriverLoad(tc.node); got != tc.want {
				t.Fatalf("waitingForDriverLoad() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestUnblockDriverLoad(t *testing.T) {
	t.Run("no-op when annotation is absent", func(t *testing.T) {
		provider := newTestNodeUpgradeStateProvider(t)
		mgr := &ClusterUpgradeStateManagerImpl{nodeUpgradeStateProvider: provider}

		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1"}}
		if err := provider.K8sClient.Create(context.Background(), node); err != nil {
			t.Fatalf("create node: %v", err)
		}
		if err := mgr.unblockDriverLoad(context.Background(), node); err != nil {
			t.Fatalf("unblockDriverLoad() unexpected error: %v", err)
		}
	})
}
