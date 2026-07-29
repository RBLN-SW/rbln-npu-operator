package upgrade

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newTestNodeUpgradeStateProvider(t *testing.T) *NodeUpgradeStateProvider {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	return NewNodeUpgradeStateProvider(k8sClient, logr.Discard(), nil)
}

func newTestNode(name string, annotations map[string]string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Annotations: annotations,
		},
	}
}

func TestCheckAnnotationTimeout(t *testing.T) {
	tests := map[string]struct {
		annotations    map[string]string
		timeoutSeconds int64
		wantTimedOut   bool
		wantErr        bool
	}{
		"sets annotation when absent and returns false": {
			annotations:    nil,
			timeoutSeconds: 60,
			wantTimedOut:   false,
		},
		"returns false when within timeout window": {
			annotations: map[string]string{
				"test-annotation": strconv.FormatInt(time.Now().Unix()-10, 10),
			},
			timeoutSeconds: 60,
			wantTimedOut:   false,
		},
		"returns true when timeout expired": {
			annotations: map[string]string{
				"test-annotation": strconv.FormatInt(time.Now().Unix()-120, 10),
			},
			timeoutSeconds: 60,
			wantTimedOut:   true,
		},
		"returns error for unparseable annotation value": {
			annotations: map[string]string{
				"test-annotation": "not-a-number",
			},
			timeoutSeconds: 60,
			wantErr:        true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			provider := newTestNodeUpgradeStateProvider(t)
			node := newTestNode("test-node", tc.annotations)
			// Register the node in the fake client so provider can patch it.
			ctx := context.Background()
			if err := provider.K8sClient.Create(ctx, node); err != nil {
				t.Fatalf("create node: %v", err)
			}

			timedOut, err := checkAnnotationTimeout(ctx, provider, node, "test-annotation", tc.timeoutSeconds)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if timedOut != tc.wantTimedOut {
				t.Fatalf("timedOut = %t, want %t", timedOut, tc.wantTimedOut)
			}
		})
	}
}
