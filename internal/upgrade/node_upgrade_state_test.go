package upgrade

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestNodeUpgradeStateProviderChangeState(t *testing.T) {
	provider := newTestNodeUpgradeStateProvider(t)
	ctx := context.Background()

	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-1"}}
	if err := provider.K8sClient.Create(ctx, node); err != nil {
		t.Fatalf("create node: %v", err)
	}

	err := provider.ChangeNodeUpgradeState(ctx, node, UpgradeStateCordonRequired)
	if err != nil {
		t.Fatalf("ChangeNodeUpgradeState() error: %v", err)
	}

	var updated corev1.Node
	if err := provider.K8sClient.Get(ctx, types.NamespacedName{Name: "worker-1"}, &updated); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got := updated.Labels[UpgradeStateLabelKey]; got != UpgradeStateCordonRequired {
		t.Fatalf("label = %q, want %q", got, UpgradeStateCordonRequired)
	}
}

func TestNodeUpgradeStateProviderSetAndRemoveAnnotation(t *testing.T) {
	provider := newTestNodeUpgradeStateProvider(t)
	ctx := context.Background()

	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-1"}}
	if err := provider.K8sClient.Create(ctx, node); err != nil {
		t.Fatalf("create node: %v", err)
	}

	// Set annotation
	err := provider.SetNodeUpgradeAnnotation(ctx, node, "test-key", "test-value")
	if err != nil {
		t.Fatalf("SetNodeUpgradeAnnotation() error: %v", err)
	}

	var updated corev1.Node
	if err := provider.K8sClient.Get(ctx, types.NamespacedName{Name: "worker-1"}, &updated); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got := updated.Annotations["test-key"]; got != "test-value" {
		t.Fatalf("annotation = %q, want %q", got, "test-value")
	}

	// Remove annotation
	err = provider.RemoveNodeUpgradeAnnotation(ctx, &updated, "test-key")
	if err != nil {
		t.Fatalf("RemoveNodeUpgradeAnnotation() error: %v", err)
	}

	if err := provider.K8sClient.Get(ctx, types.NamespacedName{Name: "worker-1"}, &updated); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if _, exists := updated.Annotations["test-key"]; exists {
		t.Fatal("annotation should have been removed")
	}
}

func TestNodeUpgradeStateProviderSetAnnotationsBatch(t *testing.T) {
	provider := newTestNodeUpgradeStateProvider(t)
	ctx := context.Background()

	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:        "worker-1",
		Annotations: map[string]string{"stale-key": "stale-value"},
	}}
	if err := provider.K8sClient.Create(ctx, node); err != nil {
		t.Fatalf("create node: %v", err)
	}

	err := provider.SetNodeUpgradeAnnotations(ctx, node, map[string]any{
		"key-a":     "value-a",
		"key-b":     "value-b",
		"stale-key": nil,
	})
	if err != nil {
		t.Fatalf("SetNodeUpgradeAnnotations() error: %v", err)
	}

	var updated corev1.Node
	if err := provider.K8sClient.Get(ctx, types.NamespacedName{Name: "worker-1"}, &updated); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got := updated.Annotations["key-a"]; got != "value-a" {
		t.Fatalf("annotation key-a = %q, want %q", got, "value-a")
	}
	if got := updated.Annotations["key-b"]; got != "value-b" {
		t.Fatalf("annotation key-b = %q, want %q", got, "value-b")
	}
	if _, exists := updated.Annotations["stale-key"]; exists {
		t.Fatal("nil-valued annotation should have been removed")
	}
}

func TestTruncateReason(t *testing.T) {
	if got := truncateReason("short reason"); got != "short reason" {
		t.Fatalf("truncateReason = %q, want unchanged", got)
	}

	long := strings.Repeat("x", 500)
	got := truncateReason(long)
	if len(got) != 403 || !strings.HasSuffix(got, "...") {
		t.Fatalf("truncateReason len = %d, want 403 ending in ellipsis", len(got))
	}
}

func TestNodeUpgradeStateProviderGetNode(t *testing.T) {
	provider := newTestNodeUpgradeStateProvider(t)
	ctx := context.Background()

	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-1"}}
	if err := provider.K8sClient.Create(ctx, node); err != nil {
		t.Fatalf("create node: %v", err)
	}

	t.Run("returns existing node", func(t *testing.T) {
		got, err := provider.GetNode(ctx, "worker-1")
		if err != nil {
			t.Fatalf("GetNode() error: %v", err)
		}
		if got.Name != "worker-1" {
			t.Fatalf("node.Name = %q, want worker-1", got.Name)
		}
	})

	t.Run("returns error for nonexistent node", func(t *testing.T) {
		_, err := provider.GetNode(ctx, "nonexistent")
		if err == nil {
			t.Fatal("expected error for nonexistent node")
		}
	})
}
