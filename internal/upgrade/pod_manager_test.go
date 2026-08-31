package upgrade

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestGetPodControllerRevisionHash(t *testing.T) {
	pm := &PodManager{}

	t.Run("returns hash when label exists", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{PodControllerRevisionHashLabelKey: "abc123"},
			},
		}
		hash, err := pm.GetPodControllerRevisionHash(pod)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if hash != "abc123" {
			t.Fatalf("hash = %q, want %q", hash, "abc123")
		}
	})

	t.Run("returns error when label is missing", func(t *testing.T) {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}}}
		_, err := pm.GetPodControllerRevisionHash(pod)
		if err == nil {
			t.Fatal("expected error for missing label")
		}
	})
}

func TestIsPodRunningOrPending(t *testing.T) {
	pm := &PodManager{}

	tests := map[string]struct {
		phase corev1.PodPhase
		want  bool
	}{
		"Running":   {phase: corev1.PodRunning, want: true},
		"Pending":   {phase: corev1.PodPending, want: true},
		"Succeeded": {phase: corev1.PodSucceeded, want: false},
		"Failed":    {phase: corev1.PodFailed, want: false},
		"Unknown":   {phase: corev1.PodUnknown, want: false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pod := corev1.Pod{Status: corev1.PodStatus{Phase: tc.phase}}
			if got := pm.IsPodRunningOrPending(t.Context(), pod); got != tc.want {
				t.Fatalf("IsPodRunningOrPending(%s) = %t, want %t", tc.phase, got, tc.want)
			}
		})
	}
}

func TestUpdateNodeToDrainOrSkipped(t *testing.T) {
	newManagerWithNode := func(t *testing.T, state string) (*PodManager, *corev1.Node) {
		t.Helper()
		scheme := runtime.NewScheme()
		if err := corev1.AddToScheme(scheme); err != nil {
			t.Fatalf("add corev1 scheme: %v", err)
		}
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name:   "evict-worker",
			Labels: map[string]string{UpgradeStateLabelKey: state},
		}}
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()
		provider := NewNodeUpgradeStateProvider(k8sClient, nil)
		return &PodManager{nodeUpgradeStateProvider: provider}, node
	}

	t.Run("drain enabled falls through to drain-required", func(t *testing.T) {
		pm, node := newManagerWithNode(t, UpgradeStatePodDeletionRequired)
		pm.updateNodeToDrainOrSkipped(context.Background(), *node, true, "pod eviction failed")

		updated := &corev1.Node{}
		if err := pm.nodeUpgradeStateProvider.K8sClient.Get(context.Background(),
			types.NamespacedName{Name: node.Name}, updated); err != nil {
			t.Fatalf("get node: %v", err)
		}
		if got := updated.Labels[UpgradeStateLabelKey]; got != UpgradeStateDrainRequired {
			t.Fatalf("state = %q, want %q", got, UpgradeStateDrainRequired)
		}
	})

	t.Run("drain disabled skips the attempt with a recorded reason", func(t *testing.T) {
		pm, node := newManagerWithNode(t, UpgradeStatePodDeletionRequired)
		pm.updateNodeToDrainOrSkipped(context.Background(), *node, false, "pod eviction failed: PDB webapp-pdb")

		updated := &corev1.Node{}
		if err := pm.nodeUpgradeStateProvider.K8sClient.Get(context.Background(),
			types.NamespacedName{Name: node.Name}, updated); err != nil {
			t.Fatalf("get node: %v", err)
		}
		if got := updated.Labels[UpgradeStateLabelKey]; got != UpgradeStateSkipped {
			t.Fatalf("state = %q, want %q", got, UpgradeStateSkipped)
		}
		if reason := updated.Annotations[UpgradeSkipReasonAnnotationKey]; !strings.Contains(reason, "PDB webapp-pdb") {
			t.Fatalf("skip reason = %q, want it to carry the eviction failure", reason)
		}
	})
}
