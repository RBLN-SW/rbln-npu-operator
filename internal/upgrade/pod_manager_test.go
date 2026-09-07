package upgrade

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
