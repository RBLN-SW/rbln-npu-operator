package upgrade

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestBuildRebootPodName(t *testing.T) {
	got := BuildRebootPodName("worker-1", 1700000000)
	want := "rbln-reboot-worker-1-1700000000"
	if got != want {
		t.Fatalf("BuildRebootPodName() = %q, want %q", got, want)
	}
}

func TestPodRebootManagerTrigger(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	mgr := &PodRebootManager{k8sClient: k8sClient, log: logr.Discard(), rebootImage: "default:v1"}

	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-1"}}
	req := RebootTriggerRequest{
		RequestedAtUnix: 1700000000,
		PreRebootBootID: "boot-1",
		Namespace:       "rbln-system",
		PodName:         "rbln-reboot-worker-1-1700000000",
		Image:           "custom:v2",
	}

	t.Run("creates reboot pod", func(t *testing.T) {
		err := mgr.Trigger(context.Background(), node, req)
		if err != nil {
			t.Fatalf("Trigger() error: %v", err)
		}

		var pod corev1.Pod
		err = k8sClient.Get(context.Background(), types.NamespacedName{
			Name: req.PodName, Namespace: req.Namespace,
		}, &pod)
		if err != nil {
			t.Fatalf("expected pod to be created: %v", err)
		}
		if pod.Spec.NodeName != "worker-1" {
			t.Fatalf("pod.Spec.NodeName = %q, want worker-1", pod.Spec.NodeName)
		}
		if pod.Spec.Containers[0].Image != "custom:v2" {
			t.Fatalf("pod image = %q, want custom:v2", pod.Spec.Containers[0].Image)
		}
	})

	t.Run("already exists is idempotent", func(t *testing.T) {
		err := mgr.Trigger(context.Background(), node, req)
		if err != nil {
			t.Fatalf("Trigger() should be idempotent, got error: %v", err)
		}
	})
}

func TestPodRebootManagerDeleteRebootPod(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	t.Run("deletes existing pod", func(t *testing.T) {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "rbln-reboot-1", Namespace: "rbln-system"}}
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
		mgr := &PodRebootManager{k8sClient: k8sClient, log: logr.Discard()}

		err := mgr.DeleteRebootPod(context.Background(), "rbln-system", "rbln-reboot-1")
		if err != nil {
			t.Fatalf("DeleteRebootPod() error: %v", err)
		}
	})

	t.Run("not found is idempotent", func(t *testing.T) {
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		mgr := &PodRebootManager{k8sClient: k8sClient, log: logr.Discard()}

		err := mgr.DeleteRebootPod(context.Background(), "rbln-system", "nonexistent")
		if err != nil {
			t.Fatalf("DeleteRebootPod() should be idempotent, got error: %v", err)
		}
	})

	t.Run("empty namespace is no-op", func(t *testing.T) {
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		mgr := &PodRebootManager{k8sClient: k8sClient, log: logr.Discard()}

		err := mgr.DeleteRebootPod(context.Background(), "", "pod-1")
		if err != nil {
			t.Fatalf("DeleteRebootPod() with empty namespace should be no-op, got error: %v", err)
		}
	})
}
