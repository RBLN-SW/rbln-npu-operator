package driver

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestSystemProbeUsesRunningOperatorImage(t *testing.T) {
	t.Setenv("RBLN_SYSTEM_PROBE_IMAGE", "")
	t.Setenv("POD_NAME", "operator-abc")
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "operator-abc", Namespace: "system"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "sidecar", Image: "sidecar:old"}, {Name: "manager", Image: "private/operator:new", ImagePullPolicy: corev1.PullAlways}}, ImagePullSecrets: []corev1.LocalObjectReference{{Name: "operator-pull"}}}}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	result, err := resolveSystemProbeImage(context.Background(), reader, "system")
	if err != nil || result.Reference != "private/operator:new" || result.PullPolicy != corev1.PullAlways || len(result.PullSecrets) != 1 || result.PullSecrets[0].Name != "operator-pull" {
		t.Fatalf("probe identity %+v %v", result, err)
	}
	if _, err := resolveSystemProbeImage(context.Background(), reader, "missing"); err == nil {
		t.Fatal("accepted missing operator Pod")
	}
}
func TestSystemProbeLocalImageOverride(t *testing.T) {
	t.Setenv("RBLN_SYSTEM_PROBE_IMAGE", "local/operator:test")
	result, err := resolveSystemProbeImage(context.Background(), nil, "system")
	if err != nil || result.Reference != "local/operator:test" {
		t.Fatalf("%+v %v", result, err)
	}
}
