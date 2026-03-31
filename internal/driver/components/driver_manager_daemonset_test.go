package components

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

func TestShouldSkipDaemonSetUpdateByDriverHash(t *testing.T) {
	h := &driverManagerPatcher{
		basePatcher: basePatcher{log: logf.Log},
	}

	tests := map[string]struct {
		current  *appsv1.DaemonSet
		digest   string
		wantSkip bool
	}{
		"nil current returns false": {
			current:  nil,
			digest:   "abc123",
			wantSkip: false,
		},
		"matching annotation hash skips update": {
			current: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{driverLastAppliedHashAnnotation: "abc123"},
				},
			},
			digest:   "abc123",
			wantSkip: true,
		},
		"different annotation hash allows update": {
			current: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{driverLastAppliedHashAnnotation: "old-hash"},
				},
			},
			digest:   "new-hash",
			wantSkip: false,
		},
		"missing annotation falls back to container hash": {
			current: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{}},
				Spec: appsv1.DaemonSetSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "test", Image: "img:v1"}},
						},
					},
				},
			},
			digest:   k8sutil.GetObjectHash([]corev1.Container{{Name: "test", Image: "img:v1"}}),
			wantSkip: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := h.shouldSkipDaemonSetUpdateByDriverHash(tc.current, tc.digest)
			if got != tc.wantSkip {
				t.Fatalf("shouldSkipDaemonSetUpdateByDriverHash() = %v, want %v", got, tc.wantSkip)
			}
		})
	}
}

func TestUpsertEnvVar(t *testing.T) {
	tests := map[string]struct {
		base   []corev1.EnvVar
		target corev1.EnvVar
		want   string
	}{
		"insert new": {
			base:   []corev1.EnvVar{{Name: "A", Value: "1"}},
			target: corev1.EnvVar{Name: "B", Value: "2"},
			want:   "2",
		},
		"update existing": {
			base:   []corev1.EnvVar{{Name: "A", Value: "old"}},
			target: corev1.EnvVar{Name: "A", Value: "new"},
			want:   "new",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := upsertEnvVar(tc.base, tc.target)
			found := false
			for _, env := range result {
				if env.Name == tc.target.Name {
					if env.Value != tc.want {
						t.Fatalf("env %q = %q, want %q", tc.target.Name, env.Value, tc.want)
					}
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("env %q not found in result", tc.target.Name)
			}
		})
	}
}

func TestDriverManagerLabels(t *testing.T) {
	h := &driverManagerPatcher{
		basePatcher: basePatcher{name: driverManagerName, instanceName: testInstanceName},
	}
	pool := nodePool{name: "ubuntu22.04-5.15.0"}

	labels := h.driverManagerLabels(pool)

	if labels[driverManagerAppLabelKey] != driverManagerName {
		t.Fatalf("app label = %q, want %q", labels[driverManagerAppLabelKey], driverManagerName)
	}
	if labels[driverManagerNodePoolLabelKey] != pool.name {
		t.Fatalf("pool label = %q, want %q", labels[driverManagerNodePoolLabelKey], pool.name)
	}
	if labels[driverManagerInstanceLabelKey] != testInstanceName {
		t.Fatalf("instance label = %q, want %q", labels[driverManagerInstanceLabelKey], testInstanceName)
	}
}
