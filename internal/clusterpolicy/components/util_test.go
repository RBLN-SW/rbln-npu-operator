package components

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

func TestComposeImageReference(t *testing.T) {
	tests := map[string]struct {
		registry string
		image    string
		want     string
	}{
		"normal path": {
			registry: "docker.io",
			image:    "rebellions/device-plugin",
			want:     "docker.io/rebellions/device-plugin",
		},
		"trailing slash on registry": {
			registry: "docker.io/",
			image:    "rebellions/device-plugin",
			want:     "docker.io/rebellions/device-plugin",
		},
		"leading slash on image": {
			registry: "docker.io",
			image:    "/rebellions/device-plugin",
			want:     "docker.io/rebellions/device-plugin",
		},
		"whitespace trimmed": {
			registry: "  docker.io  ",
			image:    "  rebellions/device-plugin  ",
			want:     "docker.io/rebellions/device-plugin",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := k8sutil.ComposeImageReference(tc.registry, tc.image)
			if got != tc.want {
				t.Fatalf("k8sutil.ComposeImageReference(%q, %q) = %q, want %q", tc.registry, tc.image, got, tc.want)
			}
		})
	}
}

func TestMergeEnvVars(t *testing.T) {
	tests := map[string]struct {
		base      []corev1.EnvVar
		additions [][]corev1.EnvVar
		wantNames []string
		wantValue map[string]string
	}{
		"empty base with additions": {
			base: nil,
			additions: [][]corev1.EnvVar{
				{{Name: "A", Value: "1"}},
			},
			wantNames: []string{"A"},
			wantValue: map[string]string{"A": "1"},
		},
		"override by name": {
			base: []corev1.EnvVar{{Name: "A", Value: "old"}},
			additions: [][]corev1.EnvVar{
				{{Name: "A", Value: "new"}},
			},
			wantNames: []string{"A"},
			wantValue: map[string]string{"A": "new"},
		},
		"later additions take precedence": {
			base: []corev1.EnvVar{{Name: "A", Value: "base"}},
			additions: [][]corev1.EnvVar{
				{{Name: "A", Value: "first"}},
				{{Name: "A", Value: "second"}},
			},
			wantNames: []string{"A"},
			wantValue: map[string]string{"A": "second"},
		},
		"disjoint names are appended": {
			base: []corev1.EnvVar{{Name: "A", Value: "1"}},
			additions: [][]corev1.EnvVar{
				{{Name: "B", Value: "2"}},
			},
			wantNames: []string{"A", "B"},
			wantValue: map[string]string{"A": "1", "B": "2"},
		},
		"nil additions returns clone of base": {
			base:      []corev1.EnvVar{{Name: "A", Value: "1"}},
			additions: nil,
			wantNames: []string{"A"},
			wantValue: map[string]string{"A": "1"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := mergeEnvVars(tc.base, tc.additions...)
			if len(result) != len(tc.wantNames) {
				t.Fatalf("len(result) = %d, want %d", len(result), len(tc.wantNames))
			}
			for i, wantName := range tc.wantNames {
				if result[i].Name != wantName {
					t.Fatalf("result[%d].Name = %q, want %q", i, result[i].Name, wantName)
				}
			}
			for k, v := range tc.wantValue {
				found := false
				for _, env := range result {
					if env.Name == k {
						if env.Value != v {
							t.Fatalf("env %q = %q, want %q", k, env.Value, v)
						}
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("env %q not found in result", k)
				}
			}
		})
	}
}

func TestCollectDevices(t *testing.T) {
	tests := map[string]struct {
		cards   []string
		want    []string
		wantErr bool
	}{
		"single valid card": {
			cards:   []string{consts.RBLNCardCA12},
			want:    []string{"1120", "1121"},
			wantErr: false,
		},
		"multiple valid cards": {
			cards:   []string{consts.RBLNCardCA12, consts.RBLNCardCA22},
			want:    []string{"1120", "1121", "1220", "1221"},
			wantErr: false,
		},
		"unknown card returns error": {
			cards:   []string{"RBLN-INVALID"},
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := collectDevices(tc.cards)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("len(got) = %d, want %d", len(got), len(tc.want))
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("got[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestSyncSpec(t *testing.T) {
	t.Run("disabled spec returns zero value", func(t *testing.T) {
		cpSpec := &rblnv1beta1.RBLNClusterPolicySpec{
			DevicePlugin: rblnv1beta1.RBLNDevicePluginSpec{Enabled: false},
		}
		result := syncSpec(cpSpec, cpSpec.DevicePlugin)
		if result.IsEnabled() {
			t.Fatal("expected disabled spec to return zero value")
		}
	})

	t.Run("nil daemonsets leaves spec unchanged", func(t *testing.T) {
		cpSpec := &rblnv1beta1.RBLNClusterPolicySpec{
			DevicePlugin: rblnv1beta1.RBLNDevicePluginSpec{
				Enabled: true,
				PodSpec: rblnv1beta1.PodSpec{
					Labels: map[string]string{"component": "dp"},
				},
			},
		}
		result := syncSpec(cpSpec, cpSpec.DevicePlugin)
		if result.Labels["component"] != "dp" {
			t.Fatal("expected label to be preserved")
		}
	})

	t.Run("daemonsets labels merge with component labels", func(t *testing.T) {
		cpSpec := &rblnv1beta1.RBLNClusterPolicySpec{
			PodDefaults: &rblnv1beta1.PodDefaultsSpec{
				Labels: map[string]string{"global": "true"},
			},
			DevicePlugin: rblnv1beta1.RBLNDevicePluginSpec{
				Enabled: true,
				PodSpec: rblnv1beta1.PodSpec{
					Labels: map[string]string{"component": "dp"},
				},
			},
		}
		result := syncSpec(cpSpec, cpSpec.DevicePlugin)
		if result.Labels["global"] != "true" {
			t.Fatal("expected global label to be inherited")
		}
		if result.Labels["component"] != "dp" {
			t.Fatal("expected component label to take precedence")
		}
	})

	t.Run("daemonsets tolerations inherited when component has none", func(t *testing.T) {
		cpSpec := &rblnv1beta1.RBLNClusterPolicySpec{
			PodDefaults: &rblnv1beta1.PodDefaultsSpec{
				Tolerations: []corev1.Toleration{{Key: "npu", Effect: corev1.TaintEffectNoSchedule}},
			},
			DevicePlugin: rblnv1beta1.RBLNDevicePluginSpec{Enabled: true},
		}
		result := syncSpec(cpSpec, cpSpec.DevicePlugin)
		if len(result.Tolerations) != 1 || result.Tolerations[0].Key != "npu" {
			t.Fatal("expected tolerations to be inherited from daemonsets")
		}
	})

	t.Run("component tolerations not overridden", func(t *testing.T) {
		cpSpec := &rblnv1beta1.RBLNClusterPolicySpec{
			PodDefaults: &rblnv1beta1.PodDefaultsSpec{
				Tolerations: []corev1.Toleration{{Key: "global"}},
			},
			DevicePlugin: rblnv1beta1.RBLNDevicePluginSpec{
				Enabled: true,
				PodSpec: rblnv1beta1.PodSpec{
					Tolerations: []corev1.Toleration{{Key: "component"}},
				},
			},
		}
		result := syncSpec(cpSpec, cpSpec.DevicePlugin)
		if len(result.Tolerations) != 1 || result.Tolerations[0].Key != "component" {
			t.Fatal("expected component tolerations to be preserved")
		}
	})
}
