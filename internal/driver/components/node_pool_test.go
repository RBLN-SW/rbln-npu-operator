package components

import (
	"testing"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func TestGetSanitizedKernelVersion(t *testing.T) {
	tests := map[string]struct {
		input string
		want  string
	}{
		"strips x86_64": {
			input: "5.14.0-427.13.1.el8_9.x86_64",
			want:  "5.14.0-427.13.1.el8.9",
		},
		"strips aarch64": {
			input: "5.15.0-100-generic.aarch64",
			want:  "5.15.0-100-generic",
		},
		"no arch suffix unchanged": {
			input: "5.15.0-100-generic",
			want:  "5.15.0-100-generic",
		},
		"underscores to dots": {
			input: "5.14.0-427_el9",
			want:  "5.14.0-427.el9",
		},
		"lowercased": {
			input: "5.15.0-ABC",
			want:  "5.15.0-abc",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := getSanitizedKernelVersion(tc.input)
			if got != tc.want {
				t.Fatalf("getSanitizedKernelVersion(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestBuildNodeSelector(t *testing.T) {
	selector := map[string]string{"custom": "label"}
	result := buildNodeSelector(selector)

	if result[driverManagerDeployLabelKey] != "true" {
		t.Fatalf("missing deploy label key")
	}
	if result["custom"] != "label" {
		t.Fatalf("missing custom label")
	}
}

func TestBuildNodePool_MissingLabels(t *testing.T) {
	logger := logf.Log

	tests := map[string]struct {
		labels map[string]string
	}{
		"missing OS release ID": {
			labels: map[string]string{
				consts.NFDOSVersionIDLabelKey: "22.04",
				consts.NFDKernelLabelKey:      "5.15.0",
			},
		},
		"missing OS version ID": {
			labels: map[string]string{
				consts.NFDOSReleaseIDLabelKey: "ubuntu",
				consts.NFDKernelLabelKey:      "5.15.0",
			},
		},
		"missing kernel version": {
			labels: map[string]string{
				consts.NFDOSReleaseIDLabelKey: "ubuntu",
				consts.NFDOSVersionIDLabelKey: "22.04",
			},
		},
		"no labels at all": {
			labels: map[string]string{},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			node := corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "test-node", Labels: tc.labels},
			}
			_, ok := buildNodePool(node, map[string]string{}, logger)
			if ok {
				t.Fatal("expected buildNodePool to return false for missing labels")
			}
		})
	}
}

func TestBuildNodePool_AllLabels(t *testing.T) {
	logger := logf.Log
	node := corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Labels: map[string]string{
				consts.NFDOSReleaseIDLabelKey: "ubuntu",
				consts.NFDOSVersionIDLabelKey: "22.04",
				consts.NFDKernelLabelKey:      "5.15.0-100-generic",
			},
		},
	}

	pool, ok := buildNodePool(node, map[string]string{}, logger)
	if !ok {
		t.Fatal("expected buildNodePool to return true")
	}
	if pool.osRelease != "ubuntu" {
		t.Fatalf("osRelease = %q, want ubuntu", pool.osRelease)
	}
	if pool.osVersion != "22.04" {
		t.Fatalf("osVersion = %q, want 22.04", pool.osVersion)
	}
	if pool.kernel != "5.15.0-100-generic" {
		t.Fatalf("kernel = %q, want 5.15.0-100-generic", pool.kernel)
	}
	if pool.getOS() != "ubuntu22.04" {
		t.Fatalf("getOS() = %q, want ubuntu22.04", pool.getOS())
	}
}
