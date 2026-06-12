package k8sutil

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestContainerBuilderWithImage(t *testing.T) {
	const img = "docker.io/rebellions/k8s-device-plugin"
	digest := "sha256:" + strings.Repeat("a", 64)

	tests := map[string]struct {
		tag        string
		pullPolicy corev1.PullPolicy
		wantImage  string
		wantPolicy corev1.PullPolicy
	}{
		"tag is joined with a colon": {
			tag:        "v0.4.1",
			pullPolicy: corev1.PullIfNotPresent,
			wantImage:  img + ":v0.4.1",
			wantPolicy: corev1.PullIfNotPresent,
		},
		"latest tag forces PullAlways": {
			tag:        "latest",
			pullPolicy: corev1.PullIfNotPresent,
			wantImage:  img + ":latest",
			wantPolicy: corev1.PullAlways,
		},
		"empty tag defaults to latest": {
			tag:        "",
			pullPolicy: corev1.PullIfNotPresent,
			wantImage:  img + ":latest",
			wantPolicy: corev1.PullAlways,
		},
		"digest is joined with an @": {
			tag:        digest,
			pullPolicy: corev1.PullIfNotPresent,
			wantImage:  img + "@" + digest,
			wantPolicy: corev1.PullIfNotPresent,
		},
		"digest with a leading @ is normalized": {
			tag:        "@" + digest,
			pullPolicy: corev1.PullIfNotPresent,
			wantImage:  img + "@" + digest,
			wantPolicy: corev1.PullIfNotPresent,
		},
		"digest never forces PullAlways": {
			tag:        digest,
			pullPolicy: "",
			wantImage:  img + "@" + digest,
			wantPolicy: "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			c := NewContainerBuilder().WithImage(img, tc.tag, tc.pullPolicy).Build()
			if c.Image != tc.wantImage {
				t.Errorf("Image = %q, want %q", c.Image, tc.wantImage)
			}
			if c.ImagePullPolicy != tc.wantPolicy {
				t.Errorf("ImagePullPolicy = %q, want %q", c.ImagePullPolicy, tc.wantPolicy)
			}
		})
	}
}
