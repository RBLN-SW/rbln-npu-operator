package components

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func newTestPatcher(t *testing.T, openshiftVersion string) *driverManagerPatcher {
	t.Helper()
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	return &driverManagerPatcher{
		basePatcher: basePatcher{
			client:           c,
			log:              logf.Log,
			scheme:           scheme,
			name:             driverManagerName,
			instanceName:     testInstanceName,
			namespace:        testNamespace,
			openshiftVersion: openshiftVersion,
			enabled:          true,
		},
		desiredSpec: &rebellionsaiv1alpha1.RBLNDriverSpec{
			Version:  "3.0.0",
			Registry: "repo.rebellions.ai",
			Image:    "rebellions/rbln-driver",
			Manager: rebellionsaiv1alpha1.DriverManagerSpec{
				Registry: "repo.rebellions.ai",
				Image:    "rebellions/k8s-driver-manager",
				Version:  "v1.0",
			},
		},
	}
}

func TestResolveImagePullPolicy(t *testing.T) {
	tests := map[string]struct {
		pullPolicy corev1.PullPolicy
		version    string
		want       corev1.PullPolicy
	}{
		"default is IfNotPresent": {
			version: "3.0.0",
			want:    corev1.PullIfNotPresent,
		},
		"explicit policy preserved": {
			pullPolicy: corev1.PullAlways,
			version:    "3.0.0",
			want:       corev1.PullAlways,
		},
		"latest forces PullAlways": {
			version: "latest",
			want:    corev1.PullAlways,
		},
		"latest overrides explicit IfNotPresent": {
			pullPolicy: corev1.PullIfNotPresent,
			version:    "latest",
			want:       corev1.PullAlways,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			h := newTestPatcher(t, "")
			h.desiredSpec.ImagePullPolicy = tc.pullPolicy
			h.desiredSpec.Version = tc.version

			got := h.resolveImagePullPolicy()
			if got != tc.want {
				t.Fatalf("resolveImagePullPolicy() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSubscriptionOS(t *testing.T) {
	tests := map[string]struct {
		openshiftVersion string
		pool             nodePool
		want             string
	}{
		"OpenShift always returns rhcos": {
			openshiftVersion: "v4.14.0",
			pool:             nodePool{osRelease: "rhel"},
			want:             "rhcos",
		},
		"RHEL without OpenShift returns rhel": {
			pool: nodePool{osRelease: "rhel"},
			want: "rhel",
		},
		"Ubuntu returns empty": {
			pool: nodePool{osRelease: "ubuntu"},
			want: "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			h := newTestPatcher(t, tc.openshiftVersion)
			got := h.subscriptionOS(tc.pool)
			if got != tc.want {
				t.Fatalf("subscriptionOS() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBuildSubscriptionMountsAndVolumes(t *testing.T) {
	t.Run("no subscription for ubuntu", func(t *testing.T) {
		h := newTestPatcher(t, "")
		mounts, vols, err := h.buildSubscriptionMountsAndVolumes(nodePool{osRelease: "ubuntu"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(mounts) != 0 || len(vols) != 0 {
			t.Fatalf("expected empty mounts/vols for ubuntu, got %d/%d", len(mounts), len(vols))
		}
	})

	t.Run("RHEL subscription mounts", func(t *testing.T) {
		h := newTestPatcher(t, "")
		mounts, vols, err := h.buildSubscriptionMountsAndVolumes(nodePool{osRelease: "rhel"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(mounts) != 3 {
			t.Fatalf("expected 3 mounts for RHEL, got %d", len(mounts))
		}
		if len(vols) != 3 {
			t.Fatalf("expected 3 volumes for RHEL, got %d", len(vols))
		}
	})

	t.Run("OpenShift subscription mounts", func(t *testing.T) {
		h := newTestPatcher(t, "v4.14.0")
		mounts, vols, err := h.buildSubscriptionMountsAndVolumes(nodePool{osRelease: "rhcos"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(mounts) != 3 {
			t.Fatalf("expected 3 mounts for RHCOS, got %d", len(mounts))
		}
		if len(vols) != 3 {
			t.Fatalf("expected 3 volumes for RHCOS, got %d", len(vols))
		}
	})
}

func TestBuildDriverManagerInitContainer(t *testing.T) {
	h := newTestPatcher(t, "")
	c := h.buildDriverManagerInitContainer()

	if c.Name != driverManagerInitContainer {
		t.Fatalf("container name = %q, want %q", c.Name, driverManagerInitContainer)
	}
	if c.SecurityContext == nil || c.SecurityContext.Privileged == nil || !*c.SecurityContext.Privileged {
		t.Fatal("expected privileged security context")
	}

	envNames := make(map[string]bool)
	for _, env := range c.Env {
		envNames[env.Name] = true
	}
	for _, required := range []string{"NODE_NAME", "ENABLE_NPU_POD_EVICTION", "OPERATOR_NAMESPACE"} {
		if !envNames[required] {
			t.Fatalf("missing required env var %q", required)
		}
	}
}

func TestHandleConfigMap(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	h := &driverManagerPatcher{
		basePatcher: basePatcher{
			client:    c,
			log:       logf.Log,
			scheme:    scheme,
			name:      driverManagerName,
			namespace: testNamespace,
		},
	}

	if err := h.handleConfigMap(context.Background()); err != nil {
		t.Fatalf("handleConfigMap() error: %v", err)
	}

	cm := &corev1.ConfigMap{}
	assertObjectExists(t, c, types.NamespacedName{
		Name:      driverManagerName + "-" + startupProbeConfigMapSuffix,
		Namespace: testNamespace,
	}, cm)

	script, ok := cm.Data[startupProbeScriptName]
	if !ok {
		t.Fatalf("ConfigMap missing key %q", startupProbeScriptName)
	}
	if len(script) == 0 {
		t.Fatal("startup probe script is empty")
	}

	// Verify script references the validations path.
	if !contains(script, consts.ValidationsMountPath) {
		t.Fatalf("script should reference %s", consts.ValidationsMountPath)
	}
}

func TestBuildDriverContainer(t *testing.T) {
	h := newTestPatcher(t, "")
	pool := nodePool{
		osRelease: "ubuntu",
		osVersion: "22.04",
		kernel:    "5.15.0-100-generic",
	}

	container, err := h.buildDriverContainer(pool, nil)
	if err != nil {
		t.Fatalf("buildDriverContainer() error: %v", err)
	}

	if container.Name != driverManagerContainer {
		t.Fatalf("container name = %q, want %q", container.Name, driverManagerContainer)
	}
	if container.ImagePullPolicy != corev1.PullIfNotPresent {
		t.Fatalf("pull policy = %q, want %q", container.ImagePullPolicy, corev1.PullIfNotPresent)
	}
	if container.StartupProbe == nil {
		t.Fatal("expected startup probe to be set")
	}
	if container.Lifecycle == nil || container.Lifecycle.PreStop == nil {
		t.Fatal("expected lifecycle preStop hook")
	}
}

func TestBuildDriverContainer_LatestVersion(t *testing.T) {
	h := newTestPatcher(t, "")
	h.desiredSpec.Version = "latest"

	pool := nodePool{
		osRelease: "ubuntu",
		osVersion: "22.04",
		kernel:    "5.15.0-100-generic",
	}

	container, err := h.buildDriverContainer(pool, nil)
	if err != nil {
		t.Fatalf("buildDriverContainer() error: %v", err)
	}

	if container.ImagePullPolicy != corev1.PullAlways {
		t.Fatalf("pull policy = %q, want %q for latest version", container.ImagePullPolicy, corev1.PullAlways)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstr(s, substr))
}

func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// Verify that the init container uses the correct configMap name format
func TestStartupProbeConfigMapName(t *testing.T) {
	h := newTestPatcher(t, "")
	want := driverManagerName + "-" + startupProbeConfigMapSuffix
	got := h.startupProbeConfigMapName()
	if got != want {
		t.Fatalf("startupProbeConfigMapName() = %q, want %q", got, want)
	}
}

// Verify that handleConfigMap creates a configmap with the correct name
func TestHandleConfigMap_Idempotent(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	h := &driverManagerPatcher{
		basePatcher: basePatcher{
			client:    c,
			log:       logf.Log,
			scheme:    scheme,
			name:      driverManagerName,
			namespace: testNamespace,
		},
	}

	ctx := context.Background()
	// Call twice — should not error.
	if err := h.handleConfigMap(ctx); err != nil {
		t.Fatalf("first handleConfigMap() error: %v", err)
	}
	if err := h.handleConfigMap(ctx); err != nil {
		t.Fatalf("second handleConfigMap() error: %v", err)
	}

	// Verify ConfigMap still exists and has correct data
	cm := &corev1.ConfigMap{}
	assertObjectExists(t, c, types.NamespacedName{
		Name:      driverManagerName + "-" + startupProbeConfigMapSuffix,
		Namespace: testNamespace,
	}, cm)
	if _, ok := cm.Data[startupProbeScriptName]; !ok {
		t.Fatalf("ConfigMap missing key %q after idempotent call", startupProbeScriptName)
	}
}

// Dummy to avoid import issues
var _ = metav1.ObjectMeta{}
