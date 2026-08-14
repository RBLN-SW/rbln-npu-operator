package components

import (
	"context"
	"strings"
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
		mounts, vols := h.buildSubscriptionMountsAndVolumes(nodePool{osRelease: "ubuntu"})
		if len(mounts) != 0 || len(vols) != 0 {
			t.Fatalf("expected empty mounts/vols for ubuntu, got %d/%d", len(mounts), len(vols))
		}
	})

	t.Run("RHEL subscription mounts", func(t *testing.T) {
		h := newTestPatcher(t, "")
		mounts, vols := h.buildSubscriptionMountsAndVolumes(nodePool{osRelease: "rhel"})
		if len(mounts) != 3 {
			t.Fatalf("expected 3 mounts for RHEL, got %d", len(mounts))
		}
		if len(vols) != 3 {
			t.Fatalf("expected 3 volumes for RHEL, got %d", len(vols))
		}
	})

	t.Run("OpenShift subscription mounts", func(t *testing.T) {
		h := newTestPatcher(t, "v4.14.0")
		mounts, vols := h.buildSubscriptionMountsAndVolumes(nodePool{osRelease: "rhcos"})
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

	envByName := make(map[string]string)
	for _, env := range c.Env {
		envByName[env.Name] = env.Value
	}
	for _, required := range []string{"NODE_NAME", "ENABLE_NPU_POD_EVICTION", "OPERATOR_NAMESPACE"} {
		if _, ok := envByName[required]; !ok {
			t.Fatalf("missing required env var %q", required)
		}
	}
	if got := envByName["PROC_ROOT"]; got != "/host/proc" {
		t.Fatalf("PROC_ROOT = %q, want /host/proc (fd-scanner needs host procfs view)", got)
	}

	// Downward-API env vars must set APIVersion explicitly. The kube-apiserver
	// defaults it to "v1" on persist, so omitting it triggers a perpetual
	// reconcile/patch loop (operator submits "", server stores "v1", diff, repeat).
	fieldRefByName := make(map[string]*corev1.ObjectFieldSelector)
	for i := range c.Env {
		if c.Env[i].ValueFrom != nil {
			fieldRefByName[c.Env[i].Name] = c.Env[i].ValueFrom.FieldRef
		}
	}
	for _, name := range []string{"NODE_NAME", "OPERATOR_NAMESPACE"} {
		fr := fieldRefByName[name]
		if fr == nil {
			t.Fatalf("init container env %q missing FieldRef", name)
		}
		if fr.APIVersion != "v1" {
			t.Fatalf("init container env %q FieldRef.APIVersion = %q, want v1 (prevents reconcile thrash)", name, fr.APIVersion)
		}
	}

	// Without a Bidirectional mount at exactly hostDriverPath the init
	// container cannot observe, let alone unmount, the host's staging tree,
	// which leaves stale bind mounts across reinstalls.
	var hostDriverMount *corev1.VolumeMount
	for i := range c.VolumeMounts {
		if c.VolumeMounts[i].Name == hostDriverVolumeName {
			hostDriverMount = &c.VolumeMounts[i]
			break
		}
	}
	if hostDriverMount == nil {
		t.Fatalf("init container missing %q volume mount", hostDriverVolumeName)
	}
	if hostDriverMount.MountPath != hostDriverPath {
		t.Fatalf("host-driver mountPath = %q, want %q", hostDriverMount.MountPath, hostDriverPath)
	}
	if hostDriverMount.MountPropagation == nil ||
		*hostDriverMount.MountPropagation != corev1.MountPropagationBidirectional {
		t.Fatalf("host-driver mountPropagation = %v, want Bidirectional",
			hostDriverMount.MountPropagation)
	}

	var hostSysMount *corev1.VolumeMount
	for i := range c.VolumeMounts {
		if c.VolumeMounts[i].Name == hostSysVolumeName {
			hostSysMount = &c.VolumeMounts[i]
			break
		}
	}
	if hostSysMount == nil {
		t.Fatalf("init container missing %q volume mount", hostSysVolumeName)
	}
	if hostSysMount.MountPath != hostSysPath {
		t.Fatalf("host-sys mountPath = %q, want %q", hostSysMount.MountPath, hostSysPath)
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

	owner := newTestOwner()
	if err := h.handleConfigMap(context.Background(), owner); err != nil {
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
	if !strings.Contains(script, consts.ValidationsMountPath) {
		t.Fatalf("script should reference %s", consts.ValidationsMountPath)
	}

	// Verify the probe wires kernel-module + marker-file checks, both env
	// vars/defaults, and the component-ready publication helper.
	for _, needle := range []string{
		"/sys/module/rebellions/refcnt",
		driverReadyDirEnvName,
		driverReadyFileEnvName,
		defaultDriverReadyDir,
		defaultDriverReadyFile,
		"DRIVER_READY_MARKER",
		"publish_component_ready",
	} {
		if !strings.Contains(script, needle) {
			t.Fatalf("script should reference %q", needle)
		}
	}

	// Regression guard: rbln-smi heuristic must not creep back in. The
	// marker contract is mandatory; legacy images that relied on rbln-smi
	// alone are intentionally unsupported.
	for _, forbidden := range []string{"rbln-smi"} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("script must not reference %q (legacy heuristic removed)", forbidden)
		}
	}

	// Verify ConfigMap carries an ownerReference to the driver instance so
	// that Kubernetes GC removes it when the RBLNDriver CR is deleted.
	assertHasOwnerRef(t, cm, owner.Name)
}

func TestBuildDriverContainer(t *testing.T) {
	h := newTestPatcher(t, "")
	const imagePath = "repo.rebellions.ai/rebellions/atom/rbln-driver:3.0.0-5.15.0-100-generic-ubuntu22.04"

	container := h.buildDriverContainer(nil, imagePath)

	if container.Image != imagePath {
		t.Fatalf("container image = %q, want %q", container.Image, imagePath)
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

	envByName := make(map[string]string, len(container.Env))
	for _, env := range container.Env {
		envByName[env.Name] = env.Value
	}
	if envByName[driverReadyDirEnvName] != defaultDriverReadyDir {
		t.Fatalf("env %s = %q, want %q",
			driverReadyDirEnvName, envByName[driverReadyDirEnvName], defaultDriverReadyDir)
	}
	if envByName[driverReadyFileEnvName] != defaultDriverReadyFile {
		t.Fatalf("env %s = %q, want %q",
			driverReadyFileEnvName, envByName[driverReadyFileEnvName], defaultDriverReadyFile)
	}

	var foundReadyMount bool
	for _, m := range container.VolumeMounts {
		if m.Name == driverReadyVolumeName {
			foundReadyMount = true
			if m.MountPath != defaultDriverReadyDir {
				t.Fatalf("%s mountPath = %q, want %q",
					driverReadyVolumeName, m.MountPath, defaultDriverReadyDir)
			}
		}
	}
	if !foundReadyMount {
		t.Fatalf("driver container missing %q volume mount", driverReadyVolumeName)
	}
}

func TestBuildDriverPodSpec_HasDriverStateVolume(t *testing.T) {
	h := newTestPatcher(t, "")
	pool := nodePool{
		osRelease: "ubuntu",
		osVersion: "22.04",
		kernel:    "5.15.0-100-generic",
		family:    "atom",
	}

	spec := h.buildDriverPodSpec(pool, "repo.rebellions.ai/rebellions/atom/rbln-driver:3.0.0-5.15.0-100-generic-ubuntu22.04")

	for _, v := range spec.Volumes {
		if v.Name == driverReadyVolumeName {
			if v.EmptyDir == nil {
				t.Fatalf("%s volume must be emptyDir, got %+v", driverReadyVolumeName, v.VolumeSource)
			}
			return
		}
	}
	t.Fatalf("pod spec missing %q volume", driverReadyVolumeName)
}

func TestDriverContainerEnvs_OperatorOverridesUser(t *testing.T) {
	userEnv := []corev1.EnvVar{
		{Name: "FOO", Value: "bar"},
		{Name: driverReadyDirEnvName, Value: "/wrong/path"},
		{Name: driverReadyFileEnvName, Value: "wrong-file"},
	}

	got := driverContainerEnvs(userEnv)

	envByName := make(map[string]string, len(got))
	for _, env := range got {
		envByName[env.Name] = env.Value
	}

	if envByName["FOO"] != "bar" {
		t.Fatalf("user-supplied FOO lost: got %q", envByName["FOO"])
	}
	if envByName[driverReadyDirEnvName] != defaultDriverReadyDir {
		t.Fatalf("operator did not override %s: got %q",
			driverReadyDirEnvName, envByName[driverReadyDirEnvName])
	}
	if envByName[driverReadyFileEnvName] != defaultDriverReadyFile {
		t.Fatalf("operator did not override %s: got %q",
			driverReadyFileEnvName, envByName[driverReadyFileEnvName])
	}
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
	owner := newTestOwner()
	// Call twice — should not error.
	if err := h.handleConfigMap(ctx, owner); err != nil {
		t.Fatalf("first handleConfigMap() error: %v", err)
	}
	if err := h.handleConfigMap(ctx, owner); err != nil {
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
