package components

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	"github.com/rebellions-sw/rbln-npu-operator/internal/drivermanager"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
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

	// k8s-driver-manager binds none of these, so leaving them rendered is a
	// setting that silently does nothing.
	for _, retired := range []string{
		"ENABLE_AUTO_DRAIN", "DRAIN_USE_FORCE", "DRAIN_POD_SELECTOR_LABEL",
		"DRAIN_TIMEOUT_SECONDS", "DRAIN_DELETE_EMPTYDIR_DATA",
	} {
		if _, ok := envByName[retired]; ok {
			t.Fatalf("env var %q is not bound by k8s-driver-manager and must not be rendered", retired)
		}
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

	container := h.buildDriverContainer(nil, imagePath, false)

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

// k8s-driver-manager decides at every driver pod start whether the loaded
// driver has to be replaced by comparing DRIVER_CONFIG_DIGEST with the digest
// the driver container recorded in the host's /run/rbln/rbln-driver.state.
// Both containers therefore need the host directory itself, not only the
// /run/rbln/driver staging tree that is torn down with the driver: the init
// container reads the file at its own path, the driver container writes it
// under /host like the rest of the host filesystem.
func TestBuildDriverPodSpec_MountsHostRunRBLNIntoBothContainers(t *testing.T) {
	h := newTestPatcher(t, "")
	pool := nodePool{osRelease: "ubuntu", osVersion: "22.04", kernel: "5.15.0-100-generic", family: "atom"}

	spec := h.buildDriverPodSpec(pool, "repo.rebellions.ai/rebellions/atom/rbln-driver:3.0.0-5.15.0-100-generic-ubuntu22.04")

	var volume *corev1.Volume
	for i := range spec.Volumes {
		if spec.Volumes[i].Name == hostRunRBLNVolumeName {
			volume = &spec.Volumes[i]
		}
	}
	if volume == nil {
		t.Fatalf("pod spec missing %q volume", hostRunRBLNVolumeName)
	}
	if volume.HostPath == nil || volume.HostPath.Path != "/run/rbln" ||
		volume.HostPath.Type == nil || *volume.HostPath.Type != corev1.HostPathDirectoryOrCreate {
		t.Fatalf("%s volume = %+v, want hostPath /run/rbln DirectoryOrCreate", hostRunRBLNVolumeName, volume.VolumeSource)
	}

	mountPath := func(c *corev1.Container) string {
		for _, m := range c.VolumeMounts {
			if m.Name == hostRunRBLNVolumeName {
				return m.MountPath
			}
		}
		return ""
	}
	if got := mountPath(&spec.InitContainers[0]); got != "/run/rbln" {
		t.Fatalf("%s mount in %s = %q, want /run/rbln", hostRunRBLNVolumeName, driverManagerInitContainer, got)
	}
	if got := mountPath(&spec.Containers[0]); got != "/host/run/rbln" {
		t.Fatalf("%s mount in %s = %q, want /host/run/rbln", hostRunRBLNVolumeName, driverManagerContainer, got)
	}
}

// The driver container is handed DRIVER_CONFIG_DIGEST to record in the state
// file, but the digest is a hash of that very container, so the env must be
// stamped after the hash is taken: inside the hash it would be an input to
// itself. Every render must then agree on the digest, or an unchanged driver
// spec would read as a new driver revision on every reconcile.
func TestHandleDaemonSetStampsDriverConfigDigestOutsideItsOwnHash(t *testing.T) {
	h := newTestPatcher(t, "")
	owner := newTestOwner()
	pool := nodePool{
		name: "atom-ubuntu22.04-5.15.0-100-generic", osRelease: "ubuntu", osVersion: "22.04",
		kernel: "5.15.0-100-generic", family: "atom",
	}
	const image = "repo.rebellions.ai/rebellions/atom/rbln-driver:3.0.0-5.15.0-100-generic-ubuntu22.04"
	ctx := context.Background()

	stampedDigest := func(pass int) string {
		t.Helper()
		if err := h.handleDaemonSet(ctx, owner, pool, image, false); err != nil {
			t.Fatalf("handleDaemonSet pass %d: %v", pass, err)
		}
		ds := &appsv1.DaemonSet{}
		key := types.NamespacedName{Name: testInstanceName + "-" + pool.name, Namespace: testNamespace}
		if err := h.client.Get(ctx, key, ds); err != nil {
			t.Fatalf("get DaemonSet %s after pass %d: %v", key.Name, pass, err)
		}
		initDigest := containerEnvValue(ds.Spec.Template.Spec.InitContainers, driverManagerInitContainer, driverConfigDigestEnv)
		driverDigest := containerEnvValue(ds.Spec.Template.Spec.Containers, driverManagerContainer, driverConfigDigestEnv)
		if initDigest == "" || initDigest != driverDigest {
			t.Fatalf("pass %d: %s is %q in %s and %q in %s; want the same non-empty digest in both",
				pass, driverConfigDigestEnv, initDigest, driverManagerInitContainer, driverDigest, driverManagerContainer)
		}
		return initDigest
	}

	first := stampedDigest(1)
	if want := k8sutil.GetObjectHash(h.buildDriverPodSpec(pool, image).Containers); first != want {
		t.Fatalf("digest = %s, want %s: the hash of the driver container as rendered, before the digest env is stamped",
			first, want)
	}
	if second := stampedDigest(2); second != first {
		t.Fatalf("digest changed across identical renders: %s vs %s", first, second)
	}
}

func containerEnvValue(containers []corev1.Container, containerName, envName string) string {
	for i := range containers {
		if containers[i].Name != containerName {
			continue
		}
		for _, env := range containers[i].Env {
			if env.Name == envName {
				return env.Value
			}
		}
	}
	return ""
}

func TestDriverContainerEnvs(t *testing.T) {
	tests := map[string]struct {
		userEnv    []corev1.EnvVar
		rdsBinding bool
		want       map[string]string // env vars that must be present with this value
		absent     []string          // env vars that must not be present
	}{
		"operator markers override user-supplied values": {
			userEnv: []corev1.EnvVar{
				{Name: "FOO", Value: "bar"},
				{Name: driverReadyDirEnvName, Value: "/wrong/path"},
				{Name: driverReadyFileEnvName, Value: "wrong-file"},
			},
			want: map[string]string{
				"FOO":                  "bar",
				driverReadyDirEnvName:  defaultDriverReadyDir,
				driverReadyFileEnvName: defaultDriverReadyFile,
			},
			absent: []string{rdsBindingEnvName},
		},
		"rds binding adds the binding env": {
			rdsBinding: true,
			want:       map[string]string{rdsBindingEnvName: rdsBindingEnabledValue},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			envByName := make(map[string]string)
			for _, e := range driverContainerEnvs(tt.userEnv, tt.rdsBinding) {
				envByName[e.Name] = e.Value
			}

			for k, want := range tt.want {
				if got := envByName[k]; got != want {
					t.Errorf("env %q = %q, want %q", k, got, want)
				}
			}
			for _, k := range tt.absent {
				if got, ok := envByName[k]; ok {
					t.Errorf("env %q = %q, want absent", k, got)
				}
			}
		})
	}
}

func TestBuildDriverPodSpec_RDS(t *testing.T) {
	tests := map[string]struct {
		rdsBindingEnabled bool
		poolRDS           bool
		wantRDSEnv        bool
		wantRDSSelector   bool
		wantAffinity      *corev1.Affinity
	}{
		"rds pool gets binding env and nodeSelector, no exclusion affinity": {
			rdsBindingEnabled: true,
			poolRDS:           true,
			wantRDSEnv:        true,
			wantRDSSelector:   true,
			wantAffinity:      nil,
		},
		"base pool is kept off rds nodes and gets no binding env": {
			rdsBindingEnabled: true,
			poolRDS:           false,
			wantRDSEnv:        false,
			wantRDSSelector:   false,
			wantAffinity:      wantRDSExclusionAffinity(),
		},
		"rds disabled leaves the base pool untouched": {
			rdsBindingEnabled: false,
			poolRDS:           false,
			wantRDSEnv:        false,
			wantRDSSelector:   false,
			wantAffinity:      nil,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			h := newTestPatcher(t, "")
			h.rdsBindingEnabled = tt.rdsBindingEnabled
			pool := nodePool{
				osRelease: "ubuntu",
				osVersion: "22.04",
				kernel:    "5.15.0-100-generic",
				rds:       tt.poolRDS,
			}
			if tt.poolRDS {
				pool.nodeSelector = map[string]string{rdsPresentLabelKey: labelValueTrue}
			}

			spec := h.buildDriverPodSpec(pool, "repo.rebellions.ai/rebellions/atom/rbln-driver:3.0.0-5.15.0-100-generic-ubuntu22.04")

			gotEnv := containerHasEnv(spec.Containers, driverManagerContainer, rdsBindingEnvName, rdsBindingEnabledValue)
			if gotEnv != tt.wantRDSEnv {
				t.Errorf("%s present = %v, want %v", rdsBindingEnvName, gotEnv, tt.wantRDSEnv)
			}

			_, gotSelector := spec.NodeSelector[rdsPresentLabelKey]
			if gotSelector != tt.wantRDSSelector {
				t.Errorf("nodeSelector[%s] present = %v, want %v", rdsPresentLabelKey, gotSelector, tt.wantRDSSelector)
			}

			if diff := cmp.Diff(tt.wantAffinity, spec.Affinity); diff != "" {
				t.Errorf("affinity mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func containerHasEnv(containers []corev1.Container, containerName, envName, envValue string) bool {
	for i := range containers {
		c := &containers[i]
		if c.Name != containerName {
			continue
		}
		for _, e := range c.Env {
			if e.Name == envName {
				return e.Value == envValue
			}
		}
	}
	return false
}

// wantRDSExclusionAffinity is the affinity a base-pool driver pod must carry
// while RDS is enabled: schedule anywhere the rds.present label is absent or
// not "true", keeping the base driver off RDS-dedicated nodes.
func wantRDSExclusionAffinity() *corev1.Affinity {
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      rdsPresentLabelKey,
						Operator: corev1.NodeSelectorOpNotIn,
						Values:   []string{labelValueTrue},
					}},
				}},
			},
		},
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

// The eviction knobs must reach k8s-driver-manager: it evicts NPU pods itself
// whenever driver auto-upgrade is off, and without them it is stuck at the
// strictest policy with no way for the user to relax it.
func TestBuildDriverManagerInitContainerRendersEvictionPolicy(t *testing.T) {
	tests := map[string]struct {
		policy drivermanager.NPUPodEvictionPolicy
		want   map[string]string
	}{
		"defaults": {
			policy: drivermanager.NPUPodEvictionPolicy{DeviceClass: consts.DefaultDRADeviceClass},
			want: map[string]string{
				"NPU_POD_EVICTION_FORCE":                "false",
				"NPU_POD_EVICTION_DELETE_EMPTYDIR_DATA": "false",
				"NPU_POD_EVICTION_DEVICE_CLASS":         consts.DefaultDRADeviceClass,
			},
		},
		"relaxed policy with a custom device class": {
			policy: drivermanager.NPUPodEvictionPolicy{Force: true, DeleteEmptyDirData: true, DeviceClass: "npu.example.com"},
			want: map[string]string{
				"NPU_POD_EVICTION_FORCE":                "true",
				"NPU_POD_EVICTION_DELETE_EMPTYDIR_DATA": "true",
				"NPU_POD_EVICTION_DEVICE_CLASS":         "npu.example.com",
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			h := newTestPatcher(t, "")
			h.evictionPolicy = tc.policy

			envByName := make(map[string]string)
			for _, env := range h.buildDriverManagerInitContainer().Env {
				envByName[env.Name] = env.Value
			}
			for key, want := range tc.want {
				if got := envByName[key]; got != want {
					t.Errorf("env %q = %q, want %q", key, got, want)
				}
			}
		})
	}
}

// The init container's env order is part of the pod template hash. Changing it
// re-stamps every driver DaemonSet and makes admitted nodes recreate their pod,
// so the order is pinned here and only a deliberate change may move it.
func TestBuildDriverManagerInitContainerEnvOrder(t *testing.T) {
	h := newTestPatcher(t, "")
	want := []string{
		"NODE_NAME",
		"ENABLE_NPU_POD_EVICTION",
		"NPU_POD_EVICTION_FORCE",
		"NPU_POD_EVICTION_DELETE_EMPTYDIR_DATA",
		"NPU_POD_EVICTION_DEVICE_CLASS",
		"OPERATOR_NAMESPACE",
		"PROC_ROOT",
	}
	env := h.buildDriverManagerInitContainer().Env
	got := make([]string, 0, len(env))
	for _, e := range env {
		got = append(got, e.Name)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("init container env order changed (-want +got):\n%s", diff)
	}
}
