package components

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	
	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func TestSandboxDevicePluginPatch(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.SandboxDevicePlugin = rblnv1beta1.RBLNSandboxDevicePluginSpec{
		Enabled:  true,
		// Registry/Image are placeholders here; assertContainerImage
		// hardcodes "docker.io" prefix for unit-test convenience.
		// Production image is validated via values.yaml/Helm.
		Registry: "docker.io",
		Image:    "rebellions-sw/sandbox-device-plugin",
		Version:  "dev",
		// ResourceList is intentionally exercised here even though the
		// alias-only plugin no longer consumes it: this guards against
		// a regression where a stray ConfigMap would silently come
		// back, or where ResourceList being non-nil would change Patch
		// behavior (it should not).
		ResourceList: []rblnv1beta1.RBLNDevicePluginResourceSpec{
			{
				ResourceName:     "ATOM",
				ResourcePrefix:   "rebellions.ai",
				ProductCardNames: []string{consts.RBLNCardCA12},
			},
		},
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNSandboxDevicePluginName
	p := NewSandboxDevicePluginPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	// ServiceAccount
	assertServiceAccountExists(t, c, name, owner.Name)

	// DaemonSet
	ds := assertDaemonSetBasics(t, c, name, owner.Name)
	assertNodeSelector(t, ds, "rebellions.ai/npu.deploy.sandbox-device-plugin")

	mainContainer := ds.Spec.Template.Spec.Containers[0]
	assertContainerImage(t, mainContainer, "rebellions-sw/sandbox-device-plugin", "dev")
	assertPrivileged(t, mainContainer)

	// v0.4.2 forwards --use-cdi=false; the alias-only binary ignores it
	// (env-only contract documented in the upstream repo README).
	if len(mainContainer.Args) != 1 || mainContainer.Args[0] != "--use-cdi=false" {
		t.Fatalf("main container args = %v, want [--use-cdi=false]", mainContainer.Args)
	}

	// v0.4.2 lifecycle: vfio-pci-validation init container gates startup.
	if len(ds.Spec.Template.Spec.InitContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(ds.Spec.Template.Spec.InitContainers))
	}
	initContainer := ds.Spec.Template.Spec.InitContainers[0]
	if initContainer.Name != "vfio-pci-validation" {
		t.Fatalf("init container name = %q, want vfio-pci-validation", initContainer.Name)
	}
	assertContainerImage(t, initContainer, "rebellions/rbln-validator", "v1.0")
	assertPrivileged(t, initContainer)
	if len(initContainer.Command) != 1 || initContainer.Command[0] != "rbln-validator" {
		t.Fatalf("init container command = %v, want [rbln-validator]", initContainer.Command)
	}
	if len(initContainer.Args) != 1 || initContainer.Args[0] != testVFIOPCIDriver {
		t.Fatalf("init container args = %v, want [vfio-pci]", initContainer.Args)
	}
	assertContainerHasVolumeMount(t, initContainer, consts.ValidationsVolumeName)
	assertContainerHasVolumeMount(t, initContainer, "host-sys")
	assertPodHasVolume(t, ds.Spec.Template.Spec, consts.ValidationsVolumeName)

	// alias-only contract: no ConfigMap is created. CRD's ResourceList
	// is ignored by the binary (env-only).
	cmName := types.NamespacedName{Name: name + "-config", Namespace: testNamespace}
	assertObjectNotExists(t, c, cmName, &corev1.ConfigMap{})

	// Volume contract: alias-only binary needs three host paths
	// + run-rbln-validations (shared with init container)
	// + host-sys (init container reads sysfs driver symlinks).
	// No config-volume — assertion stays strict so a future
	// re-introduction of the ConfigMap is caught here.
	wantVolumeNames := map[string]bool{
		"devicesock":                 false,
		"host-vfio":                  false,
		"sys-bus-pci":                false,
		"host-sys":                   false,
		consts.ValidationsVolumeName: false,
	}
	for _, v := range ds.Spec.Template.Spec.Volumes {
		if _, ok := wantVolumeNames[v.Name]; ok {
			wantVolumeNames[v.Name] = true
		}
	}
	for vname, seen := range wantVolumeNames {
		if !seen {
			t.Errorf("volume %q not found on DaemonSet", vname)
		}
	}
	if len(ds.Spec.Template.Spec.Volumes) != len(wantVolumeNames) {
		t.Errorf("unexpected extra volumes: %+v", ds.Spec.Template.Spec.Volumes)
	}

	// Env contract: alias is RBLN_PT_ALIAS, plus two path hints. The
	// old RBLN_RESOURCE_CONFIG_PATH must be gone.
	envByName := map[string]string{}
	for _, e := range mainContainer.Env {
		envByName[e.Name] = e.Value
	}
	if got := envByName["RBLN_PT_ALIAS"]; got != "npu_pt" {
		t.Errorf("RBLN_PT_ALIAS = %q, want %q", got, "npu_pt")
	}
	if got := envByName["RBLN_SYSFS_PCI_PATH"]; got != "/sys/bus/pci/devices" {
		t.Errorf("RBLN_SYSFS_PCI_PATH = %q, want /sys/bus/pci/devices", got)
	}
	if got := envByName["RBLN_KUBELET_DEVICE_PLUGIN_PATH"]; got != "/var/lib/kubelet/device-plugins" {
		t.Errorf("RBLN_KUBELET_DEVICE_PLUGIN_PATH = %q, want /var/lib/kubelet/device-plugins", got)
	}
	if _, present := envByName["RBLN_RESOURCE_CONFIG_PATH"]; present {
		t.Errorf("RBLN_RESOURCE_CONFIG_PATH should be gone (alias-only), but is present")
	}
}

func TestSandboxDevicePluginPatchRemovesLegacyConfigMap(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.SandboxDevicePlugin = rblnv1beta1.RBLNSandboxDevicePluginSpec{
		Enabled:  true,
		Registry: "docker.io",
		Image:    "rebellions-sw/sandbox-device-plugin",
		Version:  "dev",
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNSandboxDevicePluginName
	legacyCMName := types.NamespacedName{Name: name + "-config", Namespace: testNamespace}

	legacyCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      legacyCMName.Name,
			Namespace: legacyCMName.Namespace,
		},
		Data: map[string]string{
			"config.json": `{"resourceList":[{"resourceName":"ATOM","resourcePrefix":"rebellions.ai"}]}`,
		},
	}
	if err := c.Create(ctx, legacyCM); err != nil {
		t.Fatalf("failed to create legacy ConfigMap: %v", err)
	}

	p := NewSandboxDevicePluginPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")
	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	assertObjectNotExists(t, c, legacyCMName, &corev1.ConfigMap{})

	// Sanity check: the new alias-only DaemonSet is still created.
	ds := assertDaemonSetBasics(t, c, name, owner.Name)
	mainContainer := ds.Spec.Template.Spec.Containers[0]

	envByName := map[string]string{}
	for _, e := range mainContainer.Env {
		envByName[e.Name] = e.Value
	}
	if got := envByName["RBLN_PT_ALIAS"]; got != "npu_pt" {
		t.Errorf("RBLN_PT_ALIAS = %q, want %q", got, "npu_pt")
	}
}

func TestSandboxDevicePluginCleanUp(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.SandboxDevicePlugin = rblnv1beta1.RBLNSandboxDevicePluginSpec{
		Enabled:  true,
		// Registry/Image are placeholders here; assertContainerImage
		// hardcodes "docker.io" prefix for unit-test convenience.
		// Production image is validated via values.yaml/Helm.
		Registry: "docker.io",
		Image:    "rebellions-sw/sandbox-device-plugin",
		Version:  "dev",
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNSandboxDevicePluginName
	p := NewSandboxDevicePluginPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	if err := p.CleanUp(ctx, owner); err != nil {
		t.Fatalf("CleanUp() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &appsv1.DaemonSet{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &corev1.ServiceAccount{})
}
