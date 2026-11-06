package components

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	rblnv1beta1 "github.com/RBLN-SW/rbln-npu-operator/api/v1beta1"
	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
)

func TestSandboxDevicePluginPatch(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.SandboxDevicePlugin = rblnv1beta1.RBLNSandboxDevicePluginSpec{
		Enabled: true,
		// Registry/Image are placeholders here; assertContainerImage
		// hardcodes "docker.io" prefix for unit-test convenience.
		// Production image is validated via values.yaml/Helm.
		Registry: "docker.io",
		Image:    "rebellions/sandbox-device-plugin",
		Version:  "dev",
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
	assertContainerImage(t, mainContainer, "rebellions/sandbox-device-plugin", "dev")
	assertPrivileged(t, mainContainer)

	if len(mainContainer.Args) != 0 {
		t.Fatalf("main container args = %v, want []", mainContainer.Args)
	}

	assertGateInitContainer(t, ds, consts.RBLNSandboxDevicePluginName)
	initContainer := ds.Spec.Template.Spec.InitContainers[0]
	assertContainerImage(t, initContainer, "rebellions/rbln-validator", "v1.0")
	assertPrivileged(t, initContainer)
	assertContainerHasVolumeMount(t, initContainer, consts.ValidationsVolumeName)
	assertContainerHasVolumeMount(t, initContainer, "host-sys")
	assertPodHasVolume(t, ds.Spec.Template.Spec, consts.ValidationsVolumeName)

	// Volume contract: the binary needs three host paths
	// + run-rbln-validations (shared with init container)
	// + host-sys (init container reads sysfs driver symlinks).
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

	// Env contract: retain the path hints, without resource-name overrides.
	envByName := map[string]string{}
	for _, e := range mainContainer.Env {
		envByName[e.Name] = e.Value
	}
	if len(mainContainer.Env) != 2 {
		t.Fatalf("expected only the two path hints in the default environment, got %v", mainContainer.Env)
	}
	if got := envByName["RBLN_SYSFS_PCI_PATH"]; got != "/sys/bus/pci/devices" {
		t.Errorf("RBLN_SYSFS_PCI_PATH = %q, want /sys/bus/pci/devices", got)
	}
	if got := envByName["RBLN_KUBELET_DEVICE_PLUGIN_PATH"]; got != "/var/lib/kubelet/device-plugins" {
		t.Errorf("RBLN_KUBELET_DEVICE_PLUGIN_PATH = %q, want /var/lib/kubelet/device-plugins", got)
	}
}

func TestSandboxDevicePluginCleanUp(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.SandboxDevicePlugin = rblnv1beta1.RBLNSandboxDevicePluginSpec{
		Enabled: true,
		// Registry/Image are placeholders here; assertContainerImage
		// hardcodes "docker.io" prefix for unit-test convenience.
		// Production image is validated via values.yaml/Helm.
		Registry: "docker.io",
		Image:    "rebellions/sandbox-device-plugin",
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
