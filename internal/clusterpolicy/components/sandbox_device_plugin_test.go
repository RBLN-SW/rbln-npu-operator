package components

import (
	"context"
	"encoding/json"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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
		Registry: "docker.io",
		Image:    "rebellions/k8s-device-plugin",
		Version:  "latest",
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
	assertContainerImage(t, mainContainer, "rebellions/k8s-device-plugin", "latest")
	assertPrivileged(t, mainContainer)

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

	// ConfigMap (config.json with vfio-pci driver)
	cm := &corev1.ConfigMap{}
	assertObjectExists(t, c, types.NamespacedName{Name: name + "-config", Namespace: testNamespace}, cm)
	assertHasOwnerRef(t, cm, owner.Name)

	var cfg configResourceList
	if err := json.Unmarshal([]byte(cm.Data["config.json"]), &cfg); err != nil {
		t.Fatalf("config.json is not valid JSON: %v", err)
	}
	if len(cfg.ResourceList) != 1 {
		t.Fatalf("expected 1 resource, got %d", len(cfg.ResourceList))
	}
	if cfg.ResourceList[0].Selectors.Drivers[0] != testVFIOPCIDriver {
		t.Fatalf("driver = %q, want vfio-pci", cfg.ResourceList[0].Selectors.Drivers[0])
	}
}

func TestSandboxDevicePluginCleanUp(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.SandboxDevicePlugin = rblnv1beta1.RBLNSandboxDevicePluginSpec{
		Enabled:  true,
		Registry: "docker.io",
		Image:    "rebellions/k8s-device-plugin",
		Version:  "latest",
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

	if err := p.CleanUp(ctx, owner); err != nil {
		t.Fatalf("CleanUp() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &appsv1.DaemonSet{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name + "-config", Namespace: testNamespace}, &corev1.ConfigMap{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &corev1.ServiceAccount{})
}
