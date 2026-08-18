package components

import (
	"context"
	"fmt"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func TestDRAKubeletPluginPatch(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.DRAKubeletPlugin = rblnv1beta1.RBLNDRAKubeletPluginSpec{
		Enabled:                       true,
		Registry:                      "docker.io",
		Image:                         "rebellions/k8s-dra-driver-npu",
		Version:                       "latest",
		DriverName:                    "npu.rebellions.ai",
		KubeletRegistrarDirectoryPath: "/var/lib/kubelet/plugins_registry",
		KubeletPluginsDirectoryPath:   "/var/lib/kubelet/plugins",
		HealthcheckPort:               51515,
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNDRAKubeletPluginName
	p := NewDRAKubeletPluginPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	// ServiceAccount
	assertServiceAccountExists(t, c, name, owner.Name)

	// DaemonSet
	ds := assertDaemonSetBasics(t, c, name, owner.Name)
	assertNodeSelector(t, ds, "rebellions.ai/npu.deploy.dra-kubelet-plugin")

	mainContainer := ds.Spec.Template.Spec.Containers[0]
	assertContainerImage(t, mainContainer, "rebellions/k8s-dra-driver-npu", "latest")
	assertPrivileged(t, mainContainer)

	if mainContainer.LivenessProbe == nil {
		t.Fatal("expected liveness probe when HealthcheckPort > 0")
	}
	if mainContainer.LivenessProbe.GRPC == nil || mainContainer.LivenessProbe.GRPC.Port != 51515 {
		t.Fatal("liveness probe should use GRPC on port 51515")
	}

	// ClusterRole
	cr := &rbacv1.ClusterRole{}
	assertObjectExists(t, c, types.NamespacedName{Name: name + draClusterRoleSuffix}, cr)
	assertClusterRoleHasRule(t, cr, "resource.k8s.io", "resourceslices")

	// ClusterRoleBinding
	crb := &rbacv1.ClusterRoleBinding{}
	assertObjectExists(t, c, types.NamespacedName{Name: name + draClusterBindSuffix}, crb)
	if crb.RoleRef.Name != name+draClusterRoleSuffix {
		t.Fatalf("ClusterRoleBinding RoleRef.Name = %q, want %q", crb.RoleRef.Name, name+draClusterRoleSuffix)
	}
	if len(crb.Subjects) == 0 || crb.Subjects[0].Name != name {
		t.Fatal("ClusterRoleBinding subject should reference the component ServiceAccount")
	}

	// No toolkit-validation init container: the DaemonSet serves both
	// container and vm-passthrough nodes; toolkit-ready never appears on
	// the latter, so gating on it would deadlock passthrough nodes.
	if len(ds.Spec.Template.Spec.InitContainers) != 0 {
		t.Fatalf("expected no init containers, got %d", len(ds.Spec.Template.Spec.InitContainers))
	}

	// No /sys hostPath mount: the privileged container reads the host PCI
	// tree through the runtime-provided sysfs, and an explicit read-only
	// mount would shadow the rw sysfs rbln-smi may rely on.
	for _, m := range mainContainer.VolumeMounts {
		if m.MountPath == "/sys" {
			t.Fatal("the DRA plugin must not mount /sys; the runtime-provided sysfs suffices")
		}
	}

	// DeviceClass: npu (containers, extended-resource bridge)
	dc := &resourcev1.DeviceClass{}
	assertObjectExists(t, c, types.NamespacedName{Name: "npu.rebellions.ai"}, dc)
	if len(dc.Spec.Selectors) == 0 || dc.Spec.Selectors[0].CEL == nil {
		t.Fatal("DeviceClass should have CEL selector")
	}
	wantExpr := fmt.Sprintf("device.driver == %q && device.attributes[%q].type == %q",
		"npu.rebellions.ai", "npu.rebellions.ai", "npu")
	if dc.Spec.Selectors[0].CEL.Expression != wantExpr {
		t.Fatalf("DeviceClass CEL expression = %q, want %q", dc.Spec.Selectors[0].CEL.Expression, wantExpr)
	}
	if dc.Spec.ExtendedResourceName == nil || *dc.Spec.ExtendedResourceName != draExtendedResource {
		t.Fatal("npu DeviceClass should keep the extended-resource bridge")
	}

	// DeviceClass: vfio (VM passthrough, no extended-resource bridge)
	vdc := &resourcev1.DeviceClass{}
	assertObjectExists(t, c, types.NamespacedName{Name: "vfio-npu.rebellions.ai"}, vdc)
	wantVfioExpr := fmt.Sprintf("device.driver == %q && device.attributes[%q].type == %q",
		"npu.rebellions.ai", "npu.rebellions.ai", "vfio")
	if len(vdc.Spec.Selectors) == 0 || vdc.Spec.Selectors[0].CEL == nil ||
		vdc.Spec.Selectors[0].CEL.Expression != wantVfioExpr {
		t.Fatalf("vfio DeviceClass CEL expression mismatch: %+v", vdc.Spec.Selectors)
	}
	if vdc.Spec.ExtendedResourceName != nil {
		t.Fatal("vfio DeviceClass must not expose an extended resource name")
	}
}

func TestDRAKubeletPluginPatch_ContainerToolkitDisabled(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.ContainerToolkit.Enabled = false
	owner.Spec.DRAKubeletPlugin = rblnv1beta1.RBLNDRAKubeletPluginSpec{
		Enabled:    true,
		Registry:   "docker.io",
		Image:      "rebellions/k8s-dra-driver-npu",
		Version:    "latest",
		DriverName: "npu.rebellions.ai",
	}

	p := NewDRAKubeletPluginPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")
	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() should succeed without containerToolkit, got: %v", err)
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNDRAKubeletPluginName
	assertDaemonSetBasics(t, c, name, owner.Name)
}

func TestDRAKubeletPluginCleanUp(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.DRAKubeletPlugin = rblnv1beta1.RBLNDRAKubeletPluginSpec{
		Enabled:                       true,
		Registry:                      "docker.io",
		Image:                         "rebellions/k8s-dra-driver-npu",
		Version:                       "latest",
		DriverName:                    "npu.rebellions.ai",
		KubeletRegistrarDirectoryPath: "/var/lib/kubelet/plugins_registry",
		KubeletPluginsDirectoryPath:   "/var/lib/kubelet/plugins",
		HealthcheckPort:               51515,
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNDRAKubeletPluginName
	p := NewDRAKubeletPluginPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	if err := p.CleanUp(ctx, owner); err != nil {
		t.Fatalf("CleanUp() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &appsv1.DaemonSet{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name + draClusterRoleSuffix}, &rbacv1.ClusterRole{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name + draClusterBindSuffix}, &rbacv1.ClusterRoleBinding{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: "npu.rebellions.ai"}, &resourcev1.DeviceClass{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &corev1.ServiceAccount{})
}
