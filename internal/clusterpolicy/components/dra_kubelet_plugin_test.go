package components

import (
	"context"
	"fmt"
	"strings"
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

	name := testBaseName + "-" + consts.RBLNDRAKubeletPluginName
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

	// DeviceClass
	dc := &resourcev1.DeviceClass{}
	assertObjectExists(t, c, types.NamespacedName{Name: "npu.rebellions.ai"}, dc)
	if len(dc.Spec.Selectors) == 0 || dc.Spec.Selectors[0].CEL == nil {
		t.Fatal("DeviceClass should have CEL selector")
	}
	wantExpr := fmt.Sprintf("device.driver == %q", "npu.rebellions.ai")
	if dc.Spec.Selectors[0].CEL.Expression != wantExpr {
		t.Fatalf("DeviceClass CEL expression = %q, want %q", dc.Spec.Selectors[0].CEL.Expression, wantExpr)
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
	err := p.Patch(ctx, owner)
	if err == nil {
		t.Fatal("expected error when containerToolkit is disabled, got nil")
	}
	if !strings.Contains(err.Error(), "containerToolkit") {
		t.Fatalf("error should mention containerToolkit: %v", err)
	}
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

	name := testBaseName + "-" + consts.RBLNDRAKubeletPluginName
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
