package components

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func TestContainerToolkitPatch(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()

	name := consts.RBLNBaseName + "-" + consts.RBLNContainerToolkitName
	p := NewContainerToolkitPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "", consts.Containerd)

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	// ServiceAccount
	assertServiceAccountExists(t, c, name, owner.Name)

	// DaemonSet
	ds := assertDaemonSetBasics(t, c, name, owner.Name)
	assertNodeSelector(t, ds, "rebellions.ai/npu.deploy.container-toolkit")

	if !ds.Spec.Template.Spec.HostPID {
		t.Fatal("expected HostPID=true")
	}

	mainContainer := ds.Spec.Template.Spec.Containers[0]
	assertContainerImage(t, mainContainer, "rebellions/rbln-container-toolkit", "v1.0")
	assertPrivileged(t, mainContainer)

	hasContainerdSocket := false
	for _, vol := range ds.Spec.Template.Spec.Volumes {
		if vol.Name == containerdSockVolumeName {
			hasContainerdSocket = true
			break
		}
	}
	if !hasContainerdSocket {
		t.Fatal("expected containerd socket volume")
	}

	if len(ds.Spec.Template.Spec.InitContainers) != 1 {
		t.Fatalf("expected 1 init container (driver-validation), got %d", len(ds.Spec.Template.Spec.InitContainers))
	}
	if ds.Spec.Template.Spec.InitContainers[0].Name != "driver-validation" {
		t.Fatalf("init container name = %q, want driver-validation", ds.Spec.Template.Spec.InitContainers[0].Name)
	}

	// Role (apps/daemonsets)
	role := &rbacv1.Role{}
	assertObjectExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, role)
	assertHasOwnerRef(t, role, owner.Name)
	assertRoleHasRule(t, role, "apps", "daemonsets")

	// RoleBinding
	rb := &rbacv1.RoleBinding{}
	assertObjectExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, rb)
	assertHasOwnerRef(t, rb, owner.Name)

	// ConfigMap (entrypoint)
	assertConfigMapHasKey(t, c, name+"-entrypoint", testNamespace, owner.Name, containerToolkitEntrypointKey)
}

func TestContainerToolkitCleanUp(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	name := consts.RBLNBaseName + "-" + consts.RBLNContainerToolkitName
	p := NewContainerToolkitPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "", consts.Containerd)

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	if err := p.CleanUp(ctx, owner); err != nil {
		t.Fatalf("CleanUp() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &appsv1.DaemonSet{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name + "-entrypoint", Namespace: testNamespace}, &corev1.ConfigMap{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &rbacv1.RoleBinding{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &rbacv1.Role{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &corev1.ServiceAccount{})
}
