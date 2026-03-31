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

func TestValidatorPatch(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	name := consts.RBLNBaseName + "-" + consts.RBLNValidatorName
	p := NewValidatorPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	// ServiceAccount
	assertServiceAccountExists(t, c, name, owner.Name)

	// DaemonSet
	ds := assertDaemonSetBasics(t, c, name, owner.Name)
	assertNodeSelector(t, ds, "rebellions.ai/npu.deploy.operator-validator")

	// Init containers: driver-validation + toolkit-validation
	if len(ds.Spec.Template.Spec.InitContainers) != 2 {
		t.Fatalf("expected 2 init containers (driver + toolkit), got %d", len(ds.Spec.Template.Spec.InitContainers))
	}
	if ds.Spec.Template.Spec.InitContainers[0].Name != "driver-validation" {
		t.Fatalf("first init container = %q, want driver-validation", ds.Spec.Template.Spec.InitContainers[0].Name)
	}
	if ds.Spec.Template.Spec.InitContainers[1].Name != "toolkit-validation" {
		t.Fatalf("second init container = %q, want toolkit-validation", ds.Spec.Template.Spec.InitContainers[1].Name)
	}
	for _, ic := range ds.Spec.Template.Spec.InitContainers {
		assertContainerImage(t, ic, "rebellions/rbln-validator", "v1.0")
		assertPrivileged(t, ic)
	}

	// Main container: PreStop lifecycle
	mainContainer := ds.Spec.Template.Spec.Containers[0]
	assertContainerImage(t, mainContainer, "rebellions/rbln-validator", "v1.0")
	assertPrivileged(t, mainContainer)

	if mainContainer.Lifecycle == nil || mainContainer.Lifecycle.PreStop == nil {
		t.Fatal("expected PreStop lifecycle hook")
	}

	// Role (pods + daemonsets)
	role := &rbacv1.Role{}
	assertObjectExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, role)
	assertHasOwnerRef(t, role, owner.Name)
	assertRoleHasRule(t, role, "", "pods")
	assertRoleHasRule(t, role, "apps", "daemonsets")

	// RoleBinding
	rb := &rbacv1.RoleBinding{}
	assertObjectExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, rb)
	assertHasOwnerRef(t, rb, owner.Name)

	// ClusterRole (nodes access)
	cr := &rbacv1.ClusterRole{}
	assertObjectExists(t, c, types.NamespacedName{Name: name}, cr)
	assertClusterRoleHasRule(t, cr, "", "nodes")

	// ClusterRoleBinding
	crb := &rbacv1.ClusterRoleBinding{}
	assertObjectExists(t, c, types.NamespacedName{Name: name}, crb)
	if crb.RoleRef.Name != name {
		t.Fatalf("ClusterRoleBinding RoleRef.Name = %q, want %q", crb.RoleRef.Name, name)
	}
}

func TestValidatorCleanUp(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	name := consts.RBLNBaseName + "-" + consts.RBLNValidatorName
	p := NewValidatorPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	if err := p.CleanUp(ctx, owner); err != nil {
		t.Fatalf("CleanUp() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &appsv1.DaemonSet{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name}, &rbacv1.ClusterRoleBinding{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name}, &rbacv1.ClusterRole{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &rbacv1.RoleBinding{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &rbacv1.Role{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &corev1.ServiceAccount{})
}
