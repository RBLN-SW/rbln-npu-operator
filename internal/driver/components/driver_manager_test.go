package components

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
)

func TestNewDriverManagerPatcher_NilDriver(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)

	_, err := NewDriverManagerPatcher(c, logf.Log, testNamespace, nil, scheme, "")
	if err == nil {
		t.Fatal("expected error for nil driver, got nil")
	}
}

func TestDriverManagerPatcher_Patch(t *testing.T) {
	scheme := newTestScheme(t)
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker-1",
			Labels: map[string]string{
				"rebellions.ai/npu.deploy.driver":                         "true",
				"feature.node.kubernetes.io/system-os_release.ID":         "ubuntu",
				"feature.node.kubernetes.io/system-os_release.VERSION_ID": "22.04",
				"feature.node.kubernetes.io/kernel-version.full":          "5.15.0-100-generic",
			},
		},
	}
	c := newFakeClient(t, scheme, node)
	ctx := context.Background()

	owner := newTestOwner()
	p, err := NewDriverManagerPatcher(c, logf.Log, testNamespace, owner, scheme, "")
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	// ServiceAccount — no owner ref (shared)
	sa := &corev1.ServiceAccount{}
	assertObjectExists(t, c, types.NamespacedName{Name: driverManagerName, Namespace: testNamespace}, sa)
	assertNoOwnerRef(t, sa)

	// ClusterRole
	cr := &rbacv1.ClusterRole{}
	assertClusterObjectExists(t, c, driverManagerName, cr)
	assertClusterRoleHasRule(t, cr, "", "nodes")
	assertClusterRoleHasRule(t, cr, "", "pods")
	assertClusterRoleHasRule(t, cr, "apps", "daemonsets")

	// ClusterRoleBinding
	crb := &rbacv1.ClusterRoleBinding{}
	assertClusterObjectExists(t, c, driverManagerName, crb)

	// ConfigMap (startup probe)
	cm := &corev1.ConfigMap{}
	assertObjectExists(t, c, types.NamespacedName{Name: driverManagerName + "-" + startupProbeConfigMapSuffix, Namespace: testNamespace}, cm)
	if _, ok := cm.Data[startupProbeScriptName]; !ok {
		t.Fatalf("ConfigMap missing key %q", startupProbeScriptName)
	}
}

func TestDriverManagerPatcher_Patch_OpenShift(t *testing.T) {
	scheme := newTestScheme(t)
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker-ocp",
			Labels: map[string]string{
				"rebellions.ai/npu.deploy.driver":                         "true",
				"feature.node.kubernetes.io/system-os_release.ID":         "rhcos",
				"feature.node.kubernetes.io/system-os_release.VERSION_ID": "4.14",
				"feature.node.kubernetes.io/kernel-version.full":          "5.14.0-284.el9.x86_64",
			},
		},
	}
	c := newFakeClient(t, scheme, node)
	ctx := context.Background()

	owner := newTestOwner()
	p, err := NewDriverManagerPatcher(c, logf.Log, testNamespace, owner, scheme, "v4.14.0")
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	// OpenShift Role with SCC rule — no owner ref
	role := &rbacv1.Role{}
	assertObjectExists(t, c, types.NamespacedName{Name: driverManagerName, Namespace: testNamespace}, role)
	assertNoOwnerRef(t, role)
	assertRoleHasRule(t, role, "security.openshift.io", "securitycontextconstraints")

	// OpenShift RoleBinding — no owner ref
	rb := &rbacv1.RoleBinding{}
	assertObjectExists(t, c, types.NamespacedName{Name: driverManagerName, Namespace: testNamespace}, rb)
	assertNoOwnerRef(t, rb)
}

func TestDriverManagerPatcher_CleanUp(t *testing.T) {
	scheme := newTestScheme(t)
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "worker-cleanup",
			Labels: map[string]string{
				"rebellions.ai/npu.deploy.driver":                         "true",
				"feature.node.kubernetes.io/system-os_release.ID":         "ubuntu",
				"feature.node.kubernetes.io/system-os_release.VERSION_ID": "22.04",
				"feature.node.kubernetes.io/kernel-version.full":          "5.15.0-100-generic",
			},
		},
	}
	c := newFakeClient(t, scheme, node)
	ctx := context.Background()

	owner := newTestOwner()
	p, err := NewDriverManagerPatcher(c, logf.Log, testNamespace, owner, scheme, "")
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}

	// First patch to create resources.
	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	// Verify resources exist.
	assertObjectExists(t, c, types.NamespacedName{Name: driverManagerName, Namespace: testNamespace}, &corev1.ServiceAccount{})
	assertClusterObjectExists(t, c, driverManagerName, &rbacv1.ClusterRole{})
	assertClusterObjectExists(t, c, driverManagerName, &rbacv1.ClusterRoleBinding{})

	// CleanUp.
	if err := p.CleanUp(ctx, owner); err != nil {
		t.Fatalf("CleanUp() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{Name: driverManagerName, Namespace: testNamespace}, &corev1.ServiceAccount{})
	assertClusterObjectNotExists(t, c, driverManagerName, &rbacv1.ClusterRole{})
	assertClusterObjectNotExists(t, c, driverManagerName, &rbacv1.ClusterRoleBinding{})
	assertObjectNotExists(t, c, types.NamespacedName{
		Name:      driverManagerName + "-" + startupProbeConfigMapSuffix,
		Namespace: testNamespace,
	}, &corev1.ConfigMap{})
}

func TestDriverManagerPatcher_CleanUp_PreservesSharedResources(t *testing.T) {
	scheme := newTestScheme(t)

	// Create two RBLNDriver instances so shared resources are preserved.
	otherDriver := &rebellionsaiv1alpha1.RBLNDriver{
		ObjectMeta: metav1.ObjectMeta{Name: "other-driver", UID: "other-uid"},
		Spec:       rebellionsaiv1alpha1.RBLNDriverSpec{Version: "3.0.0"},
	}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: driverManagerName, Namespace: testNamespace}}
	cr := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: driverManagerName}}
	crb := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: driverManagerName}}

	c := newFakeClient(t, scheme, otherDriver, sa, cr, crb)
	ctx := context.Background()

	owner := newTestOwner()
	p, err := NewDriverManagerPatcher(c, logf.Log, testNamespace, owner, scheme, "")
	if err != nil {
		t.Fatalf("NewDriverManagerPatcher() error: %v", err)
	}

	if err := p.CleanUp(ctx, owner); err != nil {
		t.Fatalf("CleanUp() error: %v", err)
	}

	// Shared resources should still exist because another driver instance exists.
	assertObjectExists(t, c, types.NamespacedName{Name: driverManagerName, Namespace: testNamespace}, &corev1.ServiceAccount{})
	assertClusterObjectExists(t, c, driverManagerName, &rbacv1.ClusterRole{})
	assertClusterObjectExists(t, c, driverManagerName, &rbacv1.ClusterRoleBinding{})
}
