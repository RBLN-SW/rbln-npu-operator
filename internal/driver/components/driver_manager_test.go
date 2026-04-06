package components

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
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

	// ServiceAccount — ownerRef to driver instance
	sa := &corev1.ServiceAccount{}
	assertObjectExists(t, c, types.NamespacedName{Name: driverManagerName, Namespace: testNamespace}, sa)
	assertHasOwnerRef(t, sa, owner.Name)

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

	// OpenShift Role with SCC rule — ownerRef to driver
	role := &rbacv1.Role{}
	assertObjectExists(t, c, types.NamespacedName{Name: driverManagerName, Namespace: testNamespace}, role)
	assertHasOwnerRef(t, role, owner.Name)
	assertRoleHasRule(t, role, "security.openshift.io", "securitycontextconstraints")

	// OpenShift RoleBinding — ownerRef to driver
	rb := &rbacv1.RoleBinding{}
	assertObjectExists(t, c, types.NamespacedName{Name: driverManagerName, Namespace: testNamespace}, rb)
	assertHasOwnerRef(t, rb, owner.Name)
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

	// CleanUp deletes instance-specific DaemonSets and ConfigMap.
	// ClusterRole, ClusterRoleBinding, and ServiceAccount are left for GC via ownerReferences.
	assertObjectNotExists(t, c, types.NamespacedName{
		Name:      driverManagerName + "-" + startupProbeConfigMapSuffix,
		Namespace: testNamespace,
	}, &corev1.ConfigMap{})
}
