package components

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

// The legacy patcher must stay permanently disabled so the PatchComponents
// loop keeps routing it through CleanUp.
func TestLegacyRBLNDaemonCleanup_IsDisabled(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)

	p := NewLegacyRBLNDaemonCleanup(c, logf.Log, testNamespace, scheme, "")
	if p.IsEnabled() {
		t.Fatal("legacy rbln-daemon patcher must never report enabled")
	}
}

func TestLegacyRBLNDaemonCleanup_DeletesPreRenameObjects(t *testing.T) {
	scheme := newTestScheme(t)
	seed := []client.Object{
		&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: legacyRBLNDaemonName, Namespace: testNamespace}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: legacyRBLNDaemonName, Namespace: testNamespace}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: legacyRBLNDaemonName, Namespace: testNamespace}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: legacyRBLNDaemonName, Namespace: testNamespace}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: legacyRBLNDaemonName, Namespace: testNamespace}},
	}
	c := newFakeClient(t, scheme, seed...)
	ctx := context.Background()

	p := NewLegacyRBLNDaemonCleanup(c, logf.Log, testNamespace, scheme, "4.16")
	if err := p.CleanUp(ctx, &rblnv1beta1.RBLNClusterPolicy{}); err != nil {
		t.Fatalf("CleanUp() error: %v", err)
	}

	key := types.NamespacedName{Name: legacyRBLNDaemonName, Namespace: testNamespace}
	assertObjectNotExists(t, c, key, &appsv1.DaemonSet{})
	assertObjectNotExists(t, c, key, &corev1.Service{})
	assertObjectNotExists(t, c, key, &corev1.ServiceAccount{})
	assertObjectNotExists(t, c, key, &rbacv1.Role{})
	assertObjectNotExists(t, c, key, &rbacv1.RoleBinding{})

	// A second pass over an already-clean cluster must be a no-op.
	if err := p.CleanUp(ctx, &rblnv1beta1.RBLNClusterPolicy{}); err != nil {
		t.Fatalf("CleanUp() second pass error: %v", err)
	}
}
