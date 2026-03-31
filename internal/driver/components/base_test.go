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

func TestReconcileServiceAccount(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	bp := &basePatcher{
		client:    c,
		log:       logf.Log,
		scheme:    scheme,
		name:      driverManagerName,
		namespace: testNamespace,
	}

	if err := bp.reconcileServiceAccount(ctx); err != nil {
		t.Fatalf("reconcileServiceAccount() error: %v", err)
	}

	sa := &corev1.ServiceAccount{}
	assertObjectExists(t, c, types.NamespacedName{Name: driverManagerName, Namespace: testNamespace}, sa)
	assertNoOwnerRef(t, sa)
}

func TestReconcileOpenShiftRBAC(t *testing.T) {
	t.Run("no-op on non-OpenShift", func(t *testing.T) {
		scheme := newTestScheme(t)
		c := newFakeClient(t, scheme)

		bp := &basePatcher{
			client:           c,
			log:              logf.Log,
			scheme:           scheme,
			name:             driverManagerName,
			namespace:        testNamespace,
			openshiftVersion: "",
		}

		if err := bp.reconcileOpenShiftRBAC(context.Background()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		assertObjectNotExists(t, c, types.NamespacedName{Name: driverManagerName, Namespace: testNamespace}, &rbacv1.Role{})
	})

	t.Run("creates Role and RoleBinding on OpenShift", func(t *testing.T) {
		scheme := newTestScheme(t)
		c := newFakeClient(t, scheme)
		ctx := context.Background()

		bp := &basePatcher{
			client:           c,
			log:              logf.Log,
			scheme:           scheme,
			name:             driverManagerName,
			namespace:        testNamespace,
			openshiftVersion: "v4.14.0",
		}

		if err := bp.reconcileOpenShiftRBAC(ctx); err != nil {
			t.Fatalf("reconcileOpenShiftRBAC() error: %v", err)
		}

		role := &rbacv1.Role{}
		assertObjectExists(t, c, types.NamespacedName{Name: driverManagerName, Namespace: testNamespace}, role)
		assertNoOwnerRef(t, role)
		assertRoleHasRule(t, role, "security.openshift.io", "securitycontextconstraints")

		rb := &rbacv1.RoleBinding{}
		assertObjectExists(t, c, types.NamespacedName{Name: driverManagerName, Namespace: testNamespace}, rb)
		assertNoOwnerRef(t, rb)
	})
}

func TestDeleteOpenShiftRBAC(t *testing.T) {
	t.Run("no-op on non-OpenShift", func(t *testing.T) {
		scheme := newTestScheme(t)
		c := newFakeClient(t, scheme)

		bp := &basePatcher{
			client:           c,
			log:              logf.Log,
			name:             driverManagerName,
			namespace:        testNamespace,
			openshiftVersion: "",
		}

		if err := bp.deleteOpenShiftRBAC(context.Background()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("deletes Role and RoleBinding on OpenShift", func(t *testing.T) {
		scheme := newTestScheme(t)
		role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: driverManagerName, Namespace: testNamespace}}
		rb := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: driverManagerName, Namespace: testNamespace}}
		c := newFakeClient(t, scheme, role, rb)
		ctx := context.Background()

		bp := &basePatcher{
			client:           c,
			log:              logf.Log,
			name:             driverManagerName,
			namespace:        testNamespace,
			openshiftVersion: "v4.14.0",
		}

		if err := bp.deleteOpenShiftRBAC(ctx); err != nil {
			t.Fatalf("deleteOpenShiftRBAC() error: %v", err)
		}

		assertObjectNotExists(t, c, types.NamespacedName{Name: driverManagerName, Namespace: testNamespace}, &rbacv1.Role{})
		assertObjectNotExists(t, c, types.NamespacedName{Name: driverManagerName, Namespace: testNamespace}, &rbacv1.RoleBinding{})
	})
}

func TestDeleteServiceAccount(t *testing.T) {
	scheme := newTestScheme(t)
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: driverManagerName, Namespace: testNamespace}}
	c := newFakeClient(t, scheme, sa)

	bp := &basePatcher{
		client:    c,
		log:       logf.Log,
		name:      driverManagerName,
		namespace: testNamespace,
	}

	if err := bp.deleteServiceAccount(context.Background()); err != nil {
		t.Fatalf("deleteServiceAccount() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{Name: driverManagerName, Namespace: testNamespace}, &corev1.ServiceAccount{})
}

func TestDeleteIfExists_NotFound(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)

	bp := &basePatcher{client: c, log: logf.Log}

	// Should not error when object does not exist.
	if err := bp.deleteIfExists(context.Background(), &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "nonexistent", Namespace: testNamespace},
	}); err != nil {
		t.Fatalf("deleteIfExists() should not error on NotFound: %v", err)
	}
}
