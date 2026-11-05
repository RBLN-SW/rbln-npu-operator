package components

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestLegacyVFIOManagerCleanupWaitsForPodsBeforeRemovingAccess(t *testing.T) {
	const name = "rbln-vfio-manager"
	scheme := newTestScheme(t)
	meta := metav1.ObjectMeta{Name: name, Namespace: testNamespace}
	ds := &appsv1.DaemonSet{ObjectMeta: *meta.DeepCopy()}
	ds.Finalizers = []string{metav1.FinalizerDeleteDependents}
	objects := []client.Object{
		&corev1.ServiceAccount{ObjectMeta: meta},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name + "-config", Namespace: testNamespace}},
		&rbacv1.Role{ObjectMeta: meta},
		&rbacv1.RoleBinding{ObjectMeta: meta},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name}},
	}
	c := newFakeClient(t, scheme, append(objects, ds)...)
	p := NewLegacyVFIOManagerCleanup(c, logr.Discard(), testNamespace, scheme)
	if p.IsEnabled() {
		t.Fatal("retired VFIO manager must remain disabled")
	}
	ctx := context.Background()
	if err := p.CleanUp(ctx, newTestOwner()); err == nil {
		t.Fatal("replacement must wait until foreground deletion finishes")
	}
	for _, obj := range objects {
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			t.Fatalf("access removed before the old pods terminate: %v", err)
		}
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(ds), ds); err != nil {
		t.Fatal(err)
	}
	if ds.DeletionTimestamp.IsZero() {
		t.Fatal("old DaemonSet deletion was not requested")
	}
	// Simulate garbage collection after all old pods have terminated.
	ds.Finalizers = nil
	if err := c.Update(ctx, ds); err != nil {
		t.Fatal(err)
	}
	if err := p.CleanUp(ctx, newTestOwner()); err != nil {
		t.Fatal(err)
	}
	for _, obj := range objects {
		assertObjectNotExists(t, c, client.ObjectKeyFromObject(obj), obj)
	}
	if err := p.CleanUp(ctx, newTestOwner()); err != nil {
		t.Fatalf("repeated cleanup must be harmless: %v", err)
	}
}
