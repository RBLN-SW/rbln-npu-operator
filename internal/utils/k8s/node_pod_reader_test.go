package k8sutil

import (
	"context"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rblnv1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
)

func TestNodePodReaderRBACScopeAndSharedOwners(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := rblnv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	ctx := context.Background()
	for _, name := range []string{"driver-a", "driver-b"} {
		owner := &rblnv1.RBLNDriver{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name)}}
		if err := ReconcileNodePodReader(ctx, c, scheme, owner, "operator", "smd", "smd-reader", false); err != nil {
			t.Fatal(err)
		}
	}
	clusterRole := &rbacv1.ClusterRole{}
	if err := c.Get(ctx, client.ObjectKey{Name: "smd-reader"}, clusterRole); err != nil {
		t.Fatal(err)
	}
	if len(clusterRole.Rules) != 1 || len(clusterRole.Rules[0].Resources) != 1 || clusterRole.Rules[0].Resources[0] != "nodes" {
		t.Fatalf("cluster role must not grant cluster-wide pod access: %+v", clusterRole.Rules)
	}
	role := &rbacv1.Role{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "operator", Name: "smd-reader"}, role); err != nil {
		t.Fatal(err)
	}
	if len(role.Rules) != 1 || len(role.Rules[0].Verbs) != 1 || role.Rules[0].Verbs[0] != "list" || role.Rules[0].Resources[0] != "pods" {
		t.Fatalf("unexpected pod permissions: %+v", role.Rules)
	}
	for _, object := range nodePodReaderObjects("operator", "smd", "smd-reader") {
		if err := c.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
			t.Fatal(err)
		}
		if len(object.GetOwnerReferences()) != 2 {
			t.Fatalf("shared reader lost a driver owner: %+v", object.GetOwnerReferences())
		}
	}
	controllerOwner := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "controller-owner", UID: "controller-uid"}}
	if err := ReconcileNodePodReader(ctx, c, scheme, controllerOwner, "operator", "gate", "gate-reader", true); err != nil {
		t.Fatal(err)
	}
	for _, object := range nodePodReaderObjects("operator", "gate", "gate-reader") {
		if err := c.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
			t.Fatal(err)
		}
		if ref := metav1.GetControllerOf(object); ref == nil || ref.UID != controllerOwner.UID {
			t.Fatalf("controller ownership missing from %T: %+v", object, ref)
		}
	}
	if err := DeleteNodePodReader(ctx, c, "operator", "smd-reader"); err != nil {
		t.Fatal(err)
	}
	for _, object := range nodePodReaderObjects("operator", "smd", "smd-reader") {
		if err := c.Get(ctx, client.ObjectKeyFromObject(object), object); err == nil {
			t.Fatalf("reader RBAC not removed: %T", object)
		}
	}
}
