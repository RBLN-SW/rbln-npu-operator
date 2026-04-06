package components

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
)

const (
	testNamespace    = "rbln-system"
	testInstanceName = "test-driver"
)

func init() {
	logf.SetLogger(zap.New(zap.UseDevMode(true)))
}

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		rebellionsaiv1alpha1.AddToScheme,
		corev1.AddToScheme,
		appsv1.AddToScheme,
		rbacv1.AddToScheme,
	} {
		if err := add(s); err != nil {
			t.Fatalf("scheme registration failed: %v", err)
		}
	}
	return s
}

func newFakeClient(t *testing.T, scheme *runtime.Scheme, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&appsv1.DaemonSet{}).
		Build()
}

func newTestOwner() *rebellionsaiv1alpha1.RBLNDriver {
	return &rebellionsaiv1alpha1.RBLNDriver{
		ObjectMeta: metav1.ObjectMeta{
			Name: testInstanceName,
			UID:  "test-uid-12345",
		},
		Spec: rebellionsaiv1alpha1.RBLNDriverSpec{
			Version: "3.0.0",
		},
	}
}

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

func assertObjectExists(t *testing.T, c client.Client, key types.NamespacedName, obj client.Object) {
	t.Helper()
	if err := c.Get(context.Background(), key, obj); err != nil {
		t.Fatalf("expected object %s/%s to exist: %v", key.Namespace, key.Name, err)
	}
}

func assertObjectNotExists(t *testing.T, c client.Client, key types.NamespacedName, obj client.Object) {
	t.Helper()
	err := c.Get(context.Background(), key, obj)
	if err == nil {
		t.Fatalf("expected object %s/%s to not exist, but it does", key.Namespace, key.Name)
	}
}

//nolint:unparam
func assertClusterObjectExists(t *testing.T, c client.Client, name string, obj client.Object) {
	t.Helper()
	if err := c.Get(context.Background(), types.NamespacedName{Name: name}, obj); err != nil {
		t.Fatalf("expected cluster object %s to exist: %v", name, err)
	}
}

func assertRoleHasRule(t *testing.T, role *rbacv1.Role, apiGroup, resource string) {
	t.Helper()
	for _, rule := range role.Rules {
		for _, g := range rule.APIGroups {
			if g != apiGroup {
				continue
			}
			for _, res := range rule.Resources {
				if res == resource {
					return
				}
			}
		}
	}
	t.Fatalf("Role %q missing rule for %s/%s", role.Name, apiGroup, resource)
}

func assertClusterRoleHasRule(t *testing.T, cr *rbacv1.ClusterRole, apiGroup, resource string) {
	t.Helper()
	for _, rule := range cr.Rules {
		for _, g := range rule.APIGroups {
			if g != apiGroup {
				continue
			}
			for _, res := range rule.Resources {
				if res == resource {
					return
				}
			}
		}
	}
	t.Fatalf("ClusterRole %q missing rule for %s/%s", cr.Name, apiGroup, resource)
}
