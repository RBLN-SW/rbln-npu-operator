package components

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	k8sutil "github.com/rebellions-sw/rbln-npu-operator/internal/utils/k8s"
)

const testNamespace = "rbln-system"

func init() {
	logf.SetLogger(zap.New(zap.UseDevMode(true)))
}

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		rblnv1beta1.AddToScheme,
		corev1.AddToScheme,
		appsv1.AddToScheme,
		rbacv1.AddToScheme,
		resourcev1.AddToScheme,
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

const testVFIOPCIDriver = "vfio-pci"

func newTestOwner() *rblnv1beta1.RBLNClusterPolicy {
	return &rblnv1beta1.RBLNClusterPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: consts.RBLNBaseName + "-cluster-policy",
			UID:  "test-uid-12345",
		},
		Spec: rblnv1beta1.RBLNClusterPolicySpec{
			WorkloadType: consts.RBLNWorkloadConfigContainer,
			Validator: rblnv1beta1.ValidatorSpec{
				Registry: "docker.io",
				Image:    "rebellions/rbln-validator",
				Version:  "v1.0",
			},
			ContainerToolkit: rblnv1beta1.RBLNContainerToolkitSpec{
				Enabled:  true,
				Registry: "docker.io",
				Image:    "rebellions/rbln-container-toolkit",
				Version:  "v1.0",
			},
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

func assertHasOwnerRef(t *testing.T, obj metav1.Object, ownerName string) {
	t.Helper()
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Name == ownerName {
			return
		}
	}
	t.Fatalf("object %s missing owner reference to %q", obj.GetName(), ownerName)
}

func assertServiceAccountExists(t *testing.T, c client.Client, name, ownerName string) {
	t.Helper()
	sa := &corev1.ServiceAccount{}
	assertObjectExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, sa)
	assertHasOwnerRef(t, sa, ownerName)
}

func assertDaemonSetBasics(t *testing.T, c client.Client, name, ownerName string) *appsv1.DaemonSet {
	t.Helper()
	ds := &appsv1.DaemonSet{}
	assertObjectExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, ds)
	assertHasOwnerRef(t, ds, ownerName)

	if ds.Spec.Template.Spec.ServiceAccountName != name {
		t.Fatalf("DaemonSet %s: ServiceAccountName = %q, want %q", name, ds.Spec.Template.Spec.ServiceAccountName, name)
	}
	if ds.Spec.Selector == nil ||
		ds.Spec.Selector.MatchLabels[consts.LabelAppName] != consts.OperatorName ||
		ds.Spec.Selector.MatchLabels[consts.LabelAppComponent] == "" {
		t.Fatalf("DaemonSet %s: expected standard Kubernetes labels in matchLabels, got %v", name, ds.Spec.Selector.MatchLabels)
	}
	return ds
}

func assertNodeSelector(t *testing.T, ds *appsv1.DaemonSet, key string) {
	t.Helper()
	if _, ok := ds.Spec.Template.Spec.NodeSelector[key]; !ok {
		t.Fatalf("DaemonSet %s: missing NodeSelector %q", ds.Name, key)
	}
}

func assertContainerImage(t *testing.T, c corev1.Container, image, version string) {
	t.Helper()
	want := k8sutil.ComposeImageReference("docker.io", image) + ":" + version
	if c.Image != want {
		t.Fatalf("container %q: image = %q, want %q", c.Name, c.Image, want)
	}
}

func assertPrivileged(t *testing.T, c corev1.Container) {
	t.Helper()
	if c.SecurityContext == nil || c.SecurityContext.Privileged == nil || !*c.SecurityContext.Privileged {
		t.Fatalf("container %q: expected privileged=true", c.Name)
	}
}

func envValue(envs []corev1.EnvVar, name string) string {
	for _, e := range envs {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

func assertContainerHasVolumeMount(t *testing.T, c corev1.Container, volumeName string) {
	t.Helper()
	for _, vm := range c.VolumeMounts {
		if vm.Name == volumeName {
			return
		}
	}
	t.Fatalf("container %q: missing volume mount %q", c.Name, volumeName)
}

func assertPodHasVolume(t *testing.T, podSpec corev1.PodSpec, volumeName string) {
	t.Helper()
	for _, v := range podSpec.Volumes {
		if v.Name == volumeName {
			return
		}
	}
	t.Fatalf("pod spec: missing volume %q", volumeName)
}

func assertObjectNotExists(t *testing.T, c client.Client, key types.NamespacedName, obj client.Object) {
	t.Helper()
	err := c.Get(context.Background(), key, obj)
	if err == nil {
		t.Fatalf("expected object %s/%s to not exist, but it does", key.Namespace, key.Name)
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

func assertConfigMapHasKey(t *testing.T, c client.Client, name, namespace, ownerName, key string) {
	t.Helper()
	cm := &corev1.ConfigMap{}
	assertObjectExists(t, c, types.NamespacedName{Name: name, Namespace: namespace}, cm)
	assertHasOwnerRef(t, cm, ownerName)
	if _, ok := cm.Data[key]; !ok {
		t.Fatalf("ConfigMap %q missing key %q", name, key)
	}
}
