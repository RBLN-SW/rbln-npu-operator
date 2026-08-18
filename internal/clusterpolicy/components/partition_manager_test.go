package components

import (
	"context"
	"slices"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func newPartitionManagerOwner() *rblnv1beta1.RBLNClusterPolicy {
	owner := newTestOwner()
	owner.Spec.PartitionManager = rblnv1beta1.RBLNPartitionManagerSpec{
		Enabled:  true,
		Registry: "docker.io",
		Image:    "rebellions/k8s-partition-manager",
		Version:  "v0.1.0",
	}
	return owner
}

func TestPartitionManagerPatch(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newPartitionManagerOwner()
	name := consts.RBLNBaseName + "-" + consts.RBLNPartitionManagerName
	p := NewPartitionManagerPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	assertServiceAccountExists(t, c, name, owner.Name)

	cr := &rbacv1.ClusterRole{}
	assertObjectExists(t, c, types.NamespacedName{Name: name}, cr)
	assertClusterRoleHasRule(t, cr, "", "nodes")
	assertClusterRoleHasRule(t, cr, "", "nodes/status")
	assertClusterRoleHasRule(t, cr, "", "pods")
	assertClusterRoleHasRule(t, cr, "", "pods/eviction")
	assertClusterRoleHasRule(t, cr, "apps", "daemonsets")

	crb := &rbacv1.ClusterRoleBinding{}
	assertObjectExists(t, c, types.NamespacedName{Name: name}, crb)
	if crb.RoleRef.Name != name {
		t.Fatalf("ClusterRoleBinding RoleRef.Name = %q, want %q", crb.RoleRef.Name, name)
	}
	if len(crb.Subjects) != 1 || crb.Subjects[0].Name != name || crb.Subjects[0].Namespace != testNamespace {
		t.Fatal("ClusterRoleBinding subject should reference the component ServiceAccount")
	}

	ds := assertDaemonSetBasics(t, c, name, owner.Name)
	assertNodeSelector(t, ds, "rebellions.ai/npu.deploy.partition-manager")

	if !ds.Spec.Template.Spec.HostNetwork {
		t.Fatal("expected pod HostNetwork=true (dials rbln-smd node-locally at 127.0.0.1:50051)")
	}

	assertGateInitContainer(t, ds, consts.RBLNPartitionManagerName)

	mainContainer := ds.Spec.Template.Spec.Containers[0]
	assertContainerImage(t, mainContainer, "rebellions/k8s-partition-manager", "v0.1.0")
	if !slices.Equal(mainContainer.Args, []string{"reconcile-partition-state"}) {
		t.Fatalf("container args = %v, want [reconcile-partition-state]", mainContainer.Args)
	}

	if mainContainer.SecurityContext != nil && mainContainer.SecurityContext.Privileged != nil && *mainContainer.SecurityContext.Privileged {
		t.Fatal("partition manager container must not be privileged: hardware access is delegated to rbln-smd")
	}

	if ref := envFieldRef(mainContainer.Env, "NODE_NAME"); ref == nil || ref.FieldPath != "spec.nodeName" {
		t.Fatal("expected NODE_NAME env from the spec.nodeName Downward API")
	}
	if ref := envFieldRef(mainContainer.Env, "OPERATOR_NAMESPACE"); ref == nil || ref.FieldPath != "metadata.namespace" {
		t.Fatal("expected OPERATOR_NAMESPACE env from the metadata.namespace Downward API")
	}

	assertContainerHasVolumeMount(t, mainContainer, consts.ValidationsVolumeName)
	assertPodHasVolume(t, ds.Spec.Template.Spec, consts.ValidationsVolumeName)
}

func TestPartitionManagerEnvOverride(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newPartitionManagerOwner()
	owner.Spec.PartitionManager.Env = []corev1.EnvVar{
		{Name: "SMD_ADDRESS", Value: "127.0.0.1:50052"},
		{Name: "DRAIN_TIMEOUT_SECONDS", Value: "300"},
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNPartitionManagerName
	p := NewPartitionManagerPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	ds := &appsv1.DaemonSet{}
	assertObjectExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, ds)

	env := ds.Spec.Template.Spec.Containers[0].Env
	if envValue(env, "SMD_ADDRESS") != "127.0.0.1:50052" {
		t.Fatalf("SMD_ADDRESS = %q, want 127.0.0.1:50052", envValue(env, "SMD_ADDRESS"))
	}
	if envValue(env, "DRAIN_TIMEOUT_SECONDS") != "300" {
		t.Fatalf("DRAIN_TIMEOUT_SECONDS = %q, want 300", envValue(env, "DRAIN_TIMEOUT_SECONDS"))
	}
	if ref := envFieldRef(env, "NODE_NAME"); ref == nil {
		t.Fatal("spec env must merge with, not replace, the default Downward API env")
	}
}

func TestPartitionManagerCleanUp(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newPartitionManagerOwner()
	name := consts.RBLNBaseName + "-" + consts.RBLNPartitionManagerName
	p := NewPartitionManagerPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	if err := p.CleanUp(ctx, owner); err != nil {
		t.Fatalf("CleanUp() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &appsv1.DaemonSet{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name}, &rbacv1.ClusterRoleBinding{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name}, &rbacv1.ClusterRole{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &corev1.ServiceAccount{})
}

func TestPartitionManagerDisabled(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.PartitionManager = rblnv1beta1.RBLNPartitionManagerSpec{Enabled: false}

	name := consts.RBLNBaseName + "-" + consts.RBLNPartitionManagerName
	p := NewPartitionManagerPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if p.IsEnabled() {
		t.Fatal("IsEnabled() = true, want false")
	}
	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &appsv1.DaemonSet{})
}
