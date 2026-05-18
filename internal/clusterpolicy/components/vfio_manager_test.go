package components

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func TestVFIOManagerPatch(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.VFIOManager = rblnv1beta1.RBLNVFIOManagerSpec{
		Enabled:  true,
		Registry: "docker.io",
		Image:    "rebellions/rbln-vfio-manager",
		Version:  "latest",
		DriverManager: rblnv1beta1.VFIODriverManagerSpec{
			Registry: "docker.io",
			Image:    "rebellions/rbln-k8s-driver-manager",
			Version:  "v0.1.0",
		},
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNVFIOManagerName
	p := NewVFIOManagerPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	// ServiceAccount
	assertServiceAccountExists(t, c, name, owner.Name)

	// DaemonSet
	ds := assertDaemonSetBasics(t, c, name, owner.Name)
	assertNodeSelector(t, ds, "rebellions.ai/npu.deploy.vfio-manager")

	if len(ds.Spec.Template.Spec.InitContainers) != 1 {
		t.Fatalf("expected 1 init container (k8s-driver-manager), got %d", len(ds.Spec.Template.Spec.InitContainers))
	}
	initContainer := ds.Spec.Template.Spec.InitContainers[0]
	if initContainer.Name != "k8s-driver-manager" {
		t.Fatalf("init container name = %q, want k8s-driver-manager", initContainer.Name)
	}
	assertContainerImage(t, initContainer, "rebellions/rbln-k8s-driver-manager", "v0.1.0")
	assertPrivileged(t, initContainer)
	if len(initContainer.Command) != 1 || initContainer.Command[0] != "driver-manager" {
		t.Fatalf("init container command = %v, want [driver-manager]", initContainer.Command)
	}
	if len(initContainer.Args) != 1 || initContainer.Args[0] != "reconcile-vfio-state" {
		t.Fatalf("init container args = %v, want [reconcile-vfio-state]", initContainer.Args)
	}
	if initContainer.Args[0] == "reconcile-driver-state" {
		t.Fatalf("vfio-manager init container must not call reconcile-driver-state " +
			"(host-driver short-circuit incompatible with vm-passthrough nodes)")
	}
	assertContainerHasVolumeMount(t, initContainer, "host-sys")
	assertContainerHasVolumeMount(t, initContainer, "host-root")
	if envValue(initContainer.Env, "ENABLE_NPU_POD_EVICTION") != "false" {
		t.Fatalf("ENABLE_NPU_POD_EVICTION = %q, want false (would cordon+drain own host)",
			envValue(initContainer.Env, "ENABLE_NPU_POD_EVICTION"))
	}
	if envValue(initContainer.Env, "PROC_ROOT") != "/host/proc" {
		t.Fatalf("PROC_ROOT = %q, want /host/proc (fd-scanner needs host procfs view)",
			envValue(initContainer.Env, "PROC_ROOT"))
	}
	// Downward-API env vars must set APIVersion explicitly. The kube-apiserver
	// defaults it to "v1" on persist, so omitting it triggers a perpetual
	// reconcile/patch loop (operator submits "", server stores "v1", diff, repeat).
	for _, name := range []string{"NODE_NAME", "OPERATOR_NAMESPACE"} {
		fr := envFieldRef(initContainer.Env, name)
		if fr == nil {
			t.Fatalf("init container env %q missing FieldRef", name)
		}
		if fr.APIVersion != "v1" {
			t.Fatalf("init container env %q FieldRef.APIVersion = %q, want v1 (prevents reconcile thrash)", name, fr.APIVersion)
		}
	}

	role := &rbacv1.Role{}
	assertObjectExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, role)
	assertHasOwnerRef(t, role, owner.Name)
	assertRoleHasRule(t, role, "", "pods")
	assertRoleHasRule(t, role, "", "pods/eviction")
	assertRoleHasRule(t, role, "apps", "daemonsets")

	cr := &rbacv1.ClusterRole{}
	assertObjectExists(t, c, types.NamespacedName{Name: name}, cr)
	assertClusterRoleHasRule(t, cr, "", "nodes")

	crb := &rbacv1.ClusterRoleBinding{}
	assertObjectExists(t, c, types.NamespacedName{Name: name}, crb)
	if crb.RoleRef.Name != name || crb.Subjects[0].Name != name {
		t.Fatalf("ClusterRoleBinding misconfigured: roleRef=%q subject=%q want %q",
			crb.RoleRef.Name, crb.Subjects[0].Name, name)
	}

	rb := &rbacv1.RoleBinding{}
	assertObjectExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, rb)
	assertHasOwnerRef(t, rb, owner.Name)
	if rb.RoleRef.Name != name || rb.Subjects[0].Name != name {
		t.Fatalf("RoleBinding misconfigured: roleRef=%q subject=%q want %q",
			rb.RoleRef.Name, rb.Subjects[0].Name, name)
	}

	mainContainer := ds.Spec.Template.Spec.Containers[0]
	assertContainerImage(t, mainContainer, "rebellions/rbln-vfio-manager", "latest")
	assertPrivileged(t, mainContainer)

	if mainContainer.SecurityContext.SELinuxOptions == nil || mainContainer.SecurityContext.SELinuxOptions.Level != "s0" {
		t.Fatal("expected SELinuxOptions.Level=s0")
	}

	if ds.Spec.Template.Spec.TerminationGracePeriodSeconds == nil || *ds.Spec.Template.Spec.TerminationGracePeriodSeconds != 30 {
		t.Fatalf("expected TerminationGracePeriodSeconds=30, got %v", ds.Spec.Template.Spec.TerminationGracePeriodSeconds)
	}

	// ConfigMap (vfio-manage.sh)
	assertConfigMapHasKey(t, c, name+"-config", testNamespace, owner.Name, "vfio-manage.sh")

	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, types.NamespacedName{Name: name + "-config", Namespace: testNamespace}, cm); err != nil {
		t.Fatalf("get ConfigMap: %v", err)
	}
	script := cm.Data["vfio-manage.sh"]
	for _, want := range []string{
		"bind_all", "bind_pci_device", "handle_bind",
		"is_bound_to_vfio", "unbind_from_other_driver",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("vfio-manage.sh missing %q", want)
		}
	}
	for _, unwanted := range []string{
		"handle_unbind", "unbind_all", "unbind_device", "unbind_from_driver", "vfio_in_use",
		"wait_for_driver_rebind", "drivers_probe", "cleanup_device", "cleanup_all", "handle_cleanup",
	} {
		if strings.Contains(script, unwanted) {
			t.Errorf("vfio-manage.sh should not contain %q (bind-only design)", unwanted)
		}
	}
}

func TestVFIOManagerCleanUp(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.VFIOManager = rblnv1beta1.RBLNVFIOManagerSpec{
		Enabled:  true,
		Registry: "docker.io",
		Image:    "rebellions/rbln-vfio-manager",
		Version:  "latest",
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNVFIOManagerName
	p := NewVFIOManagerPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	if err := p.CleanUp(ctx, owner); err != nil {
		t.Fatalf("CleanUp() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &appsv1.DaemonSet{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name + "-config", Namespace: testNamespace}, &corev1.ConfigMap{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &rbacv1.RoleBinding{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &rbacv1.Role{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name}, &rbacv1.ClusterRoleBinding{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name}, &rbacv1.ClusterRole{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &corev1.ServiceAccount{})
}
