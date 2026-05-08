package components

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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

	// No init containers (VFIO doesn't depend on toolkit)
	if len(ds.Spec.Template.Spec.InitContainers) != 0 {
		t.Fatalf("expected 0 init containers, got %d", len(ds.Spec.Template.Spec.InitContainers))
	}

	mainContainer := ds.Spec.Template.Spec.Containers[0]
	assertContainerImage(t, mainContainer, "rebellions/rbln-vfio-manager", "latest")
	assertPrivileged(t, mainContainer)

	if mainContainer.SecurityContext.SELinuxOptions == nil || mainContainer.SecurityContext.SELinuxOptions.Level != "s0" {
		t.Fatal("expected SELinuxOptions.Level=s0")
	}
	if mainContainer.Lifecycle == nil || mainContainer.Lifecycle.PreStop == nil {
		t.Fatal("expected PreStop lifecycle hook")
	}
	preStopCmd := strings.Join(mainContainer.Lifecycle.PreStop.Exec.Command, " ")
	if !strings.Contains(preStopCmd, "cleanup --all") {
		t.Fatalf("expected PreStop to invoke cleanup --all, got %q", preStopCmd)
	}

	if ds.Spec.Template.Spec.TerminationGracePeriodSeconds == nil || *ds.Spec.Template.Spec.TerminationGracePeriodSeconds != 180 {
		t.Fatalf("expected TerminationGracePeriodSeconds=180, got %v", ds.Spec.Template.Spec.TerminationGracePeriodSeconds)
	}

	// ConfigMap (vfio-manage.sh)
	assertConfigMapHasKey(t, c, name+"-config", testNamespace, owner.Name, "vfio-manage.sh")

	// Cleanup helpers must be present in the embedded script.
	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, types.NamespacedName{Name: name + "-config", Namespace: testNamespace}, cm); err != nil {
		t.Fatalf("get ConfigMap: %v", err)
	}
	script := cm.Data["vfio-manage.sh"]
	for _, want := range []string{"wait_for_driver_rebind", "probe_device", "cleanup_device", "cleanup_all", "handle_cleanup", "drivers_probe"} {
		if !strings.Contains(script, want) {
			t.Errorf("vfio-manage.sh missing %q", want)
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
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &corev1.ServiceAccount{})
}
