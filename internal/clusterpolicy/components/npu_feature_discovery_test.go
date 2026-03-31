package components

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func TestNPUFeatureDiscoveryPatch(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.NPUFeatureDiscovery = rblnv1beta1.RBLNNPUFeatureDiscoverySpec{
		Enabled:  true,
		Registry: "docker.io",
		Image:    "rebellions/npu-feature-discovery",
		Version:  "latest",
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNFeatureDiscoveryName
	p := NewNPUFeatureDiscoveryPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	// ServiceAccount
	assertServiceAccountExists(t, c, name, owner.Name)

	// DaemonSet
	ds := assertDaemonSetBasics(t, c, name, owner.Name)
	assertNodeSelector(t, ds, "rebellions.ai/npu.deploy.npu-feature-discovery")

	mainContainer := ds.Spec.Template.Spec.Containers[0]
	assertContainerImage(t, mainContainer, "rebellions/npu-feature-discovery", "latest")
	assertPrivileged(t, mainContainer)

	if len(ds.Spec.Template.Spec.InitContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(ds.Spec.Template.Spec.InitContainers))
	}
	initContainer := ds.Spec.Template.Spec.InitContainers[0]
	if initContainer.Name != "toolkit-validation" {
		t.Fatalf("init container name = %q, want toolkit-validation", initContainer.Name)
	}
	assertContainerImage(t, initContainer, "rebellions/rbln-validator", "v1.0")
}

func TestNPUFeatureDiscoveryCleanUp(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.NPUFeatureDiscovery = rblnv1beta1.RBLNNPUFeatureDiscoverySpec{
		Enabled:  true,
		Registry: "docker.io",
		Image:    "rebellions/npu-feature-discovery",
		Version:  "latest",
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNFeatureDiscoveryName
	p := NewNPUFeatureDiscoveryPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	if err := p.CleanUp(ctx, owner); err != nil {
		t.Fatalf("CleanUp() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &appsv1.DaemonSet{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &corev1.ServiceAccount{})
}
