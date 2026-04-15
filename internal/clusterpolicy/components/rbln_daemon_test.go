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

func TestRBLNDaemonPatch(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.RBLNDaemon = rblnv1beta1.RBLNDaemonSpec{
		Enabled:  true,
		Registry: "docker.io",
		Image:    "rebellions/rbln-daemon",
		Version:  "latest",
	}

	name := consts.RBLNDaemonName
	p := NewRBLNDaemonPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	// ServiceAccount
	assertServiceAccountExists(t, c, name, owner.Name)

	// DaemonSet
	ds := assertDaemonSetBasics(t, c, name, owner.Name)
	assertNodeSelector(t, ds, "rebellions.ai/npu.deploy.rbln-daemon")

	if len(ds.Spec.Template.Spec.InitContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(ds.Spec.Template.Spec.InitContainers))
	}

	mainContainer := ds.Spec.Template.Spec.Containers[0]
	assertContainerImage(t, mainContainer, "rebellions/rbln-daemon", "latest")
	assertPrivileged(t, mainContainer)

	if mainContainer.Command[0] != rblnDaemonCommand {
		t.Fatalf("command = %q, want %q", mainContainer.Command[0], rblnDaemonCommand)
	}

	foundPort := false
	for _, port := range mainContainer.Ports {
		if port.ContainerPort == 50051 && port.HostPort == 50051 && port.Protocol == corev1.ProtocolTCP {
			foundPort = true
			break
		}
	}
	if !foundPort {
		t.Fatal("expected container port 50051 with hostPort 50051 TCP")
	}

	// Service
	svc := &corev1.Service{}
	assertObjectExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, svc)
	assertHasOwnerRef(t, svc, owner.Name)

	if svc.Spec.InternalTrafficPolicy == nil || *svc.Spec.InternalTrafficPolicy != corev1.ServiceInternalTrafficPolicyLocal {
		t.Fatal("expected InternalTrafficPolicy=Local")
	}
	if len(svc.Spec.Ports) == 0 || svc.Spec.Ports[0].Port != 50051 {
		t.Fatal("Service port should be 50051")
	}
	if svc.Spec.Selector[consts.LabelAppName] != consts.OperatorName {
		t.Fatalf("Service selector %s = %q, want %q", consts.LabelAppName, svc.Spec.Selector[consts.LabelAppName], consts.OperatorName)
	}
}

func TestRBLNDaemonCleanUp(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.RBLNDaemon = rblnv1beta1.RBLNDaemonSpec{
		Enabled:  true,
		Registry: "docker.io",
		Image:    "rebellions/rbln-daemon",
		Version:  "latest",
	}

	name := consts.RBLNDaemonName
	p := NewRBLNDaemonPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	if err := p.CleanUp(ctx, owner); err != nil {
		t.Fatalf("CleanUp() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &appsv1.DaemonSet{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &corev1.Service{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &corev1.ServiceAccount{})
}
