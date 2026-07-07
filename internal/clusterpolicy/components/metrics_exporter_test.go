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

func TestMetricsExporterPatch(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.MetricsExporter = rblnv1beta1.RBLNMetricsExporterSpec{
		Enabled:  true,
		Registry: "docker.io",
		Image:    "rebellions/metrics-exporter",
		Version:  "latest",
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNMetricExporterName
	p := NewMetricsExporterPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	// ServiceAccount
	assertServiceAccountExists(t, c, name, owner.Name)

	// DaemonSet
	ds := assertDaemonSetBasics(t, c, name, owner.Name)
	assertNodeSelector(t, ds, "rebellions.ai/npu.deploy.metrics-exporter")

	mainContainer := ds.Spec.Template.Spec.Containers[0]
	assertContainerImage(t, mainContainer, "rebellions/metrics-exporter", "latest")
	assertPrivileged(t, mainContainer)

	// HostPort is opt-in: unset (0) means no container ports are declared.
	if len(mainContainer.Ports) != 0 {
		t.Fatalf("expected no container ports when HostPort is unset, got %d", len(mainContainer.Ports))
	}

	if len(ds.Spec.Template.Spec.InitContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(ds.Spec.Template.Spec.InitContainers))
	}

	// Service
	svc := &corev1.Service{}
	assertObjectExists(t, c, types.NamespacedName{Name: name + "-service", Namespace: testNamespace}, svc)
	assertHasOwnerRef(t, svc, owner.Name)

	if svc.Annotations["prometheus.io/scrape"] != "true" {
		t.Fatal("Service missing prometheus.io/scrape annotation")
	}
	if svc.Annotations["prometheus.io/port"] != "9090" {
		t.Fatal("Service missing prometheus.io/port annotation")
	}
	if len(svc.Spec.Ports) == 0 || svc.Spec.Ports[0].Port != 9090 {
		t.Fatal("Service port should be 9090")
	}
	if svc.Spec.Selector[consts.LabelAppName] != consts.OperatorName {
		t.Fatalf("Service selector %s = %q, want %q", consts.LabelAppName, svc.Spec.Selector[consts.LabelAppName], consts.OperatorName)
	}
}

func TestMetricsExporterHostPort(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.MetricsExporter = rblnv1beta1.RBLNMetricsExporterSpec{
		Enabled:  true,
		Registry: "docker.io",
		Image:    "rebellions/metrics-exporter",
		Version:  "latest",
		HostPort: 9090,
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNMetricExporterName
	p := NewMetricsExporterPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")
	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	ds := assertDaemonSetBasics(t, c, name, owner.Name)
	mainContainer := ds.Spec.Template.Spec.Containers[0]

	foundPort := false
	for _, port := range mainContainer.Ports {
		if port.ContainerPort == 9090 && port.HostPort == 9090 && port.Protocol == corev1.ProtocolTCP {
			foundPort = true
			break
		}
	}
	if !foundPort {
		t.Fatal("expected container port 9090 with hostPort 9090 TCP")
	}
}

func TestMetricsExporterCleanUp(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.MetricsExporter = rblnv1beta1.RBLNMetricsExporterSpec{
		Enabled:  true,
		Registry: "docker.io",
		Image:    "rebellions/metrics-exporter",
		Version:  "latest",
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNMetricExporterName
	p := NewMetricsExporterPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	if err := p.CleanUp(ctx, owner); err != nil {
		t.Fatalf("CleanUp() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &appsv1.DaemonSet{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name + "-service", Namespace: testNamespace}, &corev1.Service{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &corev1.ServiceAccount{})
}
