package components

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// newSmdTestOwner mirrors the CRD defaults the fake client does not apply.
func newSmdTestOwner() *rebellionsaiv1alpha1.RBLNDriver {
	owner := newTestOwner()
	owner.Spec.Smd = rebellionsaiv1alpha1.SmdSpec{
		Registry: "docker.io",
		Image:    "rebellions/rbln-daemon",
	}
	owner.Spec.Manager = rebellionsaiv1alpha1.DriverManagerSpec{
		Registry: "docker.io",
		Image:    "rebellions/rbln-k8s-driver-manager",
		Version:  "1.0.0",
	}
	return owner
}

func smdDaemonSetKey() types.NamespacedName {
	return types.NamespacedName{Name: testInstanceName + "-" + smdDaemonSetSuffix, Namespace: testNamespace}
}

// assertSmdContainerContract pins the port and security settings the host and
// external consumers depend on.
func assertSmdContainerContract(t *testing.T, container corev1.Container) {
	t.Helper()
	port := container.Ports[0]
	// 50051 is a cross-repo contract (metrics-exporter and npu-feature-
	// discovery dial $(NODE_IP):50051), so pin the literal, not the const
	// the production code itself reads.
	if port.ContainerPort != 50051 {
		t.Fatalf("containerPort = %d, want 50051", port.ContainerPort)
	}
	if port.HostPort != 0 {
		t.Fatalf("hostPort = %d, want 0 (hostNetwork exposes the port by itself; an explicit hostPort re-adds portmap DNAT)", port.HostPort)
	}
	if port.Protocol != corev1.ProtocolTCP {
		t.Fatalf("port protocol = %q, want TCP", port.Protocol)
	}
	sc := container.SecurityContext
	if sc == nil || sc.Privileged == nil || !*sc.Privileged {
		t.Fatal("smd container must run privileged")
	}
	if sc.RunAsUser == nil || *sc.RunAsUser != 0 {
		t.Fatalf("runAsUser = %v, want 0", sc.RunAsUser)
	}
}

func TestNewSmdPatcher_NilDriver(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)

	_, err := NewSmdPatcher(c, c, logf.Log, testNamespace, nil, scheme, "")
	if err == nil {
		t.Fatal("expected error for nil driver, got nil")
	}
}

func TestSmdPatcher_Patch(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newSmdTestOwner()
	p, err := NewSmdPatcher(c, c, logf.Log, testNamespace, owner, scheme, "")
	if err != nil {
		t.Fatalf("NewSmdPatcher() error: %v", err)
	}

	if err := p.Patch(ctx, owner, true); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	sa := &corev1.ServiceAccount{}
	assertObjectExists(t, c, types.NamespacedName{Name: smdName, Namespace: testNamespace}, sa)
	assertHasOwnerRef(t, sa, owner.Name)

	ds := &appsv1.DaemonSet{}
	assertObjectExists(t, c, smdDaemonSetKey(), ds)

	// The upgrade controller sweeps DaemonSets whose component label equals
	// the driver's; smd must never carry that value.
	if got := ds.Labels[driverManagerAppLabelKey]; got != smdName || got == driverManagerName {
		t.Fatalf("component label = %q, want %q (and never %q)", got, smdName, driverManagerName)
	}
	if got := ds.Labels[driverManagerInstanceLabelKey]; got != testInstanceName {
		t.Fatalf("instance label = %q, want %q", got, testInstanceName)
	}
	if got := ds.Spec.Selector.MatchLabels[driverManagerAppLabelKey]; got != smdName {
		t.Fatalf("selector component label = %q, want %q", got, smdName)
	}

	if got := ds.Spec.UpdateStrategy.Type; got != appsv1.OnDeleteDaemonSetStrategyType {
		t.Fatalf("update strategy = %q, want OnDelete", got)
	}

	wantSelector := map[string]string{
		driverManagerDeployLabelKey:         "true",
		consts.RBLNDriverOwnerLabelKey:      testInstanceName,
		consts.RBLNDeployRBLNDaemonLabelKey: "true",
	}
	for key, want := range wantSelector {
		if got := ds.Spec.Template.Spec.NodeSelector[key]; got != want {
			t.Fatalf("nodeSelector[%s] = %q, want %q", key, got, want)
		}
	}

	podSpec := ds.Spec.Template.Spec
	if !podSpec.HostNetwork {
		t.Fatal("expected hostNetwork to be enabled")
	}
	if podSpec.TerminationGracePeriodSeconds == nil || *podSpec.TerminationGracePeriodSeconds != 0 {
		t.Fatalf("terminationGracePeriodSeconds = %v, want 0", podSpec.TerminationGracePeriodSeconds)
	}

	container := podSpec.Containers[0]
	if want := "docker.io/rebellions/rbln-daemon:" + owner.Spec.Version; container.Image != want {
		t.Fatalf("smd image = %q, want %q (tag must equal spec.version)", container.Image, want)
	}
	if container.Command[0] != smdCommand {
		t.Fatalf("command = %q, want %q", container.Command[0], smdCommand)
	}
	assertSmdContainerContract(t, container)

	if len(podSpec.InitContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(podSpec.InitContainers))
	}
	initContainer := podSpec.InitContainers[0]
	if initContainer.Name != smdInitContainerName {
		t.Fatalf("init container name = %q, want %q", initContainer.Name, smdInitContainerName)
	}
	if want := "docker.io/rebellions/rbln-k8s-driver-manager:1.0.0"; initContainer.Image != want {
		t.Fatalf("init container image = %q, want %q", initContainer.Image, want)
	}
	if !strings.Contains(initContainer.Args[0], driverCtrReadyFile) {
		t.Fatalf("init container args %q must wait for %s", initContainer.Args[0], driverCtrReadyFile)
	}

	var controllerRef *metav1.OwnerReference
	for i := range ds.OwnerReferences {
		if ds.OwnerReferences[i].Controller != nil && *ds.OwnerReferences[i].Controller {
			controllerRef = &ds.OwnerReferences[i]
		}
	}
	if controllerRef == nil || controllerRef.Name != owner.Name {
		t.Fatalf("DaemonSet must carry a controller ownerReference to %s, got %+v", owner.Name, ds.OwnerReferences)
	}
}

// Every other test uses coordinates identical to the CRD defaults (and to the
// manager spec's registry), so only this test can distinguish "image comes
// from spec.smd" from a hardcoded default or an accidental manager-spec read.
func TestSmdPatcher_Patch_CustomImageCoordinates(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newSmdTestOwner()
	owner.Spec.Smd = rebellionsaiv1alpha1.SmdSpec{
		Registry: "registry.example.com",
		Image:    "myorg/custom-smd",
	}
	p, err := NewSmdPatcher(c, c, logf.Log, testNamespace, owner, scheme, "")
	if err != nil {
		t.Fatalf("NewSmdPatcher() error: %v", err)
	}
	if err := p.Patch(ctx, owner, true); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	ds := &appsv1.DaemonSet{}
	assertObjectExists(t, c, smdDaemonSetKey(), ds)
	got := ds.Spec.Template.Spec.Containers[0].Image
	if want := "registry.example.com/myorg/custom-smd:" + owner.Spec.Version; got != want {
		t.Fatalf("smd image = %q, want %q (must come from spec.smd)", got, want)
	}
}

func TestSmdPatcher_Patch_GateOff(t *testing.T) {
	scheme := newTestScheme(t)
	existing := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testInstanceName + "-" + smdDaemonSetSuffix,
			Namespace: testNamespace,
			Labels:    map[string]string{driverManagerInstanceLabelKey: testInstanceName},
		},
	}
	c := newFakeClient(t, scheme, existing)
	ctx := context.Background()

	owner := newSmdTestOwner()
	p, err := NewSmdPatcher(c, c, logf.Log, testNamespace, owner, scheme, "")
	if err != nil {
		t.Fatalf("NewSmdPatcher() error: %v", err)
	}

	if err := p.Patch(ctx, owner, false); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	assertObjectNotExists(t, c, smdDaemonSetKey(), &appsv1.DaemonSet{})
	// Shared RBAC is still reconciled even while the DaemonSet is gated off.
	assertObjectExists(t, c, types.NamespacedName{Name: smdName, Namespace: testNamespace}, &corev1.ServiceAccount{})
}

func TestSmdPatcher_Patch_EmptyVersionKeepsExisting(t *testing.T) {
	scheme := newTestScheme(t)
	existing := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testInstanceName + "-" + smdDaemonSetSuffix,
			Namespace: testNamespace,
			Labels:    map[string]string{driverManagerInstanceLabelKey: testInstanceName},
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"a": "b"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"a": "b"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: smdName, Image: "docker.io/rebellions/rbln-daemon:2.0.0"}},
				},
			},
		},
	}
	c := newFakeClient(t, scheme, existing)
	ctx := context.Background()

	owner := newSmdTestOwner()
	owner.Spec.Version = ""
	p, err := NewSmdPatcher(c, c, logf.Log, testNamespace, owner, scheme, "")
	if err != nil {
		t.Fatalf("NewSmdPatcher() error: %v", err)
	}

	if err := p.Patch(ctx, owner, true); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	ds := &appsv1.DaemonSet{}
	assertObjectExists(t, c, smdDaemonSetKey(), ds)
	if got := ds.Spec.Template.Spec.Containers[0].Image; got != "docker.io/rebellions/rbln-daemon:2.0.0" {
		t.Fatalf("existing DaemonSet must be left as-is with an empty version, image became %q", got)
	}
}

// The core contract of the component: a driver version bump must update the
// smd DaemonSet template in place (pods are then replaced per node by the
// driver-manager eviction cycle, not by this update).
func TestSmdPatcher_Patch_UpdatesTemplateOnVersionBump(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newSmdTestOwner()
	owner.Spec.Version = "2.0.0"
	p, err := NewSmdPatcher(c, c, logf.Log, testNamespace, owner, scheme, "")
	if err != nil {
		t.Fatalf("NewSmdPatcher() error: %v", err)
	}
	if err := p.Patch(ctx, owner, true); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	owner.Spec.Version = "3.0.0"
	if err := p.Patch(ctx, owner, true); err != nil {
		t.Fatalf("Patch() after version bump error: %v", err)
	}

	ds := &appsv1.DaemonSet{}
	assertObjectExists(t, c, smdDaemonSetKey(), ds)
	if got, want := ds.Spec.Template.Spec.Containers[0].Image, "docker.io/rebellions/rbln-daemon:3.0.0"; got != want {
		t.Fatalf("image after version bump = %q, want %q", got, want)
	}
	if got := ds.Spec.UpdateStrategy.Type; got != appsv1.OnDeleteDaemonSetStrategyType {
		t.Fatalf("update strategy after update = %q, want OnDelete", got)
	}
}

func TestSmdPatcher_Patch_DeletesLegacyDaemonSet(t *testing.T) {
	scheme := newTestScheme(t)
	legacy := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: legacyRBLNDaemonName, Namespace: testNamespace},
	}
	c := newFakeClient(t, scheme, legacy)
	ctx := context.Background()

	owner := newSmdTestOwner()
	p, err := NewSmdPatcher(c, c, logf.Log, testNamespace, owner, scheme, "")
	if err != nil {
		t.Fatalf("NewSmdPatcher() error: %v", err)
	}

	if err := p.Patch(ctx, owner, true); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{Name: legacyRBLNDaemonName, Namespace: testNamespace}, &appsv1.DaemonSet{})
	assertObjectExists(t, c, smdDaemonSetKey(), &appsv1.DaemonSet{})
}

func TestSmdPatcher_Patch_RefusesCrossInstanceAdoption(t *testing.T) {
	scheme := newTestScheme(t)
	foreign := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testInstanceName + "-" + smdDaemonSetSuffix,
			Namespace: testNamespace,
			Labels:    map[string]string{driverManagerInstanceLabelKey: "other-instance"},
		},
	}
	c := newFakeClient(t, scheme, foreign)
	ctx := context.Background()

	owner := newSmdTestOwner()
	p, err := NewSmdPatcher(c, c, logf.Log, testNamespace, owner, scheme, "")
	if err != nil {
		t.Fatalf("NewSmdPatcher() error: %v", err)
	}

	err = p.Patch(ctx, owner, true)
	if err == nil || !strings.Contains(err.Error(), "refusing to adopt") {
		t.Fatalf("expected cross-instance adoption error, got: %v", err)
	}
}

func TestSmdPatcher_Status(t *testing.T) {
	cases := map[string]struct {
		seed      *appsv1.DaemonSet
		wantNil   bool
		wantState rebellionsaiv1alpha1.DriverPoolState
	}{
		"absent DaemonSet yields nil": {
			wantNil: true,
		},
		"all pods ready": {
			seed: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Name: testInstanceName + "-" + smdDaemonSetSuffix, Namespace: testNamespace},
				Status:     appsv1.DaemonSetStatus{DesiredNumberScheduled: 2, NumberReady: 2},
			},
			wantState: rebellionsaiv1alpha1.DriverPoolStateReady,
		},
		"pods catching up": {
			seed: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Name: testInstanceName + "-" + smdDaemonSetSuffix, Namespace: testNamespace},
				Status:     appsv1.DaemonSetStatus{DesiredNumberScheduled: 2, NumberReady: 1},
			},
			wantState: rebellionsaiv1alpha1.DriverPoolStateProgressing,
		},
		"zero desired counts as progressing": {
			seed: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Name: testInstanceName + "-" + smdDaemonSetSuffix, Namespace: testNamespace},
				Status:     appsv1.DaemonSetStatus{},
			},
			wantState: rebellionsaiv1alpha1.DriverPoolStateProgressing,
		},
		"unavailable pods count as progressing": {
			seed: &appsv1.DaemonSet{
				ObjectMeta: metav1.ObjectMeta{Name: testInstanceName + "-" + smdDaemonSetSuffix, Namespace: testNamespace},
				Status:     appsv1.DaemonSetStatus{DesiredNumberScheduled: 2, NumberReady: 2, NumberUnavailable: 1},
			},
			wantState: rebellionsaiv1alpha1.DriverPoolStateProgressing,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			scheme := newTestScheme(t)
			var seed []client.Object
			if tc.seed != nil {
				seed = append(seed, tc.seed)
			}
			c := newFakeClient(t, scheme, seed...)

			owner := newSmdTestOwner()
			p, err := NewSmdPatcher(c, c, logf.Log, testNamespace, owner, scheme, "")
			if err != nil {
				t.Fatalf("NewSmdPatcher() error: %v", err)
			}

			status, err := p.Status(context.Background())
			if err != nil {
				t.Fatalf("Status() error: %v", err)
			}
			if tc.wantNil {
				if status != nil {
					t.Fatalf("expected nil status, got %+v", status)
				}
				return
			}
			if status == nil {
				t.Fatal("expected status, got nil")
			}
			if status.State != tc.wantState {
				t.Fatalf("state = %q, want %q", status.State, tc.wantState)
			}
			if status.Desired != tc.seed.Status.DesiredNumberScheduled || status.Ready != tc.seed.Status.NumberReady {
				t.Fatalf("counts = %d/%d, want %d/%d", status.Ready, status.Desired,
					tc.seed.Status.NumberReady, tc.seed.Status.DesiredNumberScheduled)
			}
		})
	}
}
