package components

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func TestDevicePluginPatch(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.DevicePlugin = rblnv1beta1.RBLNDevicePluginSpec{
		Enabled:  true,
		Registry: "docker.io",
		Image:    "rebellions/k8s-device-plugin",
		Version:  "latest",
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNDevicePluginName
	p := NewDevicePluginPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	// ServiceAccount
	assertServiceAccountExists(t, c, name, owner.Name)

	// DaemonSet
	ds := assertDaemonSetBasics(t, c, name, owner.Name)
	assertNodeSelector(t, ds, "rebellions.ai/npu.deploy.device-plugin")

	mainContainer := ds.Spec.Template.Spec.Containers[0]
	assertContainerImage(t, mainContainer, "rebellions/k8s-device-plugin", "latest")
	assertPrivileged(t, mainContainer)
	if len(mainContainer.Args) != 0 {
		t.Fatalf("container-mode device-plugin must not override args, got %v", mainContainer.Args)
	}

	// NODE_NAME feeds the plugin's k8s.node.name span attribute, so it must be
	// injected unconditionally. APIVersion must be explicit: the kube-apiserver
	// defaults it to "v1" on persist, so omitting it triggers a perpetual
	// reconcile/patch loop (operator submits "", server stores "v1", diff, repeat).
	fr := envFieldRef(mainContainer.Env, "NODE_NAME")
	if fr == nil {
		t.Fatalf("NODE_NAME env var missing or has no FieldRef")
	}
	if fr.APIVersion != "v1" || fr.FieldPath != "spec.nodeName" {
		t.Fatalf("NODE_NAME FieldRef = %+v, want {APIVersion: v1, FieldPath: spec.nodeName}", fr)
	}

	if len(ds.Spec.Template.Spec.InitContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(ds.Spec.Template.Spec.InitContainers))
	}
	assertContainerImage(t, ds.Spec.Template.Spec.InitContainers[0], "rebellions/rbln-validator", "v1.0")
}

func TestDevicePluginPatch_Disabled(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.DevicePlugin = rblnv1beta1.RBLNDevicePluginSpec{Enabled: false}

	p := NewDevicePluginPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("disabled Patch() should not error: %v", err)
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNDevicePluginName
	sa := &corev1.ServiceAccount{}
	if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: testNamespace}, sa); err == nil {
		t.Fatal("ServiceAccount should not be created when component is disabled")
	}
}

func TestDevicePluginCleanUp(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.DevicePlugin = rblnv1beta1.RBLNDevicePluginSpec{
		Enabled:  true,
		Registry: "docker.io",
		Image:    "rebellions/k8s-device-plugin",
		Version:  "latest",
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNDevicePluginName
	p := NewDevicePluginPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	if err := p.CleanUp(ctx, owner); err != nil {
		t.Fatalf("CleanUp() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &appsv1.DaemonSet{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &corev1.ServiceAccount{})
}

func TestDevicePluginPatch_UseGenericResourceName(t *testing.T) {
	tests := map[string]struct {
		field bool
		want  string
	}{
		"true":  {field: true, want: "true"},
		"false": {field: false, want: "false"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			scheme := newTestScheme(t)
			c := newFakeClient(t, scheme)
			ctx := context.Background()

			owner := newTestOwner()
			owner.Spec.DevicePlugin = rblnv1beta1.RBLNDevicePluginSpec{
				Enabled:                true,
				Registry:               "docker.io",
				Image:                  "rebellions/k8s-device-plugin",
				Version:                "latest",
				UseGenericResourceName: tc.field,
			}

			p := NewDevicePluginPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")
			if err := p.Patch(ctx, owner); err != nil {
				t.Fatalf("Patch() error: %v", err)
			}

			dsName := consts.RBLNBaseName + "-" + consts.RBLNDevicePluginName
			ds := assertDaemonSetBasics(t, c, dsName, owner.Name)
			got, ok := envVarValue(ds.Spec.Template.Spec.Containers[0].Env, "USE_GENERIC_RESOURCE_NAME")
			if !ok {
				t.Fatalf("USE_GENERIC_RESOURCE_NAME env var not found")
			}
			if got != tc.want {
				t.Fatalf("USE_GENERIC_RESOURCE_NAME = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDevicePluginPatch_OtlpEndpoint(t *testing.T) {
	tests := map[string]struct {
		endpoint string
		want     string
		wantSet  bool
	}{
		"set":   {endpoint: "otel-collector:4317", want: "otel-collector:4317", wantSet: true},
		"unset": {endpoint: "", want: "", wantSet: false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			scheme := newTestScheme(t)
			c := newFakeClient(t, scheme)
			ctx := context.Background()

			owner := newTestOwner()
			owner.Spec.DevicePlugin = rblnv1beta1.RBLNDevicePluginSpec{
				Enabled:      true,
				Registry:     "docker.io",
				Image:        "rebellions/k8s-device-plugin",
				Version:      "latest",
				OtlpEndpoint: tc.endpoint,
			}

			p := NewDevicePluginPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")
			if err := p.Patch(ctx, owner); err != nil {
				t.Fatalf("Patch() error: %v", err)
			}

			dsName := consts.RBLNBaseName + "-" + consts.RBLNDevicePluginName
			ds := assertDaemonSetBasics(t, c, dsName, owner.Name)
			got, ok := envVarValue(ds.Spec.Template.Spec.Containers[0].Env, "OTEL_EXPORTER_OTLP_ENDPOINT")
			if ok != tc.wantSet {
				t.Fatalf("OTEL_EXPORTER_OTLP_ENDPOINT present = %v, want %v", ok, tc.wantSet)
			}
			if got != tc.want {
				t.Fatalf("OTEL_EXPORTER_OTLP_ENDPOINT = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDevicePluginPatch_OpenShift(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.DevicePlugin = rblnv1beta1.RBLNDevicePluginSpec{
		Enabled:  true,
		Registry: "docker.io",
		Image:    "rebellions/k8s-device-plugin",
		Version:  "latest",
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNDevicePluginName
	p := NewDevicePluginPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "4.15")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	// OpenShift Role with SCC rule
	role := &rbacv1.Role{}
	assertObjectExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, role)
	assertHasOwnerRef(t, role, owner.Name)
	assertRoleHasRule(t, role, "security.openshift.io", "securitycontextconstraints")

	// OpenShift RoleBinding
	rb := &rbacv1.RoleBinding{}
	assertObjectExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, rb)
	assertHasOwnerRef(t, rb, owner.Name)
}
