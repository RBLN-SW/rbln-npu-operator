package components

import (
	"context"
	"encoding/json"
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

func TestBuildDevicePluginConfig(t *testing.T) {
	tests := map[string]struct {
		resourceList []rblnv1beta1.RBLNDevicePluginResourceSpec
		wantErr      bool
		errContains  string
		wantStrings  []string
		noStrings    []string
	}{
		"valid resource list": {
			resourceList: []rblnv1beta1.RBLNDevicePluginResourceSpec{
				{
					ResourceName:     "ATOM",
					ResourcePrefix:   "rebellions.ai",
					ProductCardNames: []string{"RBLN-CA22", "RBLN-CA25"},
				},
			},
			wantStrings: []string{
				`"resourceName": "ATOM"`,
				`"resourcePrefix": "rebellions.ai"`,
				`"deviceType": "accelerator"`,
				`"1220"`, `"1221"`, `"1250"`, `"1251"`,
			},
			noStrings: []string{`"1120"`, `"1121"`},
		},
		"invalid product card name": {
			resourceList: []rblnv1beta1.RBLNDevicePluginResourceSpec{
				{
					ResourceName:     "ATOM",
					ResourcePrefix:   "rebellions.ai",
					ProductCardNames: []string{"RBLN-CA12", "INVALID"},
				},
			},
			wantErr:     true,
			errContains: "unknown product card name: INVALID",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			patcher := &devicePluginPatcher{
				desiredSpec: &rblnv1beta1.RBLNDevicePluginSpec{
					ResourceList: tc.resourceList,
				},
			}

			result, err := patcher.buildDevicePluginConfig()
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("error %q should contain %q", err.Error(), tc.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for _, s := range tc.wantStrings {
				if !strings.Contains(result, s) {
					t.Errorf("result should contain %q", s)
				}
			}
			for _, s := range tc.noStrings {
				if strings.Contains(result, s) {
					t.Errorf("result should NOT contain %q", s)
				}
			}
		})
	}
}

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
		ResourceList: []rblnv1beta1.RBLNDevicePluginResourceSpec{
			{
				ResourceName:     "ATOM",
				ResourcePrefix:   "rebellions.ai",
				ProductCardNames: []string{consts.RBLNCardCA12},
			},
		},
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

	if len(ds.Spec.Template.Spec.InitContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(ds.Spec.Template.Spec.InitContainers))
	}
	assertContainerImage(t, ds.Spec.Template.Spec.InitContainers[0], "rebellions/rbln-validator", "v1.0")

	// ConfigMap
	cm := &corev1.ConfigMap{}
	assertObjectExists(t, c, types.NamespacedName{Name: name + "-config", Namespace: testNamespace}, cm)
	assertHasOwnerRef(t, cm, owner.Name)

	var cfg configResourceList
	if err := json.Unmarshal([]byte(cm.Data["config.json"]), &cfg); err != nil {
		t.Fatalf("config.json is not valid JSON: %v", err)
	}
	if len(cfg.ResourceList) != 1 {
		t.Fatalf("expected 1 resource, got %d", len(cfg.ResourceList))
	}
	if cfg.ResourceList[0].ResourceName != "ATOM" {
		t.Fatalf("resourceName = %q, want ATOM", cfg.ResourceList[0].ResourceName)
	}
	if cfg.ResourceList[0].Selectors.Drivers[0] != consts.RBLNDriverName {
		t.Fatalf("driver = %q, want %q", cfg.ResourceList[0].Selectors.Drivers[0], consts.RBLNDriverName)
	}
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
		ResourceList: []rblnv1beta1.RBLNDevicePluginResourceSpec{
			{
				ResourceName:     "ATOM",
				ResourcePrefix:   "rebellions.ai",
				ProductCardNames: []string{consts.RBLNCardCA12},
			},
		},
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
	assertObjectNotExists(t, c, types.NamespacedName{Name: name + "-config", Namespace: testNamespace}, &corev1.ConfigMap{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &corev1.ServiceAccount{})
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
		ResourceList: []rblnv1beta1.RBLNDevicePluginResourceSpec{
			{
				ResourceName:     "ATOM",
				ResourcePrefix:   "rebellions.ai",
				ProductCardNames: []string{consts.RBLNCardCA12},
			},
		},
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
