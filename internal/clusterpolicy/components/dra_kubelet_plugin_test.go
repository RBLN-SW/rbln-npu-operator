package components

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	rblnv1beta1 "github.com/RBLN-SW/rbln-npu-operator/api/v1beta1"
	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
)

func TestDRAKubeletPluginPatch(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.DRAKubeletPlugin = rblnv1beta1.RBLNDRAKubeletPluginSpec{
		Enabled:                       true,
		Registry:                      "docker.io",
		Image:                         "rebellions/k8s-dra-driver-npu",
		Version:                       "latest",
		KubeletRegistrarDirectoryPath: "/var/lib/kubelet/plugins_registry",
		KubeletPluginsDirectoryPath:   "/var/lib/kubelet/plugins",
		HealthcheckPort:               51515,
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNDRAKubeletPluginName
	p := NewDRAKubeletPluginPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	// ServiceAccount
	assertServiceAccountExists(t, c, name, owner.Name)

	// DaemonSet
	ds := assertDaemonSetBasics(t, c, name, owner.Name)
	assertNodeSelector(t, ds, "rebellions.ai/npu.deploy.dra-kubelet-plugin")
	assertGateInitContainer(t, ds, consts.RBLNDRAKubeletPluginName)
	assertNodeViewerRBAC(t, c, name, owner.Name)

	mainContainer := ds.Spec.Template.Spec.Containers[0]
	assertContainerImage(t, mainContainer, "rebellions/k8s-dra-driver-npu", "latest")
	assertPrivileged(t, mainContainer)
	for _, env := range mainContainer.Env {
		if env.Name == "DRIVER_NAME" {
			t.Fatal("driver identity is fixed by the plugin; the operator must not pass an override")
		}
	}

	if mainContainer.LivenessProbe == nil {
		t.Fatal("expected liveness probe when HealthcheckPort > 0")
	}
	if mainContainer.LivenessProbe.GRPC == nil || mainContainer.LivenessProbe.GRPC.Port != 51515 {
		t.Fatal("liveness probe should use GRPC on port 51515")
	}

	// ClusterRole
	cr := &rbacv1.ClusterRole{}
	assertObjectExists(t, c, types.NamespacedName{Name: name + draClusterRoleSuffix}, cr)
	assertClusterRoleHasRule(t, cr, "resource.k8s.io", "resourceslices")

	// ClusterRoleBinding
	crb := &rbacv1.ClusterRoleBinding{}
	assertObjectExists(t, c, types.NamespacedName{Name: name + draClusterBindSuffix}, crb)
	if crb.RoleRef.Name != name+draClusterRoleSuffix {
		t.Fatalf("ClusterRoleBinding RoleRef.Name = %q, want %q", crb.RoleRef.Name, name+draClusterRoleSuffix)
	}
	if len(crb.Subjects) == 0 || crb.Subjects[0].Name != name {
		t.Fatal("ClusterRoleBinding subject should reference the component ServiceAccount")
	}

	// The component gate retains the VFIO probe and read-only sysfs mount
	// for vm-passthrough nodes, where toolkit-ready never appears.
	draReady := ds.Spec.Template.Spec.InitContainers[0]
	assertPrivileged(t, draReady)
	if draReady.SecurityContext.RunAsUser == nil || *draReady.SecurityContext.RunAsUser != 0 {
		t.Fatal("DRA component gate must run as root for the VFIO probe")
	}
	assertContainerHasVolumeMount(t, draReady, consts.ValidationsVolumeName)
	assertContainerHasVolumeMount(t, draReady, "host-sys")
	for _, m := range draReady.VolumeMounts {
		switch m.Name {
		case "host-sys":
			if !m.ReadOnly {
				t.Fatal("DRA component gate must mount /sys read-only; it only reads driver symlinks")
			}
		case consts.ValidationsVolumeName:
			if m.MountPropagation == nil || *m.MountPropagation != corev1.MountPropagationHostToContainer {
				t.Fatal("DRA component gate validations mount must use HostToContainer propagation so a later toolkit-ready is visible")
			}
		}
	}
	assertPodHasVolume(t, ds.Spec.Template.Spec, "host-sys")

	// No /sys hostPath mount: the privileged container reads the host PCI
	// tree through the runtime-provided sysfs, and an explicit read-only
	// mount would shadow the rw sysfs rbln-smi may rely on.
	for _, m := range mainContainer.VolumeMounts {
		if m.MountPath == "/sys" {
			t.Fatal("the DRA plugin must not mount /sys; the runtime-provided sysfs suffices")
		}
	}

	// DeviceClass: npu (containers, extended-resource bridge)
	dc := &resourcev1.DeviceClass{}
	assertObjectExists(t, c, types.NamespacedName{Name: "npu.rebellions.ai"}, dc)
	if len(dc.Spec.Selectors) == 0 || dc.Spec.Selectors[0].CEL == nil {
		t.Fatal("DeviceClass should have CEL selector")
	}
	wantExpr := fmt.Sprintf("device.driver == %q && device.attributes[%q].type == %q",
		"npu.rebellions.ai", "npu.rebellions.ai", "npu")
	if dc.Spec.Selectors[0].CEL.Expression != wantExpr {
		t.Fatalf("DeviceClass CEL expression = %q, want %q", dc.Spec.Selectors[0].CEL.Expression, wantExpr)
	}
	if dc.Spec.ExtendedResourceName == nil || *dc.Spec.ExtendedResourceName != draExtendedResource {
		t.Fatal("npu DeviceClass should keep the extended-resource bridge")
	}

	// DeviceClass: vfio (VM passthrough, no extended-resource bridge)
	vdc := &resourcev1.DeviceClass{}
	assertObjectExists(t, c, types.NamespacedName{Name: "vfio-npu.rebellions.ai"}, vdc)
	wantVfioExpr := fmt.Sprintf("device.driver == %q && device.attributes[%q].type == %q && has(device.attributes[%q].vf) && !device.attributes[%q].vf",
		"npu.rebellions.ai", "npu.rebellions.ai", "vfio", "npu.rebellions.ai", "npu.rebellions.ai")
	if len(vdc.Spec.Selectors) == 0 || vdc.Spec.Selectors[0].CEL == nil ||
		vdc.Spec.Selectors[0].CEL.Expression != wantVfioExpr {
		t.Fatalf("vfio DeviceClass CEL expression mismatch: %+v", vdc.Spec.Selectors)
	}
	if vdc.Spec.ExtendedResourceName != nil {
		t.Fatal("vfio DeviceClass must not expose an extended resource name")
	}
}

func TestDRAKubeletPluginPatch_ContainerToolkitDisabled(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.ContainerToolkit.Enabled = false
	owner.Spec.DRAKubeletPlugin = rblnv1beta1.RBLNDRAKubeletPluginSpec{
		Enabled:  true,
		Registry: "docker.io",
		Image:    "rebellions/k8s-dra-driver-npu",
		Version:  "latest",
	}

	p := NewDRAKubeletPluginPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")
	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() should succeed without containerToolkit, got: %v", err)
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNDRAKubeletPluginName
	assertDaemonSetBasics(t, c, name, owner.Name)
}

func TestDRAKubeletPluginCleanUp(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	ctx := context.Background()

	owner := newTestOwner()
	owner.Spec.DRAKubeletPlugin = rblnv1beta1.RBLNDRAKubeletPluginSpec{
		Enabled:                       true,
		Registry:                      "docker.io",
		Image:                         "rebellions/k8s-dra-driver-npu",
		Version:                       "latest",
		KubeletRegistrarDirectoryPath: "/var/lib/kubelet/plugins_registry",
		KubeletPluginsDirectoryPath:   "/var/lib/kubelet/plugins",
		HealthcheckPort:               51515,
	}

	name := consts.RBLNBaseName + "-" + consts.RBLNDRAKubeletPluginName
	p := NewDRAKubeletPluginPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "")

	if err := p.Patch(ctx, owner); err != nil {
		t.Fatalf("Patch() error: %v", err)
	}

	if err := p.CleanUp(ctx, owner); err != nil {
		t.Fatalf("CleanUp() error: %v", err)
	}

	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &appsv1.DaemonSet{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name + draClusterRoleSuffix}, &rbacv1.ClusterRole{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name + draClusterBindSuffix}, &rbacv1.ClusterRoleBinding{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: "npu.rebellions.ai"}, &resourcev1.DeviceClass{})
	assertObjectNotExists(t, c, types.NamespacedName{Name: name, Namespace: testNamespace}, &corev1.ServiceAccount{})
	assertNoNodeViewerRBAC(t, c, name)
}

func TestDRAKubeletPluginGatePolicyDefault(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	owner := newTestOwner()
	owner.Spec.DRAKubeletPlugin.Enabled = true
	owner.Spec.DRAKubeletPlugin.HealthcheckPort = 51515
	p := NewDRAKubeletPluginPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "").(*draKubeletPluginPatcher)
	owner.Spec.WorkloadType = consts.RBLNWorkloadConfigContainer
	before := p.buildPodSpec(owner)
	owner.Spec.WorkloadType = consts.RBLNWorkloadConfigVMPassthrough
	after := p.buildPodSpec(owner)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("policy default change rolls DRA before claim Unprepare")
	}
	if envValue(after.InitContainers[0].Env, "CLUSTER_POLICY_NAME") != owner.Name || envValue(after.InitContainers[0].Env, "DEFAULT_WORKLOAD_TYPE") != "" {
		t.Fatal("DRA must look up policy default at init time")
	}
}

func TestNativeVFDeviceClassesAreCreatedAndCleaned(t *testing.T) {
	const driver = "npu.rebellions.ai"
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	owner := newTestOwner()
	owner.Spec.DRAKubeletPlugin.Enabled = true
	p := NewDRAKubeletPluginPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "").(*draKubeletPluginPatcher)
	if err := p.handleDRAClass(context.Background(), owner); err != nil {
		t.Fatal(err)
	}
	for _, count := range []int{1, 4} {
		dc := &resourcev1.DeviceClass{}
		assertObjectExists(t, c, types.NamespacedName{Name: fmt.Sprintf("npu-vf%d.rebellions.ai", count)}, dc)
		expression := dc.Spec.Selectors[0].CEL.Expression
		if !strings.Contains(expression, `type == "npu-vf"`) || !strings.Contains(expression, fmt.Sprintf(`partitionMode == "vf%d"`, count)) || !strings.Contains(expression, fmt.Sprintf(`device.driver == %q`, driver)) {
			t.Fatalf("incorrect VF selector: %s", expression)
		}
		if dc.Spec.ExtendedResourceName == nil || *dc.Spec.ExtendedResourceName != fmt.Sprintf("rebellions.ai/npu-vf%d", count) {
			t.Fatal("missing VF extended resource mapping")
		}
	}
	if err := p.deleteDeviceClass(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"npu.rebellions.ai", "vfio-npu.rebellions.ai", "npu-vf1.rebellions.ai", "npu-vf4.rebellions.ai"} {
		assertObjectNotExists(t, c, types.NamespacedName{Name: name}, &resourcev1.DeviceClass{})
	}
}

func TestVFIODeviceClassesSelectTheirProfileAndAreCleaned(t *testing.T) {
	// Evaluate the selectors against published attributes, including inventories
	// from before partitionMode was introduced. A legacy VF must not match PF.
	env, err := cel.NewEnv(cel.Variable("device", cel.DynType))
	if err != nil {
		t.Fatal(err)
	}
	names := struct {
		driver, pf, vf1, vf4 string
	}{"npu.rebellions.ai", "vfio-npu.rebellions.ai", "vfio-npu-vf1.rebellions.ai", "vfio-npu-vf4.rebellions.ai"}
	ctx := context.Background()
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	owner := newTestOwner()
	owner.Spec.DRAKubeletPlugin.Enabled = true
	p := NewDRAKubeletPluginPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "").(*draKubeletPluginPatcher)
	// An existing installation must also replace its broad VFIO selector.
	legacyClass := &resourcev1.DeviceClass{}
	legacyClass.Name = names.pf
	legacyClass.Spec.Selectors = []resourcev1.DeviceSelector{{CEL: &resourcev1.CELDeviceSelector{
		Expression: fmt.Sprintf("device.driver == %q && device.attributes[%q].type == %q", names.driver, names.driver, "vfio"),
	}}}
	if err := c.Create(ctx, legacyClass); err != nil {
		t.Fatal(err)
	}
	if err := p.Patch(ctx, owner); err != nil {
		t.Fatal(err)
	}
	classes := &resourcev1.DeviceClassList{}
	if err := c.List(ctx, classes); err != nil {
		t.Fatal(err)
	}
	if len(classes.Items) != 6 {
		t.Fatalf("created %d classes, want 6", len(classes.Items))
	}
	for _, className := range []string{names.pf, names.vf1, names.vf4} {
		t.Run(className, func(t *testing.T) {
			dc := &resourcev1.DeviceClass{}
			assertObjectExists(t, c, types.NamespacedName{Name: className}, dc)
			if dc.Spec.ExtendedResourceName != nil {
				t.Fatal("VFIO classes must not expose an extended resource name")
			}
			if len(dc.Spec.Selectors) != 1 || dc.Spec.Selectors[0].CEL == nil {
				t.Fatal("expected one CEL selector")
			}
			ast, issues := env.Compile(dc.Spec.Selectors[0].CEL.Expression)
			if issues.Err() != nil {
				t.Fatal(issues.Err())
			}
			program, err := env.Program(ast)
			if err != nil {
				t.Fatal(err)
			}
			for _, candidate := range []struct {
				name, kind, mode, wantClass string
				vf                          *bool
				otherDriver                 bool
			}{
				{name: "PF", kind: "vfio", mode: "none", vf: ptr(false), wantClass: names.pf},
				{name: "VF1", kind: "vfio", mode: "vf1", vf: ptr(true), wantClass: names.vf1},
				{name: "VF4", kind: "vfio", mode: "vf4", vf: ptr(true), wantClass: names.vf4},
				{name: "legacy PF", kind: "vfio", vf: ptr(false), wantClass: names.pf},
				{name: "legacy VF without profile", kind: "vfio", vf: ptr(true)},
				{name: "missing VF attribute", kind: "vfio", mode: "vf4"},
				{name: "unsupported VF profile", kind: "vfio", mode: "vf2", vf: ptr(true)},
				{name: "native PF", kind: "npu", mode: "none", vf: ptr(false)},
				{name: "native VF", kind: "npu-vf", mode: "vf4", vf: ptr(true)},
				{name: "other driver PF", kind: "vfio", mode: "none", vf: ptr(false), otherDriver: true},
				{name: "other driver VF", kind: "vfio", mode: "vf4", vf: ptr(true), otherDriver: true},
			} {
				t.Run(candidate.name, func(t *testing.T) {
					attrs := map[string]any{"type": candidate.kind}
					if candidate.mode != "" {
						attrs["partitionMode"] = candidate.mode
					}
					if candidate.vf != nil {
						attrs["vf"] = *candidate.vf
					}
					driver := names.driver
					if candidate.otherDriver {
						driver = "other.example.com"
					}
					result, _, err := program.Eval(map[string]any{"device": map[string]any{
						"driver": driver, "attributes": map[string]any{names.driver: attrs},
					}})
					if err != nil {
						t.Fatal(err)
					}
					if want := candidate.wantClass == className; result.Value() != want {
						t.Fatalf("matched=%v, want %v", result.Value(), want)
					}
				})
			}
		})
	}
	if err := p.CleanUp(ctx, owner); err != nil {
		t.Fatal(err)
	}
	remaining := &resourcev1.DeviceClassList{}
	if err := c.List(ctx, remaining); err != nil {
		t.Fatal(err)
	}
	if len(remaining.Items) != 0 {
		t.Fatalf("cleanup left %d classes", len(remaining.Items))
	}
}
