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

	rblnv1beta1 "github.com/RBLN-SW/rbln-npu-operator/api/v1beta1"
	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
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
	assertClusterRoleHasRule(t, cr, "rebellions.ai", "rblndrivers")

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

	if len(ds.Spec.Template.Spec.InitContainers) != 1 {
		t.Fatal("partition-manager requires a driver readiness init container")
	}
	gate := ds.Spec.Template.Spec.InitContainers[0]
	if gate.Name != "component-gate" || !slices.Equal(gate.Command, []string{"rbln-validator"}) || !slices.Equal(gate.Args, []string{"gate", "--component", "partition-manager"}) {
		t.Fatalf("wrong driver gate: %+v", gate)
	}
	assertContainerHasVolumeMount(t, gate, consts.ValidationsVolumeName)
	if envFieldRef(gate.Env, "NODE_NAME") == nil || envFieldRef(gate.Env, "OPERATOR_NAMESPACE") == nil {
		t.Fatal("driver gate needs node and namespace identity")
	}
	if gate.SecurityContext.RunAsUser == nil || *gate.SecurityContext.RunAsUser != 0 {
		t.Fatal("driver gate must read root-owned readiness files")
	}
	if p.WorkloadType() != consts.RBLNWorkloadConfigAll {
		t.Fatal("partition-manager must serve both workloads")
	}

	mainContainer := ds.Spec.Template.Spec.Containers[0]
	assertContainerImage(t, mainContainer, "rebellions/k8s-partition-manager", "v0.1.0")
	if !slices.Equal(mainContainer.Args, []string{"reconcile-partition-state"}) {
		t.Fatalf("container args = %v, want [reconcile-partition-state]", mainContainer.Args)
	}

	assertContainerHasVolumeMount(t, mainContainer, "host-sys")
	assertContainerHasVolumeMount(t, mainContainer, "host-root")
	assertClusterRoleHasRule(t, cr, "resource.k8s.io", "resourceclaims")

	if ref := envFieldRef(mainContainer.Env, "NODE_NAME"); ref == nil || ref.FieldPath != testNodeNameFieldPath {
		t.Fatal("expected NODE_NAME env from the spec.nodeName Downward API")
	}
	if ref := envFieldRef(mainContainer.Env, "OPERATOR_NAMESPACE"); ref == nil || ref.FieldPath != "metadata.namespace" {
		t.Fatal("expected OPERATOR_NAMESPACE env from the metadata.namespace Downward API")
	}

	assertContainerHasVolumeMount(t, mainContainer, consts.ValidationsVolumeName)
	assertPodHasVolume(t, ds.Spec.Template.Spec, consts.ValidationsVolumeName)
}

func TestPartitionManagerHostAccess(t *testing.T) {
	scheme := newTestScheme(t)
	owner := newPartitionManagerOwner()
	p := NewPartitionManagerPatcher(newFakeClient(t, scheme), logf.Log, testNamespace, &owner.Spec, scheme, "").(*partitionManagerPatcher)
	spec := p.buildPodSpec(owner)
	mainContainer := spec.Containers[0]
	assertPrivileged(t, mainContainer)
	if mainContainer.SecurityContext.RunAsUser == nil || *mainContainer.SecurityContext.RunAsUser != 0 {
		t.Fatal("partition manager must run as root for PCI binding and chroot modprobe")
	}
	if mainContainer.SecurityContext.SELinuxOptions == nil || mainContainer.SecurityContext.SELinuxOptions.Level != "s0" {
		t.Fatal("expected SELinuxOptions.Level=s0 for host PCI access")
	}
	for _, want := range []struct {
		name, hostPath, mountPath string
		readOnly                  bool
	}{
		{name: "host-sys", hostPath: "/sys", mountPath: "/sys"},
		{name: "host-root", hostPath: "/", mountPath: "/host", readOnly: true},
	} {
		volumeIndex := slices.IndexFunc(spec.Volumes, func(v corev1.Volume) bool { return v.Name == want.name })
		if volumeIndex < 0 {
			t.Fatalf("missing host volume %s", want.name)
		}
		host := spec.Volumes[volumeIndex].HostPath
		if host == nil || host.Path != want.hostPath || host.Type == nil || *host.Type != corev1.HostPathDirectory {
			t.Fatalf("volume %s must use host directory %s: %+v", want.name, want.hostPath, host)
		}
		mountIndex := slices.IndexFunc(mainContainer.VolumeMounts, func(m corev1.VolumeMount) bool { return m.Name == want.name })
		if mountIndex < 0 {
			t.Fatalf("manager does not mount %s", want.name)
		}
		mount := mainContainer.VolumeMounts[mountIndex]
		if mount.MountPath != want.mountPath || mount.ReadOnly != want.readOnly {
			t.Fatalf("incorrect %s mount: %+v", want.name, mount)
		}
		if want.name == "host-root" && (mount.MountPropagation == nil || *mount.MountPropagation != corev1.MountPropagationHostToContainer) {
			t.Fatal("host root mount must receive host submounts for chroot modprobe")
		}
	}
}

func TestPartitionManagerPatchOpenShift(t *testing.T) {
	scheme := newTestScheme(t)
	c := newFakeClient(t, scheme)
	owner := newPartitionManagerOwner()
	name := consts.RBLNBaseName + "-" + consts.RBLNPartitionManagerName
	p := NewPartitionManagerPatcher(c, logf.Log, testNamespace, &owner.Spec, scheme, "4.15")
	if err := p.Patch(context.Background(), owner); err != nil {
		t.Fatal(err)
	}
	key := types.NamespacedName{Name: name, Namespace: testNamespace}
	role := &rbacv1.Role{}
	assertObjectExists(t, c, key, role)
	if len(role.Rules) != 1 || !slices.Equal(role.Rules[0].APIGroups, []string{"security.openshift.io"}) ||
		!slices.Equal(role.Rules[0].Resources, []string{"securitycontextconstraints"}) ||
		!slices.Equal(role.Rules[0].ResourceNames, []string{"privileged"}) ||
		!slices.Equal(role.Rules[0].Verbs, []string{"use"}) {
		t.Fatalf("missing privileged SCC use permission: %+v", role.Rules)
	}
	binding := &rbacv1.RoleBinding{}
	assertObjectExists(t, c, key, binding)
	if binding.RoleRef.Kind != "Role" || binding.RoleRef.Name != name || len(binding.Subjects) != 1 ||
		binding.Subjects[0].Kind != "ServiceAccount" || binding.Subjects[0].Name != name || binding.Subjects[0].Namespace != testNamespace {
		t.Fatalf("SCC permission not bound to partition-manager: %+v", binding)
	}
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

func TestPartitionManagerVMDeployment(t *testing.T) {
	owner := newPartitionManagerOwner()
	owner.Spec.WorkloadType = consts.RBLNWorkloadConfigVMPassthrough
	owner.Spec.Validator.Registry = "private.example.com"
	owner.Spec.Validator.Image = "validator"
	owner.Spec.Validator.Version = "gate-test"
	owner.Spec.Validator.ImagePullSecrets = []string{"shared", "validator-secret"}
	owner.Spec.PartitionManager.ImagePullSecrets = []string{"shared", "partition-secret"}
	p := NewPartitionManagerPatcher(newFakeClient(t, newTestScheme(t)), logf.Log, testNamespace, &owner.Spec, newTestScheme(t), "").(*partitionManagerPatcher)
	pod := p.buildPodSpec(owner)
	if pod.InitContainers[0].Image != "private.example.com/validator:gate-test" {
		t.Fatalf("wrong validator image: %s", pod.InitContainers[0].Image)
	}
	wantSecrets := []corev1.LocalObjectReference{{Name: "shared"}, {Name: "partition-secret"}, {Name: "validator-secret"}}
	if !slices.Equal(pod.ImagePullSecrets, wantSecrets) {
		t.Fatalf("image pull secrets = %v", pod.ImagePullSecrets)
	}
	if len(pod.InitContainers) != 1 || !slices.Equal(pod.InitContainers[0].Args, []string{"gate", "--component", "partition-manager"}) {
		t.Fatal("VM manager must wait only for driver installation")
	}
	if envValue(pod.Containers[0].Env, "DEFAULT_WORKLOAD_TYPE") != consts.RBLNWorkloadConfigVMPassthrough {
		t.Fatal("missing policy default for nodes without a workload label")
	}
	mounts := pod.Containers[0].VolumeMounts
	for _, mount := range mounts {
		switch mount.Name {
		case "host-sys":
			if mount.ReadOnly || mount.MountPath != "/sys" {
				t.Fatal("PCI binding needs writable host sysfs")
			}
		case "host-root":
			if !mount.ReadOnly || mount.MountPath != "/host" || mount.MountPropagation == nil || *mount.MountPropagation != corev1.MountPropagationHostToContainer {
				t.Fatal("host modprobe requires the host root and its submounts")
			}
		}
	}
}
