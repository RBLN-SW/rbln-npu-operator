package driver

import (
	"context"
	"errors"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	"github.com/rebellions-sw/rbln-npu-operator/internal/driver/components"
)

type fakeDriverPatcher struct {
	name       string
	patchErr   error
	pools      []rebellionsaiv1alpha1.RBLNDriverPoolStatus
	poolsErr   error
	patchCalls int
}

var _ components.DriverPatcher = (*fakeDriverPatcher)(nil)

func (f *fakeDriverPatcher) Patch(_ context.Context, _ *rebellionsaiv1alpha1.RBLNDriver) error {
	f.patchCalls++
	return f.patchErr
}

func (f *fakeDriverPatcher) PoolStatuses(_ context.Context) ([]rebellionsaiv1alpha1.RBLNDriverPoolStatus, error) {
	return f.pools, f.poolsErr
}
func (f *fakeDriverPatcher) ComponentName() string      { return f.name }
func (f *fakeDriverPatcher) ComponentNamespace() string { return "" }
func (f *fakeDriverPatcher) Diagnostics() components.PoolDiagnostics {
	return components.PoolDiagnostics{}
}

// A patch failure must reach the caller named, so the RBLNDriver condition
// says which component failed rather than just echoing the driver error.
func TestPatchComponents_WrapsErrorWithComponentName(t *testing.T) {
	errPatch := errors.New("patch boom")
	patcher := &fakeDriverPatcher{name: "driver-manager", patchErr: errPatch}
	service := &DriverService{singleton: &rebellionsaiv1alpha1.RBLNDriver{}, driverManager: patcher}

	err := service.PatchComponents(context.Background())

	if !errors.Is(err, errPatch) {
		t.Fatalf("PatchComponents() error = %v, want wrapped %v", err, errPatch)
	}
	if got, want := err.Error(), "patch rbln-driver: patch boom"; got != want {
		t.Errorf("PatchComponents() error = %q, want %q", got, want)
	}
	if patcher.patchCalls != 1 {
		t.Errorf("patch calls = %d, want 1", patcher.patchCalls)
	}
}

// AssembleStatus must return a non-nil empty slice when no pools exist: nil
// means "preserve previous status", which would keep stale pool data.
func TestAssembleStatus_NonNilEmpty(t *testing.T) {
	service := &DriverService{driverManager: &fakeDriverPatcher{
		name:  "driver-manager",
		pools: []rebellionsaiv1alpha1.RBLNDriverPoolStatus{},
	}}

	pools, desired, ready, err := service.AssembleStatus(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pools == nil {
		t.Fatal("pools must be non-nil empty slice, got nil")
	}
	if len(pools) != 0 {
		t.Fatalf("pools must be empty, got %d entries", len(pools))
	}
	if desired != 0 || ready != 0 {
		t.Fatalf("desired/ready must be zero, got desired=%d ready=%d", desired, ready)
	}
}

// The smd DaemonSet may only exist while the driver manager has at least one
// DaemonSet rendered for this CR: smd deploys only where the driver deploys.
func TestPatchComponents_GatesSmdOnDriverPools(t *testing.T) {
	cases := map[string]struct {
		pools     []rebellionsaiv1alpha1.RBLNDriverPoolStatus
		wantSmdDS bool
	}{
		"no driver DaemonSets keeps smd off": {
			pools:     []rebellionsaiv1alpha1.RBLNDriverPoolStatus{},
			wantSmdDS: false,
		},
		"a rendered driver DaemonSet deploys smd": {
			pools: []rebellionsaiv1alpha1.RBLNDriverPoolStatus{
				{Name: "atom-ubuntu22.04-5.15.0", Desired: 1, Ready: 1},
			},
			wantSmdDS: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{
				rebellionsaiv1alpha1.AddToScheme, corev1.AddToScheme, appsv1.AddToScheme, rbacv1.AddToScheme,
			} {
				if err := add(scheme); err != nil {
					t.Fatalf("scheme registration failed: %v", err)
				}
			}
			c := fake.NewClientBuilder().WithScheme(scheme).Build()
			ctx := context.Background()

			owner := &rebellionsaiv1alpha1.RBLNDriver{
				ObjectMeta: metav1.ObjectMeta{Name: "test-driver", UID: "test-uid"},
				Spec: rebellionsaiv1alpha1.RBLNDriverSpec{
					Version: "3.0.0",
					Smd:     rebellionsaiv1alpha1.SmdSpec{Registry: "docker.io", Image: "rebellions/rbln-smd"},
					Manager: rebellionsaiv1alpha1.DriverManagerSpec{
						Registry: "docker.io", Image: "rebellions/rbln-k8s-driver-manager", Version: "1.0.0",
					},
				},
			}
			smd, err := components.NewSmdPatcher(c, c, logf.Log, "rbln-system", owner, scheme, "")
			if err != nil {
				t.Fatalf("NewSmdPatcher() error: %v", err)
			}
			service := &DriverService{
				singleton:     owner,
				namespace:     "rbln-system",
				driverManager: &fakeDriverPatcher{name: "driver-manager", pools: tc.pools},
				smd:           smd,
			}

			if err := service.PatchComponents(ctx); err != nil {
				t.Fatalf("PatchComponents() error: %v", err)
			}

			ds := &appsv1.DaemonSet{}
			getErr := c.Get(ctx, types.NamespacedName{Name: "test-driver-smd", Namespace: "rbln-system"}, ds)
			if tc.wantSmdDS && getErr != nil {
				t.Fatalf("expected smd DaemonSet to exist: %v", getErr)
			}
			if !tc.wantSmdDS && getErr == nil {
				t.Fatal("expected no smd DaemonSet, but one exists")
			}
		})
	}
}

func TestAssembleStatus_SumsPoolCounts(t *testing.T) {
	service := &DriverService{driverManager: &fakeDriverPatcher{
		name: "driver-manager",
		pools: []rebellionsaiv1alpha1.RBLNDriverPoolStatus{
			{Name: "atom-ubuntu22.04-5.15.0", Desired: 3, Ready: 2},
			{Name: "rebel100-ubuntu22.04-5.15.0", Desired: 1, Ready: 1},
		},
	}}

	pools, desired, ready, err := service.AssembleStatus(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pools) != 2 {
		t.Fatalf("pools = %d entries, want 2", len(pools))
	}
	if desired != 4 || ready != 3 {
		t.Fatalf("desired/ready = %d/%d, want 4/3", desired, ready)
	}
}

func TestNPUPodEvictionPolicy(t *testing.T) {
	tests := map[string]struct {
		policy *rblnv1beta1.RBLNClusterPolicy
		want   components.NPUPodEvictionPolicy
	}{
		// The strictest behaviour is the default: nothing is force-evicted and
		// no emptyDir contents are discarded without the user asking.
		"no cluster policy": {
			policy: nil,
			want:   components.NPUPodEvictionPolicy{DeviceClass: consts.DefaultDRADeviceClass},
		},
		"no upgrade policy block": {
			policy: &rblnv1beta1.RBLNClusterPolicy{},
			want:   components.NPUPodEvictionPolicy{DeviceClass: consts.DefaultDRADeviceClass},
		},
		"podDeletion is mirrored": {
			policy: &rblnv1beta1.RBLNClusterPolicy{Spec: rblnv1beta1.RBLNClusterPolicySpec{
				Driver: rblnv1beta1.DriverSpec{UpgradePolicy: &rblnv1beta1.DriverUpgradePolicySpec{
					PodDeletion: &rblnv1beta1.PodDeletionSpec{Force: true, DeleteEmptyDirData: true},
				}},
			}},
			want: components.NPUPodEvictionPolicy{
				Force: true, DeleteEmptyDirData: true, DeviceClass: consts.DefaultDRADeviceClass,
			},
		},
		// autoUpgrade is deliberately not consulted: k8s-driver-manager evicts
		// on its own only while auto-upgrade is off, so gating on it would make
		// the knobs unreachable in exactly the case they exist for.
		"podDeletion is mirrored even with autoUpgrade off": {
			policy: &rblnv1beta1.RBLNClusterPolicy{Spec: rblnv1beta1.RBLNClusterPolicySpec{
				Driver: rblnv1beta1.DriverSpec{UpgradePolicy: &rblnv1beta1.DriverUpgradePolicySpec{
					AutoUpgrade: false,
					PodDeletion: &rblnv1beta1.PodDeletionSpec{DeleteEmptyDirData: true},
				}},
			}},
			want: components.NPUPodEvictionPolicy{
				DeleteEmptyDirData: true, DeviceClass: consts.DefaultDRADeviceClass,
			},
		},
		"driverName overrides the device class": {
			policy: &rblnv1beta1.RBLNClusterPolicy{Spec: rblnv1beta1.RBLNClusterPolicySpec{
				DRAKubeletPlugin: rblnv1beta1.RBLNDRAKubeletPluginSpec{DriverName: "npu.example.com"},
			}},
			want: components.NPUPodEvictionPolicy{DeviceClass: "npu.example.com"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := npuPodEvictionPolicy(tc.policy); got != tc.want {
				t.Fatalf("npuPodEvictionPolicy() = %+v, want %+v", got, tc.want)
			}
		})
	}
}
