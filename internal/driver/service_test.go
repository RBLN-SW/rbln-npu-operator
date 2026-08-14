package driver

import (
	"context"
	"errors"
	"testing"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
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
	if got, want := err.Error(), "patch driver-manager: patch boom"; got != want {
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
