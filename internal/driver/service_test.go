package driver

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/driver/components"
)

type fakeDriverPatcher struct {
	name       string
	namespace  string
	enabled    bool
	patchErr   error
	cleanupErr error
	patchCalls int
	cleanCalls int
}

var _ components.DriverPatcher = (*fakeDriverPatcher)(nil)

func (f *fakeDriverPatcher) IsEnabled() bool { return f.enabled }

func (f *fakeDriverPatcher) Patch(_ context.Context, _ *rebellionsaiv1alpha1.RBLNDriver) error {
	f.patchCalls++
	return f.patchErr
}

func (f *fakeDriverPatcher) CleanUp(_ context.Context, _ *rebellionsaiv1alpha1.RBLNDriver) error {
	f.cleanCalls++
	return f.cleanupErr
}

func (f *fakeDriverPatcher) ComponentName() string      { return f.name }
func (f *fakeDriverPatcher) ComponentNamespace() string { return f.namespace }

func TestPatchComponents(t *testing.T) {
	errPatch := errors.New("patch boom")
	errCleanup := errors.New("cleanup boom")

	type want struct {
		err        error
		errString  string
		patchCalls map[string]int
		cleanCalls map[string]int
	}

	tests := map[string]struct {
		reason   string
		patchers []*fakeDriverPatcher
		want     want
	}{
		"patches enabled components and cleans up disabled components": {
			reason: "enabled components should be patched while disabled components should be cleaned up",
			patchers: []*fakeDriverPatcher{
				{name: "driver-manager", enabled: true},
				{name: "disabled-comp", enabled: false},
			},
			want: want{
				patchCalls: map[string]int{"driver-manager": 1, "disabled-comp": 0},
				cleanCalls: map[string]int{"driver-manager": 0, "disabled-comp": 1},
			},
		},
		"wraps patch errors with the component name": {
			reason: "patch errors should be wrapped with the component name",
			patchers: []*fakeDriverPatcher{
				{name: "driver-manager", enabled: true, patchErr: errPatch},
			},
			want: want{
				err:        errPatch,
				errString:  "patch driver-manager: patch boom",
				patchCalls: map[string]int{"driver-manager": 1},
				cleanCalls: map[string]int{"driver-manager": 0},
			},
		},
		"wraps cleanup errors with the component name": {
			reason: "cleanup errors should be wrapped with the component name",
			patchers: []*fakeDriverPatcher{
				{name: "driver-manager", enabled: false, cleanupErr: errCleanup},
			},
			want: want{
				err:        errCleanup,
				errString:  "cleanup driver-manager: cleanup boom",
				patchCalls: map[string]int{"driver-manager": 0},
				cleanCalls: map[string]int{"driver-manager": 1},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			service := &DriverService{
				singleton: &rebellionsaiv1alpha1.RBLNDriver{},
				patcher:   toDriverPatchers(tc.patchers),
			}

			err := service.PatchComponents(context.Background())

			wantErr := tc.want.err != nil
			if (err != nil) != wantErr {
				t.Fatalf("%s: PatchComponents() error = %v, wantErr %t", tc.reason, err, wantErr)
			}
			if wantErr {
				if !errors.Is(err, tc.want.err) {
					t.Errorf("%s: PatchComponents() error = %v, want wrapped %v", tc.reason, err, tc.want.err)
				}
				if got := err.Error(); got != tc.want.errString {
					t.Errorf("%s: PatchComponents() error = %q, want %q", tc.reason, got, tc.want.errString)
				}
			}

			if diff := cmp.Diff(tc.want.patchCalls, patchCallCounts(tc.patchers)); diff != "" {
				t.Errorf("%s: patch call counts -want, +got:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.cleanCalls, cleanCallCounts(tc.patchers)); diff != "" {
				t.Errorf("%s: clean call counts -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func toDriverPatchers(patchers []*fakeDriverPatcher) []components.DriverPatcher {
	out := make([]components.DriverPatcher, len(patchers))
	for i, p := range patchers {
		out[i] = p
	}
	return out
}

func patchCallCounts(patchers []*fakeDriverPatcher) map[string]int {
	counts := make(map[string]int, len(patchers))
	for _, p := range patchers {
		counts[p.name] = p.patchCalls
	}
	return counts
}

func cleanCallCounts(patchers []*fakeDriverPatcher) map[string]int {
	counts := make(map[string]int, len(patchers))
	for _, p := range patchers {
		counts[p.name] = p.cleanCalls
	}
	return counts
}
