package clusterpolicy

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/clusterpolicy/components"
)

type fakePatcher struct {
	name       string
	namespace  string
	enabled    bool
	patchErr   error
	cleanupErr error
	reportErr  error
	patchCalls int
	cleanCalls int
}

var _ components.Patcher = (*fakePatcher)(nil)

func (f *fakePatcher) IsEnabled() bool { return f.enabled }

func (f *fakePatcher) Patch(context.Context, *rblnv1beta1.RBLNClusterPolicy) error {
	f.patchCalls++
	return f.patchErr
}

func (f *fakePatcher) CleanUp(context.Context, *rblnv1beta1.RBLNClusterPolicy) error {
	f.cleanCalls++
	return f.cleanupErr
}

func (f *fakePatcher) IsReady(context.Context) error {
	return f.reportErr
}

func (f *fakePatcher) ComponentName() string      { return f.name }
func (f *fakePatcher) ComponentNamespace() string { return f.namespace }

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
		patchers []*fakePatcher
		want     want
	}{
		"patches enabled components and cleans up disabled components": {
			reason: "enabled components should be patched while disabled components should be cleaned up",
			patchers: []*fakePatcher{
				{name: "device-plugin", enabled: true},
				{name: "vfio-manager", enabled: false},
			},
			want: want{
				patchCalls: map[string]int{"device-plugin": 1, "vfio-manager": 0},
				cleanCalls: map[string]int{"device-plugin": 0, "vfio-manager": 1},
			},
		},
		"wraps patch errors with the component name": {
			reason: "patch errors should be wrapped with the component name",
			patchers: []*fakePatcher{
				{name: "device-plugin", enabled: true, patchErr: errPatch},
			},
			want: want{
				err:        errPatch,
				errString:  "patch device-plugin: patch boom",
				patchCalls: map[string]int{"device-plugin": 1},
				cleanCalls: map[string]int{"device-plugin": 0},
			},
		},
		"wraps cleanup errors with the component name": {
			reason: "cleanup errors should be wrapped with the component name",
			patchers: []*fakePatcher{
				{name: "vfio-manager", enabled: false, cleanupErr: errCleanup},
			},
			want: want{
				err:        errCleanup,
				errString:  "cleanup vfio-manager: cleanup boom",
				patchCalls: map[string]int{"vfio-manager": 0},
				cleanCalls: map[string]int{"vfio-manager": 1},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			service := &ClusterPolicyService{
				policy:     &rblnv1beta1.RBLNClusterPolicy{},
				components: toPatchers(tc.patchers),
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

func TestAssembleComponentStatus(t *testing.T) {
	tests := map[string]struct {
		reason   string
		patchers []*fakePatcher
		want     []rblnv1beta1.RBLNComponentStatus
	}{
		"returns status only for enabled components": {
			reason: "enabled components should be returned with ready state derived from ConditionReport errors",
			patchers: []*fakePatcher{
				{name: "device-plugin", namespace: "rbln-system", enabled: true},
				{name: "disabled-component", namespace: "rbln-system", enabled: false},
				{name: "vfio-manager", namespace: "rbln-system", enabled: true, reportErr: errors.New("not ready")},
			},
			want: []rblnv1beta1.RBLNComponentStatus{
				{Name: "device-plugin", Namespace: "rbln-system", State: rblnv1beta1.ComponentStateReady},
				{Name: "vfio-manager", Namespace: "rbln-system", State: rblnv1beta1.ComponentStateNotReady},
			},
		},
		"returns empty slice when all components are disabled": {
			reason: "disabled components should be excluded from status entirely",
			patchers: []*fakePatcher{
				{name: "device-plugin", namespace: "rbln-system", enabled: false},
				{name: "vfio-manager", namespace: "rbln-system", enabled: false},
			},
			want: []rblnv1beta1.RBLNComponentStatus{},
		},
		"marks Ready when ConditionReport returns no error": {
			reason: "a component reporting no error should be marked as Ready",
			patchers: []*fakePatcher{
				{name: "device-plugin", namespace: "rbln-system", enabled: true},
			},
			want: []rblnv1beta1.RBLNComponentStatus{
				{Name: "device-plugin", Namespace: "rbln-system", State: rblnv1beta1.ComponentStateReady},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			service := &ClusterPolicyService{
				policy:     &rblnv1beta1.RBLNClusterPolicy{},
				components: toPatchers(tc.patchers),
			}

			got := service.AssembleComponentStatus(context.Background())
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("%s: AssembleComponentStatus() -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func toPatchers(patchers []*fakePatcher) []components.Patcher {
	out := make([]components.Patcher, len(patchers))
	for i, patcher := range patchers {
		out[i] = patcher
	}
	return out
}

func patchCallCounts(patchers []*fakePatcher) map[string]int {
	counts := make(map[string]int, len(patchers))
	for _, patcher := range patchers {
		counts[patcher.name] = patcher.patchCalls
	}
	return counts
}

func cleanCallCounts(patchers []*fakePatcher) map[string]int {
	counts := make(map[string]int, len(patchers))
	for _, patcher := range patchers {
		counts[patcher.name] = patcher.cleanCalls
	}
	return counts
}
