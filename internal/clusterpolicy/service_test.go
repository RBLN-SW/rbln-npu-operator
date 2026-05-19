package clusterpolicy

import (
	"context"
	"errors"
	"testing"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/clusterpolicy/components"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

type fakePatcher struct {
	name         string
	namespace    string
	workloadType string
	enabled      bool
	patchErr     error
	cleanupErr   error
	report       components.ReadinessReport
	patchCalls   int
	cleanCalls   int
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

func (f *fakePatcher) IsReady(context.Context, int32) components.ReadinessReport {
	return f.report
}

func (f *fakePatcher) ComponentName() string      { return f.name }
func (f *fakePatcher) ComponentNamespace() string { return f.namespace }
func (f *fakePatcher) WorkloadType() string       { return f.workloadType }

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

func TestAssembleStatus(t *testing.T) {
	containerReady := components.ReadinessReport{State: rblnv1beta1.ComponentStateReady, Desired: 1, Ready: 1}
	containerNotReady := components.ReadinessReport{State: rblnv1beta1.ComponentStateNotReady, Desired: 1, Ready: 0, Message: "not ready"}

	tests := map[string]struct {
		reason         string
		patchers       []*fakePatcher
		census         NodeCensus
		wantComponents []rblnv1beta1.RBLNComponentStatus
		wantWorkloads  []rblnv1beta1.RBLNWorkloadStatus
	}{
		"all container ready, no vm-passthrough nodes": {
			reason: "container ready and vm-passthrough empty",
			patchers: []*fakePatcher{
				{name: "device-plugin", namespace: "rbln-system", workloadType: consts.RBLNWorkloadConfigContainer, enabled: true, report: containerReady},
			},
			census: NodeCensus{TotalNPU: 1, ContainerNodes: 1, VMPassthroughNodes: 0},
			wantComponents: []rblnv1beta1.RBLNComponentStatus{
				{Name: "device-plugin", Namespace: "rbln-system", WorkloadType: consts.RBLNWorkloadConfigContainer, State: rblnv1beta1.ComponentStateReady, Desired: 1, Ready: 1},
			},
			wantWorkloads: []rblnv1beta1.RBLNWorkloadStatus{
				{Type: consts.RBLNWorkloadConfigContainer, NodeCount: 1, ComponentCount: 1, ReadyCount: 1, State: rblnv1beta1.WorkloadStateReady},
				{Type: consts.RBLNWorkloadConfigVMPassthrough, NodeCount: 0, ComponentCount: 0, ReadyCount: 0, State: rblnv1beta1.WorkloadStateEmpty},
			},
		},
		"vm-passthrough node exists but no vm-passthrough components enabled (Scenario A)": {
			reason: "vm-passthrough state should be uncovered",
			patchers: []*fakePatcher{
				{name: "device-plugin", namespace: "rbln-system", workloadType: consts.RBLNWorkloadConfigContainer, enabled: true, report: containerReady},
			},
			census: NodeCensus{TotalNPU: 2, ContainerNodes: 1, VMPassthroughNodes: 1},
			wantComponents: []rblnv1beta1.RBLNComponentStatus{
				{Name: "device-plugin", Namespace: "rbln-system", WorkloadType: consts.RBLNWorkloadConfigContainer, State: rblnv1beta1.ComponentStateReady, Desired: 1, Ready: 1},
			},
			wantWorkloads: []rblnv1beta1.RBLNWorkloadStatus{
				{Type: consts.RBLNWorkloadConfigContainer, NodeCount: 1, ComponentCount: 1, ReadyCount: 1, State: rblnv1beta1.WorkloadStateReady},
				{Type: consts.RBLNWorkloadConfigVMPassthrough, NodeCount: 1, ComponentCount: 0, ReadyCount: 0, State: rblnv1beta1.WorkloadStateUncovered, Message: "1 vm-passthrough node(s) labeled but no enabled components configured"},
			},
		},
		"vm-passthrough enabled and ready alongside container": {
			reason: "both workloads ready",
			patchers: []*fakePatcher{
				{name: "device-plugin", namespace: "rbln-system", workloadType: consts.RBLNWorkloadConfigContainer, enabled: true, report: containerReady},
				{name: "vfio-manager", namespace: "rbln-system", workloadType: consts.RBLNWorkloadConfigVMPassthrough, enabled: true, report: containerReady},
			},
			census: NodeCensus{TotalNPU: 2, ContainerNodes: 1, VMPassthroughNodes: 1},
			wantComponents: []rblnv1beta1.RBLNComponentStatus{
				{Name: "device-plugin", Namespace: "rbln-system", WorkloadType: consts.RBLNWorkloadConfigContainer, State: rblnv1beta1.ComponentStateReady, Desired: 1, Ready: 1},
				{Name: "vfio-manager", Namespace: "rbln-system", WorkloadType: consts.RBLNWorkloadConfigVMPassthrough, State: rblnv1beta1.ComponentStateReady, Desired: 1, Ready: 1},
			},
			wantWorkloads: []rblnv1beta1.RBLNWorkloadStatus{
				{Type: consts.RBLNWorkloadConfigContainer, NodeCount: 1, ComponentCount: 1, ReadyCount: 1, State: rblnv1beta1.WorkloadStateReady},
				{Type: consts.RBLNWorkloadConfigVMPassthrough, NodeCount: 1, ComponentCount: 1, ReadyCount: 1, State: rblnv1beta1.WorkloadStateReady},
			},
		},
		"container progressing → workload progressing": {
			reason: "one container component not ready",
			patchers: []*fakePatcher{
				{name: "device-plugin", namespace: "rbln-system", workloadType: consts.RBLNWorkloadConfigContainer, enabled: true, report: containerNotReady},
			},
			census: NodeCensus{TotalNPU: 1, ContainerNodes: 1, VMPassthroughNodes: 0},
			wantComponents: []rblnv1beta1.RBLNComponentStatus{
				{Name: "device-plugin", Namespace: "rbln-system", WorkloadType: consts.RBLNWorkloadConfigContainer, State: rblnv1beta1.ComponentStateNotReady, Desired: 1, Ready: 0, Message: "not ready"},
			},
			wantWorkloads: []rblnv1beta1.RBLNWorkloadStatus{
				{Type: consts.RBLNWorkloadConfigContainer, NodeCount: 1, ComponentCount: 1, ReadyCount: 0, State: rblnv1beta1.WorkloadStateProgressing, Message: "0/1 components ready on 1 container node(s)"},
				{Type: consts.RBLNWorkloadConfigVMPassthrough, NodeCount: 0, ComponentCount: 0, ReadyCount: 0, State: rblnv1beta1.WorkloadStateEmpty},
			},
		},
		"vm-passthrough enabled but 0 nodes → empty with message hint": {
			reason: "Scenario C — DS exists but workload has no nodes",
			patchers: []*fakePatcher{
				{name: "vfio-manager", namespace: "rbln-system", workloadType: consts.RBLNWorkloadConfigVMPassthrough, enabled: true, report: components.ReadinessReport{State: rblnv1beta1.ComponentStateReady}},
			},
			census: NodeCensus{TotalNPU: 1, ContainerNodes: 1, VMPassthroughNodes: 0},
			wantComponents: []rblnv1beta1.RBLNComponentStatus{
				{Name: "vfio-manager", Namespace: "rbln-system", WorkloadType: consts.RBLNWorkloadConfigVMPassthrough, State: rblnv1beta1.ComponentStateReady},
			},
			wantWorkloads: []rblnv1beta1.RBLNWorkloadStatus{
				{Type: consts.RBLNWorkloadConfigContainer, NodeCount: 1, ComponentCount: 0, ReadyCount: 0, State: rblnv1beta1.WorkloadStateUncovered, Message: "1 container node(s) labeled but no enabled components configured"},
				{Type: consts.RBLNWorkloadConfigVMPassthrough, NodeCount: 0, ComponentCount: 1, ReadyCount: 1, State: rblnv1beta1.WorkloadStateEmpty, Message: "1 component(s) configured but no vm-passthrough nodes present"},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			service := &ClusterPolicyService{
				log:        logr.Discard(),
				policy:     &rblnv1beta1.RBLNClusterPolicy{},
				components: toPatchers(tc.patchers),
			}

			gotComponents, gotWorkloads := service.AssembleStatus(context.Background(), tc.census)
			if diff := cmp.Diff(tc.wantComponents, gotComponents); diff != "" {
				t.Errorf("%s: components -want, +got:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.wantWorkloads, gotWorkloads); diff != "" {
				t.Errorf("%s: workloads -want, +got:\n%s", tc.reason, diff)
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
