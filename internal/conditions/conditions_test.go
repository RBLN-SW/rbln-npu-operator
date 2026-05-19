package conditions

import (
	"testing"

	rblnv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
)

// TestApplyDriverSummary documents and pins the nil-vs-empty contract of
// applyDriverSummary, which is the most error-prone corner of the conditions
// package: early-exit reconcile paths pass an empty struct so prior status
// data should be preserved; the happy path passes a fully populated summary
// (possibly with NodePools=[] when no pools exist yet) so prior data should
// be overwritten, including reset to zero.
func TestApplyDriverSummary(t *testing.T) {
	tests := map[string]struct {
		existing rblnv1alpha1.RBLNDriverStatus
		summary  DriverSummary
		want     rblnv1alpha1.RBLNDriverStatus
	}{
		"empty summary preserves previous status (early-exit path)": {
			existing: rblnv1alpha1.RBLNDriverStatus{
				Namespace:    "rbln-system",
				DesiredNodes: 3,
				ReadyNodes:   2,
				NodePools: []rblnv1alpha1.RBLNDriverPoolStatus{
					{Name: "ubuntu22.04-5.15", Desired: 3, Ready: 2, State: rblnv1alpha1.DriverPoolStateProgressing},
				},
			},
			summary: DriverSummary{},
			want: rblnv1alpha1.RBLNDriverStatus{
				Namespace:    "rbln-system",
				DesiredNodes: 3,
				ReadyNodes:   2,
				NodePools: []rblnv1alpha1.RBLNDriverPoolStatus{
					{Name: "ubuntu22.04-5.15", Desired: 3, Ready: 2, State: rblnv1alpha1.DriverPoolStateProgressing},
				},
			},
		},
		"empty pool slice resets counts (happy-path with no pools yet)": {
			existing: rblnv1alpha1.RBLNDriverStatus{
				DesiredNodes: 5,
				ReadyNodes:   5,
				NodePools: []rblnv1alpha1.RBLNDriverPoolStatus{
					{Name: "stale", Desired: 5, Ready: 5, State: rblnv1alpha1.DriverPoolStateReady},
				},
			},
			summary: DriverSummary{
				Namespace:    "rbln-system",
				NodePools:    []rblnv1alpha1.RBLNDriverPoolStatus{},
				DesiredNodes: 0,
				ReadyNodes:   0,
			},
			want: rblnv1alpha1.RBLNDriverStatus{
				Namespace:    "rbln-system",
				DesiredNodes: 0,
				ReadyNodes:   0,
				NodePools:    []rblnv1alpha1.RBLNDriverPoolStatus{},
			},
		},
		"populated summary overwrites prior values": {
			existing: rblnv1alpha1.RBLNDriverStatus{
				Namespace:    "old-ns",
				DesiredNodes: 1,
				ReadyNodes:   0,
			},
			summary: DriverSummary{
				Namespace: "rbln-system",
				NodePools: []rblnv1alpha1.RBLNDriverPoolStatus{
					{Name: "p1", Desired: 2, Ready: 2, State: rblnv1alpha1.DriverPoolStateReady},
				},
				DesiredNodes: 2,
				ReadyNodes:   2,
			},
			want: rblnv1alpha1.RBLNDriverStatus{
				Namespace:    "rbln-system",
				DesiredNodes: 2,
				ReadyNodes:   2,
				NodePools: []rblnv1alpha1.RBLNDriverPoolStatus{
					{Name: "p1", Desired: 2, Ready: 2, State: rblnv1alpha1.DriverPoolStateReady},
				},
			},
		},
		"summary with empty namespace preserves prior namespace": {
			existing: rblnv1alpha1.RBLNDriverStatus{
				Namespace:    "rbln-system",
				DesiredNodes: 1,
			},
			summary: DriverSummary{
				NodePools:    []rblnv1alpha1.RBLNDriverPoolStatus{},
				DesiredNodes: 0,
				ReadyNodes:   0,
			},
			want: rblnv1alpha1.RBLNDriverStatus{
				Namespace:    "rbln-system",
				DesiredNodes: 0,
				ReadyNodes:   0,
				NodePools:    []rblnv1alpha1.RBLNDriverPoolStatus{},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			driver := &rblnv1alpha1.RBLNDriver{Status: tc.existing}
			applyDriverSummary(driver, tc.summary)

			got := driver.Status
			if got.Namespace != tc.want.Namespace {
				t.Errorf("namespace: got %q, want %q", got.Namespace, tc.want.Namespace)
			}
			if got.DesiredNodes != tc.want.DesiredNodes {
				t.Errorf("desiredNodes: got %d, want %d", got.DesiredNodes, tc.want.DesiredNodes)
			}
			if got.ReadyNodes != tc.want.ReadyNodes {
				t.Errorf("readyNodes: got %d, want %d", got.ReadyNodes, tc.want.ReadyNodes)
			}
			if !poolsEqual(got.NodePools, tc.want.NodePools) {
				t.Errorf("nodePools: got %+v, want %+v", got.NodePools, tc.want.NodePools)
			}
		})
	}
}

func poolsEqual(a, b []rblnv1alpha1.RBLNDriverPoolStatus) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
