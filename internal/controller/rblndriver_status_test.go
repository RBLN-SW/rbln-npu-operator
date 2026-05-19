package controller

import (
	"strings"
	"testing"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
)

// TestSummarisePoolStates verifies the per-pool aggregation that drives the
// RBLNDriver Ready condition message. The empty-slice case must return ""
// (no pools yet means nothing is wrong); any non-ready pool must produce a
// joined message that names every offender.
func TestSummarisePoolStates(t *testing.T) {
	tests := map[string]struct {
		pools       []rebellionsaiv1alpha1.RBLNDriverPoolStatus
		wantEmpty   bool
		wantContain []string // substrings the message must contain
	}{
		"nil pools (none provisioned yet) → empty message": {
			pools:     nil,
			wantEmpty: true,
		},
		"all ready → empty message": {
			pools: []rebellionsaiv1alpha1.RBLNDriverPoolStatus{
				{Name: "ubuntu22.04-5.15", Desired: 2, Ready: 2, State: rebellionsaiv1alpha1.DriverPoolStateReady},
			},
			wantEmpty: true,
		},
		"single progressing pool": {
			pools: []rebellionsaiv1alpha1.RBLNDriverPoolStatus{
				{Name: "ubuntu22.04-5.15", Desired: 2, Ready: 1, State: rebellionsaiv1alpha1.DriverPoolStateProgressing},
			},
			wantContain: []string{"ubuntu22.04-5.15(1/2)"},
		},
		"mixed: progressing + ready (only progressing listed)": {
			pools: []rebellionsaiv1alpha1.RBLNDriverPoolStatus{
				{Name: "ready-pool", Desired: 1, Ready: 1, State: rebellionsaiv1alpha1.DriverPoolStateReady},
				{Name: "lagging-pool", Desired: 3, Ready: 0, State: rebellionsaiv1alpha1.DriverPoolStateProgressing},
			},
			wantContain: []string{"lagging-pool(0/3)"},
		},
		"multiple progressing pools joined with comma": {
			pools: []rebellionsaiv1alpha1.RBLNDriverPoolStatus{
				{Name: "pool-a", Desired: 2, Ready: 0, State: rebellionsaiv1alpha1.DriverPoolStateProgressing},
				{Name: "pool-b", Desired: 1, Ready: 0, State: rebellionsaiv1alpha1.DriverPoolStateProgressing},
			},
			wantContain: []string{"pool-a(0/2)", "pool-b(0/1)", ", "},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := summarisePoolStates(tc.pools)
			if tc.wantEmpty {
				if got != "" {
					t.Errorf("expected empty message, got %q", got)
				}
				return
			}
			if got == "" {
				t.Fatalf("expected non-empty message, got empty")
			}
			for _, want := range tc.wantContain {
				if !strings.Contains(got, want) {
					t.Errorf("message %q must contain %q", got, want)
				}
			}
		})
	}
}
