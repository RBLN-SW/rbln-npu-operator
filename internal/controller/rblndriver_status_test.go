package controller

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rebellionsaiv1alpha1 "github.com/rebellions-sw/rbln-npu-operator/api/v1alpha1"
	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	"github.com/rebellions-sw/rbln-npu-operator/internal/driver/components"
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

// TestMissingImagePoolsMessage pins the DriverImageNotFound condition
// message wording as a contract: the missing-image list must be sampled
// down to 3 entries so the message can't grow unbounded with fleet size.
func TestMissingImagePoolsMessage(t *testing.T) {
	tests := map[string]struct {
		diag           components.PoolDiagnostics
		wantContain    []string
		wantNotContain []string
	}{
		"single missing image names the pool and image": {
			diag: components.PoolDiagnostics{
				MissingImagePools: []components.MissingImagePool{
					{Pool: "pool-a", Image: "img-a"},
				},
			},
			wantContain:    []string{"1 driver pool(s)", "pool-a (img-a)", "publish the image(s) to recover"},
			wantNotContain: []string{"pull secret"},
		},
		// The unauthenticated case must not tell the user to publish an image
		// that may already exist -- that advice sends them the wrong way when
		// the real problem is the operator's credentials.
		"an unreadable pull secret replaces the publish advice with the credential caveat": {
			diag: components.PoolDiagnostics{
				MissingImagePools: []components.MissingImagePool{
					{Pool: "pool-a", Image: "img-a"},
				},
				UnreadablePullSecrets: []string{"drivercred (Forbidden)"},
			},
			wantContain: []string{
				"pool-a (img-a)",
				"image pull secret(s): drivercred (Forbidden)",
				"ran anonymously",
				"DRIVER_IMAGE_CHECK=false",
			},
			wantNotContain: []string{"; publish the image(s) to recover"},
		},
		"more than 3 missing-image pairs are sampled to 3 and flagged as a sample": {
			diag: components.PoolDiagnostics{
				MissingImagePools: []components.MissingImagePool{
					{Pool: "pool-a", Image: "img-a"},
					{Pool: "pool-b", Image: "img-b"},
					{Pool: "pool-c", Image: "img-c"},
					{Pool: "pool-d", Image: "img-d"},
				},
			},
			wantContain: []string{
				"4 driver pool(s)",
				"(e.g. pool-a (img-a), pool-b (img-b), pool-c (img-c))",
			},
			wantNotContain: []string{"pool-d"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := missingImagePoolsMessage(tc.diag)
			for _, want := range tc.wantContain {
				if !strings.Contains(got, want) {
					t.Errorf("message %q must contain %q", got, want)
				}
			}
			for _, notWant := range tc.wantNotContain {
				if strings.Contains(got, notWant) {
					t.Errorf("message %q must not contain %q", got, notWant)
				}
			}
		})
	}
}

// TestPickActivePolicy verifies the driver's ClusterPolicy election:
// prefer the non-ignored singleton, and fall back to a deterministic
// tie-break only when the ignored-filtered set is itself ambiguous.
func TestPickActivePolicy(t *testing.T) {
	older := metav1.NewTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	newer := metav1.NewTime(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC))

	policy := func(name, state string, ts metav1.Time) rblnv1beta1.RBLNClusterPolicy {
		return rblnv1beta1.RBLNClusterPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: name, CreationTimestamp: ts},
			Status:     rblnv1beta1.RBLNClusterPolicyStatus{State: state},
		}
	}

	tests := map[string]struct {
		items    []rblnv1beta1.RBLNClusterPolicy
		wantName string
	}{
		"ignored policy is skipped in favor of the active one": {
			items: []rblnv1beta1.RBLNClusterPolicy{
				policy("ignored-one", consts.RBLNStateIgnored, older),
				policy("active-one", consts.RBLNStateReady, newer),
			},
			wantName: "active-one",
		},
		"oldest creationTimestamp wins among active candidates": {
			items: []rblnv1beta1.RBLNClusterPolicy{
				policy("newer", consts.RBLNStateReady, newer),
				policy("older", consts.RBLNStateReady, older),
			},
			wantName: "older",
		},
		"equal timestamps break the tie by name": {
			items: []rblnv1beta1.RBLNClusterPolicy{
				policy("zeta", consts.RBLNStateReady, older),
				policy("alpha", consts.RBLNStateReady, older),
			},
			wantName: "alpha",
		},
		"all ignored falls back to the first list item": {
			items: []rblnv1beta1.RBLNClusterPolicy{
				policy("first", consts.RBLNStateIgnored, older),
				policy("second", consts.RBLNStateIgnored, newer),
			},
			wantName: "first",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := pickActivePolicy(tc.items)
			if got.Name != tc.wantName {
				t.Errorf("pickActivePolicy() = %q, want %q", got.Name, tc.wantName)
			}
		})
	}
}
