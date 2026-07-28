package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

func policyWith(gen, observed int64, state string) *rblnv1beta1.RBLNClusterPolicy {
	p := &rblnv1beta1.RBLNClusterPolicy{ObjectMeta: metav1.ObjectMeta{Generation: gen}}
	p.Status.ObservedGeneration = observed
	p.Status.State = state
	return p
}

// A promoted successor changes only status (ignored → ready); the upgrade
// controller must wake on that, not just on spec generation bumps.
func TestClusterPolicyUpgradePredicateUpdate(t *testing.T) {
	tests := []struct {
		name     string
		old, new *rblnv1beta1.RBLNClusterPolicy
		want     bool
	}{
		{"spec generation bump", policyWith(1, 1, consts.RBLNStateReady), policyWith(2, 1, consts.RBLNStateReady), true},
		{"status promotion ignored->ready", policyWith(1, 1, consts.RBLNStateIgnored), policyWith(1, 1, consts.RBLNStateReady), true},
		{"observed generation settles", policyWith(2, 1, consts.RBLNStateReady), policyWith(2, 2, consts.RBLNStateReady), true},
		{"no relevant change", policyWith(1, 1, consts.RBLNStateReady), policyWith(1, 1, consts.RBLNStateReady), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := clusterPolicyUpgradePredicate.UpdateFunc(event.TypedUpdateEvent[*rblnv1beta1.RBLNClusterPolicy]{
				ObjectOld: tc.old, ObjectNew: tc.new,
			})
			if got != tc.want {
				t.Fatalf("predicate = %v, want %v", got, tc.want)
			}
		})
	}
}
