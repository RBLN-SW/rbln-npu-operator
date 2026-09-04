package controller

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	"github.com/rebellions-sw/rbln-npu-operator/internal/upgrade"
)

func findCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

func TestBuildUpgradeConditions(t *testing.T) {
	now := metav1.Now()

	tests := map[string]struct {
		summary       *upgrade.ClusterUpgradeStatusSummary
		lastTransit   metav1.Time
		wantTrueTypes map[string]bool
	}{
		"idle cluster": {
			summary:     &upgrade.ClusterUpgradeStatusSummary{Total: 5, Done: 5},
			lastTransit: now,
			wantTrueTypes: map[string]bool{
				consts.RBLNConditionTypeDriverUpgradeInProgress: false,
				consts.RBLNConditionTypeUpgradeDegraded:         false,
				consts.RBLNConditionTypeUpgradeIncomplete:       false,
				consts.RBLNConditionTypeUpgradeStalled:          false,
			},
		},
		"failed node degrades immediately even mid-rollout": {
			summary: &upgrade.ClusterUpgradeStatusSummary{
				Total: 5, Done: 2, Pending: 2, Failed: 1,
				FailedNodes:       []string{"n1"},
				FailedNodeReasons: map[string]string{"n1": "pod-restart timeout"},
			},
			lastTransit: now,
			wantTrueTypes: map[string]bool{
				consts.RBLNConditionTypeDriverUpgradeInProgress: true,
				consts.RBLNConditionTypeUpgradeDegraded:         true,
				consts.RBLNConditionTypeUpgradeIncomplete:       false,
				consts.RBLNConditionTypeUpgradeStalled:          false,
			},
		},
		"settled rollout with skipped nodes is incomplete": {
			summary: &upgrade.ClusterUpgradeStatusSummary{
				Total: 5, Done: 3, Skipped: 2,
				SkippedNodes: []string{"node-a3", "node-b7"},
				SkippedNodeReasons: map[string]string{
					"node-a3": "node drain failed: PDB webapp-pdb",
				},
			},
			lastTransit: now,
			wantTrueTypes: map[string]bool{
				consts.RBLNConditionTypeDriverUpgradeInProgress: false,
				consts.RBLNConditionTypeUpgradeDegraded:         false,
				consts.RBLNConditionTypeUpgradeIncomplete:       true,
				consts.RBLNConditionTypeUpgradeStalled:          false,
			},
		},
		"skipped nodes mid-rollout are not incomplete yet": {
			summary: &upgrade.ClusterUpgradeStatusSummary{
				Total: 5, Done: 2, Pending: 1, Skipped: 2,
				SkippedNodes: []string{"node-a3", "node-b7"},
			},
			lastTransit: now,
			wantTrueTypes: map[string]bool{
				consts.RBLNConditionTypeDriverUpgradeInProgress: true,
				consts.RBLNConditionTypeUpgradeDegraded:         false,
				consts.RBLNConditionTypeUpgradeIncomplete:       false,
				consts.RBLNConditionTypeUpgradeStalled:          false,
			},
		},
		"stalled rollout": {
			summary: &upgrade.ClusterUpgradeStatusSummary{
				Total: 5, Done: 2, InProgress: 1,
			},
			lastTransit: metav1.NewTime(now.Add(-time.Hour)),
			wantTrueTypes: map[string]bool{
				consts.RBLNConditionTypeDriverUpgradeInProgress: true,
				consts.RBLNConditionTypeUpgradeDegraded:         false,
				consts.RBLNConditionTypeUpgradeIncomplete:       false,
				consts.RBLNConditionTypeUpgradeStalled:          true,
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			block := &rblnv1beta1.DriverUpgradeStatus{LastTransitionTime: tc.lastTransit}
			conditions := buildUpgradeConditions(tc.summary, block, 3, now)
			if len(conditions) != 4 {
				t.Fatalf("expected 4 conditions, got %d", len(conditions))
			}
			for conditionType, wantTrue := range tc.wantTrueTypes {
				condition := findCondition(conditions, conditionType)
				if condition == nil {
					t.Fatalf("condition %s missing", conditionType)
				}
				gotTrue := condition.Status == metav1.ConditionTrue
				if gotTrue != wantTrue {
					t.Fatalf("condition %s = %v, want true=%v (message %q)",
						conditionType, condition.Status, wantTrue, condition.Message)
				}
				if condition.ObservedGeneration != 3 {
					t.Fatalf("condition %s observedGeneration = %d, want 3",
						conditionType, condition.ObservedGeneration)
				}
			}
		})
	}
}

func TestUpgradeConditionMessagesCarryTheRunbook(t *testing.T) {
	summary := &upgrade.ClusterUpgradeStatusSummary{
		Total: 4, Done: 1, Skipped: 2, Failed: 1,
		SkippedNodes: []string{"node-a3", "node-b7"},
		SkippedNodeReasons: map[string]string{
			"node-a3": "node drain failed: PDB webapp-pdb",
		},
		FailedNodes:       []string{"node-a17"},
		FailedNodeReasons: map[string]string{"node-a17": "pod-restart timeout (ImagePullBackOff)"},
	}

	degraded := degradedMessage(summary)
	for _, want := range []string{
		"node-a17", "pod-restart timeout (ImagePullBackOff)",
		"holds an upgrade slot",
		"kubectl annotate node <name> " + upgrade.UpgradeRequestedAnnotationKey + "=true",
		"new driver version",
	} {
		if !strings.Contains(degraded, want) {
			t.Fatalf("degraded message %q missing %q", degraded, want)
		}
	}

	incomplete := incompleteMessage(summary)
	for _, want := range []string{
		"2 node(s) skipped this rollout",
		"node-a3 (node drain failed: PDB webapp-pdb)", "node-b7",
		"Retry: kubectl annotate node <name> " + upgrade.UpgradeRequestedAnnotationKey + "=true",
	} {
		if !strings.Contains(incomplete, want) {
			t.Fatalf("incomplete message %q missing %q", incomplete, want)
		}
	}
}
