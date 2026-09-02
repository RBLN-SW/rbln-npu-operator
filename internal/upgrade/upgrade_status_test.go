package upgrade

import (
	"testing"

	"github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

func TestSummarizeClusterUpgrade_CountsAndReasons(t *testing.T) {
	skipped := newNodeUpgradeState("skipped1", UpgradeStateSkipped, "rev0")
	skipped.Node.Annotations = map[string]string{
		UpgradeSkipReasonAnnotationKey: "node drain failed: PDB webapp-pdb",
	}
	failed := newNodeUpgradeState("failed1", UpgradeStateFailed, "rev0")
	failed.Node.Annotations = map[string]string{
		UpgradeFailureReasonAnnotationKey: "pod-restart timeout after 60s: ImagePullBackOff",
	}

	state := newClusterState(map[string][]*NodeUpgradeState{
		UpgradeStateDone:               {newNodeUpgradeState("done1", UpgradeStateDone, "rev1")},
		UpgradeStateUpgradeRequired:    {newNodeUpgradeState("pending1", UpgradeStateUpgradeRequired, "rev0")},
		UpgradeStatePodRestartRequired: {newNodeUpgradeState("busy1", UpgradeStatePodRestartRequired, "rev0")},
		UpgradeStateSkipped:            {skipped},
		UpgradeStateFailed:             {failed},
	})

	summary := SummarizeClusterUpgrade(state)
	if summary.Total != 5 || summary.Done != 1 || summary.Pending != 1 ||
		summary.InProgress != 1 || summary.Skipped != 1 || summary.Failed != 1 {
		t.Fatalf("unexpected counts: %+v", summary)
	}
	if len(summary.SkippedNodes) != 1 || summary.SkippedNodes[0] != "skipped1" {
		t.Fatalf("unexpected skipped nodes: %v", summary.SkippedNodes)
	}
	if summary.SkippedNodeReasons["skipped1"] == "" {
		t.Fatal("expected skip reason to be captured")
	}
	if len(summary.FailedNodes) != 1 || summary.FailedNodes[0] != "failed1" {
		t.Fatalf("unexpected failed nodes: %v", summary.FailedNodes)
	}
	if summary.FailedNodeReasons["failed1"] == "" {
		t.Fatal("expected failure reason to be captured")
	}
	if summary.RolloutSettled() {
		t.Fatal("nodes are still pending/in progress; the rollout has not settled")
	}
}

func TestSummarizeClusterUpgrade_ProgressNeverFoldsSkippedIntoDone(t *testing.T) {
	state := newClusterState(map[string][]*NodeUpgradeState{
		UpgradeStateDone: {
			newNodeUpgradeState("done1", UpgradeStateDone, "rev1"),
			newNodeUpgradeState("done2", UpgradeStateDone, "rev1"),
		},
		UpgradeStateSkipped: {newNodeUpgradeState("skipped1", UpgradeStateSkipped, "rev0")},
	})

	summary := SummarizeClusterUpgrade(state)
	if got, want := summary.Progress(), "2/3 (1 skipped)"; got != want {
		t.Fatalf("progress = %q, want %q", got, want)
	}
	if got, want := summary.State(), v1beta1.DriverUpgradeStatePartiallyComplete; got != want {
		t.Fatalf("state = %q, want %q", got, want)
	}
}

func TestSummarizeClusterUpgrade_StateDerivation(t *testing.T) {
	tests := map[string]struct {
		states map[string][]*NodeUpgradeState
		want   string
	}{
		"all done is Complete": {
			states: map[string][]*NodeUpgradeState{
				UpgradeStateDone: {newNodeUpgradeState("n1", UpgradeStateDone, "rev1")},
			},
			want: v1beta1.DriverUpgradeStateComplete,
		},
		"moving nodes are InProgress": {
			states: map[string][]*NodeUpgradeState{
				UpgradeStateDrainRequired: {newNodeUpgradeState("n1", UpgradeStateDrainRequired, "rev0")},
			},
			want: v1beta1.DriverUpgradeStateInProgress,
		},
		"failed dominates everything": {
			states: map[string][]*NodeUpgradeState{
				UpgradeStateDrainRequired: {newNodeUpgradeState("n1", UpgradeStateDrainRequired, "rev0")},
				UpgradeStateSkipped:       {newNodeUpgradeState("n2", UpgradeStateSkipped, "rev0")},
				UpgradeStateFailed:        {newNodeUpgradeState("n3", UpgradeStateFailed, "rev0")},
			},
			want: v1beta1.DriverUpgradeStateDegraded,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := SummarizeClusterUpgrade(newClusterState(tc.states)).State(); got != tc.want {
				t.Fatalf("state = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSummarizeClusterUpgrade_CountsUniqueNodesOnce(t *testing.T) {
	a := newNodeUpgradeState("n1", UpgradeStateDone, "rev1")
	b := newNodeUpgradeStateWithDS("n1", UpgradeStateDone, "rev1", "")
	b.Node = a.Node

	state := newClusterState(map[string][]*NodeUpgradeState{
		UpgradeStateDone: {a, b},
	})
	if got := SummarizeClusterUpgrade(state).Total; got != 1 {
		t.Fatalf("total = %d, want 1", got)
	}
}

func TestNodesDescriptionTruncates(t *testing.T) {
	s := &ClusterUpgradeStatusSummary{
		FailedNodes:       []string{"a", "b", "c"},
		FailedNodeReasons: map[string]string{"a": "reason-a"},
	}
	got := s.FailedNodesDescription(2)
	want := "a (reason-a), b, and 1 more"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
