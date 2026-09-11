package upgrade

import (
	"fmt"
	"sort"
	"strings"

	"github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

// ClusterUpgradeStatusSummary is a per-reconcile snapshot of rollout progress.
type ClusterUpgradeStatusSummary struct {
	Total      int32
	Done       int32
	InProgress int32
	// Pending includes not-yet-triaged (unknown) nodes.
	Pending int32
	Skipped int32
	Failed  int32

	SkippedNodes       []string
	SkippedNodeReasons map[string]string
	FailedNodes        []string
	FailedNodeReasons  map[string]string
}

// RolloutSettled reports that every node reached a terminal disposition.
func (s *ClusterUpgradeStatusSummary) RolloutSettled() bool {
	return s.InProgress == 0 && s.Pending == 0
}

// Progress renders the printcolumn summary; skipped nodes are never folded into done.
func (s *ClusterUpgradeStatusSummary) Progress() string {
	if s.Skipped > 0 {
		return fmt.Sprintf("%d/%d (%d skipped)", s.Done, s.Total, s.Skipped)
	}
	return fmt.Sprintf("%d/%d", s.Done, s.Total)
}

// State derives the rollout-level disposition; failed nodes dominate.
func (s *ClusterUpgradeStatusSummary) State() string {
	switch {
	case s.Failed > 0:
		return v1beta1.DriverUpgradeStateDegraded
	case s.InProgress+s.Pending > 0:
		return v1beta1.DriverUpgradeStateInProgress
	case s.Skipped > 0:
		return v1beta1.DriverUpgradeStatePartiallyComplete
	default:
		return v1beta1.DriverUpgradeStateComplete
	}
}

func (s *ClusterUpgradeStatusSummary) SkippedNodesDescription(maxEntries int) string {
	return describeNodes(s.SkippedNodes, s.SkippedNodeReasons, maxEntries)
}

func (s *ClusterUpgradeStatusSummary) FailedNodesDescription(maxEntries int) string {
	return describeNodes(s.FailedNodes, s.FailedNodeReasons, maxEntries)
}

func describeNodes(names []string, reasons map[string]string, maxEntries int) string {
	if len(names) == 0 {
		return ""
	}
	entries := make([]string, 0, len(names))
	for i, name := range names {
		if maxEntries > 0 && i >= maxEntries {
			entries = append(entries, fmt.Sprintf("and %d more", len(names)-maxEntries))
			break
		}
		if reason := reasons[name]; reason != "" {
			entries = append(entries, fmt.Sprintf("%s (%s)", name, reason))
		} else {
			entries = append(entries, name)
		}
	}
	return strings.Join(entries, ", ")
}

// SummarizeClusterUpgrade aggregates per-node dispositions from one reconcile's cluster state.
func SummarizeClusterUpgrade(currentClusterState *ClusterUpgradeState) *ClusterUpgradeStatusSummary {
	summary := &ClusterUpgradeStatusSummary{
		SkippedNodeReasons: map[string]string{},
		FailedNodeReasons:  map[string]string{},
	}
	if currentClusterState == nil {
		return summary
	}

	forEachUniqueNode(currentClusterState, func(nodeState *NodeUpgradeState) {
		summary.Total++
		node := nodeState.Node
		switch state := node.Labels[UpgradeStateLabelKey]; {
		case state == UpgradeStateDone:
			summary.Done++
		case state == UpgradeStateSkipped:
			summary.Skipped++
			summary.SkippedNodes = append(summary.SkippedNodes, node.Name)
			if reason := node.Annotations[UpgradeSkipReasonAnnotationKey]; reason != "" {
				summary.SkippedNodeReasons[node.Name] = reason
			}
		case state == UpgradeStateFailed:
			summary.Failed++
			summary.FailedNodes = append(summary.FailedNodes, node.Name)
			if reason := node.Annotations[UpgradeFailureReasonAnnotationKey]; reason != "" {
				summary.FailedNodeReasons[node.Name] = reason
			}
		case IsInProgressUpgradeState(state):
			summary.InProgress++
		default:
			summary.Pending++
		}
	})
	sort.Strings(summary.SkippedNodes)
	sort.Strings(summary.FailedNodes)

	return summary
}

// forEachUniqueNode visits every node exactly once, even if it appears in more
// than one bucket (e.g. an owned and an orphaned pod on the same node).
func forEachUniqueNode(state *ClusterUpgradeState, visit func(*NodeUpgradeState)) {
	seen := make(map[string]struct{})
	for _, bucket := range state.NodeStates {
		for _, nodeState := range bucket {
			if _, ok := seen[nodeState.Node.Name]; ok {
				continue
			}
			seen[nodeState.Node.Name] = struct{}{}
			visit(nodeState)
		}
	}
}
