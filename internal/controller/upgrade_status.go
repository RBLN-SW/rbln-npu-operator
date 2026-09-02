package controller

import (
	"context"
	"fmt"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	"github.com/rebellions-sw/rbln-npu-operator/internal/upgrade"
)

// upgradeStalledAfter is how long the node counts may stay unchanged before UpgradeStalled turns True.
const upgradeStalledAfter = 30 * time.Minute

// nodesDescribedInMessages caps how many nodes a condition message enumerates.
const nodesDescribedInMessages = 5

// upgradeConditionTypes are the RBLNClusterPolicy conditions owned by the upgrade controller.
var upgradeConditionTypes = []string{
	consts.RBLNConditionTypeDriverUpgradeInProgress,
	consts.RBLNConditionTypeUpgradeDegraded,
	consts.RBLNConditionTypeUpgradeIncomplete,
	consts.RBLNConditionTypeUpgradeStalled,
}

// publishUpgradeStatus writes the rollout summary and its conditions to the
// policy status. It only touches upgrade-owned fields; the policy controller's
// fields are read fresh and left as-is, and conflicts are retried.
func (r *UpgradeReconciler) publishUpgradeStatus(
	ctx context.Context,
	policy *rblnv1beta1.RBLNClusterPolicy,
	summary *upgrade.ClusterUpgradeStatusSummary,
) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		instance := &rblnv1beta1.RBLNClusterPolicy{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(policy), instance); err != nil {
			return err
		}

		now := metav1.Now()
		block := &rblnv1beta1.DriverUpgradeStatus{
			Total:              summary.Total,
			Done:               summary.Done,
			InProgress:         summary.InProgress,
			Pending:            summary.Pending,
			Skipped:            summary.Skipped,
			Failed:             summary.Failed,
			State:              summary.State(),
			Progress:           summary.Progress(),
			LastTransitionTime: now,
		}
		// The transition clock only advances when the counts change, so a
		// long-unchanged clock with nodes in progress means a stalled rollout.
		if prev := instance.Status.DriverUpgrade; prev != nil &&
			prev.Total == block.Total && prev.Done == block.Done &&
			prev.InProgress == block.InProgress && prev.Pending == block.Pending &&
			prev.Skipped == block.Skipped && prev.Failed == block.Failed &&
			!prev.LastTransitionTime.IsZero() {
			block.LastTransitionTime = prev.LastTransitionTime
		}

		instance.Status.DriverUpgrade = block
		for _, condition := range buildUpgradeConditions(summary, block, instance.Generation, now) {
			apimeta.SetStatusCondition(&instance.Status.Conditions, condition)
		}

		return r.Status().Update(ctx, instance)
	})
	if err != nil {
		return fmt.Errorf("publish driver upgrade status: %w", err)
	}
	return nil
}

// clearUpgradeStatus removes the upgrade-owned status block and conditions.
func (r *UpgradeReconciler) clearUpgradeStatus(
	ctx context.Context, policy *rblnv1beta1.RBLNClusterPolicy,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		instance := &rblnv1beta1.RBLNClusterPolicy{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(policy), instance); err != nil {
			return err
		}
		changed := instance.Status.DriverUpgrade != nil
		instance.Status.DriverUpgrade = nil
		for _, conditionType := range upgradeConditionTypes {
			if apimeta.RemoveStatusCondition(&instance.Status.Conditions, conditionType) {
				changed = true
			}
		}
		if !changed {
			return nil
		}
		return r.Status().Update(ctx, instance)
	})
}

func degradedMessage(summary *upgrade.ClusterUpgradeStatusSummary) string {
	return fmt.Sprintf(
		"%d node(s) failed at or after the driver swap — %s. Each failed node holds an upgrade slot, reducing rollout parallelism until it is resolved. "+
			"Retry: kubectl annotate node <name> %s=true, or push a new driver version.",
		summary.Failed, summary.FailedNodesDescription(nodesDescribedInMessages),
		upgrade.UpgradeRequestedAnnotationKey)
}

func incompleteMessage(summary *upgrade.ClusterUpgradeStatusSummary) string {
	return fmt.Sprintf("%d node(s) skipped this rollout — %s\nRetry: kubectl annotate node <name> %s=true",
		summary.Skipped, summary.SkippedNodesDescription(nodesDescribedInMessages),
		upgrade.UpgradeRequestedAnnotationKey)
}

func buildUpgradeConditions(
	summary *upgrade.ClusterUpgradeStatusSummary,
	block *rblnv1beta1.DriverUpgradeStatus,
	generation int64,
	now metav1.Time,
) []metav1.Condition {
	progressMessage := fmt.Sprintf("done %d/%d, in progress %d, pending %d, skipped %d, failed %d",
		summary.Done, summary.Total, summary.InProgress, summary.Pending, summary.Skipped, summary.Failed)

	inProgress := metav1.Condition{
		Type:               consts.RBLNConditionTypeDriverUpgradeInProgress,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: generation,
		Reason:             consts.RBLNConditionReasonNoUpgradeInProgress,
		Message:            progressMessage,
	}
	if summary.InProgress+summary.Pending > 0 {
		inProgress.Status = metav1.ConditionTrue
		inProgress.Reason = consts.RBLNConditionReasonUpgradeInProgress
	}

	degraded := metav1.Condition{
		Type:               consts.RBLNConditionTypeUpgradeDegraded,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: generation,
		Reason:             consts.RBLNConditionReasonNoFailedNodes,
		Message:            "no upgrade-failed nodes",
	}
	if summary.Failed > 0 {
		degraded.Status = metav1.ConditionTrue
		degraded.Reason = consts.RBLNConditionReasonFailedNodes
		degraded.Message = degradedMessage(summary)
	}

	// Incomplete turns True only once the rollout has settled — the moment a
	// partial completion could masquerade as a finished one.
	incomplete := metav1.Condition{
		Type:               consts.RBLNConditionTypeUpgradeIncomplete,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: generation,
		Reason:             consts.RBLNConditionReasonNoSkippedNodes,
		Message:            "no nodes were skipped",
	}
	switch {
	case summary.Skipped > 0 && summary.RolloutSettled():
		incomplete.Status = metav1.ConditionTrue
		incomplete.Reason = consts.RBLNConditionReasonNodesSkipped
		incomplete.Message = incompleteMessage(summary)
	case summary.Skipped > 0:
		incomplete.Reason = consts.RBLNConditionReasonRolloutInProgress
		incomplete.Message = fmt.Sprintf("%d node(s) skipped so far; rollout still in progress", summary.Skipped)
	}

	stalled := metav1.Condition{
		Type:               consts.RBLNConditionTypeUpgradeStalled,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: generation,
		Reason:             consts.RBLNConditionReasonProgressing,
		Message:            "rollout is progressing",
	}
	if summary.InProgress > 0 &&
		now.Sub(block.LastTransitionTime.Time) > upgradeStalledAfter {
		stalled.Status = metav1.ConditionTrue
		stalled.Reason = consts.RBLNConditionReasonNoRecentTransition
		stalled.Message = fmt.Sprintf(
			"%d node(s) in progress but no state transition since %s",
			summary.InProgress, block.LastTransitionTime.Format(time.RFC3339))
	}

	return []metav1.Condition{inProgress, degraded, incomplete, stalled}
}
