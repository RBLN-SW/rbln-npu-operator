package upgrade

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// cordonOwner names whose cordon a node carries, as far as the rollout can
// tell from the node alone.
type cordonOwner int

const (
	// cordonUnclaimed: nothing claims the node's cordon. If there is one, it
	// is an administrator's and no reconcile path may lift it.
	cordonUnclaimed cordonOwner = iota
	// cordonOwnedByOperator: this rollout took it, or adopted it.
	cordonOwnedByOperator
	// cordonOwnedByDriverManager: k8s-driver-manager took it on its own
	// eviction path (autoUpgrade off) and was killed or parked holding it.
	cordonOwnedByDriverManager
	// cordonOwnedByLegacyOperator: taken by an operator build that wrote no
	// claim. Such builds recorded an administrator's cordon in the
	// initial-state annotation and owned every other cordon on a node past
	// the cordon step. TODO(remove after two releases): drop once no rollout
	// started under such a build can still be in flight.
	cordonOwnedByLegacyOperator
)

// legacyOperatorCordonStates are the states a claim-less build's own cordon
// can be found in. cordon-required is excluded on purpose: a node there may
// carry an administrator's cordon the cordon step has not judged yet, and
// lifting an administrator's cordon is the worse mistake.
var legacyOperatorCordonStates = map[string]struct{}{
	UpgradeStateWaitForJobsRequired: {},
	UpgradeStatePodDeletionRequired: {},
	UpgradeStatePodRestartRequired:  {},
	UpgradeStateValidationRequired:  {},
	UpgradeStateUncordonRequired:    {},
	UpgradeStateSkipped:             {},
	UpgradeStateFailed:              {},
}

func cordonOwnerOf(node *corev1.Node) cordonOwner {
	// Presence with a value, not presence: a claim somebody emptied is not
	// one either side may act on, matching k8s-driver-manager's reading.
	switch node.Annotations[consts.DriverManagerCordonClaimAnnotation] {
	case consts.OperatorCordonClaimValue:
		return cordonOwnedByOperator
	case "":
	default:
		return cordonOwnedByDriverManager
	}
	if _, foreign := node.Annotations[UpgradeInitialStateAnnotationKey]; foreign {
		return cordonUnclaimed
	}
	if _, ok := legacyOperatorCordonStates[node.Labels[UpgradeStateLabelKey]]; ok {
		return cordonOwnedByLegacyOperator
	}
	return cordonUnclaimed
}

// OwnsCordon reports whether the rollout may lift the node's cordon on its
// own account: it took it, or a claim-less build did.
func OwnsCordon(node *corev1.Node) bool {
	owner := cordonOwnerOf(node)
	return owner == cordonOwnedByOperator || owner == cordonOwnedByLegacyOperator
}

// claimedCordon reports whether anything claims the node's cordon. Under
// autoUpgrade the rollout lifts k8s-driver-manager's cordon too, adopting it,
// because the binary never lifts one there; only an unclaimed cordon is left.
func claimedCordon(node *corev1.Node) bool {
	return cordonOwnerOf(node) != cordonUnclaimed
}

// CarriesRolloutBookkeeping reports whether a node holds anything the rollout
// wrote and must tear down: the state label, or the operator's cordon claim.
// The claim counts on its own because the label can be lost without it — an
// external tool scrubbing labels — and the cordon it marked would otherwise
// outlive every rollout as an administrator's.
func CarriesRolloutBookkeeping(node *corev1.Node) bool {
	if _, labeled := node.Labels[UpgradeStateLabelKey]; labeled {
		return true
	}
	return cordonOwnerOf(node) == cordonOwnedByOperator
}
