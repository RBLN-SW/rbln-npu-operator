package upgrade

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// CordonManagerInterface abstracts node cordon/uncordon operations for testability.
type CordonManagerInterface interface {
	// Cordon takes the node out of service for the rollout and claims the
	// cordon in the same write. A cordon already on the node is reclaimed
	// when the claim is the rollout's, adopted when it is k8s-driver-manager's,
	// and reported foreign — an administrator's, left untouched — when
	// nothing claims it.
	Cordon(ctx context.Context, node *corev1.Node) (foreign bool, err error)
	// Uncordon lifts a claimed cordon, clearing the claim and the blocked mark
	// in the same write; an unclaimed cordon is left in place.
	Uncordon(ctx context.Context, node *corev1.Node) error
}

type CordonManager struct {
	k8sInterface kubernetes.Interface
}

// The cordon writes are hand-built merge patches so the claim lands in the
// same write as the cordon: split across two, a process killed in between
// leaves either a cordon nobody claims or a claim on a schedulable node. The
// resourceVersion makes the cordon conditional on the node just read — a
// cordon somebody else places in between is refused with a Conflict instead
// of being stamped with a claim it never had, and the retry reads it back as
// foreign. The uncordon needs no condition: it only ever removes.
func cordonPatch(resourceVersion string) []byte {
	return fmt.Appendf(nil,
		`{"metadata":{"resourceVersion":%q,"annotations":{%q:%q,%q:null}},"spec":{"unschedulable":true}}`,
		resourceVersion, consts.DriverManagerCordonClaimAnnotation, consts.OperatorCordonClaimValue,
		consts.DriverManagerEvictionBlockedAnnotation)
}

// adoptPatch takes over a cordon k8s-driver-manager left, together with the
// blocked mark that explained it: both are the binary's record, and the
// rollout now answers for the cordon.
func adoptPatch(resourceVersion string) []byte {
	return fmt.Appendf(nil,
		`{"metadata":{"resourceVersion":%q,"annotations":{%q:%q,%q:null}}}`,
		resourceVersion, consts.DriverManagerCordonClaimAnnotation, consts.OperatorCordonClaimValue,
		consts.DriverManagerEvictionBlockedAnnotation)
}

var uncordonPatch = []byte(`{"metadata":{"annotations":{"` + consts.DriverManagerCordonClaimAnnotation + `":null,"` +
	consts.DriverManagerEvictionBlockedAnnotation + `":null}},"spec":{"unschedulable":false}}`)

func (m *CordonManager) Cordon(ctx context.Context, node *corev1.Node) (bool, error) {
	logger := log.FromContext(ctx).WithValues("node", node.Name)
	foreign := false
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current, err := m.k8sInterface.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		foreign = false
		if !current.Spec.Unschedulable {
			logger.Info("Cordoning node")
			return m.patch(ctx, node.Name, cordonPatch(current.ResourceVersion))
		}
		switch cordonOwnerOf(current) {
		case cordonOwnedByOperator, cordonOwnedByLegacyOperator:
			logger.Info("Node is already cordoned by this rollout, reclaiming the cordon")
			return nil
		case cordonOwnedByDriverManager:
			logger.Info("Adopting the cordon k8s-driver-manager left on the node; the rollout will lift it",
				"annotation", consts.DriverManagerCordonClaimAnnotation)
			return m.patch(ctx, node.Name, adoptPatch(current.ResourceVersion))
		default:
			logger.Info("Node is already cordoned and nothing claims the cordon; it is the administrator's and stays")
			foreign = true
			return nil
		}
	})
	if err != nil {
		return false, fmt.Errorf("failed to cordon node %q: %w", node.Name, err)
	}
	return foreign, nil
}

func (m *CordonManager) Uncordon(ctx context.Context, node *corev1.Node) error {
	logger := log.FromContext(ctx).WithValues("node", node.Name)
	current, err := m.k8sInterface.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to read node %q before uncordon: %w", node.Name, err)
	}
	if !claimedCordon(current) {
		if current.Spec.Unschedulable {
			logger.Info("Node is cordoned but nothing claims the cordon; leaving the administrator's cordon in place")
		}
		return nil
	}
	// Clearing the claim matters even when the node is already schedulable:
	// left behind, it would make the administrator's next cordon read as the
	// rollout's.
	logger.Info("Uncordoning node")
	if err := m.patch(ctx, node.Name, uncordonPatch); err != nil {
		return fmt.Errorf("failed to uncordon node %q: %w", node.Name, err)
	}
	return nil
}

func (m *CordonManager) patch(ctx context.Context, nodeName string, patch []byte) error {
	_, err := m.k8sInterface.CoreV1().Nodes().Patch(ctx, nodeName, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

func NewCordonManager(k8sInterface kubernetes.Interface) *CordonManager {
	return &CordonManager{
		k8sInterface: k8sInterface,
	}
}
