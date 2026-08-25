package upgrade

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

type NodeUpgradeStateProvider struct {
	K8sClient     client.Client
	nodeMutex     KeyedMutex
	eventRecorder record.EventRecorder
}

func NewNodeUpgradeStateProvider(
	k8sClient client.Client, eventRecorder record.EventRecorder,
) *NodeUpgradeStateProvider {
	return &NodeUpgradeStateProvider{
		K8sClient:     k8sClient,
		nodeMutex:     KeyedMutex{},
		eventRecorder: eventRecorder,
	}
}

func (p *NodeUpgradeStateProvider) GetNode(ctx context.Context, nodeName string) (*corev1.Node, error) {
	unlock := p.nodeMutex.Lock(nodeName)
	defer unlock()

	node := corev1.Node{}
	err := p.K8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, &node)
	if err != nil {
		return nil, err
	}
	return &node, nil
}

func (p *NodeUpgradeStateProvider) ChangeNodeUpgradeState(
	ctx context.Context, node *corev1.Node, newNodeState string,
) error {
	unlock := p.nodeMutex.Lock(node.Name)
	defer unlock()

	current := corev1.Node{}
	if err := p.K8sClient.Get(ctx, types.NamespacedName{Name: node.Name}, &current); err != nil {
		return err
	}
	oldNodeState := current.Labels[UpgradeStateLabelKey]
	if oldNodeState == newNodeState {
		return nil
	}

	log.FromContext(ctx).Info("Updating node upgrade state",
		"node", node.Name,
		"new state", newNodeState)

	err := p.patchNodeLabelsLocked(ctx, &current, map[string]any{UpgradeStateLabelKey: newNodeState})
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to patch node state label on a node object",
			"node", node.Name,
			"state", newNodeState)
		return err
	}

	// The client is uncached, so a successful patch IS the commit point: a
	// verification GET here could fail transiently after the label already
	// changed, and the same-state guard would then swallow the event forever.
	if current.Labels == nil {
		current.Labels = map[string]string{}
	}
	current.Labels[UpgradeStateLabelKey] = newNodeState
	p.recordStateTransitionEvent(&current, oldNodeState, newNodeState)
	if node.Labels == nil {
		node.Labels = map[string]string{}
	}
	node.Labels[UpgradeStateLabelKey] = newNodeState

	log.FromContext(ctx).Info("Successfully changed node upgrade state label",
		"node", node.Name,
		"new state", newNodeState)

	return nil
}

// recordStateTransitionEvent emits only the contract transitions (upgrade
// started/completed/failed); everything else — including the initial
// unknown→done bootstrap — stays silent.
func (p *NodeUpgradeStateProvider) recordStateTransitionEvent(node *corev1.Node, oldState, newState string) {
	switch {
	case newState == UpgradeStateCordonRequired && oldState == UpgradeStateUpgradeRequired:
		recordNodeEvent(p.eventRecorder, node, corev1.EventTypeNormal,
			consts.RBLNEventReasonDriverUpgradeStarted, "Driver upgrade started; node will be cordoned")
	case newState == UpgradeStateDone && IsInProgressUpgradeState(oldState):
		recordNodeEvent(p.eventRecorder, node, corev1.EventTypeNormal,
			consts.RBLNEventReasonDriverUpgradeCompleted, "Driver upgrade completed; node is schedulable")
	case newState == UpgradeStateFailed && oldState != UpgradeStateFailed:
		recordNodeEvent(p.eventRecorder, node, corev1.EventTypeWarning,
			consts.RBLNEventReasonDriverUpgradeFailed,
			fmt.Sprintf("Driver upgrade failed at state %q", logKeyForNodeState(oldState)))
	}
}

func (p *NodeUpgradeStateProvider) ChangeNodeUpgradeAnnotation(
	ctx context.Context, node *corev1.Node, key string, value string,
) error {
	if value == nullString {
		return p.RemoveNodeUpgradeAnnotation(ctx, node, key)
	}

	return p.SetNodeUpgradeAnnotation(ctx, node, key, value)
}

func (p *NodeUpgradeStateProvider) SetNodeUpgradeAnnotation(
	ctx context.Context, node *corev1.Node, key string, value string,
) error {
	log.FromContext(ctx).Info("Updating node upgrade annotation",
		"node", node.Name,
		"annotationKey", key,
		"annotationValue", value)

	err := p.patchNodeAnnotations(ctx, node, map[string]any{key: value})
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to patch node state annotation on a node object",
			"node", node,
			"annotationKey", key,
			"annotationValue", value)
		return err
	}

	log.FromContext(ctx).Info("Successfully changed node upgrade state annotation",
		"node", node.Name,
		"annotationKey", key,
		"annotationValue", value)

	return nil
}

func (p *NodeUpgradeStateProvider) RemoveNodeUpgradeAnnotation(
	ctx context.Context, node *corev1.Node, key string,
) error {
	log.FromContext(ctx).Info("Removing node upgrade annotation",
		"node", node.Name,
		"annotationKey", key)

	err := p.patchNodeAnnotations(ctx, node, map[string]any{key: nil})
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to patch node state annotation on a node object",
			"node", node,
			"annotationKey", key)
		return err
	}

	log.FromContext(ctx).Info("Successfully removed node upgrade annotation",
		"node", node.Name,
		"annotationKey", key)

	return nil
}

func (p *NodeUpgradeStateProvider) patchNodeAnnotations(
	ctx context.Context, node *corev1.Node, annotations map[string]any,
) error {
	unlock := p.nodeMutex.Lock(node.Name)
	defer unlock()

	patchString, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": annotations,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to marshal annotation patch for node %q: %w", node.Name, err)
	}

	patch := client.RawPatch(types.MergePatchType, patchString)
	return p.K8sClient.Patch(ctx, node, patch)
}

// patchNodeLabelsLocked assumes the caller holds the node's mutex.
func (p *NodeUpgradeStateProvider) patchNodeLabelsLocked(
	ctx context.Context, node *corev1.Node, labels map[string]any,
) error {
	patchString, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"labels": labels,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to marshal label patch for node %q: %w", node.Name, err)
	}

	patch := client.RawPatch(types.StrategicMergePatchType, patchString)
	return p.K8sClient.Patch(ctx, node, patch)
}
