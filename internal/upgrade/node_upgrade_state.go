package upgrade

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type NodeUpgradeStateProvider struct {
	K8sClient client.Client
	Log       logr.Logger
	nodeMutex KeyedMutex
}

func NewNodeUpgradeStateProvider(k8sClient client.Client, log logr.Logger) *NodeUpgradeStateProvider {
	return &NodeUpgradeStateProvider{
		K8sClient: k8sClient,
		Log:       log,
		nodeMutex: KeyedMutex{},
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
	p.Log.Info("Updating node upgrade state",
		"node", node.Name,
		"new state", newNodeState)

	err := p.patchNodeLabels(ctx, node, map[string]any{UpgradeStateLabelKey: newNodeState})
	if err != nil {
		p.Log.Error(err, "Failed to patch node state label on a node object",
			"node", node,
			"state", newNodeState)
		return err
	}

	p.Log.Info("Successfully changed node upgrade state label",
		"node", node.Name,
		"new state", newNodeState)

	return nil
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
	p.Log.Info("Updating node upgrade annotation",
		"node", node.Name,
		"annotationKey", key,
		"annotationValue", value)

	err := p.patchNodeAnnotations(ctx, node, map[string]any{key: value})
	if err != nil {
		p.Log.Error(err, "Failed to patch node state annotation on a node object",
			"node", node,
			"annotationKey", key,
			"annotationValue", value)
		return err
	}

	p.Log.Info("Successfully changed node upgrade state annotation",
		"node", node.Name,
		"annotationKey", key,
		"annotationValue", value)

	return nil
}

func (p *NodeUpgradeStateProvider) RemoveNodeUpgradeAnnotation(
	ctx context.Context, node *corev1.Node, key string,
) error {
	p.Log.Info("Removing node upgrade annotation",
		"node", node.Name,
		"annotationKey", key)

	err := p.patchNodeAnnotations(ctx, node, map[string]any{key: nil})
	if err != nil {
		p.Log.Error(err, "Failed to patch node state annotation on a node object",
			"node", node,
			"annotationKey", key)
		return err
	}

	p.Log.Info("Successfully removed node upgrade annotation",
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

func (p *NodeUpgradeStateProvider) patchNodeLabels(
	ctx context.Context, node *corev1.Node, labels map[string]any,
) error {
	unlock := p.nodeMutex.Lock(node.Name)
	defer unlock()

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
