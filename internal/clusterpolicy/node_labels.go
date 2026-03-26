package clusterpolicy

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

const (
	labelValueTrue  = "true"
	labelValueFalse = "false"
)

var rblnDeviceLabels = map[string]string{
	"feature.node.kubernetes.io/pci-1200_1eff.present": labelValueTrue,
	"feature.node.kubernetes.io/pci-1eff.present":      labelValueTrue,
}

var rblnComponentLabels = map[string]map[string]string{
	consts.RBLNWorkloadConfigContainer: {
		"rebellions.ai/npu.deploy.driver":                labelValueTrue,
		"rebellions.ai/npu.deploy.device-plugin":         labelValueTrue,
		"rebellions.ai/npu.deploy.dra-kubelet-plugin":    labelValueTrue,
		"rebellions.ai/npu.deploy.metrics-exporter":      labelValueTrue,
		"rebellions.ai/npu.deploy.rbln-daemon":           labelValueTrue,
		"rebellions.ai/npu.deploy.npu-feature-discovery": labelValueTrue,
		"rebellions.ai/npu.deploy.operator-validator":    labelValueTrue,
		"rebellions.ai/npu.deploy.container-toolkit":     labelValueTrue,
	},
	consts.RBLNWorkloadConfigVMPassthrough: {
		"rebellions.ai/npu.deploy.vfio-manager":          labelValueTrue,
		"rebellions.ai/npu.deploy.sandbox-device-plugin": labelValueTrue,
	},
}

type nodeLabelResult struct {
	deployableRBLNNode bool
}

func ListNodes(ctx context.Context, k8sClient client.Client) (*corev1.NodeList, error) {
	nodeList := &corev1.NodeList{}
	if err := k8sClient.List(ctx, nodeList); err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}

	return nodeList, nil
}

func HasNFDLabeledNodes(nodeList *corev1.NodeList) bool {
	for i := range nodeList.Items {
		if hasNFDLabels(nodeList.Items[i].Labels) {
			return true
		}
	}

	return false
}

func (s *ClusterPolicyService) ReconcileNodeLabels(ctx context.Context, nodeList *corev1.NodeList) (int, error) {
	rblnNodeCount := 0

	for i := range nodeList.Items {
		result, err := s.reconcileNodeLabels(ctx, &nodeList.Items[i])
		if err != nil {
			return 0, err
		}
		if result.deployableRBLNNode {
			rblnNodeCount++
		}
	}

	return rblnNodeCount, nil
}

func (s *ClusterPolicyService) reconcileNodeLabels(ctx context.Context, node *corev1.Node) (nodeLabelResult, error) {
	labels := node.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}

	result := nodeLabelResult{}

	shouldUpdateNode := s.reconcilePresentLabel(node.Name, labels)
	if hasRBLNDeploySkipLabel(labels) {
		if removeAllRBLNComponentLabels(labels) {
			shouldUpdateNode = true
		}
		if hasRBLNPresentLabel(labels) {
			s.log.Info(
				"Skipping RBLN component deployment for node",
				"node", node.Name,
				"label", consts.RBLNDeploySkipLabelKey,
			)
		}
	} else if hasRBLNPresentLabel(labels) {
		workloadLabelsUpdated := s.reconcileWorkloadLabels(node.Name, labels)
		shouldUpdateNode = shouldUpdateNode || workloadLabelsUpdated
		result.deployableRBLNNode = true
	}

	if !shouldUpdateNode {
		return result, nil
	}

	node.SetLabels(labels)
	if err := s.client.Update(ctx, node); err != nil {
		return nodeLabelResult{}, fmt.Errorf("update node %s labels: %w", node.Name, err)
	}

	return result, nil
}

func (s *ClusterPolicyService) reconcilePresentLabel(nodeName string, labels map[string]string) bool {
	if !hasRBLNPresentLabel(labels) && hasRBLNDeviceLabel(labels) {
		s.log.Info("Rebellions device detected. Set RBLN Present Label", "node", nodeName)
		labels[consts.RBLNPresentLabelKey] = labelValueTrue
		return true
	}

	if hasRBLNPresentLabel(labels) && !hasRBLNDeviceLabel(labels) {
		s.log.Info("Rebellions device removed. Disable RBLN Present Label", "node", nodeName)
		labels[consts.RBLNPresentLabelKey] = labelValueFalse
		removeAllRBLNComponentLabels(labels)
		return true
	}

	return false
}

func (s *ClusterPolicyService) reconcileWorkloadLabels(nodeName string, labels map[string]string) bool {
	workloadConfig, err := getWorkloadConfig(labels, s.policy.Spec.WorkloadType)
	if err != nil {
		s.log.Info(
			"Failed to get RBLN workload config for node; using default",
			"node", nodeName,
			"defaultWorkloadConfig", workloadConfig,
			"error", err.Error(),
		)
	}

	return updateRBLNComponentLabels(labels, workloadConfig)
}

func hasRBLNPresentLabel(labels map[string]string) bool {
	return labels[consts.RBLNPresentLabelKey] == labelValueTrue
}

func hasRBLNDeploySkipLabel(labels map[string]string) bool {
	return labels[consts.RBLNDeploySkipLabelKey] == labelValueTrue
}

func hasRBLNDeviceLabel(labels map[string]string) bool {
	for key, value := range labels {
		if expected, ok := rblnDeviceLabels[key]; ok && expected == value {
			return true
		}
	}
	return false
}

func getWorkloadConfig(labels map[string]string, defaultWorkload string) (string, error) {
	workloadConfig, ok := labels[consts.RBLNWorkloadConfigLabelKey]
	if !ok {
		return defaultWorkload, fmt.Errorf("no NPU workload config label found")
	}
	if !isValidWorkloadConfig(workloadConfig) {
		return defaultWorkload, fmt.Errorf("invalid NPU workload config: %s", workloadConfig)
	}
	return workloadConfig, nil
}

func isValidWorkloadConfig(workloadConfig string) bool {
	_, ok := rblnComponentLabels[workloadConfig]
	return ok
}

func removeAllRBLNComponentLabels(labels map[string]string) bool {
	modified := false
	for _, labelsMap := range rblnComponentLabels {
		for key := range labelsMap {
			if _, exists := labels[key]; !exists {
				continue
			}
			delete(labels, key)
			modified = true
		}
	}
	return modified
}

func updateRBLNComponentLabels(labels map[string]string, config string) bool {
	modified := false

	for workloadConfig, labelsMap := range rblnComponentLabels {
		if workloadConfig == config {
			continue
		}
		for key := range labelsMap {
			if _, keep := rblnComponentLabels[config][key]; keep {
				continue
			}
			if _, exists := labels[key]; exists {
				delete(labels, key)
				modified = true
			}
		}
	}

	for key, value := range rblnComponentLabels[config] {
		if _, exists := labels[key]; !exists {
			labels[key] = value
			modified = true
		}
	}

	return modified
}

func hasNFDLabels(labels map[string]string) bool {
	for key := range labels {
		if strings.HasPrefix(key, consts.NFDLabelPrefix) {
			return true
		}
	}
	return false
}
