package clusterpolicy

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

const (
	labelValueTrue  = "true"
	labelValueFalse = "false"
)

var rblnDeviceLabels = map[string]string{
	consts.NFDDevicePCIAltLabelKey: labelValueTrue,
	consts.NFDDevicePCILabelKey:    labelValueTrue,
}

var rblnComponentLabels = map[string]map[string]string{
	consts.RBLNWorkloadConfigContainer: {
		consts.RBLNDeployDriverLabelKey:                  labelValueTrue,
		"rebellions.ai/npu.deploy.device-plugin":         labelValueTrue,
		"rebellions.ai/npu.deploy.dra-kubelet-plugin":    labelValueTrue,
		"rebellions.ai/npu.deploy.metrics-exporter":      labelValueTrue,
		consts.RBLNDeployRBLNDaemonLabelKey:              labelValueTrue,
		"rebellions.ai/npu.deploy.npu-feature-discovery": labelValueTrue,
		"rebellions.ai/npu.deploy.operator-validator":    labelValueTrue,
		"rebellions.ai/npu.deploy.container-toolkit":     labelValueTrue,
	},
	consts.RBLNWorkloadConfigVMPassthrough: {
		"rebellions.ai/npu.deploy.vfio-manager":          labelValueTrue,
		"rebellions.ai/npu.deploy.sandbox-device-plugin": labelValueTrue,
		"rebellions.ai/npu.deploy.dra-kubelet-plugin": labelValueTrue,
	},
}

// ListAndClassifyNodes returns NPU-candidate nodes and whether NFD is
// installed.  It issues targeted label-selector queries so that only
// relevant nodes are fetched from the cache, avoiding a full-cluster scan
// in the common case.
func ListAndClassifyNodes(ctx context.Context, k8sClient client.Client) (candidates []corev1.Node, nfdInstalled bool, err error) {
	// 1. Nodes with NFD device labels — new NPU node discovery.
	//    If any exist, NFD is necessarily installed (it sets these labels).
	for _, key := range []string{consts.NFDDevicePCILabelKey, consts.NFDDevicePCIAltLabelKey} {
		list := &corev1.NodeList{}
		if err := k8sClient.List(ctx, list, client.MatchingLabels{key: labelValueTrue}); err != nil {
			return nil, false, fmt.Errorf("list device nodes (%s): %w", key, err)
		}
		candidates = append(candidates, list.Items...)
	}

	if len(candidates) > 0 {
		nfdInstalled = true
	}

	// 2. Already-managed RBLN nodes — handles device-removal cleanup.
	rblnList := &corev1.NodeList{}
	if err := k8sClient.List(ctx, rblnList, client.MatchingLabels{consts.RBLNPresentLabelKey: labelValueTrue}); err != nil {
		return nil, false, fmt.Errorf("list RBLN present nodes: %w", err)
	}
	candidates = deduplicateNodes(append(candidates, rblnList.Items...))

	return candidates, nfdInstalled, nil
}

func deduplicateNodes(nodes []corev1.Node) []corev1.Node {
	seen := make(map[string]struct{}, len(nodes))
	result := make([]corev1.Node, 0, len(nodes))
	for i := range nodes {
		if _, ok := seen[nodes[i].Name]; ok {
			continue
		}
		seen[nodes[i].Name] = struct{}{}
		result = append(result, nodes[i])
	}
	return result
}

// NodeCensus excludes skip-labeled and non-NPU nodes. TotalNPU may exceed
// ContainerNodes + VMPassthroughNodes when a node's workload label is
// missing or unrecognised.
type NodeCensus struct {
	TotalNPU           int32
	ContainerNodes     int32
	VMPassthroughNodes int32
}

// CountFor returns 0 for unknown workload types.
func (c NodeCensus) CountFor(workload string) int32 {
	switch workload {
	case consts.RBLNWorkloadConfigContainer:
		return c.ContainerNodes
	case consts.RBLNWorkloadConfigVMPassthrough:
		return c.VMPassthroughNodes
	case consts.RBLNWorkloadConfigAll:
		return c.ContainerNodes + c.VMPassthroughNodes
	default:
		return 0
	}
}

// ReconcileNodes issues at most one Update call per candidate node, batching
// label and annotation changes into a single PATCH.
func (s *ClusterPolicyService) ReconcileNodes(ctx context.Context, candidates []corev1.Node) (NodeCensus, error) {
	shouldEnableUpgrade := shouldEnableDriverAutoUpgrade(s.policy)
	var census NodeCensus

	for i := range candidates {
		node := &candidates[i]

		labelsChanged := s.reconcileNodeLabelsInPlace(node)
		annotationsChanged := reconcileAutoUpgradeAnnotationInPlace(node, shouldEnableUpgrade)

		if labelsChanged || annotationsChanged {
			if err := s.client.Update(ctx, node); err != nil {
				return NodeCensus{}, fmt.Errorf("update node %s: %w", node.Name, err)
			}
		}

		labels := node.GetLabels()
		if hasRBLNDeploySkipLabel(labels) || !hasRBLNPresentLabel(labels) {
			continue
		}
		census.TotalNPU++
		workload, _ := getWorkloadConfig(labels, s.policy.Spec.WorkloadType)
		switch workload {
		case consts.RBLNWorkloadConfigContainer:
			census.ContainerNodes++
		case consts.RBLNWorkloadConfigVMPassthrough:
			census.VMPassthroughNodes++
		}
	}

	return census, nil
}

// reconcileNodeLabelsInPlace adjusts RBLN labels on the node in-place
// and returns whether any label was changed.
func (s *ClusterPolicyService) reconcileNodeLabelsInPlace(node *corev1.Node) bool {
	labels := node.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}

	changed := s.reconcilePresentLabel(node.Name, labels)
	if hasRBLNDeploySkipLabel(labels) {
		if removeAllRBLNComponentLabels(labels) {
			changed = true
		}
		if hasRBLNPresentLabel(labels) {
			s.log.Info(
				"Skipping RBLN component deployment for node",
				"node", node.Name,
				"label", consts.RBLNDeploySkipLabelKey,
			)
		}
	} else if hasRBLNPresentLabel(labels) {
		if s.reconcileWorkloadLabels(node.Name, labels) {
			changed = true
		}
	}

	if changed {
		node.SetLabels(labels)
	}

	return changed
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
		s.log.V(consts.LogLevelDebug).Info(
			"Using default workload config for node",
			"node", nodeName,
			"workloadConfig", workloadConfig,
			"reason", err.Error(),
		)
	}

	return updateRBLNComponentLabels(labels, workloadConfig)
}

func hasRBLNPresentLabel(labels map[string]string) bool {
	return labels[consts.RBLNPresentLabelKey] == labelValueTrue
}

func hasDriverDeployLabel(labels map[string]string) bool {
	return labels[consts.RBLNDeployDriverLabelKey] == labelValueTrue
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
	desired := desiredComponentLabels(labels, config)
	modified := false

	for _, labelsMap := range rblnComponentLabels {
		for key := range labelsMap {
			if _, keep := desired[key]; keep {
				continue
			}
			if _, exists := labels[key]; exists {
				delete(labels, key)
				modified = true
			}
		}
	}

	for key, value := range desired {
		if _, exists := labels[key]; !exists {
			labels[key] = value
			modified = true
		}
	}

	return modified
}

func desiredComponentLabels(labels map[string]string, config string) map[string]string {
	base := rblnComponentLabels[config]
	if config != consts.RBLNWorkloadConfigContainer ||
		labels[consts.RBLNDeployDriverLabelKey] != consts.RBLNDeployDriverPreInstalled {
		return base
	}

	desired := make(map[string]string, len(base))
	for key, value := range base {
		if key == consts.RBLNDeployRBLNDaemonLabelKey {
			continue
		}
		desired[key] = value
	}
	return desired
}
