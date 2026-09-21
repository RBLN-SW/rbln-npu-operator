package clusterpolicy

import (
	"context"
	"fmt"
	"sort"

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
		consts.RBLNDeploySmdLabelKey:                     labelValueTrue,
		"rebellions.ai/npu.deploy.npu-feature-discovery": labelValueTrue,
		"rebellions.ai/npu.deploy.operator-validator":    labelValueTrue,
		"rebellions.ai/npu.deploy.container-toolkit":     labelValueTrue,
	},
	consts.RBLNWorkloadConfigVMPassthrough: {
		"rebellions.ai/npu.deploy.vfio-manager":          labelValueTrue,
		"rebellions.ai/npu.deploy.sandbox-device-plugin": labelValueTrue,
		"rebellions.ai/npu.deploy.dra-kubelet-plugin":    labelValueTrue,
	},
}

// legacyComponentLabelKeys are deploy labels the operator no longer applies.
// They are swept but never added: the prune loops below only walk
// rblnComponentLabels, so a key dropped from that map would otherwise sit on
// every already-labeled node forever, naming a component that no longer exists.
// TODO(remove after two releases): drop together with the clusterpolicy legacy
// rbln-daemon cleanup once no supported upgrade path carries these keys.
var legacyComponentLabelKeys = []string{
	consts.RBLNDeployRBLNDaemonLabelKey,
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
		// After the fill-only pass, so a stale pause is judged on the keys the
		// node should carry now.
		pausedRestored := s.restoreStalePausedLabels(ctx, node)
		annotationsChanged := reconcileAutoUpgradeAnnotationInPlace(node, shouldEnableUpgrade)

		if labelsChanged || pausedRestored || annotationsChanged {
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
		s.log.V(consts.VDebug).Info(
			"Using default workload config for node",
			"node", nodeName,
			"workloadConfig", workloadConfig,
			"error", err,
		)
	}

	modified := updateRBLNComponentLabels(labels, workloadConfig)

	// A deploy label left empty gates its component off for as long as it stays
	// empty, and the fill-only loop above will not repair it. Nothing here can
	// safely fix it, so make it visible instead of letting a node sit silently
	// without a component.
	if empty := emptyDesiredComponentLabels(labels, workloadConfig); len(empty) > 0 {
		s.log.Info(
			"Component deploy labels are empty; those components stay gated off until the value is restored",
			"node", nodeName,
			"labels", empty,
		)
	}

	return modified
}

// emptyDesiredComponentLabels returns the deploy labels this node should carry
// as "true" but whose value is empty, sorted for a stable log line.
func emptyDesiredComponentLabels(labels map[string]string, config string) []string {
	desired := desiredComponentLabels(labels, config)
	empty := make([]string, 0, len(desired))
	for key := range desired {
		if value, exists := labels[key]; exists && value == "" {
			empty = append(empty, key)
		}
	}
	sort.Strings(empty)
	return empty
}

// restoreStalePausedLabels returns a node's deploy labels from
// paused-for-driver-upgrade to "true" once the pause can no longer be live, and
// reports whether it changed any. k8s-driver-manager pauses them at the start
// of a driver (re)install and restores them at the end of the run, so a pause
// outlives its run only when the run was killed in between or its final label
// write failed. Fill-only reconciliation never repairs it, and the node's
// components stay gated off with nothing reporting why.
//
// The pause is live while any k8s-driver-manager init container on the node
// is running, or has not reported a state yet; undoing it then would
// reschedule the very components the run is evicting. Only once every such
// init container has terminated is the pause stale: a terminated run has
// already made its restore attempt, and a failed attempt's successor pauses
// again anyway. With no such pod on the node there is nothing to judge by, so
// the pause is presumed live and the next driver pod's run restores it in the
// ordinary way. Values other than the pause, the user's "false" opt-out
// included, are never rewritten.
func (s *ClusterPolicyService) restoreStalePausedLabels(ctx context.Context, node *corev1.Node) bool {
	labels := node.GetLabels()
	if hasRBLNDeploySkipLabel(labels) || !hasRBLNPresentLabel(labels) {
		return false
	}
	workload, _ := getWorkloadConfig(labels, s.policy.Spec.WorkloadType)
	paused := pausedDesiredComponentLabels(labels, workload)
	if len(paused) == 0 {
		return false
	}

	idle, err := s.driverManagerIdle(ctx, node.Name)
	if err != nil {
		s.log.Error(err, "Cannot tell whether k8s-driver-manager is running; leaving paused deploy labels alone",
			"node", node.Name, "labels", paused)
		return false
	}
	if !idle {
		s.log.V(consts.VDebug).Info("Deploy labels are paused by a k8s-driver-manager run in progress",
			"node", node.Name, "labels", paused)
		return false
	}

	for _, key := range paused {
		labels[key] = labelValueTrue
	}
	node.SetLabels(labels)
	s.log.Info("Restored deploy labels a finished k8s-driver-manager run left paused",
		"node", node.Name, "labels", paused)
	return true
}

// pausedDesiredComponentLabels returns the deploy labels this node should carry
// as "true" but which k8s-driver-manager left at paused-for-driver-upgrade,
// sorted for a stable log line.
func pausedDesiredComponentLabels(labels map[string]string, config string) []string {
	desired := desiredComponentLabels(labels, config)
	paused := make([]string, 0, len(desired))
	for key := range desired {
		if labels[key] == consts.RBLNDeployPausedForDriverUpgrade {
			paused = append(paused, key)
		}
	}
	sort.Strings(paused)
	return paused
}

// driverManagerIdle reports whether every k8s-driver-manager init container on
// the node has terminated. The driver pod and the vfio-manager pod both run
// one. It reads the API server directly rather than the cache: the question
// is asked rarely, and a stale answer would undo a live pause. A pod whose
// init container has no reported state yet counts as running; with no such
// pod on the node at all the answer is false, since there is nothing to judge
// the pause by.
func (s *ClusterPolicyService) driverManagerIdle(ctx context.Context, nodeName string) (bool, error) {
	pods := &corev1.PodList{}
	if err := s.apiReader.List(ctx, pods,
		client.InNamespace(s.namespace),
		client.MatchingFields{"spec.nodeName": nodeName},
	); err != nil {
		return false, fmt.Errorf("list pods on node %s: %w", nodeName, err)
	}

	seen := false
	for i := range pods.Items {
		runsIt, terminated := driverManagerInitState(&pods.Items[i])
		if !runsIt {
			continue
		}
		if !terminated {
			return false, nil
		}
		seen = true
	}
	return seen, nil
}

// driverManagerInitState reports whether the pod runs the k8s-driver-manager
// init container and, if so, whether that container has terminated.
func driverManagerInitState(pod *corev1.Pod) (runsIt, terminated bool) {
	for i := range pod.Spec.InitContainers {
		if pod.Spec.InitContainers[i].Name == consts.DriverManagerInitContainerName {
			runsIt = true
			break
		}
	}
	if !runsIt {
		return false, false
	}
	for i := range pod.Status.InitContainerStatuses {
		if pod.Status.InitContainerStatuses[i].Name == consts.DriverManagerInitContainerName {
			return true, pod.Status.InitContainerStatuses[i].State.Terminated != nil
		}
	}
	return true, false
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
	modified := removeLegacyComponentLabels(labels)
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

func removeLegacyComponentLabels(labels map[string]string) bool {
	modified := false
	for _, key := range legacyComponentLabelKeys {
		if _, exists := labels[key]; !exists {
			continue
		}
		delete(labels, key)
		modified = true
	}
	return modified
}

func updateRBLNComponentLabels(labels map[string]string, config string) bool {
	desired := desiredComponentLabels(labels, config)
	modified := removeLegacyComponentLabels(labels)

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
		// Fill only. An existing value is never overwritten — not even an empty
		// one — because k8s-driver-manager owns every value transition on these
		// keys (it flips them to paused-for-driver-upgrade to evict a node's
		// components) and the operator must not race it. An empty value is
		// reported by emptyDesiredComponentLabels rather than repaired here; a
		// pause left behind by a finished run is restored by
		// restoreStalePausedLabels, which first checks that no run is in progress.
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
		if key == consts.RBLNDeploySmdLabelKey {
			continue
		}
		desired[key] = value
	}
	return desired
}
