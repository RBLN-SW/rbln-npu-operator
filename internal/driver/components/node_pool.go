package components

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	nfdOSReleaseIDLabelKey = "feature.node.kubernetes.io/system-os_release.ID"
	nfdOSVersionIDLabelKey = "feature.node.kubernetes.io/system-os_release.VERSION_ID"
	nfdKernelLabelKey      = "feature.node.kubernetes.io/kernel-version.full"
)

type nodePool struct {
	name         string
	osRelease    string
	osVersion    string
	kernel       string
	nodeSelector map[string]string
}

// getNodePools partitions nodes per osVersion-kernelVersion for precompiled drivers.
func getNodePools(ctx context.Context, k8sClient client.Client, selector map[string]string) ([]nodePool, error) {
	nodePoolMap := make(map[string]nodePool)

	logger := log.FromContext(ctx)

	nodeSelector := buildNodeSelector(selector)

	nodeList := &corev1.NodeList{}
	if err := k8sClient.List(ctx, nodeList, client.MatchingLabels(nodeSelector)); err != nil {
		logger.Error(err, "failed to list nodes")
		return nil, err
	}

	for _, node := range nodeList.Items {
		nodePool, ok := buildNodePool(node, nodeSelector, logger)
		if !ok {
			continue
		}
		if _, exists := nodePoolMap[nodePool.name]; !exists {
			logger.Info("Detected new node pool", "NodePool", nodePool)
			nodePoolMap[nodePool.name] = nodePool
		}
	}

	nodePools := make([]nodePool, 0, len(nodePoolMap))
	for _, nodePool := range nodePoolMap {
		nodePools = append(nodePools, nodePool)
	}

	return nodePools, nil
}

func buildNodeSelector(selector map[string]string) map[string]string {
	nodeSelector := map[string]string{
		driverManagerDeployLabelKey: "true",
	}
	maps.Copy(nodeSelector, selector)
	return nodeSelector
}

func buildNodePool(node corev1.Node, baseSelector map[string]string, logger logr.Logger) (nodePool, bool) {
	nodeLabels := node.GetLabels()
	nodePool := nodePool{
		nodeSelector: make(map[string]string),
	}
	maps.Copy(nodePool.nodeSelector, baseSelector)

	osID, ok := getNodeLabel(nodeLabels, node.Name, nfdOSReleaseIDLabelKey, logger)
	if !ok {
		return nodePool, false
	}
	nodePool.nodeSelector[nfdOSReleaseIDLabelKey] = osID

	osVersion, ok := getNodeLabel(nodeLabels, node.Name, nfdOSVersionIDLabelKey, logger)
	if !ok {
		return nodePool, false
	}
	nodePool.nodeSelector[nfdOSVersionIDLabelKey] = osVersion
	nodePool.osRelease = osID
	nodePool.osVersion = osVersion
	nodePool.name = nodePool.getOS()

	kernelVersion, ok := getNodeLabel(nodeLabels, node.Name, nfdKernelLabelKey, logger)
	if !ok {
		return nodePool, false
	}
	nodePool.nodeSelector[nfdKernelLabelKey] = kernelVersion
	nodePool.kernel = kernelVersion
	nodePool.name = fmt.Sprintf("%s-%s", nodePool.name, getSanitizedKernelVersion(kernelVersion))

	return nodePool, true
}

func getNodeLabel(labels map[string]string, nodeName string, labelKey string, logger logr.Logger) (string, bool) {
	value, ok := labels[labelKey]
	if !ok {
		logger.Info("WARNING: Could not find NFD label for node. Is NFD installed?", "Node", nodeName, "Label", labelKey)
		return "", false
	}
	return value, true
}

func (n nodePool) getOS() string {
	return fmt.Sprintf("%s%s", n.osRelease, n.osVersion)
}

func getSanitizedKernelVersion(kernelVersion string) string {
	archRegex := regexp.MustCompile("x86_64(?:_64k)?|aarch64(?:_64k)?")
	sanitized := archRegex.ReplaceAllString(kernelVersion, "")
	sanitized = strings.ReplaceAll(sanitized, "_", ".")
	sanitized = strings.TrimSuffix(sanitized, ".")
	return strings.ToLower(sanitized)
}
