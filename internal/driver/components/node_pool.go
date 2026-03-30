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

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
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
		pool, ok := buildNodePool(node, nodeSelector, logger)
		if !ok {
			continue
		}
		if _, exists := nodePoolMap[pool.name]; !exists {
			logger.Info("Detected new node pool", "pool", pool.name, "os", pool.getOS(), "kernel", pool.kernel)
			nodePoolMap[pool.name] = pool
		}
	}

	nodePools := make([]nodePool, 0, len(nodePoolMap))
	for _, pool := range nodePoolMap {
		nodePools = append(nodePools, pool)
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

	osID, ok := getNodeLabel(nodeLabels, node.Name, consts.NFDOSReleaseIDLabelKey, logger)
	if !ok {
		return nodePool, false
	}
	nodePool.nodeSelector[consts.NFDOSReleaseIDLabelKey] = osID

	osVersion, ok := getNodeLabel(nodeLabels, node.Name, consts.NFDOSVersionIDLabelKey, logger)
	if !ok {
		return nodePool, false
	}
	nodePool.nodeSelector[consts.NFDOSVersionIDLabelKey] = osVersion
	nodePool.osRelease = osID
	nodePool.osVersion = osVersion
	nodePool.name = nodePool.getOS()

	kernelVersion, ok := getNodeLabel(nodeLabels, node.Name, consts.NFDKernelLabelKey, logger)
	if !ok {
		return nodePool, false
	}
	nodePool.nodeSelector[consts.NFDKernelLabelKey] = kernelVersion
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

var kernelArchRegex = regexp.MustCompile(`x86_64(?:_64k)?|aarch64(?:_64k)?`)

func getSanitizedKernelVersion(kernelVersion string) string {
	sanitized := kernelArchRegex.ReplaceAllString(kernelVersion, "")
	sanitized = strings.ReplaceAll(sanitized, "_", ".")
	sanitized = strings.TrimSuffix(sanitized, ".")
	return strings.ToLower(sanitized)
}
