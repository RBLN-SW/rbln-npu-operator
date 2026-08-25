package components

import (
	"fmt"
	"maps"
	"regexp"
	"sort"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

type nodePool struct {
	name         string
	osRelease    string
	osVersion    string
	kernel       string
	family       string
	nodeSelector map[string]string
}

// buildNodePools partitions nodes per family-os-kernel for precompiled
// drivers. The nodes come from the caller (the owner resolver's snapshot for
// one RBLNDriver), never from a fresh List here, so the pool view and the
// owner assignment share one snapshot by construction. A node missing the
// family label produces no pool, so it is reported in nodesWithoutFamily
// instead of staying silently uncovered.
func buildNodePools(
	nodes []corev1.Node, selector map[string]string, logger logr.Logger,
) (pools []nodePool, nodesWithoutFamily []string) {
	nodePoolMap := make(map[string]nodePool)

	nodeSelector := buildNodeSelector(selector)

	for _, node := range nodes {
		pool, ok := buildNodePool(node, nodeSelector, logger)
		if !ok {
			continue
		}
		if pool.family == "" {
			nodesWithoutFamily = append(nodesWithoutFamily, node.Name)
			continue
		}
		if _, exists := nodePoolMap[pool.name]; !exists {
			logger.V(consts.VDebug).Info("Detected node pool", "pool", pool.name, "os", pool.getOS(), "kernel", pool.kernel)
			nodePoolMap[pool.name] = pool
		}
	}
	sort.Strings(nodesWithoutFamily)

	pools = make([]nodePool, 0, len(nodePoolMap))
	for _, pool := range nodePoolMap {
		pools = append(pools, pool)
	}

	return pools, nodesWithoutFamily
}

func buildNodeSelector(selector map[string]string) map[string]string {
	nodeSelector := map[string]string{
		driverManagerDeployLabelKey: "true",
	}
	maps.Copy(nodeSelector, selector)
	return nodeSelector
}

// buildNodePool reads a node's NFD and npu.family labels into a nodePool.
// The bool return covers the os/kernel labels only; family validity is a
// separate axis, signaled by an empty pool.family for the caller to classify.
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

	// This label comes from the operator's own NodeFeatureRule, not
	// third-party NFD, so getNodeLabel's "Is NFD installed?" hint would
	// misattribute a miss. The value is spliced verbatim into DaemonSet
	// names and image paths, so it is validated and never sanitized:
	// silently lowercasing a misconfigured label would mask a real mistake.
	family, ok := nodeLabels[consts.RBLNNPUFamilyLabelKey]
	if ok {
		family = strings.TrimSpace(family)
		composedName := family + "-" + nodePool.name
		switch {
		case len(validation.IsDNS1123Label(family)) != 0:
			logger.V(consts.VDebug).Info("NPU family label value is not a valid DNS-1123 label; treating node as unlabeled",
				"node", node.Name, "label", consts.RBLNNPUFamilyLabelKey, "value", family)
		case len(composedName) > validation.LabelValueMaxLength:
			// pool.name becomes a DaemonSet label value, so a family that is
			// valid on its own can still push the composed name past the
			// 63-char cap. Letting it through would hard-fail the DaemonSet
			// create with an API validation error instead of failing here.
			logger.V(consts.VDebug).Info("NPU family label value composes a node-pool name over the 63-character label-value limit; treating node as unlabeled",
				"node", node.Name, "label", consts.RBLNNPUFamilyLabelKey, "value", family, "composedName", composedName)
		default:
			nodePool.family = family
			nodePool.nodeSelector[consts.RBLNNPUFamilyLabelKey] = family
			nodePool.name = composedName
		}
	}

	return nodePool, true
}

func getNodeLabel(labels map[string]string, nodeName string, labelKey string, logger logr.Logger) (string, bool) {
	value, ok := labels[labelKey]
	if !ok {
		logger.Info("Could not find NFD label for node. Is NFD installed?", "node", nodeName, "label", labelKey,
			"effect", "node produces no driver pool and is excluded from every driver DaemonSet")
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
