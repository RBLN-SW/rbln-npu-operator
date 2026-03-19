package clusterpolicy

import (
	"fmt"
	"strings"

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

func hasRBLNPresentLabel(labels map[string]string) bool {
	return labels[consts.RBLNPresentLabelKey] == labelValueTrue
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

func removeAllRBLNComponentLabels(labels map[string]string) {
	for _, labelsMap := range rblnComponentLabels {
		for key := range labelsMap {
			delete(labels, key)
		}
	}
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
