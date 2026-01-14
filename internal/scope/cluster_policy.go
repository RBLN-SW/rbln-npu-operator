package scope

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
	"github.com/rebellions-sw/rbln-npu-operator/internal/scope/patch"
)

// RBLNClusterPolicyScope is a scope for reconciling a RBLNClusterPolicy resource.
type RBLNClusterPolicyScope struct {
	client client.Client

	ctx       context.Context
	log       logr.Logger
	scheme    *runtime.Scheme
	singleton *rblnv1beta1.RBLNClusterPolicy
	namespace string

	patcher []patch.Patcher
}

func NewRBLNClusterPolicyScope(ctx context.Context, client client.Client, log logr.Logger, scheme *runtime.Scheme, clusterPolicy *rblnv1beta1.RBLNClusterPolicy, openshiftVersion string) (*RBLNClusterPolicyScope, error) {
	s := &RBLNClusterPolicyScope{
		client:    client,
		ctx:       ctx,
		log:       log,
		scheme:    scheme,
		singleton: clusterPolicy,
	}

	if s.singleton.Spec.Namespace != "" {
		s.namespace = s.singleton.Spec.Namespace
	} else {
		s.namespace = os.Getenv("OPERATOR_NAMESPACE")
	}

	if s.namespace == "" {
		err := fmt.Errorf("namespace is not configured. Set OPERATOR_NAMESPACE env variable or namespace spec")
		s.log.Error(err, "namespace configuration error")
		return nil, err
	}

	vmp, err := patch.NewVFIOManagerPatcher(client, log, s.namespace, &clusterPolicy.Spec, scheme, openshiftVersion)
	if err != nil {
		return s, err
	}
	s.patcher = append(s.patcher, vmp)

	nfd, err := patch.NewNPUFeatureDiscoveryPatcher(client, log, s.namespace, &clusterPolicy.Spec, scheme, openshiftVersion)
	if err != nil {
		return s, err
	}
	s.patcher = append(s.patcher, nfd)

	mep, err := patch.NewMetricsExporterPatcher(client, log, s.namespace, &clusterPolicy.Spec, scheme, openshiftVersion)
	if err != nil {
		return s, err
	}
	s.patcher = append(s.patcher, mep)

	dpp, err := patch.NewDevicePluginPatcher(client, log, s.namespace, &clusterPolicy.Spec, scheme, openshiftVersion)
	if err != nil {
		return s, err
	}
	s.patcher = append(s.patcher, dpp)

	sdp, err := patch.NewSandboxDevicePluginPatcher(client, log, s.namespace, &clusterPolicy.Spec, scheme, openshiftVersion)
	if err != nil {
		return s, err
	}
	s.patcher = append(s.patcher, sdp)

	return s, nil
}

// PatchComponents patches all components managed by the scope
func (s *RBLNClusterPolicyScope) PatchComponents(ctx context.Context) error {
	for _, p := range s.patcher {
		if p.IsEnabled() {
			if err := p.Patch(ctx, s.singleton); err != nil {
				return fmt.Errorf("failed to patch component: %v", err)
			}
		} else {
			if err := p.CleanUp(ctx, s.singleton); err != nil {
				return fmt.Errorf("failed to clean up component: %v", err)
			}
		}
	}
	return nil
}

func (s *RBLNClusterPolicyScope) AssembleComponentConditions(ctx context.Context) []rblnv1beta1.RBLNComponentStatus {
	componentsStatus := make([]rblnv1beta1.RBLNComponentStatus, 0, len(s.patcher))
	for _, p := range s.patcher {
		if p.IsEnabled() {
			componentStatus := rblnv1beta1.RBLNComponentStatus{
				Name:      p.ComponentName(),
				Namespace: p.ComponentNamespace(),
			}
			conditions, err := p.ConditionReport(ctx, s.singleton)
			componentStatus.Condition = conditions

			if err != nil {
				componentStatus.State = rblnv1beta1.ComponentStateNotReady
			} else {
				componentStatus.State = rblnv1beta1.ComponentStateReady
			}
			componentsStatus = append(componentsStatus, componentStatus)
		}
	}
	return componentsStatus
}

func (s *RBLNClusterPolicyScope) LabelRblnNodes() (bool, int, error) {
	ctx := s.ctx

	nodeList := &corev1.NodeList{}
	if err := s.client.List(ctx, nodeList, &client.ListOptions{}); err != nil {
		return false, 0, fmt.Errorf("failed to list nodes: %s", err.Error())
	}

	nfdInstalled := true
	rblnNodeCnt := 0
	updateLabels := false
	for _, node := range nodeList.Items {
		labels := node.GetLabels()
		if nfdInstalled {
			nfdInstalled = hasNFDLabels(labels)
		}
		// set rebellions present label according to device feature discovery
		if !hasRBLNPresentLabel(labels) && hasRBLNDeviceLabel(labels) {
			s.log.Info("Rebellions device detected. Set RBLN Present Label", "Node", node.Name)
			labels[consts.RBLNPresentLabelKey] = "true"
			node.SetLabels(labels)
			updateLabels = true
		} else if hasRBLNPresentLabel(labels) && !hasRBLNDeviceLabel(labels) {
			s.log.Info("Rebellions device removed. Disable RBLN Present Label", "Node", node.Name)
			labels[consts.RBLNPresentLabelKey] = "false"
			removeAllRBLNComponentLabels(labels)
			node.SetLabels(labels)
			updateLabels = true
		}
		// if a node has rbln npu, set rbln components labels depends on workload type
		if hasRBLNPresentLabel(labels) {
			workloadConfig, err := getWorkloadConfig(labels, s.singleton.Spec.WorkloadType)
			if err != nil {
				s.log.Info("WARNING: failed to get RBLN NPU workload config for node; using default workload config", "defaultWorkloadConfig", workloadConfig, "Node", node.Name)
			}
			if updateRBLNComponentLabels(labels, workloadConfig) {
				node.SetLabels(labels)
				updateLabels = true
			}
			rblnNodeCnt++
		}
		if updateLabels {
			if err := s.client.Update(ctx, &node); err != nil {
				return nfdInstalled, 0, fmt.Errorf("failed to label node %s, err: %s", node.Name, err.Error())
			}
		}
	}
	return nfdInstalled, rblnNodeCnt, nil
}

func hasRBLNPresentLabel(labels map[string]string) bool {
	if _, ok := labels[consts.RBLNPresentLabelKey]; ok {
		return labels[consts.RBLNPresentLabelKey] == "true"
	}
	return false
}

func hasRBLNDeviceLabel(labels map[string]string) bool {
	for key, val := range labels {
		if _, ok := rblnDeviceLabels[key]; ok {
			if rblnDeviceLabels[key] == val {
				return true
			}
		}
	}
	return false
}

func getWorkloadConfig(labels map[string]string, defaultWorkload string) (string, error) {
	if workloadConfig, ok := labels[consts.RBLNWorkloadConfigLabelKey]; ok {
		if isValidWorkloadConfig(workloadConfig) {
			return workloadConfig, nil
		}
		return defaultWorkload, fmt.Errorf("invalid NPU workload config: %v", workloadConfig)
	}
	return defaultWorkload, fmt.Errorf("no NPU workload config label found")
}

func isValidWorkloadConfig(workloadConfig string) bool {
	_, ok := rblnComponentLabels[workloadConfig]
	return ok
}

var rblnDeviceLabels = map[string]string{
	"feature.node.kubernetes.io/pci-1200_1eff.present": "true",
	"feature.node.kubernetes.io/pci-1eff.present":      "true",
}

var rblnComponentLabels = map[string]map[string]string{
	consts.RBLNWorkloadConfigContainer: {
		"rebellions.ai/npu.deploy.driver-manager":        "true",
		"rebellions.ai/npu.deploy.device-plugin":         "true",
		"rebellions.ai/npu.deploy.metrics-exporter":      "true",
		"rebellions.ai/npu.deploy.npu-feature-discovery": "true",
	},
	consts.RBLNWorkloadConfigVMPassthrough: {
		"rebellions.ai/npu.deploy.vfio-manager":          "true",
		"rebellions.ai/npu.deploy.sandbox-device-plugin": "true",
	},
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
			if _, ok := rblnComponentLabels[config][key]; ok {
				continue
			}
			if _, ok := labels[key]; ok {
				delete(labels, key)
				modified = true
			}
		}
	}

	for key, value := range rblnComponentLabels[config] {
		if _, ok := labels[key]; !ok {
			labels[key] = value
			modified = true
		}
	}

	return modified
}

// hasNFDLabels return true if node labels contain NFD labels
func hasNFDLabels(labels map[string]string) bool {
	for key := range labels {
		if strings.HasPrefix(key, consts.NFDLabelPrefix) {
			return true
		}
	}
	return false
}
