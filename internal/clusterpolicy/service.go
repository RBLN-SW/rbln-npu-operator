package clusterpolicy

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/clusterpolicy/components"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

const (
	driverAutoUpgradeAnnotationKey = "rebellions.ai/npu-driver-upgrade-enabled"
)

type ClusterPolicyService struct {
	client     client.Client
	log        logr.Logger
	policy     *rblnv1beta1.RBLNClusterPolicy
	namespace  string
	components []components.Patcher
}

func NewClusterPolicyService(
	client client.Client,
	log logr.Logger,
	scheme *runtime.Scheme,
	policy *rblnv1beta1.RBLNClusterPolicy,
	namespace string,
	openShiftVersion string,
	containerRuntime string,
) *ClusterPolicyService {
	return &ClusterPolicyService{
		client:    client,
		log:       log,
		policy:    policy,
		namespace: namespace,
		components: newComponents(
			client,
			log,
			namespace,
			&policy.Spec,
			scheme,
			openShiftVersion,
			containerRuntime,
		),
	}
}

func (s *ClusterPolicyService) PatchComponents(ctx context.Context) error {
	for _, c := range s.components {
		if c.IsEnabled() {
			if err := c.Patch(ctx, s.policy); err != nil {
				return fmt.Errorf("patch %s: %w", c.ComponentName(), err)
			}
			continue
		}
		if err := c.CleanUp(ctx, s.policy); err != nil {
			return fmt.Errorf("cleanup %s: %w", c.ComponentName(), err)
		}
	}
	return nil
}

func (s *ClusterPolicyService) AssembleComponentStatus(ctx context.Context) []rblnv1beta1.RBLNComponentStatus {
	statuses := make([]rblnv1beta1.RBLNComponentStatus, 0, len(s.components))

	for _, c := range s.components {
		if !c.IsEnabled() {
			continue
		}

		componentStatus := rblnv1beta1.RBLNComponentStatus{
			Name:      c.ComponentName(),
			Namespace: c.ComponentNamespace(),
		}

		conditions, err := c.ConditionReport(ctx, s.policy)
		componentStatus.Conditions = conditions
		if err != nil {
			componentStatus.State = rblnv1beta1.ComponentStateNotReady
		} else {
			componentStatus.State = rblnv1beta1.ComponentStateReady
		}

		statuses = append(statuses, componentStatus)
	}

	return statuses
}

func (s *ClusterPolicyService) LabelNodes(ctx context.Context) (bool, int, error) {
	nodeList := &corev1.NodeList{}
	if err := s.client.List(ctx, nodeList); err != nil {
		return false, 0, fmt.Errorf("list nodes: %w", err)
	}

	nfdInstalled := false
	rblnNodeCount := 0

	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		labels := node.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}

		if !nfdInstalled && hasNFDLabels(labels) {
			nfdInstalled = true
		}

		shouldUpdateNode := false

		if !hasRBLNPresentLabel(labels) && hasRBLNDeviceLabel(labels) {
			s.log.Info("Rebellions device detected. Set RBLN Present Label", "node", node.Name)
			labels[consts.RBLNPresentLabelKey] = labelValueTrue
			shouldUpdateNode = true
		} else if hasRBLNPresentLabel(labels) && !hasRBLNDeviceLabel(labels) {
			s.log.Info("Rebellions device removed. Disable RBLN Present Label", "node", node.Name)
			labels[consts.RBLNPresentLabelKey] = labelValueFalse
			removeAllRBLNComponentLabels(labels)
			shouldUpdateNode = true
		}

		if hasRBLNPresentLabel(labels) {
			workloadConfig, err := getWorkloadConfig(labels, s.policy.Spec.WorkloadType)
			if err != nil {
				s.log.Info(
					"Failed to get RBLN workload config for node; using default",
					"node", node.Name,
					"defaultWorkloadConfig", workloadConfig,
					"error", err.Error(),
				)
			}

			if updateRBLNComponentLabels(labels, workloadConfig) {
				shouldUpdateNode = true
			}
			rblnNodeCount++
		}

		if !shouldUpdateNode {
			continue
		}

		node.SetLabels(labels)
		if err := s.client.Update(ctx, node); err != nil {
			return nfdInstalled, 0, fmt.Errorf("update node %s labels: %w", node.Name, err)
		}
	}

	return nfdInstalled, rblnNodeCount, nil
}

func (s *ClusterPolicyService) ApplyDriverAutoUpgradeAnnotation(ctx context.Context) error {
	list := &corev1.NodeList{}
	if err := s.client.List(ctx, list, client.MatchingLabels{
		consts.RBLNPresentLabelKey: labelValueTrue,
	}); err != nil {
		return fmt.Errorf("list nodes for driver auto-upgrade annotation: %w", err)
	}

	shouldEnable := s.policy.Spec.Driver.UpgradePolicy != nil &&
		s.policy.Spec.Driver.UpgradePolicy.AutoUpgrade &&
		!s.policy.Spec.SandboxDevicePlugin.IsEnabled()

	for i := range list.Items {
		node := &list.Items[i]
		annotations := node.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}

		currentValue, exists := annotations[driverAutoUpgradeAnnotationKey]
		if shouldEnable && exists && currentValue == labelValueTrue {
			continue
		}
		if !shouldEnable && !exists {
			continue
		}

		if shouldEnable {
			annotations[driverAutoUpgradeAnnotationKey] = labelValueTrue
		} else {
			delete(annotations, driverAutoUpgradeAnnotationKey)
		}

		node.SetAnnotations(annotations)
		if err := s.client.Update(ctx, node); err != nil {
			return fmt.Errorf("update node %s annotations: %w", node.Name, err)
		}
	}

	return nil
}

func newComponents(
	client client.Client,
	log logr.Logger,
	namespace string,
	spec *rblnv1beta1.RBLNClusterPolicySpec,
	scheme *runtime.Scheme,
	openShiftVersion string,
	containerRuntime string,
) []components.Patcher {
	return []components.Patcher{
		components.NewVFIOManagerPatcher(client, log, namespace, spec, scheme, openShiftVersion),
		components.NewNPUFeatureDiscoveryPatcher(client, log, namespace, spec, scheme, openShiftVersion),
		components.NewMetricsExporterPatcher(client, log, namespace, spec, scheme, openShiftVersion),
		components.NewRBLNDaemonPatcher(client, log, namespace, spec, scheme, openShiftVersion),
		components.NewDevicePluginPatcher(client, log, namespace, spec, scheme, openShiftVersion),
		components.NewDRAKubeletPluginPatcher(client, log, namespace, spec, scheme, openShiftVersion),
		components.NewSandboxDevicePluginPatcher(client, log, namespace, spec, scheme, openShiftVersion),
		components.NewContainerToolkitPatcher(client, log, namespace, spec, scheme, openShiftVersion, containerRuntime),
		components.NewValidatorPatcher(client, log, namespace, spec, scheme, openShiftVersion),
	}
}
