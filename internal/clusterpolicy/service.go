package clusterpolicy

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/controller-runtime/pkg/client"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/clusterpolicy/components"
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
