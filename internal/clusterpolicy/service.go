package clusterpolicy

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/clusterpolicy/components"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// ClusterPolicyService orchestrates reconciliation of all components
// managed by a single RBLNClusterPolicy.
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

// PatchComponents applies or removes each managed component according to
// whether it is enabled in the policy spec.
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

// CleanUpAllComponents removes all managed component resources regardless of
// whether they are enabled. This is used during CR deletion to ensure
// cluster-scoped resources (ClusterRole, ClusterRoleBinding, DeviceClass) are
// cleaned up properly.
func (s *ClusterPolicyService) CleanUpAllComponents(ctx context.Context) error {
	for _, c := range s.components {
		if err := c.CleanUp(ctx, s.policy); err != nil {
			return fmt.Errorf("cleanup %s: %w", c.ComponentName(), err)
		}
	}
	return nil
}

// AssembleComponentStatus returns the readiness state of each enabled component.
// Status writing is the caller's responsibility.
func (s *ClusterPolicyService) AssembleComponentStatus(ctx context.Context) []rblnv1beta1.RBLNComponentStatus {
	statuses := make([]rblnv1beta1.RBLNComponentStatus, 0, len(s.components))

	for _, c := range s.components {
		if !c.IsEnabled() {
			continue
		}

		status := rblnv1beta1.RBLNComponentStatus{
			Name:      c.ComponentName(),
			Namespace: c.ComponentNamespace(),
		}

		if err := c.IsReady(ctx); err != nil {
			status.State = rblnv1beta1.ComponentStateNotReady
			s.log.V(consts.LogLevelDebug).Info("component not ready",
				"component", c.ComponentName(),
				"reason", err.Error(),
			)
		} else {
			status.State = rblnv1beta1.ComponentStateReady
		}

		statuses = append(statuses, status)
	}

	return statuses
}
