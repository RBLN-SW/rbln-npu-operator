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
	client client.Client
	// apiReader bypasses the informer cache. It is used only to look at the
	// pods on a node whose deploy labels k8s-driver-manager left paused, which
	// is rare, and the cached client would start a cluster-wide pod informer
	// the operator otherwise never needs.
	apiReader  client.Reader
	log        logr.Logger
	policy     *rblnv1beta1.RBLNClusterPolicy
	namespace  string
	components []components.Patcher
}

func NewClusterPolicyService(
	client client.Client,
	apiReader client.Reader,
	log logr.Logger,
	scheme *runtime.Scheme,
	policy *rblnv1beta1.RBLNClusterPolicy,
	namespace string,
	openShiftVersion string,
	containerRuntime string,
) *ClusterPolicyService {
	return &ClusterPolicyService{
		client:    client,
		apiReader: apiReader,
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
		components.NewNPUFamilyRulePatcher(client, log, namespace, scheme, openShiftVersion),
		components.NewVFIOManagerPatcher(client, log, namespace, spec, scheme, openShiftVersion),
		components.NewNPUFeatureDiscoveryPatcher(client, log, namespace, spec, scheme, openShiftVersion),
		components.NewMetricsExporterPatcher(client, log, namespace, spec, scheme, openShiftVersion),
		components.NewLegacyRBLNDaemonCleanup(client, log, namespace, scheme, openShiftVersion),
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

type workloadAggregate struct {
	componentCount int32
	readyCount     int32
}

// AssembleStatus computes the status snapshot; the caller writes it to the CR.
func (s *ClusterPolicyService) AssembleStatus(
	ctx context.Context,
	census NodeCensus,
) ([]rblnv1beta1.RBLNComponentStatus, []rblnv1beta1.RBLNWorkloadStatus) {
	componentStatuses := make([]rblnv1beta1.RBLNComponentStatus, 0, len(s.components))

	aggregates := map[string]*workloadAggregate{
		consts.RBLNWorkloadConfigContainer:     {},
		consts.RBLNWorkloadConfigVMPassthrough: {},
	}

	for _, c := range s.components {
		if !c.IsEnabled() {
			continue
		}

		wlType := c.WorkloadType()
		nodeCount := census.CountFor(wlType)
		report := c.IsReady(ctx, nodeCount)

		componentStatuses = append(componentStatuses, rblnv1beta1.RBLNComponentStatus{
			Name:         c.ComponentName(),
			Namespace:    c.ComponentNamespace(),
			WorkloadType: wlType,
			State:        report.State,
			Desired:      report.Desired,
			Ready:        report.Ready,
			Message:      report.Message,
		})

		for _, target := range aggregateTargets(wlType, census) {
			a := aggregates[target]
			a.componentCount++
			if report.State == rblnv1beta1.ComponentStateReady {
				a.readyCount++
			}
		}

		if report.State != rblnv1beta1.ComponentStateReady {
			s.log.V(consts.VDebug).Info("Component not ready",
				"component", c.ComponentName(),
				"workload", wlType,
				"message", report.Message,
			)
		}
	}

	workloadStatuses := buildWorkloadStatuses(census, aggregates)
	return componentStatuses, workloadStatuses
}

// aggregateTargets names the workload buckets a component's readiness feeds.
// A component typed for one workload feeds that bucket only. A component that
// serves every workload type (RBLNWorkloadConfigAll — the DRA kubelet plugin)
// feeds every bucket that currently has nodes, so a DaemonSet that is not
// Ready pulls each affected workload, and with it the top-level state, out of
// ready. Before this, an "all" component appeared in status.components but in
// no status.workloads entry, so the policy reported ready while the component
// was notReady. Unknown types feed nothing, as before.
func aggregateTargets(wlType string, census NodeCensus) []string {
	switch wlType {
	case consts.RBLNWorkloadConfigContainer, consts.RBLNWorkloadConfigVMPassthrough:
		return []string{wlType}
	case consts.RBLNWorkloadConfigAll:
		targets := make([]string, 0, 2)
		for _, t := range []string{consts.RBLNWorkloadConfigContainer, consts.RBLNWorkloadConfigVMPassthrough} {
			if census.CountFor(t) > 0 {
				targets = append(targets, t)
			}
		}
		return targets
	default:
		return nil
	}
}

// buildWorkloadStatuses derives an RBLNWorkloadStatus for each known workload
// type. The output order is stable (container first, vm-passthrough second)
// and contains exactly two entries.
func buildWorkloadStatuses(
	census NodeCensus,
	aggregates map[string]*workloadAggregate,
) []rblnv1beta1.RBLNWorkloadStatus {
	order := []string{consts.RBLNWorkloadConfigContainer, consts.RBLNWorkloadConfigVMPassthrough}
	out := make([]rblnv1beta1.RBLNWorkloadStatus, 0, len(order))

	for _, wlType := range order {
		nodeCount := census.CountFor(wlType)
		a := aggregates[wlType]
		ws := rblnv1beta1.RBLNWorkloadStatus{
			Type:           wlType,
			NodeCount:      nodeCount,
			ComponentCount: a.componentCount,
			ReadyCount:     a.readyCount,
		}

		switch {
		case nodeCount == 0:
			ws.State = rblnv1beta1.WorkloadStateEmpty
			if a.componentCount > 0 {
				ws.Message = fmt.Sprintf(
					"%d component(s) configured but no %s nodes present",
					a.componentCount, wlType,
				)
			}
		case a.componentCount == 0:
			ws.State = rblnv1beta1.WorkloadStateUncovered
			ws.Message = fmt.Sprintf(
				"%d %s node(s) labeled but no enabled components configured",
				nodeCount, wlType,
			)
		case a.readyCount < a.componentCount:
			ws.State = rblnv1beta1.WorkloadStateProgressing
			ws.Message = fmt.Sprintf(
				"%d/%d components ready on %d %s node(s)",
				a.readyCount, a.componentCount, nodeCount, wlType,
			)
		default:
			ws.State = rblnv1beta1.WorkloadStateReady
		}
		out = append(out, ws)
	}
	return out
}
