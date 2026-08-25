package upgrade

import (
	"context"
	"fmt"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubectl/pkg/drain"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
	"github.com/rebellions-sw/rbln-npu-operator/internal/consts"
)

// DrainManagerInterface abstracts node drain operations for testability.
type DrainManagerInterface interface {
	ScheduleNodesDrain(ctx context.Context, drainConfig *DrainConfiguration) error
}

type DrainConfiguration struct {
	Spec  *v1beta1.DrainSpec
	Nodes []*corev1.Node
}

type DrainManager struct {
	k8sInterface             kubernetes.Interface
	drainingNodes            *StringSet
	nodeUpgradeStateProvider *NodeUpgradeStateProvider
	eventRecorder            record.EventRecorder
}

func (m *DrainManager) ScheduleNodesDrain(ctx context.Context, drainConfig *DrainConfiguration) error {
	log.FromContext(ctx).Info("Drain Manager, starting Node Drain")

	if len(drainConfig.Nodes) == 0 {
		log.FromContext(ctx).Info("Drain Manager, no nodes scheduled to drain")
		return nil
	}

	drainSpec := drainConfig.Spec

	if drainSpec == nil {
		return fmt.Errorf("drain spec should not be empty")
	}
	if !drainSpec.Enable {
		log.FromContext(ctx).Info("Drain Manager, drain is disabled")
		return nil
	}

	drainHelper := &drain.Helper{
		Ctx:                 ctx,
		Client:              m.k8sInterface,
		Force:               drainSpec.Force,
		DeleteEmptyDirData:  drainSpec.DeleteEmptyDirData,
		IgnoreAllDaemonSets: true,
		GracePeriodSeconds:  -1,
		Timeout:             time.Duration(drainSpec.TimeoutSeconds) * time.Second,
		PodSelector:         drainSpec.PodSelector,
		OnPodDeletionOrEvictionFinished: func(pod *corev1.Pod, usingEviction bool, err error) {
			log := log.FromContext(ctx).WithValues("usingEviction", usingEviction, "pod", pod.Name, "namespace", pod.Namespace)
			if err != nil {
				log.Info("Drain Pod failed", "error", err)
				return
			}
			log.Info("Drain Pod finished")
		},
		Out:    os.Stdout,
		ErrOut: os.Stdout,
	}

	for _, node := range drainConfig.Nodes {
		if !m.drainingNodes.Has(node.Name) {
			log.FromContext(ctx).Info("Schedule drain for node", "node", node.Name)

			m.drainingNodes.Add(node.Name)
			go func() {
				defer m.drainingNodes.Remove(node.Name)
				err := drain.RunCordonOrUncordon(drainHelper, node, true)
				if err != nil {
					log.FromContext(ctx).Error(err, "Failed to cordon node", "node", node.Name)
					recordNodeEvent(m.eventRecorder, node, corev1.EventTypeWarning,
						consts.RBLNEventReasonNodeDrainFailed,
						fmt.Sprintf("Failed to cordon node: %v", err))
					m.changeNodeUpgradeStateAsync(ctx, node, UpgradeStateFailed)
					return
				}
				log.FromContext(ctx).Info("Cordoned the node", "node", node.Name)

				err = drain.RunNodeDrain(drainHelper, node.Name)
				if err != nil {
					log.FromContext(ctx).Error(err, "Failed to drain node", "node", node.Name)
					recordNodeEvent(m.eventRecorder, node, corev1.EventTypeWarning,
						consts.RBLNEventReasonNodeDrainFailed,
						fmt.Sprintf("Failed to drain node: %v", err))
					m.changeNodeUpgradeStateAsync(ctx, node, UpgradeStateFailed)
					return
				}
				log.FromContext(ctx).Info("Drained the node", "node", node.Name)
				recordNodeEvent(m.eventRecorder, node, corev1.EventTypeNormal,
					consts.RBLNEventReasonNodeDrained,
					"Node drained for driver upgrade (workload pods evicted)")

				m.changeNodeUpgradeStateAsync(ctx, node, UpgradeStatePodRestartRequired)
			}()
		} else {
			log.FromContext(ctx).Info("Node is already being drained, skipping", "node", node.Name)
		}
	}
	return nil
}

// changeNodeUpgradeStateAsync transitions the node upgrade state using a
// short-lived context derived from the parent so that the operation completes
// even when the parent reconcile context is close to expiring. Errors are
// logged and left for the next reconcile cycle to retry.
func (m *DrainManager) changeNodeUpgradeStateAsync(ctx context.Context, node *corev1.Node, state string) {
	stateCtx, cancel := context.WithTimeout(ctx, 30*time.Second) //nolint:contextcheck // intentional short-lived timeout for goroutine state transition
	defer cancel()
	if err := m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(stateCtx, node, state); err != nil {
		log.FromContext(ctx).Error(err, "Failed to transition node state in goroutine; will retry next reconcile",
			"node", node.Name, "targetState", state)
	}
}

func NewDrainManager(
	k8sInterface kubernetes.Interface,
	nodeUpgradeStateProvider *NodeUpgradeStateProvider,
	eventRecorder record.EventRecorder,
) *DrainManager {
	mgr := &DrainManager{
		k8sInterface:             k8sInterface,
		drainingNodes:            NewStringSet(),
		nodeUpgradeStateProvider: nodeUpgradeStateProvider,
		eventRecorder:            eventRecorder,
	}

	return mgr
}
