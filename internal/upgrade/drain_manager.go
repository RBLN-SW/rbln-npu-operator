package upgrade

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubectl/pkg/drain"

	"github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

type DrainConfiguration struct {
	Spec  *v1beta1.DrainSpec
	Nodes []*corev1.Node
}

type DrainManager struct {
	k8sInterface             kubernetes.Interface
	drainingNodes            *StringSet
	nodeUpgradeStateProvider *NodeUpgradeStateProvider
	Log                      logr.Logger
	eventRecorder            record.EventRecorder
}

func (m *DrainManager) ScheduleNodesDrain(ctx context.Context, drainConfig *DrainConfiguration) error {
	m.Log.Info("Drain Manager, starting Node Drain")

	if len(drainConfig.Nodes) == 0 {
		m.Log.Info("Drain Manager, no nodes scheduled to drain")
		return nil
	}

	drainSpec := drainConfig.Spec

	if drainSpec == nil {
		return fmt.Errorf("drain spec should not be empty")
	}
	if !drainSpec.Enable {
		m.Log.Info("Drain Manager, drain is disabled")
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
			log := m.Log.WithValues("using-eviction", usingEviction, "pod", pod.Name, "namespace", pod.Namespace)
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
			m.Log.Info("Schedule drain for node", "node", node.Name)

			m.drainingNodes.Add(node.Name)
			go func() {
				defer m.drainingNodes.Remove(node.Name)
				err := drain.RunCordonOrUncordon(drainHelper, node, true)
				if err != nil {
					m.Log.Error(err, "Failed to cordon node", "node", node.Name)
					_ = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, UpgradeStateFailed)
					return
				}
				m.Log.Info("Cordoned the node", "node", node.Name)

				err = drain.RunNodeDrain(drainHelper, node.Name)
				if err != nil {
					m.Log.Error(err, "Failed to drain node", "node", node.Name)
					_ = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, UpgradeStateFailed)
					return
				}
				m.Log.Info("Drained the node", "node", node.Name)

				_ = m.nodeUpgradeStateProvider.ChangeNodeUpgradeState(ctx, node, UpgradeStatePodRestartRequired)
			}()
		} else {
			m.Log.Info("Node is already being drained, skipping", "node", node.Name)
		}
	}
	return nil
}

func NewDrainManager(
	k8sInterface kubernetes.Interface,
	nodeUpgradeStateProvider *NodeUpgradeStateProvider,
	log logr.Logger,
	eventRecorder record.EventRecorder,
) *DrainManager {
	mgr := &DrainManager{
		k8sInterface:             k8sInterface,
		Log:                      log,
		drainingNodes:            NewStringSet(),
		nodeUpgradeStateProvider: nodeUpgradeStateProvider,
		eventRecorder:            eventRecorder,
	}

	return mgr
}
