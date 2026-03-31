package upgrade

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

// NodeUpgradeState contains a mapping between a node,
// the driver POD running on them and the daemon set, controlling this pod
type NodeUpgradeState struct {
	Node            *corev1.Node
	DriverPod       *corev1.Pod
	DriverDaemonSet *appsv1.DaemonSet
}

// IsOrphanedPod returns true if Pod is not associated to a DaemonSet
func (nus *NodeUpgradeState) IsOrphanedPod() bool {
	return nus.DriverDaemonSet == nil
}

type ClusterUpgradeState struct {
	NodeStates map[string][]*NodeUpgradeState
}

type ClusterUpgradeStateManager interface {
	WithPodDeletionEnabled(filter PodDeletionFilter) ClusterUpgradeStateManager
	WithValidationEnabled(podSelector string) ClusterUpgradeStateManager
	BuildState(ctx context.Context, namespace string,
		driverLabels map[string]string) (*ClusterUpgradeState, error)
	ApplyState(ctx context.Context,
		namespace string,
		currentState *ClusterUpgradeState, upgradePolicy *v1beta1.DriverUpgradePolicySpec) (err error)
}

type ClusterUpgradeStateManagerImpl struct {
	log                      logr.Logger
	k8sClient                client.Client
	k8sInterface             kubernetes.Interface
	nodeUpgradeStateProvider *NodeUpgradeStateProvider
	podManager               PodManagerInterface
	safeDriverLoadManager    SafeDriverLoadManagerInterface
	cordonManager            CordonManagerInterface
	drainManager             DrainManagerInterface
	rebootManager            RebootManager
	validationManager        ValidationManagerInterface

	// optional states
	podDeletionStateEnabled bool
	validationStateEnabled  bool
}

func NewClusterUpgradeStateManager(
	log logr.Logger,
	k8sConfig *rest.Config,
	eventRecorder record.EventRecorder,
	scheme *runtime.Scheme,
) (ClusterUpgradeStateManager, error) {
	k8sClient, err := client.New(k8sConfig, client.Options{Scheme: scheme})
	if err != nil {
		return &ClusterUpgradeStateManagerImpl{}, fmt.Errorf("error creating k8s client: %v", err)
	}

	k8sInterface, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return &ClusterUpgradeStateManagerImpl{}, fmt.Errorf("error creating k8s interface: %v", err)
	}

	nodeUpgradeStateProvider := NewNodeUpgradeStateProvider(k8sClient, log)
	manager := &ClusterUpgradeStateManagerImpl{
		log:                      log,
		k8sClient:                k8sClient,
		k8sInterface:             k8sInterface,
		drainManager:             NewDrainManager(k8sInterface, nodeUpgradeStateProvider, log, eventRecorder),
		rebootManager:            NewPodRebootManager(k8sClient, log),
		podManager:               NewPodManager(k8sInterface, nodeUpgradeStateProvider, log, nil),
		cordonManager:            NewCordonManager(k8sInterface, log),
		nodeUpgradeStateProvider: nodeUpgradeStateProvider,
		validationManager:        NewValidationManager(k8sInterface, log, nodeUpgradeStateProvider, ""),
		safeDriverLoadManager:    NewSafeDriverLoadManager(nodeUpgradeStateProvider, log),
	}
	return manager, nil
}

func (m *ClusterUpgradeStateManagerImpl) WithPodDeletionEnabled(filter PodDeletionFilter) ClusterUpgradeStateManager {
	if filter == nil {
		m.log.Info("Cannot enable PodDeletion state as PodDeletionFilter is nil")
		return m
	}
	m.podManager = NewPodManager(m.k8sInterface, m.nodeUpgradeStateProvider, m.log, filter)
	m.podDeletionStateEnabled = true
	return m
}

func (m *ClusterUpgradeStateManagerImpl) WithValidationEnabled(podSelector string) ClusterUpgradeStateManager {
	if podSelector == "" {
		m.log.Info("Cannot enable Validation state as podSelector is empty")
		return m
	}
	m.validationManager = NewValidationManager(m.k8sInterface, m.log, m.nodeUpgradeStateProvider,
		podSelector)
	m.validationStateEnabled = true
	return m
}
