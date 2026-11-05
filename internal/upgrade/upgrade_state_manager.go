package upgrade

import (
	"context"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/RBLN-SW/rbln-npu-operator/api/v1beta1"
)

type NodeUpgradeState struct {
	Node            *corev1.Node
	DriverPod       *corev1.Pod
	DriverDaemonSet *appsv1.DaemonSet
}

func (n *NodeUpgradeState) IsOrphanedPod() bool { return n.DriverDaemonSet == nil }

type ClusterUpgradeState struct {
	// One active-node collection is shared by execution, status and metrics.
	// Each entry keeps its observed phase even when writes refresh Node.
	nodes         []observedNode
	DepartedNodes []*corev1.Node
}

type observedNode struct {
	phase string
	node  *NodeUpgradeState
}
type ClusterUpgradeStateManager interface {
	BuildState(context.Context, string, map[string]string) (*ClusterUpgradeState, error)
	ApplyState(context.Context, *ClusterUpgradeState, *v1beta1.DriverUpgradePolicySpec, string, ValidationOptions) error
}
type ClusterUpgradeStateManagerImpl struct {
	log                                             logr.Logger
	k8sClient                                       client.Client
	k8sInterface                                    kubernetes.Interface
	eventRecorder                                   record.EventRecorder
	nodeUpgradeStateProvider                        *NodeUpgradeStateProvider
	podManager                                      PodManagerInterface
	cordonManager                                   CordonManagerInterface
	validationManager                               ValidationManagerInterface
	podDeletionStateEnabled, validationStateEnabled bool
}

// ManagerOptions fixes the optional operations at construction. Reconciliation
// consumes policy values without reconfiguring the manager's dependencies.
type ManagerOptions struct {
	PodDeletionFilter     PodDeletionFilter
	ValidationPodSelector string
}

func NewClusterUpgradeStateManager(log logr.Logger, cfg *rest.Config, rec record.EventRecorder, scheme *runtime.Scheme, options ManagerOptions) (ClusterUpgradeStateManager, error) {
	api, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, err
	}
	typed, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return newStateManager(log, api, typed, rec, options), nil
}

func newStateManager(log logr.Logger, api client.Client, typed kubernetes.Interface, rec record.EventRecorder, options ManagerOptions) *ClusterUpgradeStateManagerImpl {
	provider := NewNodeUpgradeStateProvider(api, rec)
	return &ClusterUpgradeStateManagerImpl{
		log: log, k8sClient: api, k8sInterface: typed, eventRecorder: rec,
		nodeUpgradeStateProvider: provider,
		podManager:               NewPodManager(typed, provider, options.PodDeletionFilter),
		cordonManager:            NewCordonManager(typed),
		validationManager:        NewValidationManager(typed, provider, options.ValidationPodSelector),
		podDeletionStateEnabled:  options.PodDeletionFilter != nil,
		validationStateEnabled:   options.ValidationPodSelector != "",
	}
}

func (m *ClusterUpgradeStateManagerImpl) IsPodDeletionEnabled() bool {
	return m.podDeletionStateEnabled
}
func (m *ClusterUpgradeStateManagerImpl) IsValidationEnabled() bool { return m.validationStateEnabled }
