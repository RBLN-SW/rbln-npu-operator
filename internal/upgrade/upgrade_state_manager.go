/*
Copyright 2022 NVIDIA CORPORATION & AFFILIATES

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
Modifications Copyright 2026 Rebellions Inc.

This file is derived from github.com/NVIDIA/k8s-operator-libs
(pkg/upgrade/upgrade_state.go and common_manager.go, commit a0a0256b) and has been
modified by Rebellions Inc. See NOTICE for details.
*/

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
	// DepartedNodes carry rollout bookkeeping but are no longer in the
	// driver's scope; ProcessDepartedNodes tears them down.
	DepartedNodes []*corev1.Node
}

type ClusterUpgradeStateManager interface {
	WithPodDeletionEnabled(filter PodDeletionFilter) ClusterUpgradeStateManager
	WithValidationEnabled(podSelector string) ClusterUpgradeStateManager
	BuildState(ctx context.Context, namespace string,
		driverLabels map[string]string) (*ClusterUpgradeState, error)
	// npuDeviceClass is the container-mode DRA DeviceClass whose claims mark a
	// pod as an NPU pod the eviction has to move (drivermanager.NPUDeviceClass).
	ApplyState(ctx context.Context,
		currentState *ClusterUpgradeState, upgradePolicy *v1beta1.DriverUpgradePolicySpec,
		npuDeviceClass string) (err error)
}

type ClusterUpgradeStateManagerImpl struct {
	log                      logr.Logger
	k8sClient                client.Client
	k8sInterface             kubernetes.Interface
	eventRecorder            record.EventRecorder
	nodeUpgradeStateProvider *NodeUpgradeStateProvider
	podManager               PodManagerInterface
	safeDriverLoadManager    SafeDriverLoadManagerInterface
	cordonManager            CordonManagerInterface
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

	nodeUpgradeStateProvider := NewNodeUpgradeStateProvider(k8sClient, eventRecorder)
	manager := &ClusterUpgradeStateManagerImpl{
		log:                      log,
		k8sClient:                k8sClient,
		k8sInterface:             k8sInterface,
		eventRecorder:            eventRecorder,
		podManager:               NewPodManager(k8sInterface, nodeUpgradeStateProvider, nil),
		cordonManager:            NewCordonManager(k8sInterface),
		nodeUpgradeStateProvider: nodeUpgradeStateProvider,
		validationManager:        NewValidationManager(k8sInterface, nodeUpgradeStateProvider, ""),
		safeDriverLoadManager:    NewSafeDriverLoadManager(nodeUpgradeStateProvider),
	}
	return manager, nil
}

func (m *ClusterUpgradeStateManagerImpl) WithPodDeletionEnabled(filter PodDeletionFilter) ClusterUpgradeStateManager {
	if filter == nil {
		m.log.Info("Cannot enable PodDeletion state as PodDeletionFilter is nil")
		return m
	}
	m.podManager = NewPodManager(m.k8sInterface, m.nodeUpgradeStateProvider, filter)
	m.podDeletionStateEnabled = true
	return m
}

func (m *ClusterUpgradeStateManagerImpl) WithValidationEnabled(podSelector string) ClusterUpgradeStateManager {
	if podSelector == "" {
		m.log.Info("Cannot enable Validation state as podSelector is empty")
		return m
	}
	m.validationManager = NewValidationManager(m.k8sInterface, m.nodeUpgradeStateProvider, podSelector)
	m.validationStateEnabled = true
	return m
}
