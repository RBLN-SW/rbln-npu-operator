/*
Copyright 2026 Rebellions Inc.

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

package upgrade

import (
	"context"
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
)

func (m *ClusterUpgradeStateManagerImpl) GetDriverDaemonSets(ctx context.Context, namespace string, match map[string]string) (map[types.UID]*appsv1.DaemonSet, error) {
	list := &appsv1.DaemonSetList{}
	if err := m.k8sClient.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels(match)); err != nil {
		return nil, err
	}
	sets := map[types.UID]*appsv1.DaemonSet{}
	for i := range list.Items {
		ds := &list.Items[i]
		sets[ds.UID] = ds
	}
	return sets, nil
}

func groupDriverPods(pods []corev1.Pod, sets map[types.UID]*appsv1.DaemonSet) (map[types.UID][]*corev1.Pod, []*corev1.Pod) {
	groups := map[types.UID][]*corev1.Pod{}
	var orphans []*corev1.Pod
	for i := range pods {
		pod := &pods[i]
		owner := metav1.GetControllerOf(pod)
		if owner == nil {
			orphans = append(orphans, pod)
			continue
		}
		if owner.Kind == "DaemonSet" && sets[owner.UID] != nil {
			groups[owner.UID] = append(groups[owner.UID], pod)
		}
	}
	return groups, orphans
}

func retainStableDaemonSets(sets map[types.UID]*appsv1.DaemonSet, groups map[types.UID][]*corev1.Pod) {
	for uid, ds := range sets {
		if len(groups[uid]) != int(ds.Status.DesiredNumberScheduled) {
			delete(sets, uid)
		}
	}
}

func (m *ClusterUpgradeStateManagerImpl) buildNodeUpgradeState(ctx context.Context, pod *corev1.Pod, ds *appsv1.DaemonSet, cache map[string]*corev1.Node) (*NodeUpgradeState, error) {
	node := cache[pod.Spec.NodeName]
	if node == nil {
		var err error
		node, err = m.nodeUpgradeStateProvider.GetNode(ctx, pod.Spec.NodeName)
		if err != nil {
			return nil, fmt.Errorf("unable to get node %s: %w", pod.Spec.NodeName, err)
		}
		cache[pod.Spec.NodeName] = node
	}
	return &NodeUpgradeState{Node: node, DriverPod: pod, DriverDaemonSet: ds}, nil
}

func (m *ClusterUpgradeStateManagerImpl) addPodToUpgradeState(ctx context.Context, state *ClusterUpgradeState, pod *corev1.Pod, ds *appsv1.DaemonSet, cache map[string]*corev1.Node) error {
	// A DaemonSet pod can exist before the scheduler assigns its node. It
	// contributes to the DaemonSet census, but not to a node's rollout yet.
	if pod.Spec.NodeName == "" && pod.Status.Phase == corev1.PodPending {
		return nil
	}
	n, err := m.buildNodeUpgradeState(ctx, pod, ds, cache)
	if err != nil {
		return err
	}
	// A terminating or orphaned driver pod can outlive the node's departure
	// and its bookkeeping cleanup. The deployment label remains the scope
	// authority on subsequent passes, when no state/claim is left to discover.
	if n.Node.Labels[consts.RBLNDeployDriverLabelKey] != trueString {
		return nil
	}
	key := n.Node.Labels[UpgradeStateLabelKey]
	if !IsManagedUpgradeState(key) {
		key = UpgradeStateUnknown
	}
	state.addNode(key, n)
	return nil
}

func (m *ClusterUpgradeStateManagerImpl) addDriverPods(ctx context.Context, state *ClusterUpgradeState, pods []*corev1.Pod, ds *appsv1.DaemonSet, cache map[string]*corev1.Node) error {
	for _, pod := range pods {
		if err := m.addPodToUpgradeState(ctx, state, pod, ds, cache); err != nil {
			return err
		}
	}
	return nil
}

// A lost state label must not strand an operator-owned cordon. Discover
// claim-only nodes too; annotations cannot be filtered by a label selector.
// Absence of a driver pod is not departure: DaemonSet replacement can make
// active nodes temporarily disappear from the pod-derived observation.
func (m *ClusterUpgradeStateManagerImpl) findDepartedNodes(ctx context.Context) ([]*corev1.Node, error) {
	list := &corev1.NodeList{}
	if err := m.k8sClient.List(ctx, list); err != nil {
		return nil, err
	}
	var nodes []*corev1.Node
	for i := range list.Items {
		n := &list.Items[i]
		if n.Labels[consts.RBLNDeployDriverLabelKey] != trueString && CarriesRolloutBookkeeping(n) {
			nodes = append(nodes, n)
		}
	}
	return nodes, nil
}

func (m *ClusterUpgradeStateManagerImpl) BuildState(ctx context.Context, namespace string, match map[string]string) (*ClusterUpgradeState, error) {
	state := ClusterUpgradeState{}
	sets, err := m.GetDriverDaemonSets(ctx, namespace, match)
	if err != nil {
		return nil, err
	}
	pods := &corev1.PodList{}
	if err = m.k8sClient.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabels(match)); err != nil {
		return nil, err
	}
	groups, orphans := groupDriverPods(pods.Items, sets)
	retainStableDaemonSets(sets, groups)
	cache := map[string]*corev1.Node{}
	uids := make([]types.UID, 0, len(sets))
	for uid := range sets {
		uids = append(uids, uid)
	}
	sort.Slice(uids, func(i, j int) bool { return sets[uids[i]].Name < sets[uids[j]].Name })
	for _, uid := range uids {
		group := groups[uid]
		sort.Slice(group, func(i, j int) bool { return group[i].Spec.NodeName < group[j].Spec.NodeName })
		if err = m.addDriverPods(ctx, &state, group, sets[uid], cache); err != nil {
			return nil, err
		}
	}
	if err = m.addDriverPods(ctx, &state, orphans, nil, cache); err != nil {
		return nil, err
	}
	state.DepartedNodes, err = m.findDepartedNodes(ctx)
	if err != nil {
		return nil, err
	}
	state.excludeDeparted()
	return &state, nil
}
