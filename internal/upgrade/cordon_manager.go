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
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/RBLN-SW/rbln-npu-operator/internal/consts"
)

type CordonManagerInterface interface {
	Cordon(context.Context, *corev1.Node) (bool, error)
	Uncordon(context.Context, *corev1.Node) error
}
type CordonManager struct{ k8sInterface kubernetes.Interface }

func NewCordonManager(k kubernetes.Interface) *CordonManager { return &CordonManager{k8sInterface: k} }

// Ownership and schedulability move in one write. The resource version makes
// the decision conditional on the live node, including an administrator's
// cordon or a changed claim that raced with our observation.
func ownershipPatch(rv string, claim any, unschedulable *bool) []byte {
	meta := map[string]any{"annotations": map[string]any{consts.DriverManagerCordonClaimAnnotation: claim, consts.DriverManagerEvictionBlockedAnnotation: nil}}
	if rv != "" {
		meta["resourceVersion"] = rv
	}
	body := map[string]any{"metadata": meta}
	if unschedulable != nil {
		body["spec"] = map[string]any{"unschedulable": *unschedulable}
	}
	data, _ := json.Marshal(body)
	return data
}

func cordonPatch(rv string) []byte {
	value := true
	return ownershipPatch(rv, consts.OperatorCordonClaimValue, &value)
}
func adoptPatch(rv string) []byte { return ownershipPatch(rv, consts.OperatorCordonClaimValue, nil) }

func (m *CordonManager) patch(ctx context.Context, name string, data []byte) error {
	_, err := m.k8sInterface.CoreV1().Nodes().Patch(ctx, name, types.MergePatchType, data, metav1.PatchOptions{})
	return err
}

func (m *CordonManager) Cordon(ctx context.Context, n *corev1.Node) (bool, error) {
	foreign := false
	decision := ""
	err := retryNodeConflicts(ctx, func() error {
		foreign = false
		live, err := m.k8sInterface.CoreV1().Nodes().Get(ctx, n.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		owner := cordonOwnerOf(live)
		if !live.Spec.Unschedulable {
			decision = "cordon-and-claim"
			return m.patch(ctx, n.Name, cordonPatch(live.ResourceVersion))
		}
		switch owner {
		case cordonOwnedByOperator:
			decision = "already-owned"
			return nil
		case cordonOwnedByDriverManager, cordonOwnedByLegacyOperator:
			decision = "adopt-existing-cordon"
			return m.patch(ctx, n.Name, adoptPatch(live.ResourceVersion))
		default:
			foreign = true
			decision = "preserve-administrator-cordon"
			return nil
		}
	})
	if err == nil {
		logger := log.FromContext(ctx)
		if decision == "already-owned" {
			logger = logger.V(consts.VDebug)
		}
		logger.Info("Node cordon decision completed", "node", n.Name, "decision", decision)
	}
	return foreign, err
}

func (m *CordonManager) Uncordon(ctx context.Context, n *corev1.Node) error {
	released := false
	err := retryNodeConflicts(ctx, func() error {
		released = false
		live, err := m.k8sInterface.CoreV1().Nodes().Get(ctx, n.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if cordonOwnerOf(live) == cordonUnclaimed {
			return nil
		}
		value := false
		err = m.patch(ctx, n.Name, ownershipPatch(live.ResourceVersion, nil, &value))
		released = err == nil
		return err
	})
	if err == nil {
		logger := log.FromContext(ctx)
		if released {
			logger.Info("Node scheduling restored and cordon claim cleared", "node", n.Name)
		} else {
			logger.V(consts.VDebug).Info("No claimed cordon to release", "node", n.Name)
		}
	}
	return err
}
