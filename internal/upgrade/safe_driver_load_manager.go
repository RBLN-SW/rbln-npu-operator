/*
Copyright 2023 NVIDIA CORPORATION & AFFILIATES

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
(pkg/upgrade/safe_driver_load_manager.go, commit a0a0256b) and has been
modified by Rebellions Inc. See NOTICE for details.
*/

package upgrade

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// SafeDriverLoadManagerInterface abstracts safe driver load operations for testability.
type SafeDriverLoadManagerInterface interface {
	IsWaitingForSafeDriverLoad(ctx context.Context, node *corev1.Node) bool
	UnblockLoading(ctx context.Context, node *corev1.Node) error
}

type SafeDriverLoadManager struct {
	nodeUpgradeStateProvider *NodeUpgradeStateProvider
}

func (s *SafeDriverLoadManager) IsWaitingForSafeDriverLoad(_ context.Context, node *corev1.Node) bool {
	if node == nil {
		return false
	}
	return node.Annotations[UpgradeWaitForSafeDriverLoadAnnotationKey] != ""
}

func NewSafeDriverLoadManager(
	nodeUpgradeStateProvider *NodeUpgradeStateProvider,
) *SafeDriverLoadManager {
	mgr := &SafeDriverLoadManager{
		nodeUpgradeStateProvider: nodeUpgradeStateProvider,
	}
	return mgr
}

func (s *SafeDriverLoadManager) UnblockLoading(ctx context.Context, node *corev1.Node) error {
	annotationKey := UpgradeWaitForSafeDriverLoadAnnotationKey
	if node.Annotations[annotationKey] == "" {
		return nil
	}
	err := s.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, annotationKey)
	if err != nil {
		log.FromContext(ctx).Error(
			err, "Failed to change node upgrade annotation for node", "node",
			node, "annotation", annotationKey)
		return err
	}
	return nil
}
