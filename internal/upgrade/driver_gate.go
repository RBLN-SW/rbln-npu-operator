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

	corev1 "k8s.io/api/core/v1"
)

// Any nonempty value means the driver is waiting, including values used by
// older integrations. Removing this annotation is the protocol acknowledgement.
func waitingForDriverLoad(node *corev1.Node) bool {
	return node != nil && node.Annotations[UpgradeWaitForSafeDriverLoadAnnotationKey] != ""
}

func (m *ClusterUpgradeStateManagerImpl) unblockDriverLoad(ctx context.Context, node *corev1.Node) error {
	if !waitingForDriverLoad(node) {
		return nil
	}
	return m.nodeUpgradeStateProvider.RemoveNodeUpgradeAnnotation(ctx, node, UpgradeWaitForSafeDriverLoadAnnotationKey)
}
