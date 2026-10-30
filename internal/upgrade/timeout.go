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
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// checkAnnotationTimeout tracks a timeout using a node annotation that stores
// the Unix timestamp when the timeout window started.
//
// Behaviour:
//   - If the annotation is absent, it is created with the current time and
//     the function returns (false, nil).
//   - If the annotation is present and the timeout has not elapsed, the
//     function returns (false, nil).
//   - If the annotation is present and the timeout has elapsed, the function
//     returns (true, nil).
//   - If the annotation value cannot be parsed, the function returns
//     (false, error).
func checkAnnotationTimeout(
	ctx context.Context,
	provider *NodeUpgradeStateProvider,
	node *corev1.Node,
	annotationKey string,
	timeoutSeconds int64,
) (bool, error) {
	currentTime := time.Now().Unix()

	val, present := node.Annotations[annotationKey]
	if !present {
		err := provider.SetNodeUpgradeAnnotation(ctx, node, annotationKey,
			strconv.FormatInt(currentTime, 10))
		if err != nil {
			return false, err
		}
		return false, nil
	}

	startTime, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return false, err
	}

	return currentTime > startTime+timeoutSeconds, nil
}
