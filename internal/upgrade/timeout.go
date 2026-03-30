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
