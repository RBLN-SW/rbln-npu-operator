package clusterpolicy

import (
	corev1 "k8s.io/api/core/v1"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

const (
	driverAutoUpgradeAnnotationKey = "rebellions.ai/npu-driver-upgrade-enabled"
)

func shouldEnableDriverAutoUpgrade(policy *rblnv1beta1.RBLNClusterPolicy) bool {
	return policy.Spec.Driver.UpgradePolicy != nil &&
		policy.Spec.Driver.UpgradePolicy.AutoUpgrade
}

// reconcileAutoUpgradeAnnotationInPlace adjusts the driver auto-upgrade
// annotation on the node in-place and returns whether any change was made.
// Nodes without the RBLN present label are left untouched.
func reconcileAutoUpgradeAnnotationInPlace(node *corev1.Node, shouldEnable bool) bool {
	if !hasRBLNPresentLabel(node.GetLabels()) {
		return false
	}

	if hasRBLNDeploySkipLabel(node.GetLabels()) || !hasDriverDeployLabel(node.GetLabels()) {
		shouldEnable = false
	}

	annotations := ensureNodeAnnotations(node)

	if isDriverAutoUpgradeAnnotationReconciled(annotations, shouldEnable) {
		return false
	}

	reconcileDriverAutoUpgradeAnnotation(annotations, shouldEnable)
	node.SetAnnotations(annotations)

	return true
}

func ensureNodeAnnotations(node *corev1.Node) map[string]string {
	annotations := node.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	return annotations
}

func isDriverAutoUpgradeAnnotationReconciled(
	annotations map[string]string,
	shouldEnable bool,
) bool {
	currentValue, exists := annotations[driverAutoUpgradeAnnotationKey]
	if shouldEnable {
		return exists && currentValue == labelValueTrue
	}
	return !exists
}

func reconcileDriverAutoUpgradeAnnotation(
	annotations map[string]string,
	shouldEnable bool,
) {
	if shouldEnable {
		annotations[driverAutoUpgradeAnnotationKey] = labelValueTrue
		return
	}

	delete(annotations, driverAutoUpgradeAnnotationKey)
}
